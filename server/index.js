import 'dotenv/config'
import express from 'express'
import cors from 'cors'
import pkg from 'pg'
import bcrypt from 'bcryptjs'
import jwt from 'jsonwebtoken'
import { Client as LdapClient } from 'ldapts'

process.on('unhandledRejection', (reason) => {
  // eslint-disable-next-line no-console
  console.error('UNHANDLED_REJECTION', reason)
})

process.on('uncaughtException', (err) => {
  // eslint-disable-next-line no-console
  console.error('UNCAUGHT_EXCEPTION', err)
})

// eslint-disable-next-line no-console
console.log('Starting auth server...')

const { Pool } = pkg

const app = express()
const port = process.env.PORT || 3000

app.use(cors())
app.use(express.json())

// eslint-disable-next-line no-console
console.log('PG config', {
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  user: process.env.PG_USER,
  database: process.env.PG_DATABASE,
})

const pool = new Pool({
  host: process.env.PG_HOST || 'localhost',
  port: Number(process.env.PG_PORT || 5432),
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  database: process.env.PG_DATABASE,
})

const JWT_SECRET = process.env.JWT_SECRET || 'dev_jwt_secret_change_me'
const ACCESS_TOKEN_EXPIRES_IN_SECONDS = 24 * 60 * 60 // 1 day

async function findLocalAccount(identifier) {
  const client = await pool.connect()
  try {
    const providerRes = await client.query(
      'SELECT id FROM auth_providers WHERE code = $1 AND enabled = 1',
      ['local']
    )
    if (providerRes.rowCount === 0) return null
    const providerId = providerRes.rows[0].id

    const accountRes = await client.query(
      `SELECT
         ua.*,
         u.username AS user_username,
         u.email AS user_email,
         u.name AS user_name,
         u.avatar_url AS user_avatar_url,
         u.status AS user_status
       FROM user_auth_accounts ua
       JOIN users u ON u.id = ua.user_id
       WHERE ua.provider_id = $1
         AND ua.status = 1
         AND (ua.email = $2 OR ua.username = $2 OR ua.provider_user_id = $2)
       LIMIT 1`,
      [providerId, identifier]
    )

    if (accountRes.rowCount === 0) return null
    const account = accountRes.rows[0]

    // 用户被禁用时禁止登录（users.status = 0）
    if (account.user_status === 0) {
      return null
    }

    const rolesRes = await client.query(
      `SELECT sr.code
       FROM user_system_roles usr
       JOIN system_roles sr ON sr.id = usr.system_role_id
       WHERE usr.user_id = $1`,
      [account.user_id]
    )

    const roles = rolesRes.rows.map((r) => r.code)

    return {
      account,
      user: {
        id: account.user_id,
        username: account.user_username,
        email: account.user_email,
        name: account.user_name,
        avatarUrl: account.user_avatar_url,
        roles,
      },
    }
  } finally {
    client.release()
  }
}

app.post('/api/auth/login', async (req, res) => {
  const { provider = 'auto', identifier, password } = req.body || {}

  if (!identifier || !password) {
    return res.status(400).json({ message: 'identifier and password are required' })
  }

  const tryLocalLogin = async () => {
    const result = await findLocalAccount(identifier)
    if (!result) {
      throw new Error('LOCAL_INVALID')
    }

    const { account, user } = result

    if (!account.password_hash) {
      throw new Error('LOCAL_NO_PASSWORD')
    }

    const ok = await bcrypt.compare(password, account.password_hash)
    if (!ok) {
      throw new Error('LOCAL_INVALID')
    }

    const payload = {
      sub: String(user.id),
      roles: user.roles,
      provider: 'local',
    }

    const accessToken = jwt.sign(payload, JWT_SECRET, {
      expiresIn: ACCESS_TOKEN_EXPIRES_IN_SECONDS,
    })

    return {
      accessToken,
      user,
    }
  }

  const tryLdapLogin = async () => {
    // 占位：如果未来需要 LDAP 登录，可以在这里实现
    // 当前场景只使用本地账号登录
    throw new Error('LDAP_NOT_SUPPORTED')
  }

  try {
    let result

    // 明确指定 provider
    if (provider === 'local') {
      result = await tryLocalLogin()
    } else if (provider === 'ldap') {
      result = await tryLdapLogin()
    } else {
      // provider = auto: 先尝试本地账号，失败再考虑其它
      try {
        result = await tryLocalLogin()
      } catch (err) {
        if (err instanceof Error && err.message === 'LOCAL_INVALID') {
          // 目前不真正支持 LDAP，直接按账号密码错误处理
          throw err
        }
        throw err
      }
    }

    const { accessToken, user } = result

    return res.json({ accessToken, user })
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error(err)
    if (err instanceof Error) {
      if (err.message === 'LOCAL_INVALID') {
        return res.status(401).json({ message: 'Invalid credentials' })
      }
      if (err.message === 'LOCAL_NO_PASSWORD') {
        return res.status(400).json({ message: 'Password login not configured for this account' })
      }
      if (err.message === 'LDAP_NOT_SUPPORTED') {
        return res.status(400).json({ message: 'LDAP login is not configured' })
      }
    }

    return res.status(500).json({ message: 'Internal server error' })
  }
})

app.post('/api/ldap/sync-users', authMiddleware, async (req, res) => {
  // ...
})

app.patch('/api/teams/:id', authMiddleware, async (req, res) => {
  const client = await pool.connect()
  try {
    const teamId = Number(req.params.id)
    const { name, slug, description } = req.body || {}

    if (!Number.isFinite(teamId)) {
      client.release()
      return res.status(400).json({ message: 'Invalid team id' })
    }

    const currentUserId = Number(req.user && req.user.sub)
    if (!Number.isFinite(currentUserId)) {
      client.release()
      return res.status(401).json({ message: 'Unauthorized' })
    }

    const systemRole = await getUserSystemRole(client, currentUserId)
    const isSuperAdmin = systemRole === 'super_admin'

    const teamResult = await client.query('SELECT owner_id FROM teams WHERE id = $1', [teamId])
    const team = teamResult.rows[0]
    if (!team) {
      client.release()
      return res.status(404).json({ message: 'Team not found' })
    }

    if (!isSuperAdmin && team.owner_id !== currentUserId) {
      client.release()
      return res.status(403).json({ message: 'Forbidden' })
    }

    const fields = []
    const values = []
    let idx = 1
    if (typeof name === 'string' && name.trim()) {
      fields.push(`name = $${idx++}`)
      values.push(name.trim())
    }
    if (typeof slug === 'string') {
      fields.push(`slug = $${idx++}`)
      values.push(slug.trim() || null)
    }
    if (typeof description === 'string') {
      fields.push(`description = $${idx++}`)
      values.push(description.trim() || null)
    }

    if (!fields.length) {
      client.release()
      return res.status(400).json({ message: 'No fields to update' })
    }

    values.push(teamId)
    const sql = `UPDATE teams SET ${fields.join(', ')}, updated_at = NOW() WHERE id = $$${idx} RETURNING *`
    const updateResult = await client.query(sql.replace('$$', '$'), values)

    client.release()
    return res.json(updateResult.rows[0])
  } catch (error) {
    client.release()
    // eslint-disable-next-line no-console
    console.error(error)
    return res.status(500).json({ message: 'Failed to update team' })
  }
})

app.get('/api/system-roles', authMiddleware, async (req, res) => {
  // ...
})

app.post('/api/users', authMiddleware, async (req, res) => {
  // ...
})

app.patch('/api/users/:id', authMiddleware, async (req, res) => {
  // ...
})

app.get('/api/users', authMiddleware, async (req, res) => {
  try {
    const client = await pool.connect()
    try {
      const usersRes = await client.query(
        `SELECT
           u.id,
           u.username,
           u.email,
           u.name,
           u.avatar_url,
           u.status,
           u.created_at,
           u.updated_at
         FROM users u
         ORDER BY u.id ASC`
      )

      const rolesRes = await client.query(
        `SELECT
           usr.user_id,
           sr.code,
           sr.name
         FROM user_system_roles usr
         JOIN system_roles sr ON sr.id = usr.system_role_id`
      )

      const rolesByUser = new Map()
      for (const row of rolesRes.rows) {
        const list = rolesByUser.get(row.user_id) || []
        list.push({ code: row.code, name: row.name })
        rolesByUser.set(row.user_id, list)
      }

      const users = usersRes.rows.map((row) => {
        const roleEntries = rolesByUser.get(row.id) || []
        const primaryRole = roleEntries[0]?.code || 'user'
        const roleName = roleEntries[0]?.name || null

        return {
          id: String(row.id),
          firstName: row.name,
          lastName: '',
          username: row.username,
          email: row.email,
          phoneNumber: '',
          status: row.status === 1 ? 'active' : 'inactive',
          role: primaryRole,
          accountSource: 'local',
          createdAt: row.created_at,
          updatedAt: row.updated_at,
          roleName,
        }
      })

      return res.json(users)
    } finally {
      client.release()
    }
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error(err)
    return res.status(500).json({ message: 'Internal server error' })
  }
})

function authMiddleware(req, res, next) {
  const authHeader = req.headers.authorization || ''
  const [scheme, token] = authHeader.split(' ')

  if (!token || scheme !== 'Bearer') {
    return res.status(401).json({ message: 'Unauthorized' })
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET)
    req.user = decoded
    return next()
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('authMiddleware verify error', err)
    return res.status(401).json({ message: 'Invalid token' })
  }
}

function isSuperAdmin(req) {
  const roles = Array.isArray(req.user && req.user.roles) ? req.user.roles : []
  return roles.includes('super_admin')
}

function isAdmin(req) {
  const roles = Array.isArray(req.user && req.user.roles) ? req.user.roles : []
  return roles.includes('admin') || roles.includes('super_admin')
}

async function ensureDefaultTeamExists(client, ownerUserId) {
  const existing = await client.query(
    'SELECT id FROM teams WHERE slug = $1 LIMIT 1',
    ['default']
  )

  if (existing.rows.length > 0) {
    return existing.rows[0].id
  }

  const insertRes = await client.query(
    `INSERT INTO teams (name, slug, owner_id, description)
     VALUES ($1, $2, $3, $4)
     RETURNING id`,
    ['默认团队', 'default', ownerUserId, 'Default team']
  )

  return insertRes.rows[0].id
}

app.get('/api/teams', authMiddleware, async (req, res) => {
  const currentUserId = Number(req.user && req.user.sub)

  if (!Number.isFinite(currentUserId)) {
    return res.status(400).json({ message: 'Invalid user' })
  }

  try {
    const client = await pool.connect()
    try {
      // 确保有默认团队（如果 schema 里有这个约定）
      await ensureDefaultTeamExists(client, currentUserId)

      const roles = Array.isArray(req.user && req.user.roles) ? req.user.roles : []
      const isSuper = roles.includes('super_admin')

      let teamsRes
      if (isSuper) {
        // 超级管理员：查看所有团队
        teamsRes = await client.query(
          `SELECT
             t.id,
             t.name,
             t.slug,
             t.status,
             t.owner_id,
             u.username AS owner_username,
             u.name     AS owner_name,
             t.created_at,
             t.updated_at
           FROM teams t
           JOIN users u ON u.id = t.owner_id
           ORDER BY t.id ASC`
        )
      } else {
        // 普通用户：查看自己参与的团队（拥有者或成员）
        teamsRes = await client.query(
          `SELECT DISTINCT
             t.id,
             t.name,
             t.slug,
             t.status,
             t.owner_id,
             u.username AS owner_username,
             u.name     AS owner_name,
             t.created_at,
             t.updated_at
           FROM teams t
           JOIN users u ON u.id = t.owner_id
           LEFT JOIN team_members tm ON tm.team_id = t.id
           WHERE t.owner_id = $1 OR tm.user_id = $1
           ORDER BY t.id ASC`,
          [currentUserId]
        )
      }

      const teams = teamsRes.rows.map((row) => ({
        id: String(row.id),
        name: row.name,
        slug: row.slug,
        status: row.status,
        ownerId: row.owner_id,
        ownerUsername: row.owner_username,
        ownerName: row.owner_name,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      }))

      return res.json(teams)
    } finally {
      client.release()
    }
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error(err)
    return res.status(500).json({ message: 'Internal server error' })
  }
})

app.delete('/api/teams/:id', authMiddleware, async (req, res) => {
  // ...
  const roles = Array.isArray(req.user && req.user.roles) ? req.user.roles : []
  const isSuperAdmin = roles.includes('super_admin')
  const isAdmin = isSuperAdmin || roles.includes('admin')

  if (!isAdmin) {
    return res.status(403).json({ message: 'Forbidden' })
  }

  const currentUserId = Number(req.user && req.user.sub)
  if (!Number.isFinite(currentUserId)) {
    return res.status(400).json({ message: 'Invalid user' })
  }

  try {
    const client = await pool.connect()
    try {
      await client.query('BEGIN')

      const teamRes = await client.query(
        'SELECT id, owner_id, slug FROM teams WHERE id = $1 LIMIT 1',
        [teamId]
      )

      if (teamRes.rows.length === 0) {
        await client.query('ROLLBACK')
        return res.status(404).json({ message: 'Team not found' })
      }

      const team = teamRes.rows[0]

      if (team.slug === 'default') {
        await client.query('ROLLBACK')
        return res.status(400).json({ message: 'Default team cannot be deleted' })
      }

      if (!isSuperAdmin && team.owner_id !== currentUserId) {
        await client.query('ROLLBACK')
        return res.status(403).json({ message: 'Forbidden' })
      }

      await client.query('DELETE FROM teams WHERE id = $1', [teamId])

      await client.query('COMMIT')
      return res.json({ message: 'Team deleted' })
    } catch (err) {
      await client.query('ROLLBACK')
      // eslint-disable-next-line no-console
      console.error(err)
      return res.status(500).json({ message: 'Internal server error' })
    } finally {
      client.release()
    }
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error(err)
    return res.status(500).json({ message: 'Internal server error' })
  }
})

app.post('/api/teams', authMiddleware, async (req, res) => {
  if (!isAdmin(req)) {
    return res.status(403).json({ message: 'Forbidden' })
  }

  const { name, slug, description } = req.body || {}

  if (!name) {
    return res.status(400).json({ message: 'Missing required fields' })
  }

  const ownerUserId = Number(req.user && req.user.sub)

  if (!Number.isFinite(ownerUserId)) {
    return res.status(400).json({ message: 'Invalid owner' })
  }

  try {
    const client = await pool.connect()
    try {
      await client.query('BEGIN')

      const insertTeamRes = await client.query(
        `INSERT INTO teams (name, slug, owner_id, description)
         VALUES ($1, $2, $3, $4)
         RETURNING id`,
        [name, slug || null, ownerUserId, description || null]
      )

      const teamId = insertTeamRes.rows[0].id

      const teamRolesResult = await client.query(
        "SELECT id, code FROM team_roles WHERE code IN ('owner', 'maintainer')"
      )

      let ownerRoleId = null
      let maintainerRoleId = null

      for (const row of teamRolesResult.rows) {
        if (row.code === 'owner') {
          ownerRoleId = row.id
        } else if (row.code === 'maintainer') {
          maintainerRoleId = row.id
        }
      }

      if (ownerRoleId) {
        await client.query(
          `INSERT INTO team_members (team_id, user_id, status, joined_at, created_at, updated_at)
           VALUES ($1, $2, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
           ON CONFLICT (team_id, user_id) DO NOTHING`,
          [teamId, ownerUserId]
        )

        const ownerMemberRes = await client.query(
          'SELECT id FROM team_members WHERE team_id = $1 AND user_id = $2',
          [teamId, ownerUserId]
        )

        const ownerMemberId = ownerMemberRes.rows[0]?.id

        if (ownerMemberId) {
          await client.query(
            `INSERT INTO team_member_roles (team_member_id, team_role_id)
             VALUES ($1, $2)
             ON CONFLICT (team_member_id, team_role_id) DO NOTHING`,
            [ownerMemberId, ownerRoleId]
          )
        }
      }

      if (maintainerRoleId) {
        const superAdminsRes = await client.query(
          `SELECT u.id
           FROM users u
           JOIN user_system_roles usr ON usr.user_id = u.id
           JOIN system_roles sr ON sr.id = usr.system_role_id
           WHERE sr.code = 'super_admin'`
        )

        for (const row of superAdminsRes.rows) {
          const superAdminId = row.id

          await client.query(
            `INSERT INTO team_members (team_id, user_id, status, joined_at, created_at, updated_at)
             VALUES ($1, $2, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
             ON CONFLICT (team_id, user_id) DO NOTHING`,
            [teamId, superAdminId]
          )

          const superMemberRes = await client.query(
            'SELECT id FROM team_members WHERE team_id = $1 AND user_id = $2',
            [teamId, superAdminId]
          )

          const superMemberId = superMemberRes.rows[0]?.id

          if (superMemberId) {
            await client.query(
              `INSERT INTO team_member_roles (team_member_id, team_role_id)
               VALUES ($1, $2)
               ON CONFLICT (team_member_id, team_role_id) DO NOTHING`,
              [superMemberId, maintainerRoleId]
            )
          }
        }
      }

      await client.query('COMMIT')
      return res.status(201).json({ id: teamId })
    } catch (err) {
      await client.query('ROLLBACK')
      return res.status(500).json({ message: 'Internal server error' })
    } finally {
      client.release()
    }
  } catch (err) {
    return res.status(500).json({ message: 'Internal server error' })
  }
})

app.get('/api/teams/:id/members', authMiddleware, async (req, res) => {
  const teamId = Number(req.params.id)
  if (!Number.isFinite(teamId)) {
    return res.status(400).json({ message: 'Invalid team id' })
  }

  try {
    const client = await pool.connect()
    try {
      const result = await client.query(
        `SELECT
           tm.id,
           tm.user_id,
           u.username,
           u.name,
           tr.code AS team_role_code,
           tr.name AS team_role_name
         FROM team_members tm
         JOIN users u ON u.id = tm.user_id
         LEFT JOIN team_member_roles tmr ON tmr.team_member_id = tm.id
         LEFT JOIN team_roles tr ON tr.id = tmr.team_role_id
         WHERE tm.team_id = $1
         ORDER BY tm.id ASC`,
        [teamId]
      )

      const members = result.rows.map((row) => ({
        id: String(row.id),
        userId: String(row.user_id),
        username: row.username,
        name: row.name,
        teamRoleCode: row.team_role_code || null,
        teamRoleName: row.team_role_name || null,
      }))

      return res.json(members)
    } finally {
      client.release()
    }
  } catch (err) {
    return res.status(500).json({ message: 'Internal server error' })
  }
})

app.post('/api/teams/:id/members', authMiddleware, async (req, res) => {
  if (!isAdmin(req)) {
    return res.status(403).json({ message: 'Forbidden' })
  }

  const teamId = Number(req.params.id)
  if (!Number.isFinite(teamId)) {
    return res.status(400).json({ message: 'Invalid team id' })
  }

  const { userId, role } = req.body || {}

  const memberUserId = Number(userId)
  if (!Number.isFinite(memberUserId) || !role) {
    return res.status(400).json({ message: 'Missing required fields' })
  }

  if (!['owner', 'maintainer', 'member'].includes(role)) {
    return res.status(400).json({ message: 'Invalid role' })
  }

  try {
    const client = await pool.connect()
    try {
      await client.query('BEGIN')

      await client.query(
        `INSERT INTO team_members (team_id, user_id, status, joined_at, created_at, updated_at)
         VALUES ($1, $2, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
         ON CONFLICT (team_id, user_id) DO NOTHING`,
        [teamId, memberUserId]
      )

      const memberRes = await client.query(
        'SELECT id FROM team_members WHERE team_id = $1 AND user_id = $2',
        [teamId, memberUserId]
      )

      const teamMemberId = memberRes.rows[0]?.id

      if (!teamMemberId) {
        await client.query('ROLLBACK')
        return res.status(500).json({ message: 'Failed to create team member' })
      }

      const roleRes = await client.query(
        'SELECT id FROM team_roles WHERE code = $1 LIMIT 1',
        [role]
      )

      const teamRoleId = roleRes.rows[0]?.id

      if (!teamRoleId) {
        await client.query('ROLLBACK')
        return res.status(400).json({ message: 'Invalid role' })
      }

      await client.query(
        `INSERT INTO team_member_roles (team_member_id, team_role_id)
         VALUES ($1, $2)
         ON CONFLICT (team_member_id, team_role_id) DO NOTHING`,
        [teamMemberId, teamRoleId]
      )

      await client.query('COMMIT')
      return res.status(201).json({ id: teamMemberId })
    } catch (err) {
      await client.query('ROLLBACK')
      return res.status(500).json({ message: 'Internal server error' })
    } finally {
      client.release()
    }
  } catch (err) {
    return res.status(500).json({ message: 'Internal server error' })
  }
})

app.patch('/api/teams/:id/members/:memberId', authMiddleware, async (req, res) => {
  const teamId = Number(req.params.id)
  const memberId = Number(req.params.memberId)

  if (!Number.isFinite(teamId) || !Number.isFinite(memberId)) {
    return res.status(400).json({ message: 'Invalid id' })
  }

  const { role, status } = req.body || {}

  if (!role && typeof status !== 'number') {
    return res.status(400).json({ message: 'Nothing to update' })
  }

  try {
    const client = await pool.connect()
    try {
      await client.query('BEGIN')

      const targetRes = await client.query(
        `SELECT tm.user_id
         FROM team_members tm
         WHERE tm.id = $1 AND tm.team_id = $2
         LIMIT 1`,
        [memberId, teamId]
      )

      if (targetRes.rows.length === 0) {
        await client.query('ROLLBACK')
        return res.status(404).json({ message: 'Member not found' })
      }

      const targetUserId = targetRes.rows[0].user_id

      const superCheckRes = await client.query(
        `SELECT 1
         FROM user_system_roles usr
         JOIN system_roles sr ON sr.id = usr.system_role_id
         WHERE usr.user_id = $1 AND sr.code = 'super_admin'
         LIMIT 1`,
        [targetUserId]
      )

      if (superCheckRes.rows.length > 0 && !isSuperAdmin(req)) {
        await client.query('ROLLBACK')
        return res.status(403).json({ message: 'Forbidden' })
      }

      if (typeof status === 'number') {
        await client.query(
          `UPDATE team_members
             SET status = $1,
                 updated_at = CURRENT_TIMESTAMP
           WHERE id = $2`,
          [status, memberId]
        )
      }

      if (role) {
        if (!['owner', 'maintainer', 'member'].includes(role)) {
          await client.query('ROLLBACK')
          return res.status(400).json({ message: 'Invalid role' })
        }

        const roleRes = await client.query(
          'SELECT id FROM team_roles WHERE code = $1 LIMIT 1',
          [role]
        )

        const teamRoleId = roleRes.rows[0]?.id

        if (!teamRoleId) {
          await client.query('ROLLBACK')
          return res.status(400).json({ message: 'Invalid role' })
        }

        await client.query(
          'DELETE FROM team_member_roles WHERE team_member_id = $1',
          [memberId]
        )

        await client.query(
          `INSERT INTO team_member_roles (team_member_id, team_role_id)
           VALUES ($1, $2)
           ON CONFLICT (team_member_id, team_role_id) DO NOTHING`,
          [memberId, teamRoleId]
        )
      }

      await client.query('COMMIT')
      return res.json({ message: 'Member updated' })
    } catch (err) {
      await client.query('ROLLBACK')
      return res.status(500).json({ message: 'Internal server error' })
    } finally {
      client.release()
    }
  } catch (err) {
    return res.status(500).json({ message: 'Internal server error' })
  }
})

app.delete('/api/teams/:id/members/:memberId', authMiddleware, async (req, res) => {
  const teamId = Number(req.params.id)
  const memberId = Number(req.params.memberId)

  if (!Number.isFinite(teamId) || !Number.isFinite(memberId)) {
    return res.status(400).json({ message: 'Invalid id' })
  }

  try {
    const client = await pool.connect()
    try {
      await client.query('BEGIN')

      const targetRes = await client.query(
        `SELECT tm.user_id
         FROM team_members tm
         WHERE tm.id = $1 AND tm.team_id = $2
         LIMIT 1`,
        [memberId, teamId]
      )

      if (targetRes.rows.length === 0) {
        await client.query('ROLLBACK')
        return res.status(404).json({ message: 'Member not found' })
      }

      const targetUserId = targetRes.rows[0].user_id

      const superCheckRes = await client.query(
        `SELECT 1
         FROM user_system_roles usr
         JOIN system_roles sr ON sr.id = usr.system_role_id
         WHERE usr.user_id = $1 AND sr.code = 'super_admin'
         LIMIT 1`,
        [targetUserId]
      )

      if (superCheckRes.rows.length > 0 && !isSuperAdmin(req)) {
        await client.query('ROLLBACK')
        return res.status(403).json({ message: 'Forbidden' })
      }

      await client.query(
        'DELETE FROM team_members WHERE id = $1 AND team_id = $2',
        [memberId, teamId]
      )

      await client.query('COMMIT')
      return res.json({ message: 'Member deleted' })
    } catch (err) {
      await client.query('ROLLBACK')
      return res.status(500).json({ message: 'Internal server error' })
    } finally {
      client.release()
    }
  } catch (err) {
    return res.status(500).json({ message: 'Internal server error' })
  }
})

app.get('/api/auth/me', authMiddleware, async (req, res) => {
  const userId = Number(req.user.sub)
  if (!userId) {
    return res.status(401).json({ message: 'Invalid token payload' })
  }

  try {
    const client = await pool.connect()
    try {
      const userRes = await client.query(
        `SELECT id, username, email, name, avatar_url
         FROM users
         WHERE id = $1`,
        [userId]
      )

      if (userRes.rowCount === 0) {
        return res.status(404).json({ message: 'User not found' })
      }

      const userRow = userRes.rows[0]

      const rolesRes = await client.query(
        `SELECT sr.code
         FROM user_system_roles usr
         JOIN system_roles sr ON sr.id = usr.system_role_id
         WHERE usr.user_id = $1`,
        [userId]
      )

      const roles = rolesRes.rows.map((r) => r.code)

      return res.json({
        id: userRow.id,
        username: userRow.username,
        email: userRow.email,
        name: userRow.name,
        avatarUrl: userRow.avatar_url,
        roles,
      })
    } finally {
      client.release()
    }
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error(err)
    return res.status(500).json({ message: 'Internal server error' })
  }
})

app.listen(port, () => {
  // eslint-disable-next-line no-console
  console.log(`Auth server listening on http://localhost:${port}`)
})
