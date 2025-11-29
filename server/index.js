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
      const {
        LDAP_URL,
        LDAP_BASE_DN,
        LDAP_ADMIN_DN,
        LDAP_ADMIN_PASSWORD,
        LDAP_USER_SEARCH_BASE,
        LDAP_USER_SEARCH_FILTER,
        LDAP_TIMEOUT,
      } = process.env

      if (!LDAP_URL || !LDAP_BASE_DN || !LDAP_ADMIN_DN || !LDAP_ADMIN_PASSWORD) {
        return res.status(500).json({ message: 'LDAP is not configured on the server' })
      }

      const client = new LdapClient({
        url: LDAP_URL,
        timeout: Number(LDAP_TIMEOUT || 5000),
        connectTimeout: Number(LDAP_TIMEOUT || 5000),
      })

      const searchBase = LDAP_USER_SEARCH_BASE || LDAP_BASE_DN
      const rawFilter = LDAP_USER_SEARCH_FILTER || '(|(uid={{username}})(mail={{username}}))'
      const filter = rawFilter.replace(/{{username}}/g, identifier)

      try {
        // 1) 先用管理员账号绑定
        await client.bind(LDAP_ADMIN_DN, LDAP_ADMIN_PASSWORD)

        // 2) 用 identifier 搜索用户条目，获取 dn
        const { searchEntries } = await client.search(searchBase, {
          scope: 'sub',
          filter,
          attributes: ['uid', 'gecos', 'mail'],
        })

        if (!searchEntries || searchEntries.length === 0) {
          return res.status(401).json({ message: 'Invalid credentials' })
        }

        // 简化：取第一个匹配条目
        const entry = searchEntries[0]
        const userDn = entry.dn
        const uid = Array.isArray(entry.uid) ? entry.uid[0] : entry.uid
        const mail = Array.isArray(entry.mail) ? entry.mail[0] : entry.mail

        if (!userDn || !uid) {
          return res.status(401).json({ message: 'Invalid credentials' })
        }

        // 3) 用用户凭证重新绑定验证密码
        await client.bind(userDn, password)

        const username = String(uid)
        const email = mail ? String(mail) : `${username}@ldap.local`

        // 4) 在本地根据 LDAP 账号找到已同步用户（auth_providers.code='ldap'）
        const dbClient = await pool.connect()
        try {
          const providerRes = await dbClient.query(
            "SELECT id FROM auth_providers WHERE code = 'ldap' AND enabled = 1"
          )
          if (providerRes.rowCount === 0) {
            return res.status(500).json({ message: 'LDAP provider is not enabled' })
          }
          const ldapProviderId = providerRes.rows[0].id

          const accountRes = await dbClient.query(
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
               AND ua.provider_user_id = $2
             LIMIT 1`,
            [ldapProviderId, username]
          )

          if (accountRes.rowCount === 0) {
            throw new Error('LDAP_NOT_SYNCED')
          }

          const account = accountRes.rows[0]

          // 用户被禁用时禁止登录
          if (account.user_status === 0) {
            throw new Error('USER_DISABLED')
          }

          const rolesRes = await dbClient.query(
            `SELECT sr.code
             FROM user_system_roles usr
             JOIN system_roles sr ON sr.id = usr.system_role_id
             WHERE usr.user_id = $1`,
            [account.user_id]
          )

          const roles = rolesRes.rows.map((r) => r.code)

          const user = {
            id: account.user_id,
            username: account.user_username,
            email: account.user_email || email,
            name: account.user_name,
            avatarUrl: account.user_avatar_url,
            roles,
          }

          const payload = {
            sub: String(user.id),
            roles: user.roles,
            provider: 'ldap',
          }

          const accessToken = jwt.sign(payload, JWT_SECRET, {
            expiresIn: ACCESS_TOKEN_EXPIRES_IN_SECONDS,
          })

          return {
            accessToken,
            user,
          }
        } finally {
          dbClient.release()
        }
      } catch (err) {
        // eslint-disable-next-line no-console
        console.error('LDAP login failed', err)
        throw new Error('LDAP_INVALID')
      } finally {
        await client.unbind().catch(() => {})
      }
    }

  try {
    if (provider === 'local') {
      const { accessToken, user } = await tryLocalLogin()
      return res.json({
        accessToken,
        expiresIn: ACCESS_TOKEN_EXPIRES_IN_SECONDS,
        user,
      })
    }

    if (provider === 'ldap') {
      const { accessToken, user } = await tryLdapLogin()
      return res.json({
        accessToken,
        expiresIn: ACCESS_TOKEN_EXPIRES_IN_SECONDS,
        user,
      })
    }

    // auto 模式：先尝试本地，再尝试 LDAP
    if (provider === 'auto') {
      try {
        const { accessToken, user } = await tryLocalLogin()
        return res.json({
          accessToken,
          expiresIn: ACCESS_TOKEN_EXPIRES_IN_SECONDS,
          user,
        })
      } catch (err) {
        // 本地失败再尝试 LDAP
        const { accessToken, user } = await tryLdapLogin()
        return res.json({
          accessToken,
          expiresIn: ACCESS_TOKEN_EXPIRES_IN_SECONDS,
          user,
        })
      }
    }

    return res.status(400).json({ message: 'Unsupported provider' })
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error(err)
    return res.status(500).json({ message: 'Internal server error' })
  }
})

app.post('/api/ldap/sync-users', authMiddleware, async (req, res) => {
  const {
    LDAP_URL,
    LDAP_BASE_DN,
    LDAP_ADMIN_DN,
    LDAP_ADMIN_PASSWORD,
    LDAP_USER_SEARCH_BASE,
    LDAP_USER_SEARCH_FILTER,
    LDAP_TIMEOUT,
  } = process.env

  if (!LDAP_URL || !LDAP_BASE_DN || !LDAP_ADMIN_DN || !LDAP_ADMIN_PASSWORD) {
    return res.status(500).json({
      message: 'LDAP is not fully configured on the server',
    })
  }

  const client = new LdapClient({
    url: LDAP_URL,
    timeout: Number(LDAP_TIMEOUT || 5000),
    connectTimeout: Number(LDAP_TIMEOUT || 5000),
  })

  const searchBase = LDAP_USER_SEARCH_BASE || LDAP_BASE_DN
  const rawFilter = LDAP_USER_SEARCH_FILTER || '(uid=*)'
  const searchFilter = rawFilter.includes('{{username}}')
    ? rawFilter.replace('{{username}}', '*')
    : rawFilter

  try {
    // 1) 绑定管理员账号
    await client.bind(LDAP_ADMIN_DN, LDAP_ADMIN_PASSWORD)

    // 2) 搜索所有用户
    const { searchEntries } = await client.search(searchBase, {
      scope: 'sub',
      filter: searchFilter,
      attributes: ['uid', 'gecos', 'mail', 'userPassword'],
    })

    const clientDb = await pool.connect()
    try {
      await clientDb.query('BEGIN')

      // 确保 auth_providers 中存在 LDAP provider
      let ldapProviderId
      const providerRes = await clientDb.query(
        "SELECT id FROM auth_providers WHERE code = 'ldap' LIMIT 1"
      )
      if (providerRes.rows.length > 0) {
        ldapProviderId = providerRes.rows[0].id
      } else {
        const insertProvider = await clientDb.query(
          "INSERT INTO auth_providers (code, name, enabled) VALUES ('ldap', 'LDAP', 1) RETURNING id"
        )
        ldapProviderId = insertProvider.rows[0].id
      }

      let createdCount = 0
      let updatedCount = 0
      let deactivatedCount = 0

      const getAttr = (entry, name) => {
        const value = entry[name]
        if (!value) return null
        if (Array.isArray(value)) return value[0] || null
        return value
      }

      const syncedProviderUserIds = []

      for (const entry of searchEntries) {

        const uid = getAttr(entry, 'uid')
        if (!uid) continue

        const gecos = getAttr(entry, 'gecos')
        const mail = getAttr(entry, 'mail')

        const username = String(uid)
        const email = mail ? String(mail) : `${username}@ldap.local`
        const displayName = (gecos && String(gecos)) || username

        syncedProviderUserIds.push(username)

        // 查找现有用户
        const existingUserRes = await clientDb.query(
          'SELECT id, username, email, name FROM users WHERE username = $1 OR email = $2',
          [username, email]
        )

        let userId
        let userHasLdap = false
        let userHasLocalOnly = false

        if (existingUserRes.rows.length > 0) {
          userId = existingUserRes.rows[0].id

          // 检查该用户已有关联的 provider 类型
          const { rows: userProviders } = await clientDb.query(
            `SELECT ap.code
             FROM user_auth_accounts ua
             JOIN auth_providers ap ON ap.id = ua.provider_id
             WHERE ua.user_id = $1`,
            [userId]
          )

          userHasLdap = userProviders.some((p) => p.code === 'ldap')
          userHasLocalOnly =
            userProviders.length > 0 &&
            userProviders.every((p) => p.code === 'local')

          if (userHasLocalOnly && !userHasLdap) {
            // 纯本地账号：不覆盖、不附加 LDAP，只跳过本次条目
            continue
          }

          // 已经有 ldap provider 的用户，允许用 LDAP 数据更新基础信息
          await clientDb.query(
            `UPDATE users
               SET username = $1,
                   email = $2,
                   name = $3,
                   updated_at = CURRENT_TIMESTAMP
             WHERE id = $4`,
            [username, email, displayName, userId]
          )
          updatedCount += 1
        } else {
          // 本地不存在：新建用户 + 视为 LDAP 来源
          const insertUser = await clientDb.query(
            `INSERT INTO users (username, email, name)
             VALUES ($1, $2, $3)
             RETURNING id`,
            [username, email, displayName]
          )
          userId = insertUser.rows[0].id
          createdCount += 1
        }

        // 为用户创建或更新 LDAP 认证账号，不在本地存储 LDAP 密码
        // 先按 provider_id + provider_user_id 检查，避免触发唯一约束 uk_user_auth_provider_uid
        const accountByUidRes = await clientDb.query(
          'SELECT id, user_id FROM user_auth_accounts WHERE provider_id = $1 AND provider_user_id = $2',
          [ldapProviderId, username]
        )

        if (accountByUidRes.rows.length > 0) {
          const existingAccountId = accountByUidRes.rows[0].id
          await clientDb.query(
            `UPDATE user_auth_accounts
               SET username = $1,
                   email = $2,
                   status = 1
             WHERE id = $3`,
            [username, email, existingAccountId]
          )
        } else {
          const accountRes = await clientDb.query(
            'SELECT id FROM user_auth_accounts WHERE user_id = $1 AND provider_id = $2',
            [userId, ldapProviderId]
          )

          if (accountRes.rows.length > 0) {
            await clientDb.query(
              `UPDATE user_auth_accounts
                 SET provider_user_id = $1,
                     username = $2,
                     email = $3,
                     status = 1
               WHERE user_id = $4 AND provider_id = $5`,
              [username, username, email, userId, ldapProviderId]
            )
          } else {
            await clientDb.query(
              `INSERT INTO user_auth_accounts
                 (user_id, provider_id, provider_user_id, username, email, status)
               VALUES ($1, $2, $3, $4, $5, 1)`,
              [userId, ldapProviderId, username, username, email]
            )
          }
        }
      }

      // 额外规则：如果某个 LDAP 用户在本次同步中不存在，则自动禁用
      if (syncedProviderUserIds.length > 0) {
        const deactivateRes = await clientDb.query(
          `WITH ldap_users AS (
             SELECT ua.user_id
             FROM user_auth_accounts ua
             WHERE ua.provider_id = $1
               AND NOT (ua.provider_user_id = ANY($2::text[]))
           )
           UPDATE users u
           SET status = 0,
               updated_at = CURRENT_TIMESTAMP
           WHERE u.id IN (SELECT user_id FROM ldap_users)
             AND u.status <> 0
           RETURNING u.id`,
          [ldapProviderId, syncedProviderUserIds]
        )

        deactivatedCount = deactivateRes.rowCount || 0

        // 同时将对应的 LDAP 认证账号标记为禁用
        if (deactivatedCount > 0) {
          await clientDb.query(
            `UPDATE user_auth_accounts
               SET status = 0
             WHERE provider_id = $1
               AND user_id = ANY($2::int[])`,
            [
              ldapProviderId,
              deactivateRes.rows.map((row) => row.id),
            ]
          )
        }
      }

      await clientDb.query('COMMIT')

      return res.json({
        message: `LDAP 用户同步完成：新增 ${createdCount} 条，更新 ${updatedCount} 条，禁用 ${deactivatedCount} 条`,
        created: createdCount,
        updated: updatedCount,
        deactivated: deactivatedCount,
      })
    } catch (err) {
      await clientDb.query('ROLLBACK')
      throw err
    } finally {
      clientDb.release()
    }
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('LDAP sync failed', err)
    return res
      .status(500)
      .json({ message: 'LDAP 同步失败，请检查服务器日志。' })
  } finally {
    await client.unbind()
  }
})

app.patch('/api/users/:id/status', authMiddleware, async (req, res) => {
  const userId = Number(req.params.id)
  if (!Number.isFinite(userId)) {
    return res.status(400).json({ message: 'Invalid user id' })
  }

  const { status } = req.body || {}
  if (status !== 0 && status !== 1) {
    return res.status(400).json({ message: 'Invalid status' })
  }

  try {
    const client = await pool.connect()
    try {
      await client.query(
        `UPDATE users
           SET status = $1,
               updated_at = CURRENT_TIMESTAMP
         WHERE id = $2`,
        [status, userId]
      )

      return res.json({ message: 'Status updated' })
    } finally {
      client.release()
    }
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error(err)
    return res.status(500).json({ message: 'Internal server error' })
  }
})

app.get('/api/system-roles', authMiddleware, async (req, res) => {
  try {
    const client = await pool.connect()
    try {
      const result = await client.query(
        'SELECT code, name, description FROM system_roles ORDER BY id ASC'
      )

      const roles = result.rows.map((row) => ({
        code: row.code,
        name: row.name,
        description: row.description,
      }))

      return res.json(roles)
    } finally {
      client.release()
    }
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error(err)
    return res.status(500).json({ message: 'Internal server error' })
  }
})

app.post('/api/users', authMiddleware, async (req, res) => {
  const { firstName, lastName, username, email, password, role } = req.body || {}

  if (!username || !email || !password) {
    return res.status(400).json({ message: 'Missing required fields' })
  }

  try {
    const client = await pool.connect()
    try {
      await client.query('BEGIN')

      const name = [firstName, lastName].filter(Boolean).join(' ').trim() || null

      const existingUser = await client.query(
        'SELECT id FROM users WHERE username = $1 OR email = $2',
        [username, email]
      )
      if (existingUser.rows.length > 0) {
        await client.query('ROLLBACK')
        return res.status(409).json({ message: 'User already exists' })
      }

      const insertUserResult = await client.query(
        `INSERT INTO users (username, email, name)
         VALUES ($1, $2, $3)
         RETURNING id`,
        [username, email, name]
      )
      const userId = insertUserResult.rows[0].id

      const passwordHash = await bcrypt.hash(password, 10)

      const providerResult = await client.query(
        "SELECT id FROM auth_providers WHERE code = 'local' LIMIT 1"
      )
      const providerId = providerResult.rows[0]?.id

      if (providerId) {
        await client.query(
          `INSERT INTO user_auth_accounts (user_id, provider_id, provider_user_id, username, email, password_hash)
           VALUES ($1, $2, $3, $4, $5, $6)`,
          [userId, providerId, username, username, email, passwordHash]
        )
      }

      if (role) {
        let roleCode
        switch (role) {
          case 'superadmin':
            roleCode = 'super_admin'
            break
          case 'admin':
            roleCode = 'admin'
            break
          case 'user':
          default:
            roleCode = 'user'
            break
        }

        const { rows: roleRows } = await client.query(
          'SELECT id FROM system_roles WHERE code = $1',
          [roleCode]
        )

        if (roleRows.length > 0) {
          const systemRoleId = roleRows[0].id
          await client.query(
            `INSERT INTO user_system_roles (user_id, system_role_id)
             VALUES ($1, $2)
             ON CONFLICT (user_id, system_role_id) DO NOTHING`,
            [userId, systemRoleId]
          )
        }
      }

      await client.query('COMMIT')
      return res.status(201).json({ id: userId })
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

app.patch('/api/users/:id', authMiddleware, async (req, res) => {
  const userId = Number(req.params.id)
  if (!Number.isFinite(userId)) {
    return res.status(400).json({ message: 'Invalid user id' })
  }

  const { firstName, lastName, email, role } = req.body || {}

  try {
    const client = await pool.connect()
    try {
      await client.query('BEGIN')

      // 禁止编辑有外部认证来源（如 LDAP）的用户的基础信息，仅允许修改角色
      const { rows: providerRows } = await client.query(
        `SELECT ap.code
         FROM user_auth_accounts ua
         JOIN auth_providers ap ON ap.id = ua.provider_id
         WHERE ua.user_id = $1`,
        [userId]
      )

      const hasNonLocalProvider = providerRows.some((row) => row.code !== 'local')

      const name = [firstName, lastName].filter(Boolean).join(' ').trim() || null

      // 外部账号：跳过 name/email 更新，只允许修改角色
      if (!hasNonLocalProvider && (email || name)) {
        await client.query(
          `UPDATE users
             SET
               email = COALESCE($1, email),
               name = COALESCE($2, name),
               updated_at = CURRENT_TIMESTAMP
           WHERE id = $3`,
          [email || null, name, userId]
        )
      }

      if (role) {
        let roleCode
        switch (role) {
          case 'superadmin':
            roleCode = 'super_admin'
            break
          case 'admin':
            roleCode = 'admin'
            break
          case 'user':
          default:
            roleCode = 'user'
            break
        }

        const { rows: roleRows } = await client.query(
          'SELECT id FROM system_roles WHERE code = $1',
          [roleCode]
        )

        if (roleRows.length > 0) {
          const systemRoleId = roleRows[0].id
          await client.query(
            `INSERT INTO user_system_roles (user_id, system_role_id)
             VALUES ($1, $2)
             ON CONFLICT (user_id, system_role_id) DO NOTHING`,
            [userId, systemRoleId]
          )
        }
      }

      await client.query('COMMIT')
      return res.json({ message: 'User updated' })
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

app.get('/api/users', authMiddleware, async (req, res) => {
  try {
    const client = await pool.connect()
    try {
      const result = await client.query(
        `SELECT
           u.id,
           u.username,
           u.email,
           u.name,
           u.status,
           u.created_at,
           u.updated_at,
           COALESCE(sr.code, 'user') AS role_code,
           sr.name AS role_name,
           ap.provider_name AS provider_name
         FROM users u
         LEFT JOIN user_system_roles usr ON usr.user_id = u.id
         LEFT JOIN system_roles sr ON sr.id = usr.system_role_id
         LEFT JOIN LATERAL (
           SELECT apa.user_id, ap.name AS provider_name
           FROM user_auth_accounts apa
           JOIN auth_providers ap ON ap.id = apa.provider_id
           WHERE apa.user_id = u.id
           ORDER BY COALESCE(apa.last_login_at, apa.created_at) DESC
           LIMIT 1
         ) ap ON ap.user_id = u.id
         ORDER BY u.id ASC`
      )

      const mapStatus = (status) => {
        switch (status) {
          case 0:
            return 'inactive'
          case 2:
            return 'invited'
          default:
            return 'active'
        }
      }

      const mapRole = (roleCode) => {
        switch (roleCode) {
          case 'super_admin':
            return 'superadmin'
          case 'admin':
            return 'admin'
          case 'user':
            return 'user'
          default:
            return 'user'
        }
      }

      const users = result.rows.map((row) => {
        const fullName = row.name || ''
        const [firstName, ...rest] = fullName.split(' ').filter(Boolean)
        const lastName = rest.join(' ')

        return {
          id: String(row.id),
          firstName: firstName || row.username || '',
          lastName: lastName || '',
          username: row.username,
          email: row.email,
          phoneNumber: '',
          status: mapStatus(row.status),
          role: mapRole(row.role_code),
          roleName: row.role_name || null,
          accountSource: row.provider_name || '本地账号',
          createdAt: row.created_at,
          updatedAt: row.updated_at,
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
  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null

  if (!token) {
    return res.status(401).json({ message: 'Missing token' })
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET)
    req.user = decoded
    return next()
  } catch (err) {
    return res.status(401).json({ message: 'Invalid token' })
  }
}

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
