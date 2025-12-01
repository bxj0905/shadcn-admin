import 'dotenv/config'
import express from 'express'
import cors from 'cors'
import pkg from 'pg'
import bcrypt from 'bcryptjs'
import jwt from 'jsonwebtoken'
import multer from 'multer'
import { Client as LdapClient } from 'ldapts'
import { Client as MinioClient } from 'minio'

process.on('unhandledRejection', (reason) => {
  console.error('UNHANDLED_REJECTION', reason)
})

process.on('uncaughtException', (err) => {
  console.error('UNCAUGHT_EXCEPTION', err)
})

console.log('Starting auth server...')

const { Pool } = pkg

const app = express()
const port = process.env.PORT || 3000

app.use(cors())
app.use(express.json())

// 用于导入第二步上传本地目录文件的 multipart/form-data 解析
const upload = multer({ storage: multer.memoryStorage() })

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

const MINIO_ENDPOINT = process.env.MINIO_ENDPOINT || 'http://localhost:9000'
const MINIO_ACCESS_KEY = process.env.MINIO_ACCESS_KEY || ''
const MINIO_SECRET_KEY = process.env.MINIO_SECRET_KEY || ''
const MINIO_BUCKET = process.env.MINIO_BUCKET || 'my-bucket'

const AIRFLOW_BASE_URL = process.env.AIRFLOW_BASE_URL || 'http://localhost:8080'
const AIRFLOW_USER = process.env.AIRFLOW_USER || 'airflow'
const AIRFLOW_PASSWORD = process.env.AIRFLOW_PASSWORD || 'airflow'

const minioUrl = new URL(MINIO_ENDPOINT)
const minioClient = new MinioClient({
  endPoint: minioUrl.hostname,
  port: Number(minioUrl.port || (minioUrl.protocol === 'https:' ? 443 : 9000)),
  useSSL: minioUrl.protocol === 'https:',
  accessKey: MINIO_ACCESS_KEY,
  secretKey: MINIO_SECRET_KEY,
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

// 第二步：上传本地目录文件到 MinIO，返回 rawPrefix，后续 Airflow 仅操作 MinIO
app.post('/api/datasets/:id/import/upload', authMiddleware, upload.array('files'), async (req, res) => {
  const datasetId = Number(req.params.id)
  if (!Number.isFinite(datasetId)) {
    return res.status(400).json({ message: 'Invalid dataset id' })
  }

  const currentUserId = Number(req.user && req.user.sub)
  if (!Number.isFinite(currentUserId)) {
    return res.status(401).json({ message: 'Unauthorized' })
  }

  const files = Array.isArray(req.files) ? req.files : []
  if (!files.length) {
    return res.status(400).json({ message: 'No files uploaded' })
  }

  // paths 字段与 files 一一对应，来自前端的 webkitRelativePath
  const pathsField = req.body && req.body.paths
  const paths = Array.isArray(pathsField) ? pathsField : pathsField ? [pathsField] : []

  try {
    const client = await pool.connect()
    try {
      const dsRes = await client.query('SELECT team_id FROM team_datasets WHERE id = $1 LIMIT 1', [datasetId])
      if (dsRes.rows.length === 0) {
        return res.status(404).json({ message: 'Dataset not found' })
      }

      const teamId = dsRes.rows[0].team_id
      const isAdminForTeam = await isTeamAdmin(client, teamId, currentUserId)
      const systemAdmin = isAdmin(req) || isSuperAdmin(req)
      if (!isAdminForTeam && !systemAdmin) {
        return res.status(403).json({ message: 'Forbidden' })
      }

      const statDate = (req.body && req.body.statDate) || new Date().toISOString().slice(0, 10)
      // 这里的 raw 区前缀与 DuckDB 主目录保持一致：<teamId>/<datasetId>/raw/
      // MINIO_BUCKET 已经叫 dataset，因此不再额外加一层 datasets/
      const basePrefix = `${teamId}/${datasetId}/raw`

      let uploadedCount = 0
      // @ts-ignore multer File 类型在运行时存在 buffer 属性
      for (let i = 0; i < files.length; i += 1) {
        const file = files[i]
        const rel = paths[i] || file.originalname || `file_${i}`
        const normalized = String(rel).replace(/^\\+/, '').replace(/^\/+/, '')
        const objectName = `${basePrefix}/${normalized}`

        try {
          // 内存存储下，multer File 带有 buffer
          await minioClient.putObject(MINIO_BUCKET, objectName, file.buffer)
          uploadedCount += 1
        } catch (err) {
          console.error('MinIO upload error for import/upload', err)
        }
      }

      return res.status(201).json({
        // rawPrefix 只到 raw/ 这一层，后续 Airflow 以此为根前缀进行处理
        rawPrefix: `${basePrefix}/`,
        statDate,
        uploadedCount,
      })
    } finally {
      client.release()
    }
  } catch (err) {
    console.error('dataset import upload error', err)
    return res.status(500).json({ message: 'Failed to upload files to MinIO' })
  }
})

app.post('/api/datasets/:id/import', authMiddleware, async (req, res) => {
  const datasetId = Number(req.params.id)
  if (!Number.isFinite(datasetId)) {
    return res.status(400).json({ message: 'Invalid dataset id' })
  }

  const currentUserId = Number(req.user && req.user.sub)
  if (!Number.isFinite(currentUserId)) {
    return res.status(401).json({ message: 'Unauthorized' })
  }

  const { airflowDagId, directory, rawPrefix, config } = req.body || {}
  if (!airflowDagId) {
    return res.status(400).json({ message: 'airflowDagId is required' })
  }

  try {
    const client = await pool.connect()
    try {
      const dsRes = await client.query(
        'SELECT team_id, name FROM team_datasets WHERE id = $1 LIMIT 1',
        [datasetId]
      )

      if (dsRes.rows.length === 0) {
        return res.status(404).json({ message: 'Dataset not found' })
      }

      const teamId = dsRes.rows[0].team_id

      const isAdminForTeam = await isTeamAdmin(client, teamId, currentUserId)
      const systemAdmin = isAdmin(req) || isSuperAdmin(req)
      if (!isAdminForTeam && !systemAdmin) {
        return res.status(403).json({ message: 'Forbidden' })
      }

      const conf = {
        dataset_id: datasetId,
        dataset_name: dsRes.rows[0].name,
        // 兼容阶段：同时传递 directory（本地路径）和 raw_prefix（MinIO 前缀）
        directory,
        ...(rawPrefix ? { raw_prefix: rawPrefix } : {}),
        ...(config ? { extra: config } : {}),
      }

      const run = await triggerAirflowDag(airflowDagId, conf)
      return res.status(201).json(run)
    } finally {
      client.release()
    }
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('trigger DAG error', err)
    return res.status(500).json({ message: 'Failed to trigger Airflow DAG' })
  }
})

app.get('/api/airflow/runs/:dagId/:runId', authMiddleware, async (req, res) => {
  const { dagId, runId } = req.params
  if (!dagId || !runId) {
    return res.status(400).json({ message: 'dagId and runId are required' })
  }

  try {
    const run = await getAirflowDagRun(dagId, runId)
    return res.json(run)
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('get DAG run error', err)
    return res.status(500).json({ message: 'Failed to get Airflow DAG run' })
  }
})

app.get('/api/airflow/runs/:dagId/:runId/tasks', authMiddleware, async (req, res) => {
  const { dagId, runId } = req.params
  if (!dagId || !runId) {
    return res.status(400).json({ message: 'dagId and runId are required' })
  }

  try {
    const tasks = await getAirflowDagRunTasks(dagId, runId)
    return res.json(tasks)
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('get DAG run tasks error', err)
    return res.status(500).json({ message: 'Failed to get Airflow DAG run tasks' })
  }
})

app.get(
  '/api/airflow/logs/:dagId/:runId/:taskId',
  authMiddleware,
  async (req, res) => {
    const { dagId, runId, taskId } = req.params
    if (!dagId || !runId || !taskId) {
      return res
        .status(400)
        .json({ message: 'dagId, runId and taskId are required' })
    }

    try {
      const logText = await getAirflowTaskLog(dagId, runId, taskId)
      res.setHeader('Content-Type', 'text/plain; charset=utf-8')
      return res.send(logText)
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('get DAG task log error', err)
      return res.status(500).json({ message: 'Failed to get Airflow task log' })
    }
  }
)

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
    console.error(err)
    return res.status(500).json({ message: 'Internal server error' })
  }
})

app.delete('/api/users/:id', authMiddleware, async (req, res) => {
  const userId = Number(req.params.id)
  if (!Number.isFinite(userId)) {
    return res.status(400).json({ message: 'Invalid user id' })
  }

  try {
    const client = await pool.connect()
    try {
      await client.query('BEGIN')

      // 检查账号来源，如果存在非 local provider，则禁止删除
      const { rows: providerRows } = await client.query(
        `SELECT ap.code
         FROM user_auth_accounts ua
         JOIN auth_providers ap ON ap.id = ua.provider_id
         WHERE ua.user_id = $1`,
        [userId]
      )

      const hasNonLocalProvider = providerRows.some((row) => row.code !== 'local')
      if (hasNonLocalProvider) {
        await client.query('ROLLBACK')
        return res
          .status(400)
          .json({ message: 'Only local users can be deleted' })
      }

      // 删除与该用户相关的认证账号和角色，再删除用户
      await client.query('DELETE FROM user_auth_accounts WHERE user_id = $1', [
        userId,
      ])
      await client.query('DELETE FROM user_system_roles WHERE user_id = $1', [
        userId,
      ])
      await client.query('DELETE FROM users WHERE id = $1', [userId])

      await client.query('COMMIT')
      return res.json({ message: 'User deleted' })
    } catch (err) {
      await client.query('ROLLBACK')
      console.error(err)
      return res.status(500).json({ message: 'Internal server error' })
    } finally {
      client.release()
    }
  } catch (err) {
    console.error(err)
    return res.status(500).json({ message: 'Internal server error' })
  }
})

app.patch('/api/users/:id', authMiddleware, async (req, res) => {
  const userId = Number(req.params.id)
  if (!Number.isFinite(userId)) {
    return res.status(400).json({ message: 'Invalid user id' })
  }

  const { firstName, lastName, username, email, role } = req.body || {}

  try {
    const client = await pool.connect()
    try {
      await client.query('BEGIN')

      // 判断是否有非本地认证来源（如 LDAP）
      const { rows: providerRows } = await client.query(
        `SELECT ap.code
         FROM user_auth_accounts ua
         JOIN auth_providers ap ON ap.id = ua.provider_id
         WHERE ua.user_id = $1`,
        [userId]
      )

      const hasNonLocalProvider = providerRows.some((row) => row.code !== 'local')

      const name = [firstName, lastName].filter(Boolean).join(' ').trim() || null

      // 仅本地账号允许修改 username/email/name；外部账号跳过
      if (!hasNonLocalProvider && (username || email || name)) {
        await client.query(
          `UPDATE users
             SET
               username = COALESCE($1, username),
               email    = COALESCE($2, email),
               name     = COALESCE($3, name),
               updated_at = CURRENT_TIMESTAMP
           WHERE id = $4`,
          [username || null, email || null, name, userId]
        )
      }

      // 角色更新：本地和外部账号都允许，保证每个用户只有一个主系统角色
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

          // 先删除该用户已有的系统角色关联
          await client.query('DELETE FROM user_system_roles WHERE user_id = $1', [
            userId,
          ])

          // 再插入当前选择的角色
          await client.query(
            `INSERT INTO user_system_roles (user_id, system_role_id)
             VALUES ($1, $2)`,
            [userId, systemRoleId]
          )
        }
      }

      await client.query('COMMIT')
      return res.json({ message: 'User updated' })
    } catch (err) {
      await client.query('ROLLBACK')
      console.error(err)
      return res.status(500).json({ message: 'Internal server error' })
    } finally {
      client.release()
    }
  } catch (err) {
    console.error(err)
    return res.status(500).json({ message: 'Internal server error' })
  }
})

app.get('/api/teams/:teamId/datasets', authMiddleware, async (req, res) => {
  const teamId = Number(req.params.teamId)
  if (!Number.isFinite(teamId)) {
    return res.status(400).json({ message: 'Invalid team id' })
  }

  const currentUserId = Number(req.user && req.user.sub)
  if (!Number.isFinite(currentUserId)) {
    return res.status(401).json({ message: 'Unauthorized' })
  }

  try {
    const client = await pool.connect()
    try {
      const rowsRes = await client.query(
        `SELECT DISTINCT ON (d.id)
           d.id,
           d.team_id,
           d.name,
           d.description,
           d.type,
           d.storage_path,
           d.created_by,
           d.created_at,
           d.updated_at,
           t.owner_id,
           tr.code AS team_role_code,
           tdm.permission AS member_permission
         FROM team_datasets d
         JOIN teams t ON t.id = d.team_id
         LEFT JOIN team_members tm ON tm.team_id = d.team_id AND tm.user_id = $2 AND tm.status = 1
         LEFT JOIN team_member_roles tmr ON tmr.team_member_id = tm.id
         LEFT JOIN team_roles tr ON tr.id = tmr.team_role_id
         LEFT JOIN team_dataset_members tdm
           ON tdm.dataset_id = d.id AND tdm.user_id = $2
         WHERE d.team_id = $1
         ORDER BY d.id ASC`,
        [teamId, currentUserId]
      )

      const datasets = rowsRes.rows.map((row) => {
        let permission = null
        if (row.owner_id === currentUserId) {
          permission = 'admin'
        } else if (row.team_role_code === 'owner' || row.team_role_code === 'maintainer') {
          permission = 'admin'
        } else if (row.member_permission) {
          permission = row.member_permission
        }

        return {
          id: String(row.id),
          teamId: String(row.team_id),
          name: row.name,
          description: row.description,
          type: row.type,
          storagePath: row.storage_path,
          createdBy: String(row.created_by),
          createdAt: row.created_at,
          updatedAt: row.updated_at,
          permission,
        }
      })

      return res.json(datasets)
    } finally {
      client.release()
    }
  } catch (err) {
    console.error(err)
    return res.status(500).json({ message: 'Internal server error' })
  }
})

app.post('/api/teams/:teamId/datasets', authMiddleware, async (req, res) => {
  const teamId = Number(req.params.teamId)
  if (!Number.isFinite(teamId)) {
    return res.status(400).json({ message: 'Invalid team id' })
  }

  const currentUserId = Number(req.user && req.user.sub)
  if (!Number.isFinite(currentUserId)) {
    return res.status(401).json({ message: 'Unauthorized' })
  }

  const { name, description, type } = req.body || {}

  if (!name || !type || !['duckdb', 'pgsql'].includes(type)) {
    return res.status(400).json({ message: 'Missing or invalid fields' })
  }

  try {
    const client = await pool.connect()
    try {
      await client.query('BEGIN')

      const memberRes = await client.query(
        'SELECT id FROM team_members WHERE team_id = $1 AND user_id = $2 AND status = 1 LIMIT 1',
        [teamId, currentUserId]
      )

      if (memberRes.rows.length === 0) {
        await client.query('ROLLBACK')
        return res.status(403).json({ message: 'Forbidden' })
      }

      const teamAdmin = await isTeamAdmin(client, teamId, currentUserId)
      const systemAdmin = isAdmin(req) || isSuperAdmin(req)
      if (!teamAdmin && !systemAdmin) {
        await client.query('ROLLBACK')
        return res.status(403).json({ message: 'Forbidden' })
      }

      const insertRes = await client.query(
        `INSERT INTO team_datasets (team_id, name, description, type, storage_path, created_by)
         VALUES ($1, $2, $3, $4, '', $5)
         RETURNING id`,
        [teamId, name, description || null, type, currentUserId]
      )

      const datasetId = insertRes.rows[0].id

      let storagePath

      if (type === 'duckdb') {
        storagePath = buildDuckdbPath(teamId, datasetId)
        try {
          await ensureMinioBucketExists()
          const objectName = `${storagePath}.keep`
          await minioClient.putObject(MINIO_BUCKET, objectName, '')
        } catch (err) {
          console.error('MinIO create dataset dir error', err)
        }
      } else {
        storagePath = buildPgsqlDatabaseName(teamId, datasetId)
        try {
          // CREATE DATABASE 不能在显式事务块中执行，这里使用连接池在事务外单独执行
          await pool.query(`CREATE DATABASE "${storagePath}"`)
        } catch (err) {
          console.error('Create PG database for dataset error', err)
        }
      }

      await client.query(
        'UPDATE team_datasets SET storage_path = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2',
        [storagePath, datasetId]
      )

      await client.query(
        `INSERT INTO team_dataset_members (dataset_id, user_id, permission)
         VALUES ($1, $2, $3)
         ON CONFLICT (dataset_id, user_id) DO NOTHING`,
        [datasetId, currentUserId, 'editor']
      )

      await client.query('COMMIT')

      return res.status(201).json({
        id: String(datasetId),
        teamId: String(teamId),
        name,
        description: description || null,
        type,
        storagePath,
        createdBy: String(currentUserId),
      })
    } catch (err) {
      await client.query('ROLLBACK')
      console.error(err)
      return res.status(500).json({ message: 'Internal server error' })
    } finally {
      client.release()
    }
  } catch (err) {
    console.error(err)
    return res.status(500).json({ message: 'Internal server error' })
  }
})

app.get('/api/datasets/:id/members', authMiddleware, async (req, res) => {
  const datasetId = Number(req.params.id)
  if (!Number.isFinite(datasetId)) {
    return res.status(400).json({ message: 'Invalid dataset id' })
  }

  const currentUserId = Number(req.user && req.user.sub)
  if (!Number.isFinite(currentUserId)) {
    return res.status(401).json({ message: 'Unauthorized' })
  }

  try {
    const client = await pool.connect()
    try {
      const dsRes = await client.query(
        'SELECT team_id FROM team_datasets WHERE id = $1 LIMIT 1',
        [datasetId]
      )

      if (dsRes.rows.length === 0) {
        return res.status(404).json({ message: 'Dataset not found' })
      }

      const teamId = dsRes.rows[0].team_id

      const isAdminForTeam = await isTeamAdmin(client, teamId, currentUserId)
      const systemAdmin = isAdmin(req) || isSuperAdmin(req)
      if (!isAdminForTeam && !systemAdmin) {
        return res.status(403).json({ message: 'Forbidden' })
      }

      const membersRes = await client.query(
        `SELECT
           tdm.id,
           tdm.user_id,
           u.username,
           u.name,
           tdm.permission
         FROM team_dataset_members tdm
         JOIN users u ON u.id = tdm.user_id
         WHERE tdm.dataset_id = $1
         ORDER BY tdm.id ASC`,
        [datasetId]
      )

      const members = membersRes.rows.map((row) => ({
        id: String(row.id),
        userId: String(row.user_id),
        username: row.username,
        name: row.name,
        permission: row.permission,
      }))

      return res.json(members)
    } finally {
      client.release()
    }
  } catch (err) {
    console.error(err)
    return res.status(500).json({ message: 'Internal server error' })
  }
})

app.post('/api/datasets/:id/members', authMiddleware, async (req, res) => {
  const datasetId = Number(req.params.id)
  if (!Number.isFinite(datasetId)) {
    return res.status(400).json({ message: 'Invalid dataset id' })
  }

  const currentUserId = Number(req.user && req.user.sub)
  if (!Number.isFinite(currentUserId)) {
    return res.status(401).json({ message: 'Unauthorized' })
  }

  const { userId, permission } = req.body || {}
  const targetUserId = Number(userId)

  if (!Number.isFinite(targetUserId) || !['viewer', 'editor'].includes(permission)) {
    return res.status(400).json({ message: 'Missing or invalid fields' })
  }

  try {
    const client = await pool.connect()
    try {
      const dsRes = await client.query(
        'SELECT team_id FROM team_datasets WHERE id = $1 LIMIT 1',
        [datasetId]
      )

      if (dsRes.rows.length === 0) {
        return res.status(404).json({ message: 'Dataset not found' })
      }

      const teamId = dsRes.rows[0].team_id

      const isAdminForTeam = await isTeamAdmin(client, teamId, currentUserId)
      const systemAdmin = isAdmin(req) || isSuperAdmin(req)
      if (!isAdminForTeam && !systemAdmin) {
        return res.status(403).json({ message: 'Forbidden' })
      }

      await client.query(
        `INSERT INTO team_dataset_members (dataset_id, user_id, permission)
         VALUES ($1, $2, $3)
         ON CONFLICT (dataset_id, user_id) DO UPDATE SET permission = EXCLUDED.permission, updated_at = CURRENT_TIMESTAMP`,
        [datasetId, targetUserId, permission]
      )

      return res.status(201).json({ message: 'Member added or updated' })
    } finally {
      client.release()
    }
  } catch (err) {
    console.error(err)
    return res.status(500).json({ message: 'Internal server error' })
  }
})

app.delete('/api/datasets/:id/members/:memberId', authMiddleware, async (req, res) => {
  const datasetId = Number(req.params.id)
  const memberId = Number(req.params.memberId)

  if (!Number.isFinite(datasetId) || !Number.isFinite(memberId)) {
    return res.status(400).json({ message: 'Invalid id' })
  }

  const currentUserId = Number(req.user && req.user.sub)
  if (!Number.isFinite(currentUserId)) {
    return res.status(401).json({ message: 'Unauthorized' })
  }

  try {
    const client = await pool.connect()
    try {
      const dsRes = await client.query(
        'SELECT team_id FROM team_datasets WHERE id = $1 LIMIT 1',
        [datasetId]
      )

      if (dsRes.rows.length === 0) {
        return res.status(404).json({ message: 'Dataset not found' })
      }

      const teamId = dsRes.rows[0].team_id

      const isAdminForTeam = await isTeamAdmin(client, teamId, currentUserId)
      const systemAdmin = isAdmin(req) || isSuperAdmin(req)
      if (!isAdminForTeam && !systemAdmin) {
        return res.status(403).json({ message: 'Forbidden' })
      }

      await client.query(
        'DELETE FROM team_dataset_members WHERE id = $1 AND dataset_id = $2',
        [memberId, datasetId]
      )

      return res.json({ message: 'Member removed' })
    } finally {
      client.release()
    }
  } catch (err) {
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
      attributes: ['uid', 'gecos', 'mail'],
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
          "INSERT INTO auth_providers (code, name, type, enabled) VALUES ('ldap', 'LDAP', 'ldap', 1) RETURNING id"
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

          const hasLdap = userProviders.some((p) => p.code === 'ldap')
          const localOnly =
            userProviders.length > 0 &&
            userProviders.every((p) => p.code === 'local')

          if (localOnly && !hasLdap) {
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
    console.error('LDAP sync failed', err)
    return res
      .status(500)
      .json({ message: 'LDAP 同步失败，请检查服务器日志。' })
  } finally {
    await client.unbind().catch(() => {})
  }
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

      const providersRes = await client.query(
        `SELECT
           ua.user_id,
           ap.code
         FROM user_auth_accounts ua
         JOIN auth_providers ap ON ap.id = ua.provider_id`
      )

      const rolesByUser = new Map()
      for (const row of rolesRes.rows) {
        const list = rolesByUser.get(row.user_id) || []
        list.push({ code: row.code, name: row.name })
        rolesByUser.set(row.user_id, list)
      }

      const providersByUser = new Map()
      for (const row of providersRes.rows) {
        const list = providersByUser.get(row.user_id) || []
        list.push(row.code)
        providersByUser.set(row.user_id, list)
      }

      const users = usersRes.rows.map((row) => {
        const roleEntries = rolesByUser.get(row.id) || []
        const primaryRoleCode = roleEntries[0]?.code || 'user'
        const roleName = roleEntries[0]?.name || null

        // 映射数据库中的角色 code 到前端使用的 key
        let role
        switch (primaryRoleCode) {
          case 'super_admin':
            role = 'superadmin'
            break
          case 'admin':
            role = 'admin'
            break
          case 'user':
          default:
            role = 'user'
            break
        }

        const providerCodes = providersByUser.get(row.id) || []
        let accountSource = '本地账号'
        if (providerCodes.length > 0) {
          const hasNonLocal = providerCodes.some((code) => code !== 'local')
          if (hasNonLocal) {
            accountSource = 'LDAP 账号'
          }
        }

        return {
          id: String(row.id),
          firstName: row.name,
          lastName: '',
          username: row.username,
          email: row.email,
          phoneNumber: '',
          status: row.status === 1 ? 'active' : 'inactive',
          role,
          accountSource,
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

async function getUserSystemRole(client, userId) {
  const res = await client.query(
    `SELECT sr.code
     FROM user_system_roles usr
     JOIN system_roles sr ON sr.id = usr.system_role_id
     WHERE usr.user_id = $1
     ORDER BY usr.assigned_at ASC
     LIMIT 1`,
    [userId]
  )

  return res.rows[0]?.code || null
}

async function ensureMinioBucketExists() {
  try {
    const exists = await minioClient.bucketExists(MINIO_BUCKET)
    if (!exists) {
      await minioClient.makeBucket(MINIO_BUCKET)
    }
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('MinIO bucket init error', err)
  }
}

async function isTeamAdmin(client, teamId, userId) {
  const res = await client.query(
    `SELECT tr.code
     FROM team_members tm
     JOIN team_member_roles tmr ON tmr.team_member_id = tm.id
     JOIN team_roles tr ON tr.id = tmr.team_role_id
     WHERE tm.team_id = $1 AND tm.user_id = $2
     LIMIT 1`,
    [teamId, userId]
  )
  const roleCode = res.rows[0]?.code
  return roleCode === 'owner' || roleCode === 'maintainer'
}

function buildDuckdbPath(teamId, datasetId) {
  // MINIO_BUCKET 已经叫 dataset，这里不再额外加一层 datasets/
  // 直接按 <teamId>/<datasetId>/ 组织 DuckDB 存储路径
  return `${teamId}/${datasetId}/`
}

function buildPgsqlDatabaseName(teamId, datasetId) {
  return `team_${teamId}_ds_${datasetId}`
}

async function getAirflowToken() {
  const res = await fetch(`${AIRFLOW_BASE_URL}/auth/token`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      username: AIRFLOW_USER,
      password: AIRFLOW_PASSWORD,
    }),
  })

  if (!res.ok) {
    const text = await res.text()
    throw new Error(`Failed to get Airflow token: ${res.status} ${text}`)
  }

  const data = await res.json()
  if (!data.access_token) {
    throw new Error('Airflow token response missing access_token')
  }
  return data.access_token
}

async function triggerAirflowDag(dagId, conf) {
  const token = await getAirflowToken()
  const res = await fetch(`${AIRFLOW_BASE_URL}/api/v2/dags/${encodeURIComponent(dagId)}/dagRuns`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify({
      // Airflow 3 的 DAG Run API 要求 logical_date 字段
      logical_date: new Date().toISOString(),
      conf,
    }),
  })

  if (!res.ok) {
    const text = await res.text()
    throw new Error(`Failed to trigger DAG ${dagId}: ${res.status} ${text}`)
  }

  const data = await res.json()
  return {
    dagId: data.dag_id || dagId,
    runId: data.dag_run_id,
    state: data.state || 'queued',
  }
}

async function getAirflowDagRun(dagId, runId) {
  const token = await getAirflowToken()
  const res = await fetch(
    `${AIRFLOW_BASE_URL}/api/v2/dags/${encodeURIComponent(dagId)}/dagRuns/${encodeURIComponent(runId)}`,
    {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${token}`,
      },
    }
  )

  if (!res.ok) {
    const text = await res.text()
    throw new Error(`Failed to get DAG run ${dagId}/${runId}: ${res.status} ${text}`)
  }

  const data = await res.json()
  return {
    dagId: data.dag_id || dagId,
    runId: data.dag_run_id || runId,
    state: data.state,
    startDate: data.start_date,
    endDate: data.end_date,
  }
}

async function getAirflowDagRunTasks(dagId, runId) {
  const token = await getAirflowToken()
  const res = await fetch(
    `${AIRFLOW_BASE_URL}/api/v2/dags/${encodeURIComponent(dagId)}/dagRuns/${encodeURIComponent(runId)}/taskInstances`,
    {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${token}`,
      },
    }
  )

  if (!res.ok) {
    const text = await res.text()
    throw new Error(`Failed to get DAG run tasks ${dagId}/${runId}: ${res.status} ${text}`)
  }

  const data = await res.json()
  // data.task_instances is an array
  const list = Array.isArray(data.task_instances) ? data.task_instances : []
  return list.map((t) => ({
    taskId: t.task_id,
    state: t.state,
    startDate: t.start_date,
    endDate: t.end_date,
  }))
}

async function getAirflowTaskLog(dagId, runId, taskId) {
  const token = await getAirflowToken()

  // 使用 Airflow 3 的 v2 日志 API
  const url = `${AIRFLOW_BASE_URL}/api/v2/dags/${encodeURIComponent(
    dagId
  )}/dagRuns/${encodeURIComponent(runId)}/taskInstances/${encodeURIComponent(
    taskId
  )}/logs/1`

  const res = await fetch(url, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: 'application/json',
    },
  })

  if (!res.ok) {
    const text = await res.text()
    throw new Error(`Failed to get task log ${dagId}/${runId}/${taskId}: ${res.status} ${text}`)
  }

  const data = await res.json()
  // v2 日志接口通常返回 { content: string | string[] | object[] | null, ... }
  const raw = data && data.content

  if (Array.isArray(raw)) {
    const lines = raw.map((entry) => {
      if (typeof entry === 'string') return entry
      if (entry && typeof entry === 'object') {
        // 优先使用 message / log / content 字段
        if (typeof entry.message === 'string') return entry.message
        if (typeof entry.log === 'string') return entry.log
        if (typeof entry.content === 'string') return entry.content
        return JSON.stringify(entry)
      }
      return String(entry)
    })
    return lines.join('\n')
  }

  if (typeof raw === 'string') {
    return raw
  }

  // 兜底：直接把整个对象序列化出来，方便排查
  return JSON.stringify(data, null, 2)
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

  const teamId = Number(req.params.id)
  if (!Number.isFinite(teamId)) {
    return res.status(400).json({ message: 'Invalid team id' })
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

          // 如果团队拥有者本身就是 super_admin，则不要再给 TA 维护者角色，避免一个成员两个角色
          if (superAdminId === ownerUserId) {
            continue
          }

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
        `WITH member_roles AS (
           SELECT
             tm.id           AS member_id,
             tm.user_id      AS user_id,
             u.username      AS username,
             u.name          AS name,
             tr.code         AS team_role_code,
             tr.name         AS team_role_name,
             ROW_NUMBER() OVER (
               PARTITION BY tm.id
               ORDER BY CASE tr.code
                 WHEN 'owner' THEN 1
                 WHEN 'maintainer' THEN 2
                 WHEN 'member' THEN 3
                 ELSE 4
               END
             ) AS rn
           FROM team_members tm
           JOIN users u ON u.id = tm.user_id
           LEFT JOIN team_member_roles tmr ON tmr.team_member_id = tm.id
           LEFT JOIN team_roles tr ON tr.id = tmr.team_role_id
           WHERE tm.team_id = $1
         )
         SELECT
           member_id AS id,
           user_id,
           username,
           name,
           team_role_code,
           team_role_name
         FROM member_roles
         WHERE rn = 1
         ORDER BY id ASC`,
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
