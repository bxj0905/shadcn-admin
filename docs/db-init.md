# 数据库初始化文档（PostgreSQL）

本文档说明本项目在本地使用 PostgreSQL 作为数据库时的初始化方式，包括：

- 容器环境（docker-compose）
- 数据库建表脚本（schema）
- 初始化基础数据
- 创建 admin 超级管理员账号

> 当前约定：
>
> - 数据库：`PostgreSQL`
> - 数据库名：`shadcndb`
> - 用户：`admin`
> - 密码：`StrongPassword!123`
> - 时区：`Asia/Shanghai`（北京时间）

---

## 1. Docker 环境准备

项目根目录已提供 `docker-compose-sql.yml`：

```yaml
services:
  postgres:
    image: postgres:17
    container_name: postgres
    restart: always
    environment:
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: StrongPassword!123
      POSTGRES_DB: shadcndb
      TZ: Asia/Shanghai
    ports:
      - 5000:5432
    volumes:
      - ./pgdata:/var/lib/postgresql/data
      - ./db-init:/docker-entrypoint-initdb.d
```

含义说明：

- 容器名：`postgres`
- 宿主机端口：`5000` → 容器 `5432`
- 数据目录：`./pgdata`
- 初始化脚本目录：`./db-init`（容器第一次初始化数据目录时自动执行）

### 启动数据库容器

在项目根目录执行：

```bash
docker compose -f docker-compose-sql.yml up -d
```

查看容器状态：

```bash
docker compose -f docker-compose-sql.yml ps
```

显示 `postgres` 状态为 `Up` 即成功。

> 注意：如果已经启动过并且 `./pgdata` 中已有数据，
> 再次启动时 **不会自动执行初始化 SQL**。此时需要手工执行脚本或清空数据目录重新初始化。

---

## 2. 初始化脚本目录结构

项目根目录下的 `db-init` 目录用于存放初始化 SQL 文件：

```text
nuxt-portal/
  db-init/
    001_schema.sql    # 建表 + 初始化数据
```

Postgres 官方镜像会按文件名顺序执行 `/docker-entrypoint-initdb.d` 下的 `.sql` 文件。

- **首次初始化容器数据目录时**，`001_schema.sql` 会被自动执行。
- 若数据目录已存在，则不会自动执行，需要手动导入。

---

## 3. 建表脚本内容概览（001_schema.sql）

`db-init/001_schema.sql` 主要包含以下部分：

1. **基本设置**
   - 设置时区：`SET TIME ZONE 'Asia/Shanghai';`
   - （可选）设置默认时区：`ALTER DATABASE shadcndb SET TIMEZONE TO 'Asia/Shanghai';`

2. **核心业务表**
   - `users`：用户主表
   - `user_profiles`：用户扩展资料
   - `auth_providers`：认证提供方（local、ldap、cas、github、google 等）
   - `user_auth_accounts`：用户在各认证提供方下的账号
   - `system_roles` / `user_system_roles`：系统级角色与用户关联
   - `system_permissions` / `system_role_permissions`：系统级权限与角色关联
   - `teams` / `team_members`：团队与团队成员
   - `team_roles` / `team_permissions`：团队内角色与权限
   - `team_member_roles` / `team_role_permissions`：团队内角色-成员-权限关联
   - `audit_logs`：审计/操作日志
   - `system_settings`：系统设置（标题、副标题、Logo 等）

3. **外键约束（FK）**
   - 各表均通过 `FOREIGN KEY` 与 `users` / `auth_providers` / `teams` / `team_roles` 等关联。

4. **初始化数据**
   - 插入默认的 `auth_providers`：`local`, `ldap`, `cas`, `github`, `google`, `wechat`, `dingtalk`
   - 插入默认的系统角色：`super_admin`, `admin`, `user`
   - 插入默认的团队角色：`owner`, `maintainer`, `member`
   - 插入一些基础的系统权限 & 团队权限
   - 初始化一条 `system_settings`（例如站点 title、副标题）

> 具体 SQL 已写入 `db-init/001_schema.sql`，如需修改字段或新增权限，建议在该文件基础上迭代。

---

## 4. 手工执行初始化脚本（非首次启动）

如果已经启动过容器并且 `./pgdata` 中已有数据，
初始化脚本不会自动执行。这时有两种方式：

### 4.1 方式 A：手动导入（推荐，不清空数据）

在项目根目录执行：

```bash
# 将本地 SQL 文件内容通过标准输入传递给容器内的 psql
cat db-init/001_schema.sql | docker exec -i postgres psql -U admin -d shadcndb
```

> Windows PowerShell 下可使用：
>
> ```powershell
> type db-init/001_schema.sql | docker exec -i postgres psql -U admin -d shadcndb
> ```

执行完成后，可以进入 psql 检查表：

```bash
docker exec -it postgres psql -U admin -d shadcndb
```

在 psql 中执行：

```sql
\dt
```

应能看到 `users`, `teams`, `system_roles`, `team_permissions`, `audit_logs`, `system_settings` 等表。

### 4.2 方式 B：清空数据目录重新初始化（会丢数据）

> 仅适用于本地开发环境、数据库内没有需要保留的数据时。

1. 停止容器：

   ```bash
   docker compose -f docker-compose-sql.yml down
   ```

2. 删除 `pgdata` 目录：

   ```powershell
   # 在 PowerShell 中执行
   Remove-Item -Recurse -Force .\pgdata
   ```

3. 重新启动容器：

   ```bash
   docker compose -f docker-compose-sql.yml up -d
   ```

此时容器会重新初始化数据库，并自动执行 `db-init/001_schema.sql`。

---

## 5. 创建 admin 超级管理员账号

初始化表结构和基础数据后，需要手动创建一个应用层的 admin 用户。

### 5.1 生成密码哈希

项目中提供了 `scripts/hash-password.cjs`，使用 `bcryptjs` 生成密码哈希。

在项目根目录执行：

```bash
node scripts/hash-password.cjs
```

按提示输入明文密码（例如 `Admin123!`），脚本会输出类似：

```text
生成的 bcrypt 哈希为:

$2b$10$Zr7QlxQNcSaFnYP0.G01xOQXzNUQJoLwYODlHeVyN78Mu6zd.44Hi

请将此哈希写入 user_auth_accounts.password_hash 字段。
```

复制整串哈希备用。

### 5.2 在数据库中插入 admin 用户和本地账号

进入容器：

```bash
docker exec -it postgres psql -U admin -d shadcndb
```

在 psql 中执行以下 SQL：

```sql
-- 1) 创建 admin 用户（若已存在则忽略）
INSERT INTO users (username, email, name, avatar_url, status)
VALUES ('admin', 'admin@example.com', 'Super Admin', NULL, 1)
ON CONFLICT (email) DO NOTHING
RETURNING id;

-- 2) 确保存在本地认证提供方 local
INSERT INTO auth_providers (code, name, type, enabled)
VALUES ('local', '本地账号', 'password', 1)
ON CONFLICT (code) DO NOTHING;

-- 3) 为 admin 创建本地账号（若已存在则忽略）
INSERT INTO user_auth_accounts (
  user_id,
  provider_id,
  provider_user_id,
  username,
  email,
  display_name,
  status
)
SELECT u.id,
       p.id,
       'admin',
       'admin',
       'admin@example.com',
       'Super Admin',
       1
FROM users u, auth_providers p
WHERE u.email = 'admin@example.com' AND p.code = 'local'
ON CONFLICT (provider_id, provider_user_id) DO NOTHING;
```

然后为 admin 账号写入密码哈希（替换为你刚才生成的哈希）：

```sql
UPDATE user_auth_accounts
SET password_hash = '$2b$10$Zr7QlxQNcSaFnYP0.G01xOQXzNUQJoLwYODlHeVyN78Mu6zd.44Hi'
WHERE email = 'admin@example.com'
  AND provider_id = (SELECT id FROM auth_providers WHERE code = 'local');
```

> 注意：上面的哈希是示例，把它替换成你实际生成的值。

### 5.3 给 admin 绑定系统角色 super_admin

在 psql 中继续执行：

```sql
-- 查出 admin 用户和 super_admin 角色的 ID
SELECT id, email FROM users WHERE email = 'admin@example.com';
SELECT id, code FROM system_roles WHERE code = 'super_admin';

-- 假设查到 admin.id = 1, super_admin.id = 1，请按实际 ID 替换
INSERT INTO user_system_roles (user_id, system_role_id)
VALUES (1, 1)
ON CONFLICT (user_id, system_role_id) DO NOTHING;
```

完成后，admin 用户即可通过：

- Email：`admin@example.com`
- Password：你在 hash 脚本中输入的明文密码（如 `Admin123!`）

在前端登录页 `/login` 登录，并作为超级管理员使用系统。

---

## 6. 与 Nuxt 应用的连接

应用通过 `server/utils/db.ts` 读取环境变量连接 PostgreSQL：

```ts
// server/utils/db.ts
import { Pool } from 'pg'

const pool = new Pool({
  host: process.env.PG_HOST || 'localhost',
  port: Number(process.env.PG_PORT || 5432),
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  database: process.env.PG_DATABASE,
  ssl: process.env.PG_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
})

export function getDb() {
  return pool
}
```

本地开发时在项目根目录配置 `.env` 或 `.env.local`：

```env
PG_HOST=localhost
PG_PORT=5000
PG_USER=admin
PG_PASSWORD=StrongPassword!123
PG_DATABASE=shadcndb
PG_SSL=false

JWT_SECRET=your_strong_jwt_secret_here
```

确保 `.env` 和 docker-compose 中的配置一致，即可通过 `pnpm dev` 启动 Nuxt，完成与数据库及认证体系的集成。
