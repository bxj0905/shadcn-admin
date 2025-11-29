-- ==========================================
-- PostgreSQL RBAC + 用户/团队/审计/系统设置
-- ==========================================
-- 可选：显式指定 schema
SET search_path TO public;
-- 可选：统一时区
SET TIME ZONE 'Asia/Shanghai';

-- 1. 用户与资料
CREATE TABLE IF NOT EXISTS users (
  id              BIGSERIAL PRIMARY KEY,
  username        VARCHAR(50) NOT NULL UNIQUE,
  email           VARCHAR(255) NOT NULL UNIQUE,
  name            VARCHAR(100),
  avatar_url      VARCHAR(512),
  status          SMALLINT NOT NULL DEFAULT 1, -- 1=正常,0=禁用,2=待激活
  last_login_at   TIMESTAMP,
  created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- updated_at 自动更新（PG 里一般用触发器或应用层更新，这里先靠应用层）

CREATE TABLE IF NOT EXISTS user_profiles (
  id          BIGSERIAL PRIMARY KEY,
  user_id     BIGINT NOT NULL UNIQUE,
  bio         VARCHAR(255),
  dob         DATE,
  location    VARCHAR(100),
  urls_json   JSONB,   -- 个人网址列表 JSON 数组
  extra       JSONB,   -- 预留扩展
  created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_user_profiles_user_id
    FOREIGN KEY (user_id) REFERENCES users(id)
      ON UPDATE CASCADE ON DELETE CASCADE
);

-- 2. 认证提供方与账号来源
CREATE TABLE IF NOT EXISTS auth_providers (
  id          BIGSERIAL PRIMARY KEY,
  code        VARCHAR(50) NOT NULL UNIQUE, -- local, ldap, cas, github 等
  name        VARCHAR(100) NOT NULL,
  type        VARCHAR(50) NOT NULL,        -- password, ldap, cas, oauth2, oidc
  enabled     SMALLINT NOT NULL DEFAULT 1,
  config      JSONB,
  created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_auth_accounts (
  id               BIGSERIAL PRIMARY KEY,
  user_id          BIGINT NOT NULL,
  provider_id      BIGINT NOT NULL,
  provider_user_id VARCHAR(191) NOT NULL,  -- Provider 下唯一标识
  username         VARCHAR(191),
  email            VARCHAR(255),
  display_name     VARCHAR(191),
  avatar_url       VARCHAR(512),
  password_hash    VARCHAR(255),           -- 仅 provider=local 时使用
  access_token     VARCHAR(512),
  refresh_token    VARCHAR(512),
  token_expires_at TIMESTAMP,
  status           SMALLINT NOT NULL DEFAULT 1, -- 1=正常,0=禁用
  last_login_at    TIMESTAMP,
  created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uk_user_auth_provider_uid
    UNIQUE (provider_id, provider_user_id),
  CONSTRAINT fk_user_auth_user_id
    FOREIGN KEY (user_id) REFERENCES users(id)
      ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT fk_user_auth_provider_id
    FOREIGN KEY (provider_id) REFERENCES auth_providers(id)
      ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_user_auth_user_id
  ON user_auth_accounts(user_id);

CREATE INDEX IF NOT EXISTS idx_user_auth_provider_id
  ON user_auth_accounts(provider_id);

-- 3. 系统角色与系统权限
CREATE TABLE IF NOT EXISTS system_roles (
  id          BIGSERIAL PRIMARY KEY,
  code        VARCHAR(50) NOT NULL UNIQUE, -- super_admin, admin, user
  name        VARCHAR(100) NOT NULL,
  description VARCHAR(255),
  created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_system_roles (
  id             BIGSERIAL PRIMARY KEY,
  user_id        BIGINT NOT NULL,
  system_role_id BIGINT NOT NULL,
  assigned_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uk_user_system_role
    UNIQUE (user_id, system_role_id),
  CONSTRAINT fk_usr_user_id
    FOREIGN KEY (user_id) REFERENCES users(id)
      ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT fk_usr_system_role_id
    FOREIGN KEY (system_role_id) REFERENCES system_roles(id)
      ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_usr_user_id
  ON user_system_roles(user_id);

CREATE INDEX IF NOT EXISTS idx_usr_system_role_id
  ON user_system_roles(system_role_id);

CREATE TABLE IF NOT EXISTS system_permissions (
  id          BIGSERIAL PRIMARY KEY,
  code        VARCHAR(100) NOT NULL UNIQUE, -- user.view, user.manage 等
  name        VARCHAR(100) NOT NULL,
  description VARCHAR(255),
  category    VARCHAR(100),
  created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS system_role_permissions (
  id                   BIGSERIAL PRIMARY KEY,
  system_role_id       BIGINT NOT NULL,
  system_permission_id BIGINT NOT NULL,
  granted_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uk_system_role_permission
    UNIQUE (system_role_id, system_permission_id),
  CONSTRAINT fk_srp_role_id
    FOREIGN KEY (system_role_id) REFERENCES system_roles(id)
      ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT fk_srp_permission_id
    FOREIGN KEY (system_permission_id) REFERENCES system_permissions(id)
      ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_srp_role_id
  ON system_role_permissions(system_role_id);

CREATE INDEX IF NOT EXISTS idx_srp_permission_id
  ON system_role_permissions(system_permission_id);

-- 4. 团队、团队角色与团队权限
CREATE TABLE IF NOT EXISTS teams (
  id          BIGSERIAL PRIMARY KEY,
  name        VARCHAR(100) NOT NULL,
  slug        VARCHAR(100) UNIQUE,
  owner_id    BIGINT NOT NULL,
  description VARCHAR(255),
  status      SMALLINT NOT NULL DEFAULT 1, -- 1=正常,0=禁用
  created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_teams_owner_id
    FOREIGN KEY (owner_id) REFERENCES users(id)
      ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_teams_owner_id
  ON teams(owner_id);

CREATE TABLE IF NOT EXISTS team_roles (
  id          BIGSERIAL PRIMARY KEY,
  code        VARCHAR(50) NOT NULL UNIQUE, -- owner, maintainer, member
  name        VARCHAR(100) NOT NULL,
  description VARCHAR(255),
  created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS team_permissions (
  id          BIGSERIAL PRIMARY KEY,
  code        VARCHAR(100) NOT NULL UNIQUE, -- team.member.manage 等
  name        VARCHAR(100) NOT NULL,
  description VARCHAR(255),
  category    VARCHAR(100),
  created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS team_members (
  id         BIGSERIAL PRIMARY KEY,
  team_id    BIGINT NOT NULL,
  user_id    BIGINT NOT NULL,
  status     SMALLINT NOT NULL DEFAULT 1, -- 1=正常,0=禁用,2=待邀请
  joined_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  extra      JSONB,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uk_team_members_team_user
    UNIQUE (team_id, user_id),
  CONSTRAINT fk_team_members_team_id
    FOREIGN KEY (team_id) REFERENCES teams(id)
      ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT fk_team_members_user_id
    FOREIGN KEY (user_id) REFERENCES users(id)
      ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_team_members_user_id
  ON team_members(user_id);

CREATE INDEX IF NOT EXISTS idx_team_members_team_id
  ON team_members(team_id);

CREATE TABLE IF NOT EXISTS team_member_roles (
  id             BIGSERIAL PRIMARY KEY,
  team_member_id BIGINT NOT NULL,
  team_role_id   BIGINT NOT NULL,
  assigned_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uk_team_member_role
    UNIQUE (team_member_id, team_role_id),
  CONSTRAINT fk_tmr_team_member_id
    FOREIGN KEY (team_member_id) REFERENCES team_members(id)
      ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT fk_tmr_team_role_id
    FOREIGN KEY (team_role_id) REFERENCES team_roles(id)
      ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tmr_member_id
  ON team_member_roles(team_member_id);

CREATE INDEX IF NOT EXISTS idx_tmr_role_id
  ON team_member_roles(team_role_id);

CREATE TABLE IF NOT EXISTS team_role_permissions (
  id                 BIGSERIAL PRIMARY KEY,
  team_role_id       BIGINT NOT NULL,
  team_permission_id BIGINT NOT NULL,
  granted_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uk_team_role_permission
    UNIQUE (team_role_id, team_permission_id),
  CONSTRAINT fk_trp_role_id
    FOREIGN KEY (team_role_id) REFERENCES team_roles(id)
      ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT fk_trp_permission_id
    FOREIGN KEY (team_permission_id) REFERENCES team_permissions(id)
      ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_trp_role_id
  ON team_role_permissions(team_role_id);

CREATE INDEX IF NOT EXISTS idx_trp_permission_id
  ON team_role_permissions(team_permission_id);

-- 5. 审计日志 & 系统设置
CREATE TABLE IF NOT EXISTS audit_logs (
  id             BIGSERIAL PRIMARY KEY,
  user_id        BIGINT,
  action         VARCHAR(100) NOT NULL, -- 如 auth.login, user.update
  resource_type  VARCHAR(100),
  resource_id    BIGINT,
  ip_address     VARCHAR(45),
  user_agent     VARCHAR(512),
  request_path   VARCHAR(255),
  request_method VARCHAR(10),
  metadata       JSONB,
  created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_audit_logs_user_id
    FOREIGN KEY (user_id) REFERENCES users(id)
      ON UPDATE CASCADE ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_user_id
  ON audit_logs(user_id);

CREATE INDEX IF NOT EXISTS idx_audit_logs_action
  ON audit_logs(action);

CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at
  ON audit_logs(created_at);

CREATE TABLE IF NOT EXISTS system_settings (
  id            BIGSERIAL PRIMARY KEY,
  site_title    VARCHAR(200) NOT NULL,
  site_subtitle VARCHAR(255),
  logo_url      VARCHAR(512),
  logo_format   VARCHAR(10), -- png, svg
  extra         JSONB,
  created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 6. 初始化数据（id 使用自然主键冲突时用 ON CONFLICT）
INSERT INTO auth_providers (code, name, type, enabled)
VALUES
  ('local',    '本地账号',     'password', 1),
  ('ldap',     'LDAP',         'ldap',     1),
  ('cas',      'CAS',          'cas',      1),
  ('github',   'GitHub OAuth', 'oauth2',   1),
  ('google',   'Google OAuth', 'oauth2',   1),
  ('wechat',   '微信登录',      'oauth2',   1),
  ('dingtalk', '钉钉登录',      'oauth2',   1)
ON CONFLICT (code) DO UPDATE
  SET name = EXCLUDED.name,
      type = EXCLUDED.type,
      enabled = EXCLUDED.enabled;

INSERT INTO system_roles (code, name, description)
VALUES
  ('super_admin', '超级管理员', '拥有系统全部权限'),
  ('admin',       '管理员',     '拥有大部分系统管理权限'),
  ('user',        '普通用户',   '普通系统使用者')
ON CONFLICT (code) DO UPDATE
  SET name = EXCLUDED.name,
      description = EXCLUDED.description;

INSERT INTO team_roles (code, name, description)
VALUES
  ('owner',      '拥有者',   '团队创建者/拥有者，拥有该团队的全部权限'),
  ('maintainer', '维护者',   '可以管理团队的大部分资源'),
  ('member',     '成员',     '普通成员，日常使用团队资源')
ON CONFLICT (code) DO UPDATE
  SET name = EXCLUDED.name,
      description = EXCLUDED.description;

INSERT INTO system_permissions (code, name, description, category)
VALUES
  ('system.admin',    '系统管理',     '访问系统管理后台',       'system'),
  ('user.view',       '查看用户',     '查看系统用户列表',       'user'),
  ('user.manage',     '管理用户',     '创建/修改/禁用用户',     'user'),
  ('team.view_all',   '查看所有团队', '查看系统所有团队',       'team'),
  ('team.manage_all', '管理所有团队', '创建/修改/删除团队',     'team')
ON CONFLICT (code) DO UPDATE
  SET name = EXCLUDED.name,
      description = EXCLUDED.description,
      category = EXCLUDED.category;

INSERT INTO team_permissions (code, name, description, category)
VALUES
  ('team.member.view',     '查看团队成员',   '查看本团队成员列表',   'member'),
  ('team.member.manage',   '管理团队成员',   '邀请/移除/变更角色',   'member'),
  ('team.settings.view',   '查看团队设置',   '查看本团队设置',       'settings'),
  ('team.settings.manage', '管理团队设置',   '修改本团队设置',       'settings'),
  ('team.project.view',    '查看团队项目',   '查看项目/看板/资源',   'project'),
  ('team.project.manage',  '管理团队项目',   '创建/修改项目资源',    'project')
ON CONFLICT (code) DO UPDATE
  SET name = EXCLUDED.name,
      description = EXCLUDED.description,
      category = EXCLUDED.category;

INSERT INTO system_settings (id, site_title, site_subtitle, logo_url, logo_format)
VALUES (1, 'Nuxt Portal', 'Next-gen admin portal', NULL, NULL)
ON CONFLICT (id) DO UPDATE
  SET site_title   = EXCLUDED.site_title,
      site_subtitle = EXCLUDED.site_subtitle,
      logo_url     = EXCLUDED.logo_url,
      logo_format  = EXCLUDED.logo_format;

-- =====================================================
-- 7. 将指定用户设置为超级管理员（示例说明，不自动执行）
-- =====================================================
-- 以下示例说明如何在应用部署后，将某个已有用户
-- （例如 username='admin'）设置为超级管理员。
-- 注意：请在 psql 或客户端中手动执行，并根据实际情况替换 ID。

-- 1）确保存在 super_admin 角色：
-- INSERT INTO system_roles (code, name, description)
-- VALUES ('super_admin', '超级管理员', '拥有系统全部权限')
-- ON CONFLICT (code) DO UPDATE
--   SET name = EXCLUDED.name,
--       description = EXCLUDED.description;

-- 2）确保存在 system.admin 权限：
-- INSERT INTO system_permissions (code, name, description, category)
-- VALUES ('system.admin', '系统管理', '访问系统管理后台', 'system')
-- ON CONFLICT (code) DO UPDATE
--   SET name = EXCLUDED.name,
--       description = EXCLUDED.description,
--       category = EXCLUDED.category;

-- 3）给 super_admin 角色授予 system.admin 权限（请将 1 替换为实际 ID）：
-- INSERT INTO system_role_permissions (system_role_id, system_permission_id)
-- VALUES (1, 1)
-- ON CONFLICT (system_role_id, system_permission_id) DO NOTHING;

-- 4）将指定用户绑定为 super_admin（同样用实际的 user_id / role_id 替换）：
-- INSERT INTO user_system_roles (user_id, system_role_id)
-- VALUES (1, 1)
-- ON CONFLICT (user_id, system_role_id) DO NOTHING;

-- 5）可选：验证某个用户是否拥有 system.admin 权限（示例）：
-- SELECT sp.code
-- FROM user_system_roles usr
-- JOIN system_roles sr ON sr.id = usr.system_role_id
-- JOIN system_role_permissions srp ON srp.system_role_id = sr.id
-- JOIN system_permissions sp ON sp.id = srp.system_permission_id
-- WHERE usr.user_id = 1;