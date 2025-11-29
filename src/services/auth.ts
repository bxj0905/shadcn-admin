import axios from 'axios'

export interface LoginResponseUser {
  id: number
  username: string
  email: string
  name: string | null
  avatarUrl: string | null
  roles: string[]
}

export interface LoginResponse {
  accessToken: string
  refreshToken?: string
  expiresIn: number
  user: LoginResponseUser
}

export type AuthProvider = 'local' | 'ldap' | 'auto'

export async function login(
  provider: AuthProvider,
  identifier: string,
  password: string,
) {
  const res = await axios.post<LoginResponse>('/api/auth/login', {
    provider,
    identifier,
    password,
  })
  return res.data
}

// 向后兼容：默认使用本地登录
export async function loginLocal(identifier: string, password: string) {
  return login('local', identifier, password)
}

export async function fetchCurrentUser() {
  const res = await axios.get<LoginResponseUser>('/api/auth/me')
  return res.data
}
