import axios from 'axios'

export type SystemRole = {
  code: string
  name: string
  description?: string | null
}

export async function fetchSystemRoles() {
  const res = await axios.get<SystemRole[]>('/api/system-roles')
  return res.data
}
