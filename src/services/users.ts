import axios from 'axios'
import type { User } from '@/features/users/data/schema'

export async function fetchUsers() {
  const res = await axios.get<User[]>('/api/users')
  return res.data
}

type BaseUserPayload = {
  firstName: string
  lastName: string
  username: string
  email: string
  phoneNumber?: string
  role: string
  password?: string
}

export async function createUser(payload: BaseUserPayload) {
  const res = await axios.post('/api/users', payload)
  return res.data
}

export async function updateUser(id: string, payload: BaseUserPayload) {
  const res = await axios.patch(`/api/users/${id}`, payload)
  return res.data
}

export async function updateUserStatus(id: string, status: 0 | 1) {
  const res = await axios.patch(`/api/users/${id}/status`, { status })
  return res.data
}
