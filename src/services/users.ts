import axios from 'axios'
import type { User } from '@/features/users/data/schema'

export async function fetchUsers() {
  try {
    const res = await axios.get<User[]>('/api/users')
    return res.data
  } catch (error) {
    if (axios.isAxiosError(error)) {
      const status = error.response?.status
      if (status === 401 || status === 403) {
        window.location.href = '/401'
      }
    }
    throw error
  }
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
  try {
    const res = await axios.post('/api/users', payload)
    return res.data
  } catch (error) {
    if (axios.isAxiosError(error)) {
      const status = error.response?.status
      if (status === 401 || status === 403) {
        window.location.href = '/401'
      }
    }
    throw error
  }
}

export async function updateUser(id: string, payload: BaseUserPayload) {
  try {
    const res = await axios.patch(`/api/users/${id}`, payload)
    return res.data
  } catch (error) {
    if (axios.isAxiosError(error)) {
      const status = error.response?.status
      if (status === 401 || status === 403) {
        window.location.href = '/401'
      }
    }
    throw error
  }
}

export async function updateUserStatus(id: string, status: 0 | 1) {
  try {
    const res = await axios.patch(`/api/users/${id}/status`, { status })
    return res.data
  } catch (error) {
    if (axios.isAxiosError(error)) {
      const statusCode = error.response?.status
      if (statusCode === 401 || statusCode === 403) {
        window.location.href = '/401'
      }
    }
    throw error
  }
}

export async function deleteUser(id: string) {
  try {
    const res = await axios.delete(`/api/users/${id}`)
    return res.data
  } catch (error) {
    if (axios.isAxiosError(error)) {
      const status = error.response?.status
      if (status === 401 || status === 403) {
        window.location.href = '/401'
      }
    }
    throw error
  }
}
