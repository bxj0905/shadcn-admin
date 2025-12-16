import axios from 'axios'

export type Team = {
  id: string
  name: string
  slug: string | null
  status: number
  ownerId: number
  ownerUsername: string
  ownerName: string | null
  createdAt: string
  updatedAt: string
}

export type TeamMember = {
  id: string
  userId: string
  username: string
  name: string | null
  teamRoleCode: 'owner' | 'maintainer' | 'member' | null
  teamRoleName: string | null
}

export async function fetchTeams() {
  try {
    const res = await axios.get<Team[]>('/api/teams')
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

export async function createTeam(payload: { name: string; slug?: string; description?: string }) {
  try {
    const res = await axios.post('/api/teams', payload)
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

export async function updateTeam(
  id: string,
  payload: { name?: string; slug?: string; description?: string },
) {
  try {
    const res = await axios.patch(`/api/teams/${id}`, payload)
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

export async function fetchTeamMembers(teamId: string) {
  try {
    const res = await axios.get<TeamMember[]>(`/api/teams/${teamId}/members`)
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

export async function addTeamMember(teamId: string, payload: { userId: string; role: 'owner' | 'maintainer' | 'member' }) {
  try {
    const res = await axios.post(`/api/teams/${teamId}/members`, payload)
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

export async function updateTeamMember(
  teamId: string,
  memberId: string,
  payload: { role?: 'owner' | 'maintainer' | 'member'; status?: number }
) {
  try {
    const res = await axios.patch(`/api/teams/${teamId}/members/${memberId}`, payload)
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

export async function deleteTeamMember(teamId: string, memberId: string) {
  try {
    const res = await axios.delete(`/api/teams/${teamId}/members/${memberId}`)
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

export async function deleteTeam(id: string) {
  try {
    const res = await axios.delete(`/api/teams/${id}`)
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
