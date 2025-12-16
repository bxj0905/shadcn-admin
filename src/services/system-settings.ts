import axios from 'axios'

export type SystemSettingsResponse = {
  id: number
  name: string
  tagline: string | null
  logoUrl: string | null
  logoFormat: string | null
  browserIconUrl: string | null
  browserIconFormat: string | null
  extra: Record<string, unknown> | null
  updatedAt: string
}

export type SystemLicenseResponse = {
  id: number
  status: 'DRAFT' | 'ACTIVE' | 'EXPIRED'
  licenseName: string | null
  issuer: string | null
  validFrom: string | null
  validTo: string | null
  notes: string | null
  fileUrl: string | null
  fileFormat: string | null
  updatedAt: string
}

function handleAuthRedirect(error: unknown) {
  if (axios.isAxiosError(error)) {
    const status = error.response?.status
    if (status === 401 || status === 403) {
      window.location.href = '/401'
    }
  }
}

export async function fetchSystemSettings() {
  try {
    const res = await axios.get<SystemSettingsResponse>('/api/system-settings')
    return res.data
  } catch (error) {
    handleAuthRedirect(error)
    throw error
  }
}

export async function updateSystemSettings(payload: { name: string; tagline?: string | null }) {
  try {
    const res = await axios.put<SystemSettingsResponse>('/api/system-settings', payload)
    return res.data
  } catch (error) {
    handleAuthRedirect(error)
    throw error
  }
}

export async function uploadSystemLogo(file: File) {
  const formData = new FormData()
  formData.append('logo', file)

  try {
    const res = await axios.post<SystemSettingsResponse>('/api/system-settings/logo', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    })
    return res.data
  } catch (error) {
    handleAuthRedirect(error)
    throw error
  }
}

export async function deleteSystemLogo() {
  try {
    const res = await axios.delete<SystemSettingsResponse>('/api/system-settings/logo')
    return res.data
  } catch (error) {
    handleAuthRedirect(error)
    throw error
  }
}

export async function uploadBrowserIcon(file: File) {
  const formData = new FormData()
  formData.append('browserIcon', file)

  try {
    const res = await axios.post<SystemSettingsResponse>(
      '/api/system-settings/browser-icon',
      formData,
      {
        headers: { 'Content-Type': 'multipart/form-data' },
      },
    )
    return res.data
  } catch (error) {
    handleAuthRedirect(error)
    throw error
  }
}

export async function deleteBrowserIcon() {
  try {
    const res = await axios.delete<SystemSettingsResponse>('/api/system-settings/browser-icon')
    return res.data
  } catch (error) {
    handleAuthRedirect(error)
    throw error
  }
}

export async function fetchSystemLicense() {
  try {
    const res = await axios.get<SystemLicenseResponse>('/api/system-settings/license')
    return res.data
  } catch (error) {
    handleAuthRedirect(error)
    throw error
  }
}

export async function updateSystemLicense(payload: {
  status?: string
  licenseName?: string | null
  issuer?: string | null
  validFrom?: string | null
  validTo?: string | null
  notes?: string | null
}) {
  try {
    const res = await axios.put<SystemLicenseResponse>('/api/system-settings/license', payload)
    return res.data
  } catch (error) {
    handleAuthRedirect(error)
    throw error
  }
}

export async function uploadLicenseFile(file: File) {
  const formData = new FormData()
  formData.append('licenseFile', file)

  try {
    const res = await axios.post<SystemLicenseResponse>(
      '/api/system-settings/license/file',
      formData,
      {
        headers: { 'Content-Type': 'multipart/form-data' },
      },
    )
    return res.data
  } catch (error) {
    handleAuthRedirect(error)
    throw error
  }
}

export async function deleteLicenseFile() {
  try {
    const res = await axios.delete<SystemLicenseResponse>('/api/system-settings/license/file')
    return res.data
  } catch (error) {
    handleAuthRedirect(error)
    throw error
  }
}
