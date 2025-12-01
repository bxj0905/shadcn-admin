import axios from 'axios'

export type DatasetPermission = 'admin' | 'editor' | 'viewer' | null

export type Dataset = {
  id: string
  teamId: string
  name: string
  description: string | null
  type: 'duckdb' | 'pgsql'
  storagePath: string
  createdBy: string
  createdAt: string
  updatedAt: string
  permission: DatasetPermission
}

export type DatasetMemberPermission = 'viewer' | 'editor'

export type DatasetMember = {
  id: string
  userId: string
  username: string
  name: string | null
  permission: DatasetMemberPermission
}

export async function fetchTeamDatasets(teamId: string) {
  const res = await axios.get<Dataset[]>(`/api/teams/${teamId}/datasets`)
  return res.data
}

export async function createDataset(teamId: string, payload: { name: string; description?: string; type: 'duckdb' | 'pgsql' }) {
  const res = await axios.post(`/api/teams/${teamId}/datasets`, payload)
  return res.data as Dataset
}

export async function fetchDatasetMembers(datasetId: string) {
  const res = await axios.get<DatasetMember[]>(`/api/datasets/${datasetId}/members`)
  return res.data
}

export async function upsertDatasetMember(datasetId: string, payload: { userId: string; permission: DatasetMemberPermission }) {
  const res = await axios.post(`/api/datasets/${datasetId}/members`, payload)
  return res.data
}

export async function deleteDatasetMember(datasetId: string, memberRecordId: string) {
  const res = await axios.delete(`/api/datasets/${datasetId}/members/${memberRecordId}`)
  return res.data
}

export async function triggerDatasetImport(
  datasetId: string | number,
  payload: {
    airflowDagId: string
    directory?: string
    rawPrefix?: string | null
    config?: unknown
  }
): Promise<{ dagId: string; runId: string; state: string }> {
  const res = await axios.post(`/api/datasets/${datasetId}/import`, payload)
  return res.data
}

export async function uploadDatasetImportFiles(
  datasetId: string | number,
  files: File[],
  paths: string[],
  statDate?: string,
): Promise<{ rawPrefix: string; statDate: string; uploadedCount: number }> {
  const formData = new FormData()
  files.forEach((file, index) => {
    formData.append('files', file)
    formData.append('paths', paths[index] || file.name)
  })
  if (statDate) {
    formData.append('statDate', statDate)
  }

  const res = await axios.post(`/api/datasets/${datasetId}/import/upload`, formData, {
    headers: {
      'Content-Type': 'multipart/form-data',
    },
  })
  return res.data
}

export async function fetchDatasetImportRun(dagId: string, runId: string) {
  const res = await axios.get(`/api/airflow/runs/${dagId}/${runId}`)
  return res.data as { dagId: string; runId: string; state: string; startDate?: string; endDate?: string }
}

export type DatasetImportTask = {
  taskId: string
  state: string | null
  startDate?: string | null
  endDate?: string | null
}

export async function fetchDatasetImportTasks(dagId: string, runId: string) {
  const res = await axios.get(`/api/airflow/runs/${dagId}/${runId}/tasks`)
  return res.data as DatasetImportTask[]
}

export async function fetchDatasetTaskLog(dagId: string, runId: string, taskId: string) {
  const res = await axios.get(`/api/airflow/logs/${dagId}/${runId}/${taskId}`, {
    responseType: 'text',
  })
  return res.data as string
}
