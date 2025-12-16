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
  // 最近一次导入运行状态（来自 dataset_import_runs.status），例如 queued/running/success/failed 等
  lastImportStatus?: string | null
}

export type DatasetMemberPermission = 'viewer' | 'editor'

export type DatasetMember = {
  id: string
  userId: string
  username: string
  name: string | null
  permission: DatasetMemberPermission
}

export type DatasetImportRun = {
  id: string
  datasetId: string
  airflowDagId: string | null
  airflowRunId: string | null
  directory: string | null
  rawPrefix: string | null
  status: string | null
  createdAt: string
  updatedAt: string
}

export type PgDatasetTable = {
  schema: string
  name: string
}

export type PgDatasetTableRowsResponse = {
  rows: any[]
  total: number
  limit: number
  offset: number
}

export async function fetchTeamDatasets(teamId: string) {
  const res = await axios.get<Dataset[]>(`/api/teams/${teamId}/datasets`)
  return res.data
}

export async function fetchPgDatasetTables(datasetId: string | number) {
  const res = await axios.get<PgDatasetTable[]>(`/api/datasets/${datasetId}/pg/tables`)
  return res.data
}

export async function fetchPgDatasetTableRows(
  datasetId: string | number,
  tableName: string,
  params?: { limit?: number; offset?: number },
) {
  const res = await axios.get<PgDatasetTableRowsResponse>(
    `/api/datasets/${datasetId}/pg/tables/${tableName}`,
    { params },
  )
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

export async function fetchDatasetImportRuns(datasetId: string | number, limit = 20) {
  const res = await axios.get<DatasetImportRun[]>(
    `/api/datasets/${datasetId}/import-runs`,
    {
      params: { limit },
    }
  )
  return res.data
}

export async function createDatasetImportRun(
  datasetId: string | number,
  payload: {
    airflowDagId?: string
    airflowRunId?: string
    directory?: string | null
    rawPrefix?: string | null
    status?: string | null
    // 额外信息，例如统计日期等
    extra?: unknown
  }
): Promise<DatasetImportRun> {
  const res = await axios.post(`/api/datasets/${datasetId}/import-runs`, payload)
  return res.data as DatasetImportRun
}

export async function updateDatasetImportRun(
  datasetId: string | number,
  runId: string | number,
  payload: {
    airflowDagId?: string
    airflowRunId?: string
    directory?: string | null
    rawPrefix?: string | null
    status?: string | null
    extra?: unknown
  }
): Promise<DatasetImportRun> {
  const res = await axios.patch(`/api/datasets/${datasetId}/import-runs/${runId}`, payload)
  return res.data as DatasetImportRun
}

export type MasterValidationRow = {
  dataset?: string | null
  source_file?: string | null
  issue_type: string
  '统一社会信用代码'?: string | null
  '统一社会信用代码_source'?: string | null
  '统一社会信用代码_base候选'?: string | null
  '单位详细名称'?: string | null
  '单位详细名称_source'?: string | null
  '单位详细名称_base'?: string | null
  '单位详细名称_base候选'?: string | null
}

export async function fetchDatasetMasterValidation(datasetId: string | number) {
  const res = await axios.get<{
    teamId: string
    datasetId: string
    key?: string
    rows: MasterValidationRow[]
  }>(`/api/datasets/${datasetId}/master-validation`)
  return res.data
}
