import axios from 'axios'

export type DatasetPermission = 'admin' | 'editor' | 'viewer' | null

export type DatasetType = 'duckdb' | 'pgsql' | 'mysql' | 'oceanbase' | 'mssql' | 'tidb'

export type Dataset = {
  id: string
  teamId: string
  name: string
  description: string | null
  type: DatasetType
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
  prefectFlowId: string | null
  prefectRunId: string | null
  directory: string | null
  rawPrefix: string | null
  status: string | null
  extra: unknown | null
  createdAt: string
  updatedAt: string
}

export type PgDatasetTable = {
  schema: string
  name: string
}

export type PgDatasetTableRowsResponse = {
  rows: unknown[]
  total: number
}

export type ValidationIssue = {
  file: string
  code: string
  name?: string
  row_index?: number
  length?: number | string
  pattern?: string
  note?: string
  existing_name?: string
  new_name?: string
  existing_code?: string
  new_code?: string
}

export type ValidationReport = {
  timestamp: string
  prefix: string
  flow_run_id?: string
  status: 'pending_user_action' | 'resolved'
  issues: {
    truncated_codes: ValidationIssue[]
    one_to_many_code: ValidationIssue[]
    one_to_many_name: ValidationIssue[]
    missing_codes: ValidationIssue[]
  }
  authority_table_size: number
  summary: {
    truncated_codes_count: number
    one_to_many_code_count: number
    one_to_many_name_count: number
    missing_codes_count: number
  }
  instructions?: {
    action_required?: string
    truncated_codes?: string
    one_to_many?: string
  }
}

export type DatasetImportTask = {
  taskId: string
  state: string
  startDate?: string | null
  endDate?: string | null
  duration?: number | null
}

export type MasterValidationRow = {
  code: string
  name: string
  [key: string]: unknown
}

function handleAuthError(error: unknown) {
  if (axios.isAxiosError(error)) {
    if (error.response?.status === 401) {
      window.location.href = '/login'
    }
  }
  throw error
}

export async function fetchDatasets(teamId: string) {
  try {
    const res = await axios.get<Dataset[]>(`/api/teams/${encodeURIComponent(teamId)}/datasets`)
  return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

// Alias for fetchDatasets
export const fetchTeamDatasets = fetchDatasets

export async function fetchDataset(datasetId: string) {
  try {
    const res = await axios.get<Dataset>(`/api/datasets/${encodeURIComponent(datasetId)}`)
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export async function createDataset(teamId: string, payload: { name: string; description?: string; type: DatasetType }) {
  try {
    const res = await axios.post<Dataset>(`/api/teams/${encodeURIComponent(teamId)}/datasets`, payload)
  return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export async function updateDataset(datasetId: string, payload: { name?: string; description?: string }) {
  try {
    const res = await axios.patch<Dataset>(`/api/datasets/${encodeURIComponent(datasetId)}`, payload)
  return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export async function deleteDataset(datasetId: string) {
  try {
    await axios.delete(`/api/datasets/${encodeURIComponent(datasetId)}`)
    return { success: true }
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export async function fetchDatasetMembers(datasetId: string) {
  try {
    const res = await axios.get<DatasetMember[]>(`/api/datasets/${encodeURIComponent(datasetId)}/members`)
  return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export async function upsertDatasetMember(
  datasetId: string,
  payload: { userId: string; permission: DatasetMemberPermission },
) {
  try {
    const res = await axios.post<DatasetMember>(`/api/datasets/${encodeURIComponent(datasetId)}/members`, payload)
  return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export async function deleteDatasetMember(datasetId: string, memberRecordId: string) {
  try {
    await axios.delete(`/api/datasets/${encodeURIComponent(datasetId)}/members/${encodeURIComponent(memberRecordId)}`)
    return { success: true }
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export async function uploadDatasetImportFiles(
  datasetId: string,
  files: File[] | FormData,
  paths?: string[],
  statDate?: string,
  options?: {
    onUploadProgress?: (percent: number) => void
  },
) {
  try {
    let formData: FormData
    if (files instanceof FormData) {
      formData = files
    } else {
      formData = new FormData()
      files.forEach((file) => {
    formData.append('files', file)
      })
      // 后端期望 paths 作为数组，需要每个路径单独 append
      if (paths && paths.length > 0) {
        paths.forEach((path) => {
          formData.append('paths', path)
  })
      }
  if (statDate) {
    formData.append('statDate', statDate)
      }
  }

    const config: {
      headers: { 'Content-Type': string }
      onUploadProgress?: (progressEvent: any) => void
    } = {
    headers: {
      'Content-Type': 'multipart/form-data',
    },
    }

    if (options?.onUploadProgress && typeof options.onUploadProgress === 'function') {
      config.onUploadProgress = (progressEvent: any) => {
        if (progressEvent.total) {
          const percent = Math.round((progressEvent.loaded * 100) / progressEvent.total)
      options.onUploadProgress(percent)
        }
      }
    }

    const res = await axios.post<{ rawPrefix: string; statDate: string; uploadedCount: number }>(
      `/api/datasets/${encodeURIComponent(datasetId)}/import/upload`,
      formData,
      config,
  )
  return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export async function createDatasetImportRun(
  datasetId: string,
  payload: {
    airflowDagId?: string
    airflowRunId?: string
    directory?: string | null
    rawPrefix?: string | null
    status?: string
    extra?: unknown
  },
) {
  try {
    const res = await axios.post<DatasetImportRun>(`/api/datasets/${encodeURIComponent(datasetId)}/import-runs`, {
      prefectFlowId: payload.airflowDagId || null,
      prefectRunId: payload.airflowRunId || null,
      directory: payload.directory || null,
      rawPrefix: payload.rawPrefix || null,
      status: payload.status || null,
      extra: payload.extra || null,
    })
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export async function updateDatasetImportRun(
  datasetId: string,
  importRunId: string,
  payload: {
    airflowDagId?: string
    airflowRunId?: string
    directory?: string | null
    rawPrefix?: string | null
    status?: string
    extra?: unknown
  },
) {
  try {
    const res = await axios.patch<DatasetImportRun>(
      `/api/datasets/${encodeURIComponent(datasetId)}/import-runs/${encodeURIComponent(importRunId)}`,
      {
        prefectFlowId: payload.airflowDagId !== undefined ? payload.airflowDagId || null : undefined,
        prefectRunId: payload.airflowRunId !== undefined ? payload.airflowRunId || null : undefined,
        directory: payload.directory !== undefined ? payload.directory || null : undefined,
        rawPrefix: payload.rawPrefix !== undefined ? payload.rawPrefix || null : undefined,
        status: payload.status !== undefined ? payload.status || null : undefined,
        extra: payload.extra !== undefined ? payload.extra || null : undefined,
      },
    )
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export async function fetchDatasetImportRuns(datasetId: string, limit?: number) {
  try {
    const res = await axios.get<DatasetImportRun[]>(`/api/datasets/${encodeURIComponent(datasetId)}/import-runs`, {
      params: limit ? { limit } : undefined,
    })
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

/**
 * 从 S3 读取数据校准验证报告
 */
export async function fetchValidationReport(prefix: string): Promise<ValidationReport | null> {
  try {
    const res = await axios.get<ValidationReport>(`/api/datasets/validation-report`, {
      params: { prefix },
    })
    return res.data
  } catch (error) {
    if (axios.isAxiosError(error) && error.response?.status === 404) {
      return null // 报告不存在
    }
    handleAuthError(error)
    throw error
  }
}

/**
 * 提交数据校准修复结果
 */
export async function submitValidationResolutions(
  prefix: string,
  resolutions: {
    truncated_codes?: Array<{ code: string; fixed_code: string; file: string; row_index: number }>
    one_to_many_code?: Array<{ code: string; selected_name: string; file: string }>
    one_to_many_name?: Array<{ name: string; selected_code: string; file: string }>
    missing_codes?: Array<{ name: string; code: string; file: string; row_index: number }>
  },
): Promise<{ success: boolean; message: string }> {
  try {
    const res = await axios.post<{ success: boolean; message: string }>(
      `/api/datasets/validation-report/resolve`,
      {
        prefix,
        resolutions,
      },
    )
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

/**
 * 获取 Airflow/Prefect 运行状态
 */
export async function fetchDatasetImportRun(dagId: string, runId: string) {
  try {
    const res = await axios.get<{ dagId: string; runId: string; state: string }>(
      `/api/airflow/dags/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}`,
    )
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

/**
 * 获取 Airflow/Prefect 运行的任务列表
 */
export async function fetchDatasetImportTasks(dagId: string, runId: string): Promise<DatasetImportTask[]> {
  try {
    const res = await axios.get<DatasetImportTask[]>(
      `/api/airflow/dags/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}/tasks`,
    )
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

/**
 * 获取 Airflow/Prefect 任务日志
 */
export async function fetchDatasetTaskLog(dagId: string, runId: string, taskId: string): Promise<string> {
  try {
    const res = await axios.get<string>(
      `/api/airflow/dags/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}/tasks/${encodeURIComponent(taskId)}/logs`,
    )
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

/**
 * 触发数据集导入
 */
export async function triggerDatasetImport(
  datasetId: string,
  payload: {
    airflowDagId?: string
    directory?: string
    rawPrefix?: string
    config?: string
  },
) {
  try {
    const res = await axios.post<{ dagId: string; runId: string; state: string }>(
      `/api/datasets/${encodeURIComponent(datasetId)}/import/trigger`,
      payload,
    )
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

/**
 * 获取数据集主表验证数据
 */
export async function fetchDatasetMasterValidation(datasetId: string): Promise<{
    teamId: string
    datasetId: string
    key?: string
    rows: MasterValidationRow[]
}> {
  try {
    const res = await axios.get<{ teamId: string; datasetId: string; key?: string; rows: MasterValidationRow[] }>(
      `/api/datasets/${encodeURIComponent(datasetId)}/master-validation`,
    )
  return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}
