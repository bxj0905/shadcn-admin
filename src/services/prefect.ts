import axios from 'axios'

export type PrefectFlow = {
  id: string
  name: string
  tags?: string[]
  labels?: Record<string, unknown> | null
  created?: string
  updated?: string
  /**
   * 前端用于展示层级关系的可选字段：
   * - isSubflow: 是否为某个主 Flow 的子 Flow（在列表中缩进展示）
   * - parentFlowId: 所属主 Flow 的 ID
   *
   * 这两个字段不会由后端 Prefect API 返回，仅在前端 UI 侧按需填充。
   */
  isSubflow?: boolean
  parentFlowId?: string
}

export type PrefectListFlowsResponse = {
  flows: PrefectFlow[]
  total: number
}

function handleAuthError(error: unknown) {
  if (axios.isAxiosError(error)) {
    const status = error.response?.status
    if (status === 401 || status === 403) {
      window.location.href = '/401'
    }
  }
}

export async function fetchPrefectFlows(params?: { limit?: number; offset?: number }) {
  try {
    const res = await axios.get<PrefectListFlowsResponse>('/api/prefect/flows', {
      params,
    })
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export async function createPrefectFlow(payload: {
  name: string
  tags?: string[]
  labels?: Record<string, unknown>
  code?: string
}) {
  try {
    const res = await axios.post<PrefectFlow>('/api/prefect/flows', payload)
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export async function updatePrefectFlow(
  id: string,
  payload: { tags?: string[]; labels?: Record<string, unknown> },
) {
  try {
    const res = await axios.patch<PrefectFlow>(`/api/prefect/flows/${encodeURIComponent(id)}`, payload)
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export async function deletePrefectFlow(id: string) {
  try {
    const res = await axios.delete(`/api/prefect/flows/${encodeURIComponent(id)}`)
    return res.data as { success: boolean }
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export type PrefectFlowRun = {
  id: string
  flow_id: string
  state_type: string
  name: string
  created?: string
}

export type PrefectFlowCode = {
  code: string
}

export async function runPrefectFlow(
  flowId: string,
  parameters?: Record<string, unknown>,
) {
  try {
    const res = await axios.post<PrefectFlowRun>(
      `/api/prefect/flows/${encodeURIComponent(flowId)}/run`,
      parameters && Object.keys(parameters).length > 0 ? { parameters } : undefined,
    )
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export type PrefectFlowTestResult = {
  runId: string
  state_type: string
  state_name?: string
  status: 'COMPLETED' | 'FAILED' | 'CANCELLED' | 'CRASHED' | 'TIMEOUT' | 'UNKNOWN'
  detail?: unknown
}

export type PrefectFlowLatestRun = {
  id: string
  flow_id: string
  state_type?: string
  state_name?: string
  created?: string
  start_time?: string
  end_time?: string
} | null

export async function fetchPrefectFlowCode(flowId: string) {
  try {
    const res = await axios.get<PrefectFlowCode>(
      `/api/prefect/flows/${encodeURIComponent(flowId)}/code`,
    )
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export async function updatePrefectFlowCode(flowId: string, code: string) {
  try {
    const res = await axios.patch<{ success: boolean }>(
      `/api/prefect/flows/${encodeURIComponent(flowId)}/code`,
      { code },
    )
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export async function testPrefectFlow(flowId: string, code: string) {
  try {
    const res = await axios.post<PrefectFlowTestResult>(
      `/api/prefect/flows/${encodeURIComponent(flowId)}/test`,
      { code },
    )
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export async function fetchPrefectFlowLatestRun(flowId: string) {
  try {
    const res = await axios.get<PrefectFlowLatestRun>(
      `/api/prefect/flows/${encodeURIComponent(flowId)}/latest-run`,
    )
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export type PrefectFlowRunStatus = {
  id: string
  flow_id: string
  state_type?: string
  state_name?: string
  created?: string
  start_time?: string
  end_time?: string
}

export async function fetchPrefectFlowRunStatus(runId: string) {
  try {
    const res = await axios.get<PrefectFlowRunStatus>(
      `/api/prefect/flow-runs/${encodeURIComponent(runId)}`,
    )
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export type PrefectFlowRunLog = {
  id: string
  created: string
  level: string
  message: string
  timestamp: string
}

export type PrefectFlowRunLogsResponse = {
  logs: PrefectFlowRunLog[]
  total: number
}

export async function downloadPrefectFlowRunLogs(runId: string) {
  try {
    const res = await axios.get<ArrayBuffer>(
      `/api/prefect/flow-runs/${encodeURIComponent(runId)}/logs/download`,
      { responseType: 'arraybuffer' },
    )
    return res
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export async function fetchPrefectFlowRunLogs(
  runId: string,
  limit?: number,
  offset?: number,
) {
  try {
    const params: Record<string, string> = {}
    if (limit !== undefined) {
      params.limit = limit.toString()
    }
    if (offset !== undefined) {
      params.offset = offset.toString()
    }
    const res = await axios.get<PrefectFlowRunLogsResponse>(
      `/api/prefect/flow-runs/${encodeURIComponent(runId)}/logs`,
      { params },
    )
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

// 将本地编辑的 Flow 文件上传到 RustFS（后端会落到 dataflow bucket）
export async function uploadPrefectFlowFiles(
  files: Array<{
    path: string
    code: string
    flowType?: 'main' | 'feature' | 'subflow'
    name?: string
  }>,
  options?: { basePrefix?: string },
) {
  try {
    const res = await axios.post<{ uploaded: number; keys: string[] }>(
      '/api/prefect/flows/upload-files',
      { files, basePrefix: options?.basePrefix },
    )
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export async function fetchPrefectFlowFiles(
  flowId: string,
): Promise<{
  files: Array<{ path: string; code: string }>;
  prefix: string;
  mainRelativePath?: string;
}> {
  try {
    const res = await axios.get<{
      files: Array<{ path: string; code: string }>;
      prefix: string;
      mainRelativePath?: string;
    }>(
      `/api/prefect/flows/${encodeURIComponent(flowId)}/files`,
    )
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export async function resumePrefectFlowRun(runId: string) {
  try {
    const res = await axios.post<PrefectFlowRun>(
      `/api/prefect/flow-runs/${encodeURIComponent(runId)}/resume`,
    )
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}

export type BatchRegisterFlowConfig = {
  name: string
  entrypoint: string
  filePath: string
  flowType: 'main' | 'feature' | 'subflow'
  parentFlow?: string
  description?: string
  tags?: string[]
  code?: string
}

export type BatchRegisterFlowResult = {
  name: string
  flow: PrefectFlow
  error?: string
}

export async function batchRegisterPrefectFlows(
  flows: BatchRegisterFlowConfig[],
) {
  try {
    const res = await axios.post<BatchRegisterFlowResult[]>(
      '/api/prefect/flows/batch-register',
      { flows },
    )
    return res.data
  } catch (error) {
    handleAuthError(error)
    throw error
  }
}
