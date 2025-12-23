import React, { useState, useEffect } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { ChevronLeft, ChevronRight } from 'lucide-react'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import {
  uploadDatasetImportFiles,
  createDatasetImportRun,
  updateDatasetImportRun,
  fetchDatasetImportRuns,
  type Dataset,
} from '@/services/datasets'
import { fetchPrefectFlows, runPrefectFlow, fetchPrefectFlowRunStatus, type PrefectFlowRunStatus } from '@/services/prefect'
import { DatasetImportStep } from './steps/dataset-import-step'
import { DatasetFlowSelectionStep } from './steps/dataset-flow-selection-step'
import { DatasetFlowProgressStep } from './steps/dataset-flow-progress-step'
import { DatasetStepper } from './steps/dataset-stepper'

type DatasetsEditDialogProps = {
  dataset: Dataset
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function DatasetsEditDialog({ dataset, open, onOpenChange }: DatasetsEditDialogProps) {
  const queryClient = useQueryClient()
  const [step, setStep] = useState<1 | 2 | 3>(1)
  const [duckdbUploading, setDuckdbUploading] = useState(false)
  const [duckdbUploadStatus, setDuckdbUploadStatus] = useState<'idle' | 'uploading' | 'success' | 'error'>('idle')
  const [duckdbUploadMessage, setDuckdbUploadMessage] = useState<string>('')
  const [duckdbUploadPercent, setDuckdbUploadPercent] = useState<number>(0)
  const [duckdbUploadTotalFiles, setDuckdbUploadTotalFiles] = useState<number>(0)
  const [duckdbUploadedCount, setDuckdbUploadedCount] = useState<number>(0)
  const [duckdbCurrentFile, setDuckdbCurrentFile] = useState<string>('')
  const [selectedFlowId, setSelectedFlowId] = useState<string | null>(null)
  const [runFlowMessage, setRunFlowMessage] = useState<string>('')
  const [runFlowError, setRunFlowError] = useState<string>('')
  const [currentImportRunId, setCurrentImportRunId] = useState<string | null>(null)
  const [duckdbRawPrefix, setDuckdbRawPrefix] = useState<string | null>(null)
  const [flowRunId, setFlowRunId] = useState<string | null>(null)
  const [flowRunStatus, setFlowRunStatus] = useState<PrefectFlowRunStatus | null>(null)
  const [hasImportedData, setHasImportedData] = useState(false)

  // 打开对话框时检查是否已导入数据
  useEffect(() => {
    if (!open) return
    let cancelled = false

    void (async () => {
      try {
        // 检查 lastImportStatus
        if (dataset.lastImportStatus === 'success') {
          if (!cancelled) {
            setHasImportedData(true)
            setStep(2) // 直接进入步骤2（选择处理模式）
          }
          return
        }

        // 如果没有 lastImportStatus，检查 import runs
        const runs = await fetchDatasetImportRuns(dataset.id, 1)
        if (cancelled) return

        const latest = runs[0]
        if (latest) {
          const latestState = (latest.status || '').toLowerCase()
          // 如果状态是 success、uploaded，或者有 rawPrefix（表示数据已上传），都认为已导入数据
          if (latestState === 'success' || latestState === 'uploaded' || latest.rawPrefix) {
            setHasImportedData(true)
            setCurrentImportRunId(latest.id)
            if (latest.rawPrefix) {
              setDuckdbRawPrefix(latest.rawPrefix)
            }
            // 如果已成功导入，直接进入步骤2
            if (latestState === 'success') {
              setStep(2)
            } else if (latestState === 'uploaded') {
              // 如果只是上传了但未处理，保持在步骤1但显示已上传状态
              setDuckdbUploadStatus('success')
              setDuckdbUploadMessage('数据已上传，可以继续选择处理模式')
            } else if (latest.rawPrefix) {
              // 如果有 rawPrefix 但状态不是 success 或 uploaded（可能是 failed/queued 等），也显示已上传
              setDuckdbUploadStatus('success')
              setDuckdbUploadMessage('数据已上传，可以继续选择处理模式')
            }
          }
        }
      } catch (err) {
        // eslint-disable-next-line no-console
        console.error('check import status error', err)
      }
    })()

    return () => {
      cancelled = true
    }
  }, [open, dataset.id, dataset.lastImportStatus])

  const handleClose = async (value: boolean) => {
    if (!value) {
      // 重置所有状态
      setStep(1)
      setDuckdbUploading(false)
      setDuckdbUploadStatus('idle')
      setDuckdbUploadMessage('')
      setDuckdbUploadPercent(0)
      setDuckdbUploadTotalFiles(0)
      setDuckdbUploadedCount(0)
      setDuckdbCurrentFile('')
      setSelectedFlowId(null)
      setRunFlowMessage('')
      setRunFlowError('')
      setCurrentImportRunId(null)
      setDuckdbRawPrefix(null)
      setFlowRunId(null)
      setFlowRunStatus(null)
      setHasImportedData(false)
    }
    onOpenChange(value)
  }

  const handleNext = async () => {
    if (step === 1) {
      // 步骤1：如果已导入数据或上传成功，进入步骤2
      if (hasImportedData || duckdbUploadStatus === 'success') {
        setStep(2)
      }
      return
    }

    if (step === 2) {
      // 步骤2：选择处理模式后，触发flow run并进入步骤3
      if (!selectedFlowId) return
      
      try {
        setRunFlowError('')
        setRunFlowMessage('')

        const datasetParams: Record<string, unknown> = {
          dataset_id: dataset.id,
          team_id: dataset.teamId,
          dataset_type: dataset.type,
          storage_path: dataset.storagePath,
        }

        if (dataset.type === 'duckdb') {
          // 优先使用已有的 rawPrefix，如果没有则使用默认值
          const prefix = duckdbRawPrefix || `team-${dataset.teamId}/dataset-${dataset.id}/sourcedata/`
          datasetParams.raw_prefix = prefix
          // 如果没有设置 rawPrefix，更新它
          if (!duckdbRawPrefix && prefix) {
            setDuckdbRawPrefix(prefix)
          }
        }

        // Flow 期望的参数名是 prefix，传入 raw_prefix 值
        const prefix = datasetParams.raw_prefix as string | undefined
        const run = await runPrefectFlow(selectedFlowId, prefix ? { prefix } : {})

        setFlowRunId(run.id)
        setRunFlowMessage(`已触发 Prefect Flow 运行（run id: ${run.id}）`)

        // 更新 import run 记录
        if (currentImportRunId) {
          await updateDatasetImportRun(dataset.id, currentImportRunId, {
            prefectFlowId: selectedFlowId,
            prefectRunId: run.id,
            status: 'queued',
          })
        } else {
          const created = await createDatasetImportRun(dataset.id, {
            prefectFlowId: selectedFlowId,
            prefectRunId: run.id,
            rawPrefix: duckdbRawPrefix || null,
            status: 'queued',
          })
          setCurrentImportRunId(created.id)
        }

        // 进入步骤3显示进度
        setStep(3)
      } catch (err) {
        console.error('run flow error', err)
        setRunFlowError(err instanceof Error ? err.message : '触发 Flow 运行失败')
      }
      return
    }

    // 步骤3：已完成，可以关闭
    handleClose(false)
  }

  // 第 2 步：加载 Prefect flows
  const { data: prefectFlowsData, isLoading: prefectFlowsLoading, isError: prefectFlowsError } = useQuery({
    queryKey: ['prefect', 'flows', 'for-dataset-edit'],
    queryFn: () => fetchPrefectFlows({ limit: 50, offset: 0 }),
    enabled: open && step >= 2,
  })

  // 第 3 步：轮询 flow run 状态
  const { data: flowRunStatusData } = useQuery({
    queryKey: ['prefect', 'flow-run', flowRunId],
    queryFn: () => fetchPrefectFlowRunStatus(flowRunId!),
    enabled: open && step === 3 && !!flowRunId,
    refetchInterval: (data) => {
      // 如果状态是 CANCELLED 或 CRASHED，停止轮询
      if (data?.state_type && ['CANCELLED', 'CRASHED'].includes(data.state_type)) {
        return false
      }
      // 如果状态是 COMPLETED，继续轮询但间隔长一些（10秒），因为可能存在验证报告需要显示
      // 如果状态是 FAILED，继续轮询（间隔5秒），因为可能存在验证报告需要显示
      // 如果状态是暂停状态（PAUSED, SUSPENDED），继续轮询但间隔长一些（5秒）
      // 因为用户可能在处理问题，不需要太频繁的轮询
      if (data?.state_type && ['PAUSED', 'SUSPENDED', 'FAILED'].includes(data.state_type)) {
        return 5000
      }
      // COMPLETED 状态也继续轮询，以便检测验证报告
      if (data?.state_type === 'COMPLETED') {
        return 10000 // 10秒轮询一次，检查是否有验证报告
      }
      // 否则每2秒轮询一次
      return 2000
    },
  })

  useEffect(() => {
    if (flowRunStatusData) {
      setFlowRunStatus(flowRunStatusData)
      // 更新 import run 记录的状态
      if (currentImportRunId && flowRunStatusData.state_type) {
        const statusMap: Record<string, string> = {
          COMPLETED: 'success',
          FAILED: 'failed',
          CRASHED: 'failed',
          CANCELLED: 'cancelled',
          RUNNING: 'running',
          PENDING: 'queued',
          SCHEDULED: 'queued',
          PAUSED: 'paused',
          SUSPENDED: 'paused',
        }
        const mappedStatus = statusMap[flowRunStatusData.state_type] || 'queued'
        updateDatasetImportRun(dataset.id, currentImportRunId, {
          status: mappedStatus,
        }).catch((err) => {
          // eslint-disable-next-line no-console
          console.error('update import run status error', err)
        })
      }
    }
  }, [flowRunStatusData, dataset.id, currentImportRunId])

  const prefectFlows = prefectFlowsData?.flows ?? []

  const handleDuckdbFilesChange = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const files = event.target.files
    if (!files || files.length === 0) {
      return
    }

    try {
      setDuckdbUploading(true)
      setDuckdbUploadStatus('uploading')
      setDuckdbUploadMessage('')
      setDuckdbUploadPercent(0)
      setDuckdbUploadTotalFiles(files.length)
      setDuckdbUploadedCount(0)
      setDuckdbCurrentFile('')

      const fileList = Array.from(files)
      const paths = fileList.map((f) => {
        const anyFile = f as File & { webkitRelativePath?: string }
        return anyFile.webkitRelativePath && anyFile.webkitRelativePath.length > 0
          ? anyFile.webkitRelativePath
          : anyFile.name
      })
      const today = new Date()
      const statDate = `${today.getFullYear()}${String(today.getMonth() + 1).padStart(2, '0')}${String(
        today.getDate(),
      ).padStart(2, '0')}`

      // 按文件逐个上传，并在前端聚合整体进度
      let lastRawPrefix: string | null = null
      for (let i = 0; i < fileList.length; i += 1) {
        const file = fileList[i]
        const relPath = paths[i] || file.name
        setDuckdbCurrentFile(relPath)

        const result = await uploadDatasetImportFiles(dataset.id, [file], [relPath], statDate, {
          onUploadProgress: (percent) => {
            const total = fileList.length
            const base = (i / total) * 100
            const perFile = percent / total
            const overall = Math.min(99, Math.round(base + perFile))
            setDuckdbUploadPercent(overall)
          },
        })

        // 保存最后一个上传结果的 rawPrefix（所有文件应该都在同一个前缀下）
        if (result.rawPrefix) {
          lastRawPrefix = result.rawPrefix
        }

        setDuckdbUploadedCount((prev) => prev + 1)
      }

      // 所有文件上传并由后端同步完成，才算 100%
      setDuckdbUploadPercent(100)
      setDuckdbUploadStatus('success')

      // 使用最后一个上传结果的 rawPrefix，或使用默认值
      const rawPrefix = lastRawPrefix || `team-${dataset.teamId}/dataset-${dataset.id}/sourcedata/`
      setDuckdbRawPrefix(rawPrefix)

      setDuckdbUploadMessage(
        `已完成同步，共上传 ${fileList.length} 个文件（100%）。`,
      )

      // 创建或更新 import run 记录
      try {
        if (currentImportRunId) {
          await updateDatasetImportRun(dataset.id, currentImportRunId, {
            rawPrefix,
            status: 'uploaded',
            extra: { statDate, uploadedCount: fileList.length },
          })
        } else {
          const created = await createDatasetImportRun(dataset.id, {
            rawPrefix,
            status: 'uploaded',
            extra: { statDate, uploadedCount: fileList.length },
          })
          setCurrentImportRunId(created.id)
        }
      } catch (err) {
        console.error('create/update import run error', err)
        setDuckdbUploadMessage(
          `文件上传完成，但状态记录保存失败：${err instanceof Error ? err.message : '未知错误'}`,
        )
      }
    } catch (error) {
      console.error('duckdb upload error', error)
      setDuckdbUploadStatus('error')
      setDuckdbUploadMessage('上传失败，请稍后重试。')
    } finally {
      setDuckdbUploading(false)
      // 允许重复选择同一文件
      const input = event.target
      if (input) {
        input.value = ''
      }
    }
  }

  const renderStepContent = () => {
    if (step === 1) {
        return (
        <DatasetImportStep
          datasetType={dataset.type}
          hasImportedData={hasImportedData}
          uploadStatus={duckdbUploadStatus}
          uploadMessage={duckdbUploadMessage}
          uploadPercent={duckdbUploadPercent}
          uploadTotalFiles={duckdbUploadTotalFiles}
          uploadedCount={duckdbUploadedCount}
          currentFile={duckdbCurrentFile}
          rawPrefix={duckdbRawPrefix}
          datasetCreated={true}
          onFilesChange={handleDuckdbFilesChange}
        />
      )
    }

    if (step === 2) {
      return (
        <DatasetFlowSelectionStep
          flows={prefectFlows}
          isLoading={prefectFlowsLoading}
          isError={prefectFlowsError}
          selectedFlowId={selectedFlowId}
          runFlowMessage={runFlowMessage}
          runFlowError={runFlowError}
          onFlowSelect={setSelectedFlowId}
        />
      )
    }

    // 步骤3：显示 flow run 进度
    const prefix = duckdbRawPrefix || `team-${dataset.teamId}/dataset-${dataset.id}/sourcedata/`
    return (
      <DatasetFlowProgressStep
        flowRunId={flowRunId}
        flowRunStatus={flowRunStatus}
        prefix={prefix}
        selectedFlowId={selectedFlowId}
        onRetry={async () => {
          // 重新获取流程状态
          if (flowRunId) {
            try {
              const status = await fetchPrefectFlowRunStatus(flowRunId)
              setFlowRunStatus(status)
            } catch (err) {
              console.error('Failed to refresh flow status:', err)
            }
          }
        }}
      />
    )
  }

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className='sm:w-[1200px] sm:max-w-[1200px] sm:min-h-[620px]'>
        <div className='flex max-h-[80vh] flex-col space-y-6'>
          <DialogHeader className='space-y-3'>
            <DialogTitle>编辑数据集：{dataset.name}</DialogTitle>
            <DialogDescription>导入数据、选择处理模式并查看处理进度。</DialogDescription>
          </DialogHeader>

          {/* 顶部 Stepper */}
          <DatasetStepper
            steps={[
              { id: 1, title: '导入数据' },
              { id: 2, title: '选择处理模式' },
              { id: 3, title: '处理进度' },
            ]}
            currentStep={step}
          />

          {/* 中间内容：随步骤变化 */}
          <div className='flex-1 overflow-y-auto pt-2 pb-4'>{renderStepContent()}</div>

          {/* 底部导航按钮 */}
          <div className='mt-4 flex items-center justify-between border-t pt-4'>
            <Button
              type='button'
              variant='outline'
              onClick={() => {
                if (step === 1) {
                  handleClose(false)
                } else {
                  setStep((prev) => (prev > 1 ? ((prev - 1) as typeof step) : prev))
                }
              }}
            >
              <ChevronLeft className='mr-1 h-4 w-4' />
              <span>{step === 1 ? '取消' : '上一步'}</span>
            </Button>

            {step < 3 ? (
              <Button
                type='button'
                onClick={async () => {
                  await handleNext()
                }}
                disabled={
                  (step === 1 && !hasImportedData && dataset.type === 'duckdb' && duckdbUploadStatus !== 'success') ||
                  (step === 2 && !selectedFlowId)
                }
                className='bg-foreground text-background hover:bg-foreground/90'
              >
                <span>{step === 2 ? '开始处理' : '继续'}</span>
                <ChevronRight className='ml-1 h-4 w-4' />
              </Button>
            ) : (
              <Button
                type='button'
                onClick={() => {
                  handleClose(false)
                }}
                className='bg-foreground text-background hover:bg-foreground/90'
                disabled={
                  flowRunStatus?.state_type &&
                  !['COMPLETED', 'FAILED', 'CANCELLED', 'CRASHED'].includes(flowRunStatus.state_type)
                }
              >
                <span>完成</span>
                <ChevronRight className='ml-1 h-4 w-4' />
              </Button>
            )}
          </div>
        </div>
      </DialogContent>
    </Dialog>
  )
}

