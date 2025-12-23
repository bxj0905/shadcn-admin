import React, { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ChevronLeft, ChevronRight } from 'lucide-react'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import {
  createDataset,
  fetchDatasetMembers,
  upsertDatasetMember,
  deleteDatasetMember,
  uploadDatasetImportFiles,
  createDatasetImportRun,
  updateDatasetImportRun,
  type Dataset,
  type DatasetType,
} from '@/services/datasets'
import { fetchTeamMembers } from '@/services/teams'
import {
  fetchPrefectFlows,
  runPrefectFlow,
  fetchPrefectFlowRunStatus,
  type PrefectFlowRunStatus,
} from '@/services/prefect'
import { DatasetBasicInfoStep } from './steps/dataset-basic-info-step'
import { DatasetTypeStep } from './steps/dataset-type-step'
import { DatasetPermissionsStep } from './steps/dataset-permissions-step'
import { DatasetImportStep } from './steps/dataset-import-step'
import { DatasetFlowSelectionStep } from './steps/dataset-flow-selection-step'
import { DatasetFlowProgressStep } from './steps/dataset-flow-progress-step'
import { DatasetStepper } from './steps/dataset-stepper'

type DatasetsCreateDialogProps = {
  open: boolean
  onOpenChange: (open: boolean) => void
  teamId: string
  onCreated?: (dataset: Dataset) => void
}

export function DatasetsCreateDialog({ open, onOpenChange, teamId, onCreated }: DatasetsCreateDialogProps) {
  const queryClient = useQueryClient()
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [type, setType] = useState<DatasetType>('duckdb')
  const [step, setStep] = useState<1 | 2 | 3 | 4 | 5 | 6>(1)
  const [datasetCreated, setDatasetCreated] = useState(false)
  const [createdDataset, setCreatedDataset] = useState<Dataset | null>(null)
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

  // 从缓存中读取当前团队已有的数据集，用于名称去重校验
  const existingDatasets =
    (queryClient.getQueryData<Dataset[]>(['team-datasets', teamId]) as Dataset[] | undefined) || []
  const trimmedName = name.trim()
  const nameDuplicated = !!trimmedName && existingDatasets.some((d) => d.name.trim() === trimmedName)

  const { mutateAsync, isPending } = useMutation({
    mutationFn: () =>
      createDataset(teamId, {
        name: trimmedName,
        description: description.trim() || undefined,
        type,
      }),
    onSuccess: async (created: Dataset) => {
      await queryClient.invalidateQueries({ queryKey: ['team-datasets', teamId] })
      onCreated?.(created)
      setDatasetCreated(true)
      setCreatedDataset(created)

      // 数据集创建后立即创建一条初始记录，状态为 'created'
      try {
        const importRun = await createDatasetImportRun(created.id, {
          status: 'created',
          extra: { note: '数据集已创建，等待导入数据' },
        })
        setCurrentImportRunId(importRun.id)
      } catch (err) {
        // 静默处理错误，不影响数据集创建
      }
    },
  })

  const handleClose = async (value: boolean) => {
    if (!value) {
      // 在关闭前，如果有未保存的状态，尝试保存
      if (createdDataset) {
        try {
          // 如果上传了数据但记录还没有更新，尝试保存
          if (createdDataset.type === 'duckdb' && duckdbUploadStatus === 'success' && duckdbRawPrefix) {
            if (currentImportRunId) {
              await updateDatasetImportRun(createdDataset.id, currentImportRunId, {
                rawPrefix: duckdbRawPrefix,
                status: 'uploaded',
              })
            } else {
              const created = await createDatasetImportRun(createdDataset.id, {
                rawPrefix: duckdbRawPrefix,
                status: 'uploaded',
                extra: { note: '数据已导入但未选择处理模型' },
              })
              setCurrentImportRunId(created.id)
            }
          }
        } catch (err) {
          // 静默处理错误
        }
      }

      // 重置所有状态
      setStep(1)
      setName('')
      setDescription('')
      setType('duckdb')
      setDatasetCreated(false)
      setCreatedDataset(null)
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
    }
    onOpenChange(value)
  }

  const canNextFromStep1 = !!trimmedName && !nameDuplicated

  const handleCreateDataset = async () => {
    if (!trimmedName || nameDuplicated || datasetCreated) return
    await mutateAsync()
  }

  const handleNext = async () => {
    if (step === 1) {
      if (!canNextFromStep1) return
      setStep(2)
      return
    }

    if (step === 2) {
      // 在选择完数据库类型后创建数据集，便于后续直接配置权限
      if (!datasetCreated) {
        await handleCreateDataset()
      }
      setStep(3)
      return
    }

    if (step === 3) {
      // 第3步：权限设置完成后，进入第4步导入数据
      setStep(4)
      return
    }

    if (step === 4) {
      setStep(5)
      return
    }

    if (step === 5) {
      // 第5步：选择处理模式后，触发flow run并进入第6步
      if (!selectedFlowId || !createdDataset) return
      
      try {
        setRunFlowError('')
        setRunFlowMessage('')

        const datasetParams: Record<string, unknown> = {
          dataset_id: createdDataset.id,
          team_id: createdDataset.teamId,
          dataset_type: createdDataset.type,
          storage_path: createdDataset.storagePath,
        }

        if (createdDataset.type === 'duckdb') {
          datasetParams.raw_prefix = duckdbRawPrefix || `team-${createdDataset.teamId}/dataset-${createdDataset.id}/sourcedata/`
        }

        // Flow 期望的参数名是 prefix，传入 raw_prefix 值
        const prefix = datasetParams.raw_prefix as string | undefined
        const run = await runPrefectFlow(selectedFlowId, prefix ? { prefix } : {})

        setFlowRunId(run.id)
        setRunFlowMessage(`已触发 Prefect Flow 运行（run id: ${run.id}）`)

        // 更新 import run 记录
        if (currentImportRunId) {
          await updateDatasetImportRun(createdDataset.id, currentImportRunId, {
            prefectFlowId: selectedFlowId,
            prefectRunId: run.id,
            status: 'queued',
          })
        } else {
          const created = await createDatasetImportRun(createdDataset.id, {
            prefectFlowId: selectedFlowId,
            prefectRunId: run.id,
            rawPrefix: duckdbRawPrefix || null,
            status: 'queued',
          })
          setCurrentImportRunId(created.id)
        }

        // 进入第6步显示进度
        setStep(6)
      } catch (err) {
        setRunFlowError(err instanceof Error ? err.message : '触发 Flow 运行失败')
      }
      return
    }

    // 第6步：已完成，可以关闭
    handleClose(false)
  }

  // 第 3 步权限设置所需：加载团队成员和当前数据集成员
  const { data: teamMembers = [] } = useQuery({
    queryKey: ['team-members', teamId],
    queryFn: () => fetchTeamMembers(teamId),
    enabled: open,
  })

  const { data: datasetMembers = [] } = useQuery({
    queryKey: ['dataset-members', createdDataset?.id],
    queryFn: () => fetchDatasetMembers(createdDataset!.id),
    enabled: open && !!createdDataset && step >= 3,
  })

  // 第 5 步：加载 Prefect flows
  const { data: prefectFlowsData, isLoading: prefectFlowsLoading, isError: prefectFlowsError } = useQuery({
    queryKey: ['prefect', 'flows', 'for-dataset-onboarding'],
    queryFn: () => fetchPrefectFlows({ limit: 50, offset: 0 }),
    enabled: open && step >= 5,
  })

  // 第 6 步：轮询 flow run 状态
  const { data: flowRunStatusData } = useQuery({
    queryKey: ['prefect', 'flow-run', flowRunId],
    queryFn: () => fetchPrefectFlowRunStatus(flowRunId!),
    enabled: open && step === 6 && !!flowRunId,
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

  React.useEffect(() => {
    if (flowRunStatusData) {
      setFlowRunStatus(flowRunStatusData)
      // 更新 import run 记录的状态
      if (createdDataset && currentImportRunId && flowRunStatusData.state_type) {
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
        updateDatasetImportRun(createdDataset.id, currentImportRunId, {
          status: mappedStatus,
        }).catch(() => {
          // 静默处理错误
        })
      }
    }
  }, [flowRunStatusData, createdDataset, currentImportRunId])

  const prefectFlows = prefectFlowsData?.flows ?? []

  const { mutateAsync: setMemberPermission, isPending: setMemberPending } = useMutation({
    mutationFn: async ({ userId, permission }: { userId: string; permission: 'viewer' | 'editor' | null }) => {
      if (!createdDataset) return
      if (!permission) {
        const record = membersByUserId.get(userId)
        if (record) {
          await deleteDatasetMember(createdDataset.id, record.id)
        }
        return
      }
      await upsertDatasetMember(createdDataset.id, { userId, permission })
    },
    onSuccess: async () => {
      if (!createdDataset) return
      await queryClient.invalidateQueries({ queryKey: ['dataset-members', createdDataset.id] })
    },
  })

  const handleChangePermission = async (userId: string, value: string) => {
    const v = value === 'none' ? null : (value as 'viewer' | 'editor')
    await setMemberPermission({ userId, permission: v })
  }

  const handleDuckdbFilesChange = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const files = event.target.files
    if (!files || files.length === 0 || !createdDataset) {
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

        const result = await uploadDatasetImportFiles(createdDataset.id, [file], [relPath], statDate, {
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
      const rawPrefix = lastRawPrefix || `team-${createdDataset.teamId}/dataset-${createdDataset.id}/sourcedata/`
      setDuckdbRawPrefix(rawPrefix)

      setDuckdbUploadMessage(
        `已完成同步，共上传 ${fileList.length} 个文件（100%）。`,
      )

      // 创建或更新 import run 记录（第四步：上传完成）
      // 状态设置为 'uploaded' 表示数据已导入但还未处理
      try {
        if (currentImportRunId) {
          await updateDatasetImportRun(createdDataset.id, currentImportRunId, {
            rawPrefix,
            status: 'uploaded',
            extra: { statDate, uploadedCount: fileList.length },
          })
        } else {
          const created = await createDatasetImportRun(createdDataset.id, {
            rawPrefix,
            status: 'uploaded',
            extra: { statDate, uploadedCount: fileList.length },
          })
          setCurrentImportRunId(created.id)
        }
      } catch (err) {
        // 显示错误提示给用户
        setDuckdbUploadMessage(
          `文件上传完成，但状态记录保存失败：${err instanceof Error ? err.message : '未知错误'}`,
        )
      }
    } catch (error) {
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
        <DatasetBasicInfoStep
          name={name}
          description={description}
          nameDuplicated={nameDuplicated}
          onNameChange={setName}
          onDescriptionChange={setDescription}
                />
      )
    }

    if (step === 2) {
      return <DatasetTypeStep type={type} onTypeChange={setType} />
    }

    if (step === 3) {
      return (
        <DatasetPermissionsStep
          dataset={createdDataset}
          teamMembers={teamMembers}
          datasetMembers={datasetMembers}
          setMemberPending={setMemberPending}
          onChangePermission={handleChangePermission}
        />
      )
    }

    if (step === 4) {
      return (
        <DatasetImportStep
          datasetType={type}
          uploadStatus={duckdbUploadStatus}
          uploadMessage={duckdbUploadMessage}
          uploadPercent={duckdbUploadPercent}
          uploadTotalFiles={duckdbUploadTotalFiles}
          uploadedCount={duckdbUploadedCount}
          currentFile={duckdbCurrentFile}
          rawPrefix={duckdbRawPrefix}
          datasetCreated={datasetCreated}
          onFilesChange={handleDuckdbFilesChange}
        />
      )
    }

    if (step === 5) {
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

    // 第 6 步：显示 flow run 进度
    const prefix = duckdbRawPrefix || (createdDataset ? `team-${createdDataset.teamId}/dataset-${createdDataset.id}/sourcedata/` : null)
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
              // eslint-disable-next-line no-console
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
            <DialogTitle>新建数据集</DialogTitle>
            <DialogDescription>通过 6 步向导配置基础信息、权限、数据库类型、导入数据、选择处理模式并查看处理进度。</DialogDescription>
          </DialogHeader>

          {/* 顶部 Stepper */}
          <DatasetStepper
            steps={[
              { id: 1, title: '基础信息' },
              { id: 2, title: '数据库类型' },
              { id: 3, title: '权限设置' },
              { id: 4, title: '导入数据' },
              { id: 5, title: '处理模式' },
              { id: 6, title: '处理进度' },
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
              disabled={isPending}
            >
              <ChevronLeft className='mr-1 h-4 w-4' />
              <span>{step === 1 ? '取消' : '上一步'}</span>
            </Button>

            {step < 6 ? (
              <Button
                type='button'
                onClick={async () => {
                  await handleNext()
                }}
                disabled={
                  (step === 1 && !canNextFromStep1) ||
                  (step === 5 && !selectedFlowId) ||
                  isPending
                }
                className='bg-foreground text-background hover:bg-foreground/90'
              >
                <span>{step === 5 ? '开始处理' : '继续'}</span>
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
