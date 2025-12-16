import { useEffect, useRef, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'
import { FolderOpen, Leaf, Users, ShieldAlert, GraduationCap, Database, CheckCircle2 } from 'lucide-react'
import {
  fetchDatasetImportRun,
  fetchDatasetImportTasks,
  fetchDatasetTaskLog,
  fetchDatasetImportRuns,
  createDatasetImportRun,
  updateDatasetImportRun,
  triggerDatasetImport,
  uploadDatasetImportFiles,
  fetchDatasetMasterValidation,
  type MasterValidationRow,
  type Dataset,
  type DatasetImportTask,
  type DatasetImportRun,
} from '@/services/datasets'

type DatasetsImportDialogProps = {
  dataset: Dataset
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function DatasetsImportDialog({ dataset, open, onOpenChange }: DatasetsImportDialogProps) {
  const queryClient = useQueryClient()
  const [step, setStep] = useState<1 | 2 | 3>(1)
  const [airflowDagId, setAirflowDagId] = useState('wu_jing_pu_etl')
  const [config, setConfig] = useState('')
  const [directory, setDirectory] = useState('')
  const [rawPrefix, setRawPrefix] = useState<string | null>(null)
  const [uploadStatus, setUploadStatus] = useState<
    'idle' | 'uploading' | 'success' | 'error'
  >('idle')
  const [uploadTotal, setUploadTotal] = useState<number>(0)
  const [uploadError, setUploadError] = useState<string | null>(null)
  const [uploadedFiles, setUploadedFiles] = useState<string[]>([])
  const [submitting, setSubmitting] = useState(false)
  const [importRun, setImportRun] = useState<DatasetImportRun | null>(null)
  const [runInfo, setRunInfo] = useState<{
    dagId: string
    runId: string
    state: string
  } | null>(null)
  const [tasks, setTasks] = useState<DatasetImportTask[]>([])
  const tasksContainerRef = useRef<HTMLDivElement | null>(null)
  const [logDialogOpen, setLogDialogOpen] = useState(false)
  const [logDialogTaskId, setLogDialogTaskId] = useState<string | null>(null)
  const [logDialogContent, setLogDialogContent] = useState<string>('')
  const [logDialogLoading, setLogDialogLoading] = useState(false)
  const [validationDialogOpen, setValidationDialogOpen] = useState(false)

  const {
    data: masterValidation,
    isLoading: masterValidationLoading,
    isError: masterValidationError,
  } = useQuery<{ teamId: string; datasetId: string; key?: string; rows: MasterValidationRow[] }>(
    {
      queryKey: ['dataset-master-validation', dataset.id],
      queryFn: () => fetchDatasetMasterValidation(dataset.id),
      enabled: validationDialogOpen,
      staleTime: 60_000,
    },
  )

  const TASK_LABELS: Record<string, string> = {
    get_runtime_config: '读取运行配置',
    collect_and_upload_raw_files: '收集并上传原始文件',
    convert_to_parquet: '转换为 Parquet',
    clean_data__clean_schema: '结构清洗',
    clean_data__clean_values: '值清洗',
    clean_data__deduplicate: '去重',
    clean_data__validate: '质量校验',
    // 一些 Airflow UI 或 API 可能返回 group.task 形式的 task_id，这里做额外映射
    'clean_data.clean_schema': '结构清洗',
    'clean_data.clean_values': '值清洗',
    'clean_data.deduplicate': '去重',
    'clean_data.validate': '质量校验',
    mask_sensitive_fields: '脱敏敏感字段',
    build_aggregated_tables: '生成汇总表',
  }

  const onClose = (value: boolean) => {
    if (!value) {
      // 重置为第一步，避免下次打开直接停在配置页
      setStep(1)
      setRunInfo(null)
      setTasks([])
      setDirectory('')
      setRawPrefix(null)
      setUploadStatus('idle')
      setUploadTotal(0)
      setUploadError(null)
      setImportRun(null)
      setUploadedFiles([])
    }
    onOpenChange(value)
  }

  // 打开对话框时加载最近一次导入记录；如果是成功态，则直接跳到第三步用于查看日志
  useEffect(() => {
    if (!open) return
    let cancelled = false

    void (async () => {
      try {
        const runs = await fetchDatasetImportRuns(dataset.id, 1)
        if (cancelled) return
        const latest = runs[0]
        if (latest) {
          setImportRun(latest)
          if (latest.airflowDagId) {
            setAirflowDagId(latest.airflowDagId)
          }

          const latestState = (latest.status || '').toLowerCase()

          // 如果最近一次导入已成功，且记录了 Airflow 运行信息，则直接展示第 3 步，方便查看当时日志
          if (
            latestState === 'success' &&
            latest.airflowDagId &&
            latest.airflowRunId
          ) {
            setStep(3)
            setRunInfo({
              dagId: latest.airflowDagId,
              runId: latest.airflowRunId,
              state: latest.status || 'success',
            })
          } else if (latestState === 'uploaded') {
            // 最近一次导入停留在第二步（已完成上传但尚未触发处理），恢复第二步的状态
            setStep(2)
            setDirectory(latest.directory || '')
            setRawPrefix(latest.rawPrefix || null)
            setUploadStatus('success')
          }
        } else {
          setImportRun(null)
        }
      } catch (err) {
        // eslint-disable-next-line no-console
        console.error('fetchDatasetImportRuns error', err)
      }
    })()

    return () => {
      cancelled = true
    }
  }, [open, dataset.id])

  const handleStartProcessing = async () => {
    if (submitting || !airflowDagId) return

    // wu_jing_pu_etl 模式下必须先完成上传，拿到 rawPrefix
    if (airflowDagId === 'wu_jing_pu_etl' && !rawPrefix) {
      return
    }

    try {
      setSubmitting(true)

      const data = await triggerDatasetImport(dataset.id, {
        airflowDagId,
        directory,
        rawPrefix: rawPrefix || undefined,
        config: config || undefined,
      })
      setRunInfo({
        dagId: data.dagId,
        runId: data.runId,
        state: data.state || 'queued',
      })
      // 记录或更新导入运行记录
      try {
        if (!importRun) {
          const created = await createDatasetImportRun(dataset.id, {
            airflowDagId,
            airflowRunId: data.runId,
            directory: directory || null,
            rawPrefix: rawPrefix || null,
            status: data.state || 'queued',
          })
          setImportRun(created)
        } else {
          const updated = await updateDatasetImportRun(dataset.id, importRun.id, {
            airflowDagId,
            airflowRunId: data.runId,
            directory: directory || null,
            rawPrefix: rawPrefix || null,
            status: data.state || 'queued',
          })
          setImportRun(updated)
        }
      } catch (err) {
        // eslint-disable-next-line no-console
        console.error('updateDatasetImportRun on trigger error', err)
      }
      setStep(3)
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('triggerDatasetImport error', err)
    } finally {
      setSubmitting(false)
    }
  }

  const canGoNextFromStep1 = !!airflowDagId
  const canGoNextFromStep2 = airflowDagId === 'wu_jing_pu_etl' ? !!directory : true

  // 第三步：轮询 Airflow 运行状态
  useEffect(() => {
    if (!runInfo || step !== 3) return

    let timer: number | undefined

    const poll = async () => {
      try {
        const [run, taskList] = await Promise.all([
          fetchDatasetImportRun(runInfo.dagId, runInfo.runId),
          fetchDatasetImportTasks(runInfo.dagId, runInfo.runId),
        ])
        setRunInfo((prev) => (prev ? { ...prev, state: run.state } : prev))
        setTasks(taskList)
        if (run.state === 'success' || run.state === 'failed') {
          // 导入完成时，同步更新导入记录状态，并刷新数据集列表 lastImportStatus
          try {
            if (importRun) {
              const updated = await updateDatasetImportRun(dataset.id, importRun.id, {
                airflowDagId: run.dagId,
                airflowRunId: run.runId,
                status: run.state,
              })
              setImportRun(updated)
            }
          } catch (err) {
            // eslint-disable-next-line no-console
            console.error('updateDatasetImportRun on finish error', err)
          }

          try {
            await queryClient.invalidateQueries({ queryKey: ['team-datasets', dataset.teamId] })
          } catch (err) {
            // eslint-disable-next-line no-console
            console.error('invalidate team-datasets query error', err)
          }

          return
        }
        timer = window.setTimeout(poll, 5000)
      } catch (err) {
        // eslint-disable-next-line no-console
        console.error('fetchDatasetImportRun error', err)
      }
    }

    poll()
    return () => {
      if (timer) window.clearTimeout(timer)
    }
  }, [runInfo, step])

  // 任务步骤列表自动跟随当前任务（只在第 3 步且运行中时生效）
  useEffect(() => {
    if (step !== 3) return
    // 运行已结束时不再自动滚动，允许完全自由滑动
    if (runInfo && (runInfo.state === 'success' || runInfo.state === 'failed')) return
    const el = tasksContainerRef.current
    if (!el) return

    // 选择第一个“未完成”的任务作为当前任务；如果都完成，则使用最后一个任务
    let targetIndex = -1
    for (let i = 0; i < tasks.length; i += 1) {
      const s = (tasks[i].state || '').toLowerCase()
      if (s !== 'success' && s !== 'skipped') {
        targetIndex = i
        break
      }
    }
    if (targetIndex === -1 && tasks.length > 0) {
      targetIndex = tasks.length - 1
    }

    if (targetIndex >= 0) {
      const rows = el.querySelectorAll<HTMLDivElement>('[data-task-row="1"]')
      const target = rows[targetIndex]
      if (target) {
        // 使用 scrollIntoView 确保当前任务始终出现在容器可视区域中间
        target.scrollIntoView({ block: 'center' })
        return
      }
    }
  }, [tasks, step, runInfo])

  const handleViewTaskLog = async (taskId: string) => {
    if (!runInfo) return
    try {
      setLogDialogOpen(true)
      setLogDialogTaskId(taskId)
      setLogDialogLoading(true)
      setLogDialogContent('')
      const text = await fetchDatasetTaskLog(runInfo.dagId, runInfo.runId, taskId)
      setLogDialogContent(text || '')
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('fetchDatasetTaskLog error', err)
      setLogDialogContent('获取日志失败，请稍后重试。')
    } finally {
      setLogDialogLoading(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className='w-[90vw] max-w-[1400px] min-h-[60vh] max-h-[80vh] overflow-hidden'>
        <div className='flex h-full flex-col gap-8'>
          {/* 顶部：标题 + Stepper，始终占满整个 Dialog 宽度 */}
          <div className='pt-1 px-8'>
            <DialogHeader className='space-y-3'>
              <DialogTitle>导入数据</DialogTitle>
              <DialogDescription>
                为数据集「{dataset.name}」发起一次数据导入任务。后端将对接 Apache Airflow 来执行具体的导入流程。
              </DialogDescription>
            </DialogHeader>

            {/* Stepper 指示器：图标 + 标题 + 状态 */}
            <div className='mt-4 flex items-stretch justify-between gap-4 border-b pb-4 text-sm'>
              {/* Step 1 */}
              <div className='flex flex-1 items-center gap-3'>
                <div
                  className={`flex h-9 w-9 items-center justify-center rounded-full text-xs font-medium shadow-sm ${
                    step >= 1 ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground'
                  }`}
                >
                  <Database className='h-4 w-4' />
                </div>
                <div className='flex flex-col gap-1'>
                  <span className='text-[11px] font-medium tracking-wide text-muted-foreground'>STEP 1</span>
                  <span className='text-sm font-medium'>选择数据处理模式</span>
                  <span
                    className={`inline-flex w-fit rounded-full px-2 py-0.5 text-[11px] font-medium ${
                      step > 1
                        ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-200'
                        : step === 1
                          ? 'bg-primary/10 text-primary'
                          : 'bg-muted text-muted-foreground'
                    }`}
                  >
                    {step > 1 ? '已完成' : step === 1 ? '进行中' : '等待中'}
                  </span>
                </div>
              </div>

              {/* 连接线 */}
              <div className='hidden flex-[0_0_auto] items-center md:flex'>
                <div className='h-px w-10 bg-border' />
              </div>

              {/* Step 2 */}
              <div className='flex flex-1 items-center gap-3'>
                <div
                  className={`flex h-9 w-9 items-center justify-center rounded-full text-xs font-medium shadow-sm ${
                    step >= 2 ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground'
                  }`}
                >
                  <FolderOpen className='h-4 w-4' />
                </div>
                <div className='flex flex-col gap-1'>
                  <span className='text-[11px] font-medium tracking-wide text-muted-foreground'>STEP 2</span>
                  <span className='text-sm font-medium'>选择数据源目录</span>
                  {(() => {
                    const completed = step > 2 || (step === 2 && uploadStatus === 'success')
                    const active = step === 2 && uploadStatus !== 'success'
                    return (
                      <span
                        className={`inline-flex w-fit rounded-full px-2 py-0.5 text-[11px] font-medium ${
                          completed
                            ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-200'
                            : active
                              ? 'bg-primary/10 text-primary'
                              : 'bg-muted text-muted-foreground'
                        }`}
                      >
                        {completed ? '已完成' : active ? '进行中' : '等待中'}
                      </span>
                    )
                  })()}
                </div>
              </div>

              {/* Step 3 */}
              <div className='flex flex-1 items-center gap-3'>
                <div
                  className={`flex h-9 w-9 items-center justify-center rounded-full text-xs font-medium shadow-sm ${
                    runInfo?.state === 'success'
                      ? 'bg-emerald-500 text-emerald-50'
                      : runInfo?.state === 'failed'
                        ? 'bg-red-500 text-red-50'
                        : step === 3
                          ? 'bg-primary text-primary-foreground'
                          : 'bg-muted text-muted-foreground'
                  }`}
                >
                  <CheckCircle2 className='h-4 w-4' />
                </div>
                <div className='flex flex-col gap-1'>
                  <span className='text-[11px] font-medium tracking-wide text-muted-foreground'>STEP 3</span>
                  <span className='text-sm font-medium'>数据处理</span>
                  <span
                    className={`inline-flex w-fit rounded-full px-2 py-0.5 text-[11px] font-medium ${
                      runInfo?.state === 'success'
                        ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-200'
                        : runInfo?.state === 'failed'
                          ? 'bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-200'
                          : step === 3
                            ? 'bg-primary/10 text-primary'
                            : 'bg-muted text-muted-foreground'
                    }`}
                  >
                    {runInfo?.state === 'success'
                      ? '已完成'
                      : runInfo?.state === 'failed'
                        ? '失败'
                        : step === 3
                          ? '进行中'
                          : '等待中'}
                  </span>
                </div>
              </div>
            </div>
          </div>

          {/* 中间内容：随步骤变化，可滚动，内部内容限定宽度 */}
          <div className='flex-1 overflow-y-auto px-8 pt-6'>
            <div className='mx-auto max-w-[1200px]'>
            {step === 1 ? (
              <div className='space-y-3'>
                <div className='space-y-1'>
                  <Label>选择数据处理模式</Label>
                  <p className='text-muted-foreground text-xs'>根据不同业务场景选择对应的处理流程，后续会触发相应的 Airflow DAG。</p>
                </div>

                <div className='grid gap-3 sm:grid-cols-1 md:grid-cols-3'>
                  {/* 各种处理模式卡片 */}
                  {/* 五经普 */}
                  <button
                    type='button'
                    className={`flex min-h-[120px] flex-col items-start gap-2 rounded-lg border p-4 text-left text-sm transition-colors ${
                      airflowDagId === 'wu_jing_pu_etl'
                        ? 'border-primary bg-primary/5'
                        : 'border-border hover:bg-muted/40'
                    }`}
                    onClick={() => setAirflowDagId('wu_jing_pu_etl')}
                  >
                    <div className='flex items-center gap-2'>
                      <Leaf className='h-4 w-4 text-primary' />
                      <div className='font-medium'>五经普数据处理</div>
                    </div>
                    <p className='text-muted-foreground text-xs'>
                      使用 wu_jing_pu_etl DAG，对五经普相关原始数据进行清洗、转换并导入当前数据集。
                    </p>
                  </button>

                  {/* 农业普查 */}
                  <button
                    type='button'
                    className={`flex min-h-[120px] flex-col items-start gap-2 rounded-lg border p-4 text-left text-sm transition-colors ${
                      airflowDagId === 'si_nong_pu_placeholder'
                        ? 'border-primary bg-primary/5'
                        : 'border-border hover:bg-muted/40'
                    }`}
                    onClick={() => setAirflowDagId('si_nong_pu_placeholder')}
                  >
                    <div className='flex items-center gap-2'>
                      <Users className='h-4 w-4 text-primary' />
                      <div className='font-medium'>四农普数据处理</div>
                      <span className='rounded-full bg-emerald-100 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-200'>
                        Coming soon
                      </span>
                    </div>
                    <p className='text-muted-foreground text-xs'>
                      四农普数据处理流程的模式，后续会接入对应的 Airflow DAG，目前仅作为占位展示。
                    </p>
                  </button>

                  {/* GEP 核算 */}
                  <button
                    type='button'
                    className={`flex min-h-[120px] flex-col items-start gap-2 rounded-lg border p-4 text-left text-sm transition-colors ${
                      airflowDagId === 'gep_accounting_placeholder'
                        ? 'border-primary bg-primary/5'
                        : 'border-border hover:bg-muted/40'
                    }`}
                    onClick={() => setAirflowDagId('gep_accounting_placeholder')}
                  >
                    <div className='flex items-center gap-2'>
                      <Leaf className='h-4 w-4 text-emerald-500' />
                      <div className='font-medium'>GEP 核算</div>
                      <span className='rounded-full bg-emerald-100 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-200'>
                        Coming soon
                      </span>
                    </div>
                    <p className='text-muted-foreground text-xs'>
                      生态系统产品（GEP）核算处理流程的模式，后续会接入对应的 Airflow DAG。
                    </p>
                  </button>

                  {/* 人口普查 */}
                  <button
                    type='button'
                    className={`flex min-h-[120px] flex-col items-start gap-2 rounded-lg border p-4 text-left text-sm transition-colors ${
                      airflowDagId === 'population_census_placeholder'
                        ? 'border-primary bg-primary/5'
                        : 'border-border hover:bg-muted/40'
                    }`}
                    onClick={() => setAirflowDagId('population_census_placeholder')}
                  >
                    <div className='flex items-center gap-2'>
                      <Users className='h-4 w-4 text-sky-500' />
                      <div className='font-medium'>人口普查</div>
                      <span className='rounded-full bg-emerald-100 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-200'>
                        Coming soon
                      </span>
                    </div>
                    <p className='text-muted-foreground text-xs'>
                      人口普查数据处理流程的模式，后续会接入对应的 Airflow DAG。
                    </p>
                  </button>

                  {/* 应急管理 */}
                  <button
                    type='button'
                    className={`flex min-h-[120px] flex-col items-start gap-2 rounded-lg border p-4 text-left text-sm transition-colors ${
                      airflowDagId === 'emergency_management_placeholder'
                        ? 'border-primary bg-primary/5'
                        : 'border-border hover:bg-muted/40'
                    }`}
                    onClick={() => setAirflowDagId('emergency_management_placeholder')}
                  >
                    <div className='flex items-center gap-2'>
                      <ShieldAlert className='h-4 w-4 text-red-500' />
                      <div className='font-medium'>应急管理</div>
                      <span className='rounded-full bg-emerald-100 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-200'>
                        Coming soon
                      </span>
                    </div>
                    <p className='text-muted-foreground text-xs'>
                      应急管理相关数据处理流程的模式，后续会接入对应的 Airflow DAG。
                    </p>
                  </button>

                  {/* 教育 */}
                  <button
                    type='button'
                    className={`flex min-h-[120px] flex-col items-start gap-2 rounded-lg border p-4 text-left text-sm transition-colors ${
                      airflowDagId === 'education_placeholder'
                        ? 'border-primary bg-primary/5'
                        : 'border-border hover:bg-muted/40'
                    }`}
                    onClick={() => setAirflowDagId('education_placeholder')}
                  >
                    <div className='flex items-center gap-2'>
                      <GraduationCap className='h-4 w-4 text-violet-500' />
                      <div className='font-medium'>教育数据处理</div>
                      <span className='rounded-full bg-emerald-100 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-200'>
                        Coming soon
                      </span>
                    </div>
                    <p className='text-muted-foreground text-xs'>
                      教育领域数据处理流程的模式，后续会接入对应的 Airflow DAG。
                    </p>
                  </button>
                </div>
              </div>
            ) : step === 2 ? (
              <div className='space-y-3'>
                {airflowDagId === 'wu_jing_pu_etl' ? (
                  <>
                    <div className='space-y-1'>
                      <Label htmlFor='sourceDir'>选择本地数据目录</Label>
                      <p className='text-muted-foreground text-xs'>
                        从当前电脑选择一个目录，系统会读取其中的文件并在后端 / Airflow 中处理。
                      </p>
                    </div>

                    <input
                      id='sourceDir'
                      type='file'
                      // @ts-expect-error webkitdirectory is一个非标准但被广泛支持的属性
                      webkitdirectory='true'
                      multiple
                      className='hidden'
                      onChange={(e) => {
                        const list = e.target.files ? Array.from(e.target.files) : []
                        if (list.length) {
                          const rawRelPaths = list.map((f) => {
                            const withPath = f as File & { webkitRelativePath?: string }
                            return withPath.webkitRelativePath || withPath.name || ''
                          })
                          const firstRel = rawRelPaths[0]
                          const firstSlash = firstRel.indexOf('/')
                          const topDir = firstSlash > 0 ? firstRel.slice(0, firstSlash + 1) : ''
                          // 用于界面展示：仍然显示用户选择的顶层目录名
                          const displayDir = topDir || ''
                          setDirectory(displayDir)

                          // 上传到 MinIO 时去掉顶层目录名，使对象直接位于 raw/ 之下
                          const relPaths = rawRelPaths.map((p) => {
                            if (topDir && p.startsWith(topDir)) {
                              return p.slice(topDir.length)
                            }
                            return p
                          })
                          setUploadedFiles(relPaths)
                          setUploadStatus('uploading')
                          setUploadTotal(list.length)
                          setUploadError(null)
                          setRawPrefix(null)

                          // 选中目录后立即开始上传
                          void (async () => {
                            try {
                              const uploadRes = await uploadDatasetImportFiles(
                                dataset.id,
                                list,
                                relPaths
                              )
                              setRawPrefix(uploadRes.rawPrefix)
                              setUploadStatus('success')
                              // 上传成功后创建或更新导入记录（第二步）
                              try {
                                if (!importRun) {
                                  const created = await createDatasetImportRun(dataset.id, {
                                    airflowDagId,
                                    directory: displayDir || null,
                                    rawPrefix: uploadRes.rawPrefix,
                                    status: 'uploaded',
                                  })
                                  setImportRun(created)
                                } else {
                                  const updated = await updateDatasetImportRun(dataset.id, importRun.id, {
                                    airflowDagId,
                                    directory: displayDir || null,
                                    rawPrefix: uploadRes.rawPrefix,
                                    status: 'uploaded',
                                  })
                                  setImportRun(updated)
                                }
                              } catch (err) {
                                // eslint-disable-next-line no-console
                                console.error('updateDatasetImportRun on upload error', err)
                              }
                            } catch (err) {
                              // eslint-disable-next-line no-console
                              console.error('uploadDatasetImportFiles error', err)
                              setUploadStatus('error')
                              setUploadError('上传失败，请重试选择目录。')
                            }
                          })()
                        } else {
                          setDirectory('')
                          setRawPrefix(null)
                          setUploadStatus('idle')
                          setUploadTotal(0)
                          setUploadError(null)
                          setUploadedFiles([])
                        }
                      }}
                    />

                    {uploadStatus !== 'success' ? (
                      <button
                        type='button'
                        className='mt-2 flex w-full flex-col items-center justify-center gap-3 rounded-lg border border-dashed border-border bg-muted/30 px-8 py-12 text-center transition-colors hover:border-primary/60 hover:bg-muted'
                        onClick={() => {
                          const input = document.getElementById('sourceDir') as HTMLInputElement | null
                          input?.click()
                        }}
                      >
                        <div className='flex h-12 w-12 items-center justify-center rounded-full bg-background shadow-sm'>
                          <FolderOpen className='h-6 w-6 text-primary' />
                        </div>
                        <div className='space-y-1'>
                          <p className='text-sm font-medium'>点击选择本地目录</p>
                          <p className='text-muted-foreground text-xs'>系统会读取该目录下的所有文件，并在后续的数据处理流程中使用。</p>
                        </div>
                        <span className='mt-1 inline-flex h-8 items-center rounded-md bg-primary px-4 text-xs font-medium text-primary-foreground'>
                          浏览本地目录
                        </span>
                      </button>
                    ) : (
                      <div className='mt-2 space-y-2 rounded-lg border border-dashed border-border bg-muted/30 p-4 text-xs'>
                        <div className='flex items-center justify-between gap-2'>
                          <span className='font-medium'>已上传文件</span>
                          <Button
                            type='button'
                            variant='outline'
                            size='sm'
                            onClick={() => {
                              const input = document.getElementById('sourceDir') as HTMLInputElement | null
                              input?.click()
                            }}
                          >
                            重新选择目录
                          </Button>
                        </div>
                        <div className='max-h-48 overflow-auto rounded bg-background px-3 py-2'>
                          {uploadedFiles.length ? (
                            <ul className='space-y-1'>
                              {uploadedFiles.slice(0, 50).map((name, idx) => (
                                <li key={idx} className='truncate'>
                                  {name}
                                </li>
                              ))}
                              {uploadedFiles.length > 50 && (
                                <li className='text-muted-foreground'>
                                  仅展示前 50 个文件，共 {uploadedFiles.length} 个。
                                </li>
                              )}
                            </ul>
                          ) : (
                            <p className='text-muted-foreground'>本次上传的文件列表暂不可用。</p>
                          )}
                        </div>
                      </div>
                    )}

                    {directory && (
                      <div className='space-y-1'>
                        <p className='text-xs text-muted-foreground'>已选择目录：{directory}</p>
                        {uploadStatus === 'uploading' && (
                          <p className='text-xs text-blue-600'>
                            正在上传目录下文件（共 {uploadTotal} 个），请稍候...
                          </p>
                        )}
                        {uploadStatus === 'success' && (
                          <p className='text-xs text-emerald-600'>
                            上传完成，可点击下方“开始处理”按钮触发 Airflow 任务。
                          </p>
                        )}
                        {uploadStatus === 'error' && (
                          <p className='text-xs text-red-600'>
                            {uploadError || '上传失败，请重新选择目录重试。'}
                          </p>
                        )}
                      </div>
                    )}
                  </>
                ) : (
                  <>
                    <div className='space-y-1'>
                      <Label htmlFor='config'>导入配置（JSON 或 YAML）</Label>
                      <p className='text-muted-foreground text-xs'>可选：会作为 Airflow DAG 运行时的 conf 传入，例如数据源路径、分区、时间范围等。</p>
                    </div>
                    <Textarea
                      id='config'
                      placeholder='例如: {"source": "s3://...", "partition": "2025-11-30"} 或对应的 YAML 配置'
                      value={config}
                      onChange={(e) => setConfig(e.target.value)}
                      rows={6}
                    />
                  </>
                )}
              </div>
            ) : (
              <div className='space-y-4'>
                <div className='space-y-1'>
                  <Label>数据处理进度</Label>
                  <p className='text-muted-foreground text-xs'>当前导入任务由 Airflow 执行，下面显示其运行状态。</p>
                </div>

                <div className='space-y-2 rounded-md border p-3 text-sm'>
                  <div className='flex justify-between gap-4'>
                    <span className='text-muted-foreground'>目标数据集</span>
                    <span className='font-medium'>{dataset.name}</span>
                  </div>
                  <div className='flex justify-between gap-4'>
                    <span className='text-muted-foreground'>Airflow DAG</span>
                    <span className='font-mono text-xs'>{airflowDagId}</span>
                  </div>
                  <div className='flex justify-between gap-4'>
                    <span className='text-muted-foreground'>数据目录</span>
                    <span className='max-w-[60%] truncate text-right text-xs'>{directory || '未选择'}</span>
                  </div>
                  <div className='flex justify-between gap-4'>
                    <span className='text-muted-foreground'>当前状态</span>
                    <span className='font-medium'>
                      {runInfo?.state === 'queued' && '排队中'}
                      {runInfo?.state === 'running' && '处理中'}
                      {runInfo?.state === 'success' && '已完成'}
                      {runInfo?.state === 'failed' && '失败'}
                      {!runInfo?.state && '等待中'}
                    </span>
                  </div>
                </div>

                {tasks.length > 0 && (
                  <div className='space-y-2 rounded-md border p-3 text-sm'>
                    <div className='flex items-center justify-between'>
                      <span className='font-medium'>任务步骤</span>
                      <span className='text-muted-foreground'>来自 Airflow TaskInstances</span>
                    </div>

                    {/* 垂直步骤条，内部滚动，不挤占外部容器高度 */}
                    <div ref={tasksContainerRef} className='mt-1 max-h-56 overflow-y-auto pr-1'>
                      <div className='relative flex flex-col gap-2'>
                        {tasks.map((t, index) => {
                          const isLast = index === tasks.length - 1
                          return (
                            <div key={t.taskId} className='flex gap-3' data-task-row='1'>
                              {/* 左侧圆点 + 竖线 */}
                              <div className='flex flex-col items-center'>
                                <div
                                  className={`flex h-4 w-4 items-center justify-center rounded-full border text-[10px] ${
                                    t.state === 'success'
                                      ? 'border-emerald-500 bg-emerald-500 text-white'
                                      : t.state === 'running'
                                        ? 'border-primary bg-primary text-primary-foreground'
                                        : t.state === 'failed'
                                          ? 'border-red-500 bg-red-500 text-white'
                                          : 'border-border bg-background text-muted-foreground'
                                  }`}
                                >
                                  {index + 1}
                                </div>
                                {!isLast && <div className='mt-1 h-6 w-px bg-border' />}
                              </div>

                              {/* 右侧任务信息 */}
                              <div className='flex-1 pb-2'>
                                <div className='flex items-center justify-between gap-2'>
                                  <div className='flex flex-col items-start gap-0.5'>
                                    <span className='text-xs font-medium'>
                                      {TASK_LABELS[t.taskId] || t.taskId}
                                    </span>
                                    {TASK_LABELS[t.taskId] && (
                                      <span className='font-mono text-[10px] text-muted-foreground'>{t.taskId}</span>
                                    )}
                                  </div>
                                  <div className='flex items-center gap-2'>
                                    <span className='text-xs font-medium'>
                                      {t.state === 'queued' && '排队中'}
                                      {t.state === 'running' && '运行中'}
                                      {t.state === 'success' && '成功'}
                                      {t.state === 'failed' && '失败'}
                                      {t.state === 'up_for_retry' && '重试中'}
                                      {t.state === 'skipped' && '已跳过'}
                                      {!t.state && '等待中'}
                                    </span>
                                    <Button
                                      type='button'
                                      variant='outline'
                                      size='sm'
                                      onClick={() => handleViewTaskLog(t.taskId)}
                                    >
                                      查看日志
                                    </Button>
                                  </div>
                                </div>
                              </div>
                            </div>
                          )
                        })}
                      </div>
                    </div>
                  </div>
                )}

                {/* 主数据校验结果查看入口（弹窗触发） */}
                <div className='space-y-2 rounded-md border p-3 text-sm'>
                  <div className='flex items-center justify-between gap-2'>
                    <div className='flex flex-col gap-0.5'>
                      <span className='font-medium'>主数据校验结果</span>
                      <span className='text-muted-foreground text-xs'>展示本次导入过程中发现的一对多主数据冲突，需要人工确认。</span>
                    </div>
                    <Button
                      type='button'
                      variant='outline'
                      size='sm'
                      onClick={() => setValidationDialogOpen(true)}
                    >
                      查看主数据校验结果
                    </Button>
                  </div>
                  <p className='text-muted-foreground text-[11px]'>仅统计需要人工确认的一对多冲突，详细结果以弹窗表格展示。</p>
                </div>
              </div>
            )}
            </div>
          </div>

          {/* 底部按钮：固定在窗体内容底部，宽度与中间内容一致（1200px 居中） */}
          <div className='mt-4 px-8 pt-3'>
            <div className='mx-auto flex max-w-[1200px] justify-between gap-3'>
              <Button
                type='button'
                variant='outline'
                onClick={() => {
                  if (step === 1) {
                    onClose(false)
                  } else if (step === 2) {
                    setStep(1)
                  } else {
                    setStep(2)
                  }
                }}
                disabled={submitting}
              >
                {step === 1 ? '取消' : '上一步'}
              </Button>

              {step === 1 ? (
                <Button type='button' onClick={() => setStep(2)} disabled={!canGoNextFromStep1}>
                  下一步
                </Button>
              ) : step === 2 ? (
                <Button
                  type='button'
                  onClick={handleStartProcessing}
                  disabled={!canGoNextFromStep2 || submitting}
                >
                  {submitting ? '开始处理中…' : '开始处理'}
                </Button>
              ) : (
                <Button type='button' onClick={() => onClose(false)}>
                  关闭
                </Button>
              )}
            </div>
          </div>

          {logDialogOpen && (
            <div className='fixed inset-0 z-50 flex items-center justify-center bg-black/40'>
              <div className='max-h-[80vh] w-[min(900px,90vw)] rounded-md bg-background p-4 shadow-lg'>
                <div className='mb-2 flex items-center justify-between gap-2'>
                  <div className='flex flex-col gap-1'>
                    <span className='text-sm font-medium'>任务日志</span>
                    {logDialogTaskId && (
                      <span className='font-mono text-[11px] text-muted-foreground'>
                        {logDialogTaskId}
                      </span>
                    )}
                  </div>
                  <Button
                    type='button'
                    variant='outline'
                    size='sm'
                    onClick={() => setLogDialogOpen(false)}
                  >
                    关闭
                  </Button>
                </div>
                <div className='rounded border bg-muted px-2 py-1 text-[11px] text-muted-foreground'>
                  {logDialogLoading ? '日志加载中…' : '以下为来自 Airflow 的原始任务日志。'}
                </div>
                <div className='mt-2 max-h-[60vh] overflow-auto rounded border bg-black/90 p-3 text-xs text-green-100'>
                  <pre className='whitespace-pre-wrap break-all font-mono text-[11px]'>
                    {logDialogLoading ? '...' : logDialogContent || '暂无日志内容。'}
                  </pre>
                </div>
              </div>
            </div>
          )}

          {validationDialogOpen && (
            <div className='fixed inset-0 z-50 flex items-center justify-center bg-black/40'>
              <div className='max-h-[80vh] w-[min(1100px,95vw)] rounded-md bg-background p-4 shadow-lg'>
                <div className='mb-2 flex items-center justify-between gap-2'>
                  <div className='flex flex-col gap-1'>
                    <span className='text-sm font-medium'>主数据校验结果</span>
                    <span className='text-muted-foreground text-[11px]'>仅展示需要人工确认的一对多冲突。</span>
                  </div>
                  <Button
                    type='button'
                    variant='outline'
                    size='sm'
                    onClick={() => setValidationDialogOpen(false)}
                  >
                    关闭
                  </Button>
                </div>
                <div className='rounded border bg-muted px-2 py-1 text-[11px] text-muted-foreground'>
                  {masterValidationLoading
                    ? '校验结果加载中…'
                    : '以下为本次导入过程中发现的一对多主数据冲突列表。'}
                </div>
                <div className='mt-2 max-h-[60vh] overflow-auto rounded border bg-background p-2 text-xs'>
                  {masterValidationError && !masterValidationLoading && (
                    <div className='text-red-600'>加载校验结果失败，请稍后重试。</div>
                  )}
                  {!masterValidationLoading && !masterValidationError && (
                    (() => {
                      if (!masterValidation || !masterValidation.rows) {
                        return <div className='text-muted-foreground'>当前没有可展示的主数据校验结果。</div>
                      }

                      const conflictTypes = new Set([
                        'NEW_CODE_ONE_TO_MANY_NAMES',
                        'NEW_NAME_ONE_TO_MANY_CODES',
                      ])
                      const conflictRows = masterValidation.rows.filter((row) =>
                        conflictTypes.has(row.issue_type),
                      )

                      if (conflictRows.length === 0) {
                        return <div className='text-muted-foreground'>当前没有需要人工确认的一对多冲突。</div>
                      }

                      return (
                        <div className='space-y-1'>
                          <div className='flex items-center justify-between text-[11px] text-muted-foreground'>
                            <span>共 {conflictRows.length} 条需要人工确认的一对多冲突</span>
                            {masterValidation.key && <span>来源：{masterValidation.key}</span>}
                          </div>
                          <div className='max-h-[52vh] overflow-auto rounded border bg-background'>
                            <table className='min-w-full border-collapse text-[11px]'>
                              <thead className='bg-muted/60'>
                                <tr>
                                  <th className='border-b px-2 py-1 text-left font-medium'>类型</th>
                                  <th className='border-b px-2 py-1 text-left font-medium'>来源文件</th>
                                  <th className='border-b px-2 py-1 text-left font-medium'>统一社会信用代码</th>
                                  <th className='border-b px-2 py-1 text-left font-medium'>单位详细名称</th>
                                </tr>
                              </thead>
                              <tbody>
                                {conflictRows.slice(0, 500).map((row, idx) => (
                                  <tr key={idx} className='hover:bg-muted/40'>
                                    <td className='border-b px-2 py-1 align-top'>{row.issue_type}</td>
                                    <td className='border-b px-2 py-1 align-top'>{row.source_file || ''}</td>
                                    <td className='border-b px-2 py-1 align-top'>
                                      {row['统一社会信用代码'] || row['统一社会信用代码_source'] || ''}
                                    </td>
                                    <td className='border-b px-2 py-1 align-top'>
                                      {row['单位详细名称'] || row['单位详细名称_source'] || row['单位详细名称_base'] || ''}
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                            {conflictRows.length > 500 && (
                              <div className='border-t px-2 py-1 text-[11px] text-muted-foreground'>
                                仅展示前 500 条记录，完整结果请通过下载文件查看。
                              </div>
                            )}
                          </div>
                        </div>
                      )
                    })()
                  )}
                </div>
              </div>
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}
