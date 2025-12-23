import React from 'react'
import axios from 'axios'
import { useQuery } from '@tanstack/react-query'
import { Label } from '@/components/ui/label'
import { Card, CardContent } from '@/components/ui/card'
import { cn } from '@/lib/utils'
import { type PrefectFlowRunStatus } from '@/services/prefect'
import { DatasetValidationIssuesStep } from './dataset-validation-issues-step'
import { fetchValidationReport } from '@/services/datasets'

type DatasetFlowProgressStepProps = {
  flowRunId: string | null
  flowRunStatus: PrefectFlowRunStatus | null
  prefix?: string | null
  selectedFlowId?: string | null
  onRetry?: () => void
}

export function DatasetFlowProgressStep({
  flowRunId,
  flowRunStatus,
  prefix,
  selectedFlowId,
  onRetry,
}: DatasetFlowProgressStepProps) {
  // 检查流程是否处于暂停状态（需要用户处理验证问题）
  // Prefect 3 使用 PAUSED 状态，Prefect 2.x 使用 SUSPENDED 状态
  const isPaused = 
    flowRunStatus?.state_type === 'PAUSED' || 
    flowRunStatus?.state_name === 'Paused' ||
    flowRunStatus?.state_type === 'SUSPENDED' || 
    flowRunStatus?.state_name === 'Suspended'
  
  // 检查流程是否失败（可能是暂停方法失败导致的，但验证报告已保存）
  const isFailed = 
    flowRunStatus?.state_type === 'FAILED' || 
    flowRunStatus?.state_type === 'CRASHED'
  
  // 检查是否存在验证报告（即使 flow 状态是 COMPLETED，如果存在验证报告，也应该显示）
  // 这里对 404 做特殊处理：返回 null 而不是错误，以便持续轮询等待报告生成
  const { data: validationReport } = useQuery<Awaited<ReturnType<typeof fetchValidationReport>> | null>({
    queryKey: ['validation-report', prefix],
    queryFn: async () => {
      if (!prefix) return null
      try {
        return await fetchValidationReport(prefix)
      } catch (err) {
        if (axios.isAxiosError(err) && err.response?.status === 404) {
          // 报告暂不存在，返回 null，配合 refetchInterval 继续轮询
          return null
        }
        throw err
      }
    },
    enabled: !!prefix, // 只要有 prefix 就检查验证报告，不依赖状态
    retry: (failureCount, err) => {
      if (axios.isAxiosError(err) && err.response?.status === 404) {
        // 404 的情况由 queryFn 返回 null 处理，这里不需要额外重试
        return false
      }
      // 其他错误最多重试 2 次
      return failureCount < 2
    },
    // 即使 flow 已完成，也继续检查验证报告（因为可能是在完成后才生成的）
    staleTime: 0, // 不缓存，总是获取最新状态
    refetchInterval: (query) => {
      const data = query.state.data as Awaited<ReturnType<typeof fetchValidationReport>> | null | undefined
      // 只要还没有拿到报告（data 为 null 或 undefined），就每 5 秒轮询一次，直到拿到报告或组件卸载
      if (!data) {
        return 5000
      }
      return false
    },
  })
  
  const hasValidationReport = validationReport !== null && validationReport !== undefined
  
  // 显示验证问题处理界面的条件：
  // 仅在存在报告，或主 flow 处于 Paused/Suspended/Failed 时进入处理界面。
  // 主 flow Completed 且无报告时，不再停留在处理页。
  // 显示验证问题处理界面的条件：
  // - 有待处理验证报告（子 flow 已经暂停并生成报告）
  // - 或主 flow 自身被标记为 Paused/Suspended/Failed
  // 提交后我们在验证页会主动清空报告缓存，因此不依赖 RUNNING/COMPLETED 状态切换
  const shouldShowValidationIssues =
    !!prefix &&
    (
      hasValidationReport ||  // 有待处理验证报告
      isPaused ||             // 主 flow Paused/Suspended
      isFailed               // 主 flow 失败
    )
  
  if (shouldShowValidationIssues) {
    return (
      <DatasetValidationIssuesStep
        prefix={prefix}
        flowRunId={flowRunId}
        selectedFlowId={selectedFlowId || null}
        onResolved={() => {
          // 验证问题解决后，可以刷新流程状态
          if (onRetry) {
            onRetry()
          }
        }}
        onRetry={() => {
          // 重新运行流程
          if (onRetry) {
            onRetry()
          }
        }}
      />
    )
  }
  return (
    <div className='space-y-6'>
      <div>
        <Label className='text-base font-semibold'>处理进度</Label>
        <p className='text-muted-foreground mt-1 text-sm'>
          当前 Flow 正在运行，下面显示其运行状态和进度。
        </p>
      </div>

      <Card>
        <CardContent className='space-y-4 py-6'>
          {!flowRunId ? (
            <p className='text-sm text-muted-foreground'>正在启动 Flow 运行...</p>
          ) : !flowRunStatus ? (
            <p className='text-sm text-muted-foreground'>正在获取运行状态...</p>
          ) : (
            <div className='space-y-4'>
              <div className='space-y-2'>
                <div className='flex items-center justify-between'>
                  <span className='text-sm text-muted-foreground'>Flow Run ID</span>
                  <span className='font-mono text-xs'>{flowRunId}</span>
                </div>
                <div className='flex items-center justify-between'>
                  <span className='text-sm text-muted-foreground'>状态</span>
                  <span
                    className={cn(
                      'rounded-full px-2 py-1 text-xs font-medium',
                      flowRunStatus.state_type === 'COMPLETED'
                        ? 'bg-emerald-500/10 text-emerald-600'
                        : flowRunStatus.state_type === 'FAILED' || flowRunStatus.state_type === 'CRASHED'
                          ? 'bg-red-500/10 text-red-600'
                          : flowRunStatus.state_type === 'CANCELLED'
                            ? 'bg-amber-500/10 text-amber-600'
                            : flowRunStatus.state_type === 'PAUSED' || flowRunStatus.state_type === 'SUSPENDED'
                              ? 'bg-amber-500/10 text-amber-600'
                              : flowRunStatus.state_type === 'RUNNING'
                                ? 'bg-sky-500/10 text-sky-600'
                                : 'bg-muted text-muted-foreground',
                    )}
                  >
                    {flowRunStatus.state_name || flowRunStatus.state_type || '未知'}
                  </span>
                </div>
                {flowRunStatus.created && (
                  <div className='flex items-center justify-between'>
                    <span className='text-sm text-muted-foreground'>创建时间</span>
                    <span className='text-xs'>
                      {new Date(flowRunStatus.created).toLocaleString('zh-CN')}
                    </span>
                  </div>
                )}
                {flowRunStatus.start_time && (
                  <div className='flex items-center justify-between'>
                    <span className='text-sm text-muted-foreground'>开始时间</span>
                    <span className='text-xs'>
                      {new Date(flowRunStatus.start_time).toLocaleString('zh-CN')}
                    </span>
                  </div>
                )}
                {flowRunStatus.end_time && (
                  <div className='flex items-center justify-between'>
                    <span className='text-sm text-muted-foreground'>结束时间</span>
                    <span className='text-xs'>
                      {new Date(flowRunStatus.end_time).toLocaleString('zh-CN')}
                    </span>
                  </div>
                )}
              </div>

              {flowRunStatus.state_type === 'COMPLETED' && (
                <div className='rounded-lg bg-emerald-50 p-3 text-sm text-emerald-700 dark:bg-emerald-900/20 dark:text-emerald-400'>
                  Flow 运行已成功完成！
                </div>
              )}

              {(flowRunStatus.state_type === 'FAILED' || flowRunStatus.state_type === 'CRASHED') && (
                <div className='rounded-lg bg-red-50 p-3 text-sm text-red-700 dark:bg-red-900/20 dark:text-red-400'>
                  Flow 运行失败，请检查 Prefect 控制台查看详细错误信息。
                </div>
              )}

              {flowRunStatus.state_type === 'RUNNING' && (
                <div className='rounded-lg bg-sky-50 p-3 text-sm text-sky-700 dark:bg-sky-900/20 dark:text-sky-400'>
                  Flow 正在运行中，请稍候...
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
