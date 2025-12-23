import React, { useState } from 'react'
import axios from 'axios'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Label } from '@/components/ui/label'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { AlertTriangle, CheckCircle2, RefreshCw, Search, Upload, Download, X, CheckSquare } from 'lucide-react'
import {
  fetchValidationReport,
  submitValidationResolutions,
  type ValidationIssue,
  type ValidationReport,
} from '@/services/datasets'
import { runPrefectFlow, resumePrefectFlowRun } from '@/services/prefect'
import { cn } from '@/lib/utils'

type DatasetValidationIssuesStepProps = {
  prefix: string
  flowRunId: string | null
  selectedFlowId: string | null
  onResolved: () => void
  onRetry: () => void
}

export function DatasetValidationIssuesStep({
  prefix,
  onResolved,
  flowRunId,
  selectedFlowId,
  onRetry,
}: DatasetValidationIssuesStepProps) {
  const queryClient = useQueryClient()
  const [fixedIssues, setFixedIssues] = useState<Set<string>>(new Set())
  const [issueFixes, setIssueFixes] = useState<Record<string, { code?: string; name?: string }>>({})
  const [searchQuery, setSearchQuery] = useState<string>('')
  const [showBulkImport, setShowBulkImport] = useState(false)
  const [bulkImportText, setBulkImportText] = useState('')
  const [selectedIssues, setSelectedIssues] = useState<Set<string>>(new Set())

  // 调试：打印报告数据（仅在开发环境）
  React.useEffect(() => {
    // 这个 effect 会在组件渲染时执行，不依赖于 report 的存在
  }, [])

  // 读取验证报告：404 视为不存在（返回 null），保持轮询等待报告生成
  const { data: report, isLoading, error, refetch } = useQuery<ValidationReport | null>({
    queryKey: ['validation-report', prefix],
    queryFn: async () => {
      try {
        return await fetchValidationReport(prefix)
      } catch (err) {
        if (axios.isAxiosError(err) && err.response?.status === 404) {
          // 报告未生成或已被删除，视为 null，后续继续轮询
          return null
        }
        throw err
      }
    },
    enabled: !!prefix,
    retry: (failureCount, err) => {
      // 404 不重试（已转为返回 null），其他错误最多重试 3 次
      if (axios.isAxiosError(err) && err.response?.status === 404) {
        return false
      }
      return failureCount < 3
    },
    refetchInterval: (query) => {
      const data = query.state.data
      // 报告不存在或仍需用户处理时，保持轮询
      if (!data || data.status === 'pending_user_action') {
        return 5000 // 每5秒轮询一次
      }
      return false
    },
  })

  // 调试：打印报告数据（仅在开发环境）
  React.useEffect(() => {
    if (report && process.env.NODE_ENV === 'development') {
      // eslint-disable-next-line no-console
      console.log('Validation Report:', report)
      // eslint-disable-next-line no-console
      console.log('Report Summary:', report.summary)
      // eslint-disable-next-line no-console
      console.log('Report Issues:', report.issues)
      // eslint-disable-next-line no-console
      console.log('Truncated Codes:', report.issues?.truncated_codes?.length || 0)
      // eslint-disable-next-line no-console
      console.log('One to Many Code:', report.issues?.one_to_many_code?.length || 0)
      // eslint-disable-next-line no-console
      console.log('One to Many Name:', report.issues?.one_to_many_name?.length || 0)
      // eslint-disable-next-line no-console
      console.log('Missing Codes:', report.issues?.missing_codes?.length || 0)
    }
  }, [report])

  // 提交修复结果
  const { mutateAsync: submitResolutions, isPending: isSubmitting } = useMutation({
    mutationFn: async () => {
      // 收集所有修复结果
      const resolutions: {
        truncated_codes?: Array<{ code: string; fixed_code: string; file: string; row_index: number }>
        one_to_many_code?: Array<{ code: string; selected_name: string; file: string }>
        one_to_many_name?: Array<{ name: string; selected_code: string; file: string }>
        missing_codes?: Array<{ name: string; code: string; file: string; row_index: number }>
      } = {}

      // 收集截断代码的修复
      if (report?.issues.truncated_codes) {
        resolutions.truncated_codes = report.issues.truncated_codes
          .map((issue, index) => {
            const issueKey = `truncated-${issue.file}-${index}`
            const fix = issueFixes[issueKey]
            if (fix?.code && fixedIssues.has(issueKey)) {
              return {
                code: issue.code || '',
                fixed_code: fix.code,
                file: issue.file || '',
                row_index: issue.row_index || 0,
              }
            }
            return null
          })
          .filter((item): item is NonNullable<typeof item> => item !== null)
      }

      // 收集一对多 code 的修复
      if (report?.issues.one_to_many_code) {
        resolutions.one_to_many_code = report.issues.one_to_many_code
          .map((issue, index) => {
            const issueKey = `code-${index}`
            if (fixedIssues.has(issueKey)) {
              return {
                code: issue.code || '',
                selected_name: issue.existing_name || issue.new_name || '',
                file: issue.file || '',
              }
            }
            return null
          })
          .filter((item): item is NonNullable<typeof item> => item !== null)
      }

      // 收集一对多 name 的修复
      if (report?.issues.one_to_many_name) {
        resolutions.one_to_many_name = report.issues.one_to_many_name
          .map((issue, index) => {
            const issueKey = `name-${index}`
            if (fixedIssues.has(issueKey)) {
              return {
                name: issue.name || '',
                selected_code: issue.existing_code || issue.new_code || '',
                file: issue.file || '',
              }
            }
            return null
          })
          .filter((item): item is NonNullable<typeof item> => item !== null)
      }

      // 收集缺失 code 的修复
      // 使用稳定的唯一标识符：name + file + row_index
      if (report?.issues.missing_codes) {
        resolutions.missing_codes = report.issues.missing_codes
          .map((issue, index) => {
            // 使用稳定的唯一标识符，不依赖 index
            const issueKey = `missing-${issue.file}-${issue.name || ''}-${issue.row_index ?? index}`
            const fix = issueFixes[issueKey]
            if (fix?.code && fixedIssues.has(issueKey)) {
              return {
                name: issue.name || '',
                code: fix.code,
                file: issue.file || '',
                row_index: issue.row_index || 0,
              }
            }
            return null
          })
          .filter((item): item is NonNullable<typeof item> => item !== null)
      }

      return submitValidationResolutions(prefix, resolutions)
    },
    onSuccess: async () => {
      // 提交成功后，优先恢复暂停的「验证子 Flow」
      // 报告中带有 flow_run_id（dataset-validation-flow 的 run id）时，优先使用该 id
      const targetFlowRunId =
        report?.flow_run_id && report.flow_run_id !== 'unknown'
          ? report.flow_run_id
          : flowRunId

      if (!targetFlowRunId) {
        throw new Error('无法恢复流程：缺少 Flow Run ID。请手动在 Prefect 中恢复或重新运行流程。')
      }

      // 恢复暂停的 flow run（通常是验证子 Flow）
      await resumePrefectFlowRun(targetFlowRunId)

      // 提交后立即让前端回到进度视图：清空当前缓存的验证报告
      queryClient.setQueryData(['validation-report', prefix], null)

      // 调用 onResolved 通知父组件问题已解决，应该返回到运行显示状态
      // 这会触发状态刷新，流程恢复后会显示运行进度和日志
      onResolved()
    },
  })

  // 重新运行 Flow（保留用于手动重试）
  const { isPending: isRetrying } = useMutation({
    mutationFn: async () => {
      if (!selectedFlowId) {
        throw new Error('未选择 Flow')
      }
      return runPrefectFlow(selectedFlowId, { prefix })
    },
    onSuccess: () => {
      onRetry()
    },
  })

  // 注意：isRetrying 用于禁用按钮，但当前未使用
  void isRetrying

  if (isLoading) {
    return (
      <div className='space-y-6'>
        <div>
          <Label className='text-base font-semibold'>数据校准问题</Label>
          <p className='text-muted-foreground mt-1 text-sm'>正在加载验证报告...</p>
        </div>
      </div>
    )
  }

  // 如果正在加载，显示加载状态
  if (isLoading) {
    return (
      <div className='space-y-6'>
        <div>
          <Label className='text-base font-semibold'>数据校准问题</Label>
          <p className='text-muted-foreground mt-1 text-sm'>正在加载验证报告...</p>
        </div>
        <Alert>
          <AlertTriangle className='h-4 w-4' />
          <AlertTitle>流程已暂停</AlertTitle>
          <AlertDescription>
            流程已暂停，等待验证报告加载。如果报告长时间未加载，请检查后端日志或刷新页面。
          </AlertDescription>
        </Alert>
      </div>
    )
  }

  // 如果查询出错（非 404），显示错误信息
  if (error && (error as { response?: { status?: number } })?.response?.status !== 404) {
    return (
      <div className='space-y-6'>
        <div>
          <Label className='text-base font-semibold'>数据校准问题</Label>
          <p className='text-muted-foreground mt-1 text-sm'>加载验证报告时出错</p>
        </div>
        <Alert variant='destructive'>
          <AlertTriangle className='h-4 w-4' />
          <AlertTitle>错误</AlertTitle>
          <AlertDescription>
            {String(error)}
            <br />
            <br />
            <Button variant='outline' size='sm' onClick={() => refetch()} className='mt-2'>
              <RefreshCw className='mr-2 h-4 w-4' />
              重试
            </Button>
          </AlertDescription>
        </Alert>
      </div>
    )
  }

  // 如果报告不存在（404），显示等待状态（可能报告还在上传中）
  if (!report) {
    return (
      <div className='space-y-6'>
        <div>
          <Label className='text-base font-semibold'>数据校准问题</Label>
          <p className='text-muted-foreground mt-1 text-sm'>等待验证报告生成...</p>
        </div>
        <Alert>
          <AlertTriangle className='h-4 w-4' />
          <AlertTitle>流程已暂停</AlertTitle>
          <AlertDescription>
            流程已暂停，正在等待验证报告生成。系统会自动刷新，请稍候。
            <br />
            <br />
            <Button variant='outline' size='sm' onClick={() => refetch()} className='mt-2'>
              <RefreshCw className='mr-2 h-4 w-4' />
              手动刷新
            </Button>
          </AlertDescription>
        </Alert>
      </div>
    )
  }

  // 如果报告状态不是 pending_user_action，显示相应状态
  if (report.status !== 'pending_user_action') {
    return (
      <div className='space-y-6'>
        <div>
          <Label className='text-base font-semibold'>数据校准问题</Label>
          <p className='text-muted-foreground mt-1 text-sm'>
            {report.status === 'resolved' ? '所有问题已解决' : `报告状态: ${report.status}`}
          </p>
        </div>
        {report.status === 'resolved' && (
          <Alert>
            <CheckCircle2 className='h-4 w-4' />
            <AlertTitle>已完成</AlertTitle>
            <AlertDescription>所有数据校准问题已解决，可以继续处理流程。</AlertDescription>
          </Alert>
        )}
      </div>
    )
  }

  // 生成唯一的18位随机统一社会信用代码
  const generateUniqueCode = (): string => {
    // 获取所有已使用的代码（包括已修复的和已输入的）
    const usedCodes = new Set<string>()
    
    // 收集所有已修复的代码
    Object.values(issueFixes).forEach((fix) => {
      if (fix.code && fix.code.length === 18) {
        usedCodes.add(fix.code)
      }
    })
    
    // 生成随机代码，确保唯一性
    let newCode: string
    let attempts = 0
    do {
      // 统一社会信用代码格式：18位，前17位是数字，最后1位可能是数字或字母
      // 生成17位随机数字
      const digits = Array.from({ length: 17 }, () => Math.floor(Math.random() * 10)).join('')
      // 最后一位可以是数字或字母（A-Z，但通常用数字）
      const lastChar = Math.floor(Math.random() * 10).toString()
      newCode = digits + lastChar
      attempts++
      // 防止无限循环
      if (attempts > 1000) {
        // 如果尝试1000次还没找到唯一值，使用时间戳作为后缀
        const timestamp = Date.now().toString().slice(-6)
        newCode = digits.slice(0, 12) + timestamp
      }
    } while (usedCodes.has(newCode) && attempts <= 1000)
    
    return newCode
  }

  const handleFixIssue = (issueKey: string, fix: { code?: string; name?: string }) => {
    setIssueFixes((prev) => ({ ...prev, [issueKey]: fix }))
    setFixedIssues((prev) => new Set(prev).add(issueKey))
  }

  const handleMarkAsFixed = (issueKey: string) => {
    const currentFix = issueFixes[issueKey]
    
    // 如果没有输入代码，自动生成一个唯一的随机代码
    if (!currentFix?.code || currentFix.code.length !== 18) {
      const generatedCode = generateUniqueCode()
      setIssueFixes((prev) => ({ ...prev, [issueKey]: { ...currentFix, code: generatedCode } }))
    }
    
    setFixedIssues((prev) => new Set(prev).add(issueKey))
  }

  // 批量自动生成并标记
  const handleBatchAutoGenerate = () => {
    selectedIssues.forEach((issueKey) => {
      if (!fixedIssues.has(issueKey)) {
        handleMarkAsFixed(issueKey)
      }
    })
    // 清空选择
    setSelectedIssues(new Set())
  }

  // 全选/取消全选
  const handleSelectAll = (checked: boolean, filteredIssues: Array<{ file?: string; name?: string; row_index?: number }>) => {
    if (checked) {
      const allKeys = new Set<string>()
      filteredIssues.forEach((issue, index) => {
        // 使用稳定的唯一标识符
        const issueKey = `missing-${issue.file}-${issue.name || ''}-${issue.row_index ?? index}`
        if (!fixedIssues.has(issueKey)) {
          allKeys.add(issueKey)
        }
      })
      setSelectedIssues(allKeys)
    } else {
      setSelectedIssues(new Set())
    }
  }

  // 切换单个选择
  const handleToggleSelect = (issueKey: string, checked: boolean) => {
    if (checked) {
      setSelectedIssues((prev) => new Set(prev).add(issueKey))
    } else {
      setSelectedIssues((prev) => {
        const next = new Set(prev)
        next.delete(issueKey)
        return next
      })
    }
  }

  const renderTruncatedCodeIssue = (issue: ValidationIssue, index: number) => {
    const issueKey = `truncated-${issue.file}-${index}`
    const isFixed = fixedIssues.has(issueKey)
    const fix = issueFixes[issueKey] || {}

    return (
      <TableRow key={issueKey} className={cn(isFixed && 'bg-muted/50')}>
        <TableCell className='font-mono text-xs'>{issue.code}</TableCell>
        <TableCell>{issue.name || '-'}</TableCell>
        <TableCell>
          <Badge variant='outline'>{issue.file}</Badge>
        </TableCell>
        <TableCell className='text-xs text-muted-foreground'>{issue.note || '-'}</TableCell>
        <TableCell>
          {isFixed ? (
            <Badge variant='outline' className='bg-emerald-50 text-emerald-700'>
              已修复
            </Badge>
          ) : (
            <div className='space-y-2'>
              <Input
                placeholder='输入正确的18位统一社会信用代码'
                value={fix?.code || ''}
                onChange={(e) => handleFixIssue(issueKey, { ...fix, code: e.target.value })}
                className='w-64'
              />
                                  <Button
                                    size='sm'
                                    variant='outline'
                                    onClick={() => handleMarkAsFixed(issueKey)}
                                  >
                                    {fix?.code && fix.code.length === 18
                                      ? '标记为已修复'
                                      : '自动生成并标记'}
                                  </Button>
            </div>
          )}
        </TableCell>
      </TableRow>
    )
  }

  const renderOneToManyIssue = (issue: ValidationIssue, index: number, type: 'code' | 'name') => {
    const issueKey = `${type}-${index}`
    const isFixed = fixedIssues.has(issueKey)

    return (
      <TableRow key={issueKey} className={cn(isFixed && 'bg-muted/50')}>
        <TableCell className='font-mono text-xs'>
          {type === 'code' ? issue.code : issue.name}
        </TableCell>
        <TableCell>
          {type === 'code' ? (
            <div className='space-y-1'>
              <div className='text-xs text-muted-foreground'>现有名称: {issue.existing_name}</div>
              <div className='text-xs text-muted-foreground'>新名称: {issue.new_name}</div>
            </div>
          ) : (
            <div className='space-y-1'>
              <div className='text-xs text-muted-foreground'>现有代码: {issue.existing_code}</div>
              <div className='text-xs text-muted-foreground'>新代码: {issue.new_code}</div>
            </div>
          )}
        </TableCell>
        <TableCell>
          <Badge variant='outline'>{issue.file}</Badge>
        </TableCell>
        <TableCell>
          {isFixed ? (
            <Badge variant='outline' className='bg-emerald-50 text-emerald-700'>
              已修复
            </Badge>
          ) : (
            <Button size='sm' variant='outline' onClick={() => handleMarkAsFixed(issueKey)}>
              标记为已修复
            </Button>
          )}
        </TableCell>
      </TableRow>
    )
  }

  return (
    <div className='space-y-6'>
      <div>
        <Label className='text-base font-semibold'>数据校准问题</Label>
        <p className='text-muted-foreground mt-1 text-sm'>
          检测到数据校准问题，请修复以下问题后继续处理流程。
        </p>
      </div>

      {report.instructions && (
        <Alert>
          <AlertTriangle className='h-4 w-4' />
          <AlertTitle>需要处理的问题</AlertTitle>
          <AlertDescription className='space-y-2'>
            <p>{report.instructions.action_required}</p>
            {report.instructions.truncated_codes && (
              <p className='text-sm'>{report.instructions.truncated_codes}</p>
            )}
            {report.instructions.one_to_many && (
              <p className='text-sm'>{report.instructions.one_to_many}</p>
            )}
          </AlertDescription>
        </Alert>
      )}

      <div className='flex items-center justify-between'>
        <div className='flex gap-2'>
          <Button variant='outline' size='sm' onClick={() => refetch()}>
            <RefreshCw className='mr-2 h-4 w-4' />
            刷新
          </Button>
        </div>
        <div className='text-sm text-muted-foreground'>
          问题总数: {(report.summary?.truncated_codes_count || 0) + (report.summary?.one_to_many_code_count || 0) + (report.summary?.one_to_many_name_count || 0) + (report.summary?.missing_codes_count || 0)}
        </div>
      </div>

      <Tabs defaultValue='truncated' className='w-full'>
        <TabsList>
          <TabsTrigger value='truncated'>
            截断的 Code ({report.summary?.truncated_codes_count || report.issues?.truncated_codes?.length || 0})
          </TabsTrigger>
          <TabsTrigger value='one-to-many-code'>
            一对多 Code ({report.summary?.one_to_many_code_count || report.issues?.one_to_many_code?.length || 0})
          </TabsTrigger>
          <TabsTrigger value='one-to-many-name'>
            一对多 Name ({report.summary?.one_to_many_name_count || report.issues?.one_to_many_name?.length || 0})
          </TabsTrigger>
          {((report.summary?.missing_codes_count || 0) > 0 || (report.issues?.missing_codes?.length || 0) > 0) && (
            <TabsTrigger value='missing-codes'>
              缺失的 Code ({report.summary?.missing_codes_count || report.issues?.missing_codes?.length || 0})
            </TabsTrigger>
          )}
        </TabsList>

        <TabsContent value='truncated' className='space-y-4'>
          <Card>
            <CardHeader>
              <CardTitle>截断的统一社会信用代码</CardTitle>
              <CardDescription>
                以下统一社会信用代码被转换为科学计数法格式或长度不正确，需要手动修复为正确的18位格式。
              </CardDescription>
            </CardHeader>
            <CardContent>
              {(!report.issues?.truncated_codes || report.issues.truncated_codes.length === 0) ? (
                <p className='text-sm text-muted-foreground'>没有截断的代码</p>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>原始 Code</TableHead>
                      <TableHead>单位名称</TableHead>
                      <TableHead>文件</TableHead>
                      <TableHead>说明</TableHead>
                      <TableHead>修复</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {(report.issues?.truncated_codes || []).map((issue, index) =>
                      renderTruncatedCodeIssue(issue, index),
                    )}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value='one-to-many-code' className='space-y-4'>
          <Card>
            <CardHeader>
              <CardTitle>一对多 Code 关系</CardTitle>
              <CardDescription>
                以下统一社会信用代码对应多个不同的单位名称，需要确认并修复为一一对应关系。
              </CardDescription>
            </CardHeader>
            <CardContent>
              {(!report.issues?.one_to_many_code || report.issues.one_to_many_code.length === 0) ? (
                <p className='text-sm text-muted-foreground'>没有一对多 code 关系</p>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Code</TableHead>
                      <TableHead>名称冲突</TableHead>
                      <TableHead>文件</TableHead>
                      <TableHead>操作</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {(report.issues?.one_to_many_code || []).map((issue, index) =>
                      renderOneToManyIssue(issue, index, 'code'),
                    )}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value='one-to-many-name' className='space-y-4'>
          <Card>
            <CardHeader>
              <CardTitle>一对多 Name 关系</CardTitle>
              <CardDescription>
                以下单位名称对应多个不同的统一社会信用代码，需要确认并修复为一一对应关系。
              </CardDescription>
            </CardHeader>
            <CardContent>
              {(!report.issues?.one_to_many_name || report.issues.one_to_many_name.length === 0) ? (
                <p className='text-sm text-muted-foreground'>没有一对多 name 关系</p>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Name</TableHead>
                      <TableHead>Code 冲突</TableHead>
                      <TableHead>文件</TableHead>
                      <TableHead>操作</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {(report.issues?.one_to_many_name || []).map((issue, index) =>
                      renderOneToManyIssue(issue, index, 'name'),
                    )}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {(report.summary?.missing_codes_count || 0) > 0 && (
          <TabsContent value='missing-codes' className='space-y-4'>
            <Card>
              <CardHeader>
                <CardTitle>缺失的统一社会信用代码</CardTitle>
                <CardDescription>
                  以下单位名称缺少统一社会信用代码，需要手动补充正确的 18 位统一社会信用代码。
                </CardDescription>
              </CardHeader>
              <CardContent className='space-y-4'>
                {(!report.issues?.missing_codes || report.issues.missing_codes.length === 0) ? (
                  <p className='text-sm text-muted-foreground'>没有缺失的代码</p>
                ) : (
                  <>
                    {/* 搜索和批量操作工具栏 */}
                    <div className='flex items-center gap-2 flex-wrap'>
                      <div className='relative flex-1 min-w-[200px]'>
                        <Search className='absolute left-2 top-2.5 h-4 w-4 text-muted-foreground' />
                        <Input
                          placeholder='搜索单位名称...'
                          value={searchQuery}
                          onChange={(e) => setSearchQuery(e.target.value)}
                          className='pl-8'
                        />
                        {searchQuery && (
                          <Button
                            variant='ghost'
                            size='sm'
                            className='absolute right-1 top-1 h-7 w-7 p-0'
                            onClick={() => setSearchQuery('')}
                          >
                            <X className='h-4 w-4' />
                          </Button>
                        )}
                      </div>
                      {selectedIssues.size > 0 && (
                        <Button
                          variant='default'
                          size='sm'
                          onClick={handleBatchAutoGenerate}
                          className='bg-primary text-primary-foreground'
                        >
                          <CheckSquare className='mr-2 h-4 w-4' />
                          批量自动生成 ({selectedIssues.size})
                        </Button>
                      )}
                      <Dialog open={showBulkImport} onOpenChange={setShowBulkImport}>
                        <DialogTrigger asChild>
                          <Button variant='outline' size='sm'>
                            <Upload className='mr-2 h-4 w-4' />
                            批量导入
                          </Button>
                        </DialogTrigger>
                        <DialogContent className='max-w-2xl'>
                          <DialogHeader>
                            <DialogTitle>批量导入统一社会信用代码</DialogTitle>
                            <DialogDescription>
                              请按以下格式输入：每行一个，格式为"单位名称,统一社会信用代码"或"单位名称:统一社会信用代码"
                              <br />
                              例如：
                              <br />
                              恩阳区天平寺,935119033094572520
                              <br />
                              恩阳区双胜基督教聚会点:935119033094572521
                            </DialogDescription>
                          </DialogHeader>
                          <div className='space-y-4'>
                            <Textarea
                              placeholder='粘贴或输入数据，每行一个...'
                              value={bulkImportText}
                              onChange={(e) => setBulkImportText(e.target.value)}
                              className='min-h-[300px] font-mono text-sm'
                            />
                            <div className='flex justify-end gap-2'>
                              <Button variant='outline' onClick={() => setShowBulkImport(false)}>
                                取消
                              </Button>
                              <Button
                                onClick={() => {
                                  // 解析批量导入文本
                                  const lines = bulkImportText
                                    .split('\n')
                                    .map((line) => line.trim())
                                    .filter((line) => line.length > 0)

                                  let importedCount = 0
                                  lines.forEach((line) => {
                                    // 支持逗号或冒号分隔
                                    const separator = line.includes(',') ? ',' : ':'
                                    const parts = line.split(separator).map((p) => p.trim())
                                    if (parts.length >= 2) {
                                      const [name, code] = parts
                                      // 查找匹配的问题
                                      const matchingIssue = report.issues?.missing_codes?.find(
                                        (issue) => issue.name === name,
                                      )
                                      if (matchingIssue && code.length === 18) {
                                        // 使用稳定的唯一标识符
                                        const issueKey = `missing-${matchingIssue.file}-${matchingIssue.name || ''}-${matchingIssue.row_index ?? 0}`
                                        handleFixIssue(issueKey, { code })
                                        handleMarkAsFixed(issueKey)
                                        importedCount++
                                      }
                                    }
                                  })

                                  if (importedCount > 0) {
                                    setBulkImportText('')
                                    setShowBulkImport(false)
                                  }
                                }}
                              >
                                导入 ({bulkImportText.split('\n').filter((l) => l.trim().length > 0).length} 行)
                              </Button>
                            </div>
                          </div>
                        </DialogContent>
                      </Dialog>
                      <Button
                        variant='outline'
                        size='sm'
                        onClick={() => {
                          // 导出为CSV
                          const csvContent = [
                            ['单位名称', '文件', '行号', '统一社会信用代码'].join(','),
                            ...(report.issues?.missing_codes || [])
                              .filter((issue) => {
                                if (!searchQuery) return true
                                return issue.name?.toLowerCase().includes(searchQuery.toLowerCase())
                              })
                              .map((issue, index) => {
                                const issueKey = `missing-${issue.file}-${issue.name || ''}-${issue.row_index ?? index}`
                                const fix = issueFixes[issueKey]
                                return [
                                  `"${issue.name || ''}"`,
                                  `"${issue.file || ''}"`,
                                  issue.row_index !== undefined ? issue.row_index + 1 : '',
                                  fix?.code || '',
                                ].join(',')
                              }),
                          ].join('\n')

                          const blob = new Blob(['\ufeff' + csvContent], { type: 'text/csv;charset=utf-8;' })
                          const link = document.createElement('a')
                          const url = URL.createObjectURL(blob)
                          link.setAttribute('href', url)
                          link.setAttribute('download', `缺失代码列表_${new Date().toISOString().split('T')[0]}.csv`)
                          link.style.visibility = 'hidden'
                          document.body.appendChild(link)
                          link.click()
                          document.body.removeChild(link)
                        }}
                      >
                        <Download className='mr-2 h-4 w-4' />
                        导出CSV
                      </Button>
                    </div>

                    {/* 过滤后的列表 */}
                    <div className='text-sm text-muted-foreground'>
                      显示{' '}
                      {
                        report.issues.missing_codes.filter((issue) => {
                          if (!searchQuery) return true
                          return issue.name?.toLowerCase().includes(searchQuery.toLowerCase())
                        }).length
                      }{' '}
                      / {report.issues.missing_codes.length} 条记录
                      {searchQuery && (
                        <Button
                          variant='ghost'
                          size='sm'
                          className='ml-2 h-auto p-0 text-xs'
                          onClick={() => setSearchQuery('')}
                        >
                          清除筛选
                        </Button>
                      )}
                    </div>

                    <div className='rounded-md border'>
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead className='w-12'>
                              <Checkbox
                                checked={
                                  report.issues.missing_codes
                                    .filter((issue) => {
                                      if (!searchQuery) return true
                                      return issue.name?.toLowerCase().includes(searchQuery.toLowerCase())
                                    })
                                    .every((issue, index) => {
                                      const issueKey = `missing-${issue.file}-${issue.name || ''}-${issue.row_index ?? index}`
                                      return fixedIssues.has(issueKey) || selectedIssues.has(issueKey)
                                    }) &&
                                  report.issues.missing_codes
                                    .filter((issue) => {
                                      if (!searchQuery) return true
                                      return issue.name?.toLowerCase().includes(searchQuery.toLowerCase())
                                    })
                                    .some((issue, index) => {
                                      const issueKey = `missing-${issue.file}-${issue.name || ''}-${issue.row_index ?? index}`
                                      return !fixedIssues.has(issueKey)
                                    })
                                }
                                onCheckedChange={(checked) => {
                                  const filteredIssues = report.issues.missing_codes.filter((issue) => {
                                    if (!searchQuery) return true
                                    return issue.name?.toLowerCase().includes(searchQuery.toLowerCase())
                                  })
                                  handleSelectAll(checked as boolean, filteredIssues)
                                }}
                              />
                            </TableHead>
                            <TableHead>单位名称</TableHead>
                            <TableHead>文件</TableHead>
                            <TableHead>行号</TableHead>
                            <TableHead>修复</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {report.issues.missing_codes
                            .filter((issue) => {
                              if (!searchQuery) return true
                              return issue.name?.toLowerCase().includes(searchQuery.toLowerCase())
                            })
                            .map((issue, index) => {
                              // 使用稳定的唯一标识符：name + file + row_index，避免 index 变化导致的问题
                              const issueKey = `missing-${issue.file}-${issue.name || ''}-${issue.row_index ?? index}`
                              const isFixed = fixedIssues.has(issueKey)
                              const fix = issueFixes[issueKey] || {}
                              const isSelected = selectedIssues.has(issueKey)

                              return (
                                <TableRow
                                  key={issueKey}
                                  className={cn(
                                    isFixed && 'bg-muted/50',
                                    isSelected && !isFixed && 'bg-primary/5',
                                  )}
                                >
                                  <TableCell>
                                    {!isFixed && (
                                      <Checkbox
                                        checked={isSelected}
                                        onCheckedChange={(checked) => handleToggleSelect(issueKey, checked as boolean)}
                                      />
                                    )}
                                  </TableCell>
                                  <TableCell>{issue.name || '-'}</TableCell>
                                  <TableCell>
                                    <Badge variant='outline'>{issue.file}</Badge>
                                  </TableCell>
                                  <TableCell className='text-xs text-muted-foreground'>
                                    {issue.row_index !== undefined ? issue.row_index + 1 : '-'}
                                  </TableCell>
                                  <TableCell>
                                    {isFixed ? (
                                      <Badge variant='outline' className='bg-emerald-50 text-emerald-700'>
                                        已修复
                                      </Badge>
                                    ) : (
                                      <div className='space-y-2'>
                                        <Input
                                          placeholder='输入正确的18位统一社会信用代码'
                                          value={fix?.code || ''}
                                          onChange={(e) => handleFixIssue(issueKey, { ...fix, code: e.target.value })}
                                          className='w-64'
                                        />
                                        <Button
                                          size='sm'
                                          variant='outline'
                                          onClick={() => handleMarkAsFixed(issueKey)}
                                        >
                                          {fix?.code && fix.code.length === 18
                                            ? '标记为已修复'
                                            : '自动生成并标记'}
                                        </Button>
                                      </div>
                                    )}
                                  </TableCell>
                                </TableRow>
                              )
                            })}
                        </TableBody>
                      </Table>
                    </div>
                  </>
                )}
              </CardContent>
            </Card>
          </TabsContent>
        )}
      </Tabs>

      <div className='flex justify-end gap-2'>
        <Button variant='outline' onClick={() => refetch()} disabled={isSubmitting || isRetrying}>
          <RefreshCw className='mr-2 h-4 w-4' />
          刷新报告
        </Button>
        <Button
          onClick={() => submitResolutions()}
          disabled={!selectedFlowId || isSubmitting || isRetrying || fixedIssues.size === 0}
        >
          {isSubmitting ? (
            <>
              <RefreshCw className='mr-2 h-4 w-4 animate-spin' />
              提交修复结果中...
            </>
          ) : (
            <>
              <CheckCircle2 className='mr-2 h-4 w-4' />
              提交修复结果并继续流程
            </>
          )}
        </Button>
      </div>

      <Alert>
        <AlertTriangle className='h-4 w-4' />
        <AlertTitle>提示</AlertTitle>
        <AlertDescription>
          <p className='text-sm'>
            请修复上述问题，修复完成后点击"提交修复结果并继续流程"按钮。
            系统会将修复结果保存到后端，然后重新运行流程。如果所有问题已解决，流程将继续执行。
          </p>
        </AlertDescription>
      </Alert>
    </div>
  )
}


