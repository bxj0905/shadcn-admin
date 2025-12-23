import { useEffect, useState } from 'react'
import { DotsHorizontalIcon } from '@radix-ui/react-icons'
import type { Row } from '@tanstack/react-table'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Button } from '@/components/ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { ConfirmDialog } from '@/components/confirm-dialog'
import { CodeEditor } from '@/components/ui/shadcn-io/code-editor'
import {
  deletePrefectFlow,
  updatePrefectFlow,
  runPrefectFlow,
  fetchPrefectFlowCode,
  updatePrefectFlowCode,
  testPrefectFlow,
  type PrefectFlow,
} from '@/services/prefect'
import { useTranslation } from 'react-i18next'
import { toast } from 'sonner'

type PrefectFlowsRowActionsProps = {
  row: Row<PrefectFlow>
}

export function PrefectFlowsRowActions({ row }: PrefectFlowsRowActionsProps) {
  const flow = row.original
  const { t } = useTranslation()
  const [editTagsOpen, setEditTagsOpen] = useState(false)
  const [deleteOpen, setDeleteOpen] = useState(false)
  const [editCodeOpen, setEditCodeOpen] = useState(false)

  const runMutation = useMutation({
    mutationFn: (flowId: string) => runPrefectFlow(flowId),
    onSuccess: () => {
      toast.success(t('prefect.actions.runFlow.success'), {
        description: t('prefect.actions.runFlow.successDesc', { name: flow.name }),
      })
    },
    onError: (error) => {
      toast.error(t('prefect.actions.runFlow.error'), {
        description:
          error instanceof Error
            ? error.message
            : t('prefect.actions.runFlow.errorDesc'),
      })
    },
  })

  const handleRunFlow = () => {
    runMutation.mutate(flow.id)
  }

  return (
    <>
      <DropdownMenu modal={false}>
        <DropdownMenuTrigger asChild>
          <Button
            variant='ghost'
            className='flex size-8 p-0 data-[state=open]:bg-muted'
          >
            <DotsHorizontalIcon className='size-4' />
            <span className='sr-only'>{t('prefect.actions.openMenu')}</span>
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align='end' className='w-[200px]'>
          <DropdownMenuLabel>{flow.name}</DropdownMenuLabel>
          <DropdownMenuSeparator />
          <DropdownMenuItem
            onClick={handleRunFlow}
            disabled={runMutation.isPending}
          >
            <svg
              xmlns='http://www.w3.org/2000/svg'
              width='16'
              height='16'
              viewBox='0 0 24 24'
              fill='none'
              stroke='currentColor'
              strokeWidth='2'
              strokeLinecap='round'
              strokeLinejoin='round'
              className='mr-2 size-4'
            >
              <polygon points='5 3 19 12 5 21 5 3' />
            </svg>
            {t('prefect.actions.runFlow')}
          </DropdownMenuItem>
          <DropdownMenuItem onClick={() => setEditCodeOpen(true)}>
            {t('prefect.actions.editCode')}
          </DropdownMenuItem>
          <DropdownMenuItem onClick={() => setEditTagsOpen(true)}>
            {t('prefect.actions.editTags')}
          </DropdownMenuItem>
          <DropdownMenuSeparator />
          <DropdownMenuItem
            onClick={() => setDeleteOpen(true)}
            className='text-destructive'
          >
            {t('prefect.actions.deleteFlow')}
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>

      <PrefectFlowEditTagsDialog
        open={editTagsOpen}
        onOpenChange={setEditTagsOpen}
        flow={flow}
      />
      <PrefectFlowEditCodeDialog
        open={editCodeOpen}
        onOpenChange={setEditCodeOpen}
        flow={flow}
      />
      <PrefectFlowDeleteDialog
        open={deleteOpen}
        onOpenChange={setDeleteOpen}
        flow={flow}
      />
    </>
  )
}

type PrefectFlowEditTagsDialogProps = {
  open: boolean
  onOpenChange: (open: boolean) => void
  flow: PrefectFlow
}

function PrefectFlowEditTagsDialog({
  open,
  onOpenChange,
  flow,
}: PrefectFlowEditTagsDialogProps) {
  const { t } = useTranslation()
  const [tagsInput, setTagsInput] = useState((flow.tags ?? []).join(', '))

  const queryClient = useQueryClient()
  const mutation = useMutation({
    mutationFn: (payload: { id: string; tags: string[] }) =>
      updatePrefectFlow(payload.id, { tags: payload.tags }),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['prefect', 'flows'] })
    },
  })

  const handleConfirm = () => {
    const tags = tagsInput
      .split(',')
      .map((t) => t.trim())
      .filter((t) => t.length > 0)

    mutation.mutate(
      { id: flow.id, tags },
      {
        onSuccess: () => {
          onOpenChange(false)
        },
      },
    )
  }

  const disabled = mutation.isPending

  return (
    <ConfirmDialog
      open={open}
      onOpenChange={(nextOpen) => {
        if (!nextOpen) {
          setTagsInput((flow.tags ?? []).join(', '))
        }
        onOpenChange(nextOpen)
      }}
      handleConfirm={handleConfirm}
      disabled={disabled}
      isLoading={mutation.isPending}
      title={t('prefect.dialog.editTags.title', { name: flow.name })}
      desc={
        <div className='space-y-4'>
          <div>
            <Label htmlFor='prefect-flow-tags-edit'>
              {t('prefect.dialog.editTags.label')}
            </Label>
            <Input
              id='prefect-flow-tags-edit'
              value={tagsInput}
              onChange={(e) => setTagsInput(e.target.value)}
              placeholder={t('prefect.dialog.editTags.placeholder')}
            />
          </div>
        </div>
      }
      confirmText={t('prefect.dialog.editTags.confirm')}
    />
  )
}

type PrefectFlowDeleteDialogProps = {
  open: boolean
  onOpenChange: (open: boolean) => void
  flow: PrefectFlow
}

function PrefectFlowDeleteDialog({
  open,
  onOpenChange,
  flow,
}: PrefectFlowDeleteDialogProps) {
  const { t } = useTranslation()
  const queryClient = useQueryClient()
  const mutation = useMutation({
    mutationFn: (id: string) => deletePrefectFlow(id),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['prefect', 'flows'] })
    },
  })

  const handleConfirm = () => {
    mutation.mutate(flow.id, {
      onSuccess: () => {
        onOpenChange(false)
      },
    })
  }

  const disabled = mutation.isPending

  return (
    <ConfirmDialog
      open={open}
      onOpenChange={onOpenChange}
      handleConfirm={handleConfirm}
      disabled={disabled}
      isLoading={mutation.isPending}
      title={t('prefect.dialog.delete.title')}
      desc={
        <div className='space-y-2'>
          <p>
            {t('prefect.dialog.delete.desc1', { name: flow.name })}
          </p>
          <p>{t('prefect.dialog.delete.desc2')}</p>
        </div>
      }
      confirmText={t('prefect.dialog.delete.confirm')}
      destructive
    />
  )
}

type PrefectFlowEditCodeDialogProps = {
  open: boolean
  onOpenChange: (open: boolean) => void
  flow: PrefectFlow
}

function PrefectFlowEditCodeDialog({
  open,
  onOpenChange,
  flow,
}: PrefectFlowEditCodeDialogProps) {
  const { t } = useTranslation()
  const queryClient = useQueryClient()
  const [code, setCode] = useState('')
  const [isLoadingCode, setIsLoadingCode] = useState(false)
  const [lastTestResult, setLastTestResult] = useState<string | null>(null)

  // 打开弹窗时拉取当前代码
  useEffect(() => {
    if (!open) return
    let cancelled = false
    ;(async () => {
      setIsLoadingCode(true)
      try {
        const res = await fetchPrefectFlowCode(flow.id)
        if (!cancelled) {
          setCode(res.code ?? '')
        }
      } catch (error) {
        console.error(error)
        if (!cancelled) {
          toast.error(t('prefect.dialog.editCode.loadError'))
        }
      } finally {
        if (!cancelled) {
          setIsLoadingCode(false)
        }
      }
    })()

    return () => {
      cancelled = true
    }
  }, [open, flow.id, t])

  const mutation = useMutation({
    mutationFn: (payload: { id: string; code: string }) =>
      updatePrefectFlowCode(payload.id, payload.code),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['prefect', 'flows'] })
    },
  })

  const testMutation = useMutation({
    mutationFn: (payload: { id: string; code: string }) =>
      testPrefectFlow(payload.id, payload.code),
    onSuccess: (result) => {
      const status = result.status
      const msg =
        status === 'COMPLETED'
          ? t('prefect.dialog.editCode.testSuccess')
          : status === 'TIMEOUT'
            ? t('prefect.dialog.editCode.testTimeout')
            : t('prefect.dialog.editCode.testFailed', {
                state: result.state_type,
              })
      setLastTestResult(msg)
      toast.info(msg)
    },
    onError: (error) => {
      console.error(error)
      const msg = t('prefect.dialog.editCode.testError')
      setLastTestResult(msg)
      toast.error(msg)
    },
  })

  const handleConfirm = () => {
    const trimmed = code.trim()
    if (!trimmed) return
    mutation.mutate(
      { id: flow.id, code: trimmed },
      {
        onSuccess: () => {
          onOpenChange(false)
        },
        onError: (error) => {
          console.error(error)
          toast.error(t('prefect.dialog.editCode.saveError'))
        },
      },
    )
  }

  const disabled = mutation.isPending || isLoadingCode || testMutation.isPending

  const handleTest = () => {
    const trimmed = code.trim()
    if (!trimmed) return
    setLastTestResult(null)
    testMutation.mutate({ id: flow.id, code: trimmed })
  }

  return (
    <ConfirmDialog
      open={open}
      onOpenChange={(nextOpen) => {
        if (!nextOpen) {
          setCode('')
        }
        onOpenChange(nextOpen)
      }}
      handleConfirm={handleConfirm}
      disabled={disabled}
      isLoading={mutation.isPending}
      title={t('prefect.dialog.editCode.title', { name: flow.name })}
      className='w-[1200px] max-w-none h-[800px] max-h-[95vh]'
      desc={
        <div className='flex h-[620px] flex-col space-y-4'>
          {isLoadingCode ? (
            <div className='text-sm text-muted-foreground'>
              {t('prefect.dialog.editCode.loading')}
            </div>
          ) : (
            <>
              <div className='flex flex-1 flex-col space-y-2'>
                <Label htmlFor='prefect-flow-code-edit'>
                  {t('prefect.dialog.editCode.codeLabel')}
                </Label>
                <CodeEditor
                  value={code}
                  onChange={setCode}
                  language='python'
                  height='100%'
                  placeholder={t('prefect.dialog.create.codePlaceholder')}
                  className='border-border flex-1'
                />
              </div>
              <div className='flex items-center justify-between pt-2'>
                <Button
                  type='button'
                  size='sm'
                  variant='outline'
                  onClick={handleTest}
                  disabled={testMutation.isPending || isLoadingCode}
                >
                  {testMutation.isPending
                    ? t('prefect.dialog.editCode.testing')
                    : t('prefect.dialog.editCode.testButton')}
                </Button>
                <div className='text-xs text-muted-foreground text-right min-h-[1.25rem]'>
                  {lastTestResult}
                </div>
              </div>
            </>
          )}
        </div>
      }
      confirmText={t('prefect.dialog.editCode.confirm')}
    />
  )
}
