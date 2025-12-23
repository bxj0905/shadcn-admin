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
import {
  deletePrefectFlow,
  updatePrefectFlow,
  runPrefectFlow,
  type PrefectFlow,
} from '@/services/prefect'
import { useTranslation } from 'react-i18next'
import { toast } from 'sonner'
import { PrefectFlowEditDialog } from './prefect-flow-edit-dialog'

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
      <PrefectFlowEditDialog open={editCodeOpen} onOpenChange={setEditCodeOpen} flow={flow} />
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

