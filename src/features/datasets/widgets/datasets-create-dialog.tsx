import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription, DialogFooter } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group'
import { createDataset } from '@/services/datasets'

type DatasetsCreateDialogProps = {
  open: boolean
  onOpenChange: (open: boolean) => void
  teamId: string
  onCreated?: () => void
}

export function DatasetsCreateDialog({ open, onOpenChange, teamId, onCreated }: DatasetsCreateDialogProps) {
  const queryClient = useQueryClient()
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [type, setType] = useState<'duckdb' | 'pgsql'>('duckdb')

  const { mutateAsync, isPending } = useMutation({
    mutationFn: () =>
      createDataset(teamId, {
        name: name.trim(),
        description: description.trim() || undefined,
        type,
      }),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['team-datasets', teamId] })
      onCreated?.()
      setName('')
      setDescription('')
      setType('duckdb')
      onOpenChange(false)
    },
  })

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!name.trim()) return
    await mutateAsync()
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className='sm:w-[1200px] sm:max-w-[1200px]'>
        <form onSubmit={onSubmit} className='space-y-4'>
          <DialogHeader>
            <DialogTitle>新建数据集</DialogTitle>
            <DialogDescription>选择数据集类型，并为当前团队创建一个新的数据集。</DialogDescription>
          </DialogHeader>

          <div className='space-y-2'>
            <Label htmlFor='dataset-name'>名称</Label>
            <Input
              id='dataset-name'
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder='例如：销售分析数据集'
            />
          </div>

          <div className='space-y-2'>
            <Label htmlFor='dataset-description'>描述</Label>
            <Textarea
              id='dataset-description'
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder='可选：对这个数据集做一个简单说明'
              rows={3}
            />
          </div>

          <div className='space-y-2'>
            <Label>类型</Label>
            <RadioGroup
              value={type}
              onValueChange={(v) => setType(v as 'duckdb' | 'pgsql')}
              className='grid gap-3 sm:grid-cols-2'
            >
              <Label
                htmlFor='dataset-type-duckdb'
                className='border-input hover:bg-accent flex cursor-pointer flex-col items-start gap-2 rounded-md border p-3'
              >
                <div className='flex w-full items-center gap-3'>
                  <RadioGroupItem id='dataset-type-duckdb' value='duckdb' />
                  <div className='flex h-9 w-9 items-center justify-center rounded-md bg-primary/10'>
                    <img
                      src='/images/DuckDB_icon-lightmode.svg'
                      alt='DuckDB'
                      className='block h-7 w-7 dark:hidden'
                    />
                    <img
                      src='/images/DuckDB_icon-darkmode.svg'
                      alt='DuckDB'
                      className='hidden h-7 w-7 dark:block'
                    />
                  </div>
                  <div className='min-w-0 flex-1'>
                    <div className='font-medium'>DuckDB</div>
                    <div className='text-muted-foreground text-xs'>存储在 MinIO 的文件型数据集，适合本地分析。</div>
                  </div>
                </div>
              </Label>

              <Label
                htmlFor='dataset-type-pgsql'
                className='border-input hover:bg-accent flex cursor-pointer flex-col items-start gap-2 rounded-md border p-3'
              >
                <div className='flex w-full items-center gap-3'>
                  <RadioGroupItem id='dataset-type-pgsql' value='pgsql' />
                  <div className='flex h-9 w-9 items-center justify-center rounded-md bg-primary/10'>
                    <img
                      src='/images/postgresql-icon.svg'
                      alt='PostgreSQL'
                      className='h-7 w-7'
                    />
                  </div>
                  <div className='min-w-0 flex-1'>
                    <div className='font-medium'>PostgreSQL</div>
                    <div className='text-muted-foreground text-xs'>为团队自动创建一个独立的 PostgreSQL 数据库。</div>
                  </div>
                </div>
              </Label>
            </RadioGroup>
          </div>

          <DialogFooter>
            <Button type='button' variant='outline' onClick={() => onOpenChange(false)} disabled={isPending}>
              取消
            </Button>
            <Button type='submit' disabled={isPending || !name.trim()}>
              {isPending ? '创建中...' : '创建'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}
