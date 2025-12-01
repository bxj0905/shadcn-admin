import { useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from '@/components/ui/select'
import { ScrollArea } from '@/components/ui/scroll-area'
import {
  fetchDatasetMembers,
  upsertDatasetMember,
  deleteDatasetMember,
  type Dataset,
} from '@/services/datasets'
import { fetchTeamMembers } from '@/services/teams'

type DatasetsMembersDialogProps = {
  dataset: Dataset
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function DatasetsMembersDialog({ dataset, open, onOpenChange }: DatasetsMembersDialogProps) {
  const queryClient = useQueryClient()
  const teamId = dataset.teamId

  const { data: members = [] } = useQuery({
    queryKey: ['dataset-members', dataset.id],
    queryFn: () => fetchDatasetMembers(dataset.id),
    enabled: open,
  })

  const { data: teamMembers = [] } = useQuery({
    queryKey: ['team-members', teamId],
    queryFn: () => fetchTeamMembers(teamId),
    enabled: open,
  })

  const membersByUserId = useMemo(() => {
    const map = new Map<string, (typeof members)[number]>()
    for (const m of members) {
      map.set(m.userId, m)
    }
    return map
  }, [members])

  const { mutateAsync: setPermission, isPending } = useMutation({
    mutationFn: async ({ userId, permission }: { userId: string; permission: 'viewer' | 'editor' | null }) => {
      if (!permission) {
        const record = membersByUserId.get(userId)
        if (record) {
          await deleteDatasetMember(dataset.id, record.id)
        }
        return
      }
      await upsertDatasetMember(dataset.id, { userId, permission })
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['dataset-members', dataset.id] })
    },
  })

  const onChangePermission = async (userId: string, value: string) => {
    const v = value === 'none' ? null : (value as 'viewer' | 'editor')
    await setPermission({ userId, permission: v })
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className='max-w-lg'>
        <DialogHeader>
          <DialogTitle>成员权限</DialogTitle>
          <DialogDescription>
            为团队成员配置此数据集的访问权限（只读或可编辑）。
          </DialogDescription>
        </DialogHeader>

        <div className='space-y-2'>
          <Label className='text-xs text-muted-foreground'>数据集：{dataset.name}</Label>
        </div>

        <ScrollArea className='mt-2 max-h-80 pr-2'>
          <div className='space-y-3'>
            {teamMembers.map((member) => {
              const record = membersByUserId.get(member.userId)
              const value = record?.permission ?? 'none'

              return (
                <div key={member.id} className='flex items-center justify-between gap-3 rounded-md border px-3 py-2 text-sm'>
                  <div className='min-w-0 flex-1'>
                    <div className='font-medium leading-none'>{member.name || member.username}</div>
                    <div className='text-muted-foreground mt-1 text-xs'>@{member.username}</div>
                  </div>
                  <Select
                    value={value}
                    onValueChange={(v) => onChangePermission(member.userId, v)}
                    disabled={isPending}
                  >
                    <SelectTrigger className='w-[120px]'>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value='none'>无</SelectItem>
                      <SelectItem value='viewer'>只读</SelectItem>
                      <SelectItem value='editor'>可编辑</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              )
            })}
          </div>
        </ScrollArea>

        <div className='mt-3 flex justify-end gap-2'>
          <Button type='button' variant='outline' size='sm' onClick={() => onOpenChange(false)} disabled={isPending}>
            关闭
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  )
}
