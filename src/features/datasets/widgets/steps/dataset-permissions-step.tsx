import React from 'react'
import { Label } from '@/components/ui/label'
import { Card, CardContent } from '@/components/ui/card'
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from '@/components/ui/select'
import { type Dataset, type DatasetMember } from '@/services/datasets'
import { type TeamMember } from '@/services/teams'

type DatasetPermissionsStepProps = {
  dataset: Dataset | null
  teamMembers: TeamMember[]
  datasetMembers: DatasetMember[]
  setMemberPending: boolean
  onChangePermission: (userId: string, value: string) => Promise<void>
}

export function DatasetPermissionsStep({
  dataset,
  teamMembers,
  datasetMembers,
  setMemberPending,
  onChangePermission,
}: DatasetPermissionsStepProps) {
  const membersByUserId = new Map<string, DatasetMember>()
  for (const m of datasetMembers) {
    membersByUserId.set(m.userId, m)
  }

  return (
    <div className='space-y-6'>
      <div>
        <Label className='text-base font-semibold'>权限设置</Label>
        <p className='text-muted-foreground mt-1 text-sm'>
          为团队成员配置此数据集的访问权限。创建者默认拥有管理员权限，且不可修改；系统超级管理员拥有读写权限。
        </p>
      </div>

      <Card>
        <CardContent className='pt-4'>
          {!dataset ? (
            <p className='text-muted-foreground text-sm'>正在创建数据集，请稍候……</p>
          ) : (
            <div className='space-y-2'>
              <div className='text-xs text-muted-foreground'>
                数据集：<span className='font-medium text-foreground'>{dataset.name}</span>
              </div>
              <div className='mt-3 space-y-3'>
                {teamMembers.map((member) => {
                  const record = membersByUserId.get(member.userId)
                  const value = record?.permission ?? 'none'

                  // 创建者（owner）行不可修改
                  const isCreator = dataset && String(dataset.createdBy) === member.userId

                  return (
                    <div
                      key={member.id}
                      className='flex items-center justify-between gap-3 rounded-md border px-3 py-2 text-sm'
                    >
                      <div className='min-w-0 flex-1'>
                        <div className='font-medium leading-none'>
                          {member.name || member.username}
                        </div>
                        <div className='text-muted-foreground mt-1 text-xs'>
                          @{member.username}
                          {isCreator && ' · 创建者（管理员，无法修改）'}
                        </div>
                      </div>
                      {isCreator ? (
                        <div className='rounded-full bg-emerald-500/10 px-3 py-1 text-xs font-medium text-emerald-400'>
                          管理员
                        </div>
                      ) : (
                        <Select
                          value={value}
                          onValueChange={(v) => onChangePermission(member.userId, v)}
                          disabled={setMemberPending || !dataset}
                        >
                          <SelectTrigger className='w-[140px]'>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value='none'>无</SelectItem>
                            <SelectItem value='viewer'>只读</SelectItem>
                            <SelectItem value='editor'>可编辑</SelectItem>
                          </SelectContent>
                        </Select>
                      )}
                    </div>
                  )
                })}
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

