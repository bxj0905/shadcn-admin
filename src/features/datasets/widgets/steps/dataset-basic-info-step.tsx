import React from 'react'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'

type DatasetBasicInfoStepProps = {
  name: string
  description: string
  nameDuplicated: boolean
  onNameChange: (name: string) => void
  onDescriptionChange: (description: string) => void
}

export function DatasetBasicInfoStep({
  name,
  description,
  nameDuplicated,
  onNameChange,
  onDescriptionChange,
}: DatasetBasicInfoStepProps) {
  return (
    <div className='space-y-6'>
      <div>
        <Label className='text-base font-semibold'>基础信息</Label>
        <p className='text-muted-foreground mt-1 text-sm'>
          配置数据集名称和简介，作为后续权限、导入和处理流程的基础。
        </p>
      </div>

      <div className='grid gap-6 md:grid-cols-[minmax(0,2fr)_minmax(0,3fr)]'>
        <div className='space-y-4'>
          <div className='space-y-2'>
            <Label htmlFor='dataset-name'>名称</Label>
            <Input
              id='dataset-name'
              value={name}
              onChange={(e) => onNameChange(e.target.value)}
              placeholder='例如：销售分析数据集'
            />
            {nameDuplicated && (
              <p className='text-xs text-red-500'>
                当前团队下已存在同名数据集，请换一个名称。
              </p>
            )}
          </div>

          <div className='space-y-2'>
            <Label htmlFor='dataset-description'>描述</Label>
            <Textarea
              id='dataset-description'
              value={description}
              onChange={(e) => onDescriptionChange(e.target.value)}
              placeholder='可选：对这个数据集做一个简单说明'
              rows={3}
            />
          </div>
        </div>

        <div className='space-y-3 rounded-lg border bg-muted/60 p-4 text-xs'>
          <div className='font-medium'>说明</div>
          <p className='text-muted-foreground'>
            数据集名称会在团队的「数据集列表」和下游分析应用中展示，建议使用清晰的业务语义命名，例如「五经普—主业务表」、「年度财报—明细」等。
          </p>
          <p className='text-muted-foreground'>
            描述字段用于补充更多上下文信息，方便其他成员理解数据来源、口径与用途。
          </p>
        </div>
      </div>
    </div>
  )
}

