import { memo } from 'react'
import { Handle, Position, type NodeProps } from '@xyflow/react'
import { cn } from '@/lib/utils'
import { BaseNode } from '@/components/workflow/primitives/base-node'
import {
  NodeHeader,
  NodeHeaderTitle,
  NodeHeaderIcon,
} from '@/components/workflow/primitives/node-header'
import { Badge } from '@/components/ui/badge'
import type { PrefectFlowNodeData } from './prefect-flow-editor'

export type PrefectFlowNodeType = {
  id: string
  type: 'prefect-flow-node'
  position: { x: number; y: number }
  data: PrefectFlowNodeData
}

const flowTypeConfig = {
  main: {
    label: '主 Flow',
    color: 'bg-blue-500',
    borderColor: 'border-blue-500',
    badgeVariant: 'default' as const,
  },
  feature: {
    label: '功能 Flow',
    color: 'bg-green-500',
    borderColor: 'border-green-500',
    badgeVariant: 'secondary' as const,
  },
  subflow: {
    label: '子 Flow',
    color: 'bg-orange-500',
    borderColor: 'border-orange-500',
    badgeVariant: 'outline' as const,
  },
}

export function PrefectFlowNode({
  data,
  selected,
  width,
  height,
}: NodeProps<PrefectFlowNodeType>) {
  const { label, flowType, description } = data
  const config = flowTypeConfig[flowType] || flowTypeConfig.feature
  
  // 主 Flow 显示为大框（容器）
  const isMainFlow = flowType === 'main'
  const isContainer = isMainFlow

  return (
    <BaseNode
      selected={selected}
      className={cn(
        isContainer ? 'min-w-[600px] min-h-[400px]' : 'min-w-[200px]',
        selected && config.borderColor,
        isContainer && 'border-2',
      )}
      style={isContainer ? { width, height } : undefined}
    >
      {!isContainer && (
        <Handle
          type='target'
          position={Position.Top}
          className='!bg-primary !h-3 !w-3 !border-2 !border-background'
        />
      )}
      
      <NodeHeader>
        <NodeHeaderIcon>
          <div className={cn('h-2 w-2 rounded-full', config.color)} />
        </NodeHeaderIcon>
        <NodeHeaderTitle className={cn(isContainer ? 'text-base' : 'text-sm')}>
          {label || '未命名 Flow'}
        </NodeHeaderTitle>
        <Badge variant={config.badgeVariant} className='text-xs'>
          {config.label}
        </Badge>
      </NodeHeader>
      
      <div className={cn('space-y-2 px-3', isContainer && 'pb-4')}>
        {description && (
          <div className='text-xs text-muted-foreground line-clamp-2'>
            {description}
          </div>
        )}
        
        <div className='text-xs text-muted-foreground'>
          代码: {data.code ? `${data.code.length} 字符` : '未设置'}
        </div>
        
        {isContainer && (
          <div className='mt-4 pt-4 border-t border-border'>
            <div className='text-xs text-muted-foreground'>
              功能 Flow 将显示在此容器内
            </div>
          </div>
        )}
      </div>

      {!isContainer && (
        <Handle
          type='source'
          position={Position.Bottom}
          className='!bg-primary !h-3 !w-3 !border-2 !border-background'
        />
      )}
    </BaseNode>
  )
}

export default memo(PrefectFlowNode)

