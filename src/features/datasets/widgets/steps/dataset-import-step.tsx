import React from 'react'
import { Check } from 'lucide-react'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Card, CardContent } from '@/components/ui/card'
import { type DatasetType } from '@/services/datasets'

type DatasetImportStepProps = {
  datasetType: DatasetType
  hasImportedData?: boolean
  uploadStatus: 'idle' | 'uploading' | 'success' | 'error'
  uploadMessage: string
  uploadPercent: number
  uploadTotalFiles: number
  uploadedCount: number
  currentFile: string
  rawPrefix: string | null
  datasetCreated?: boolean
  onFilesChange: (event: React.ChangeEvent<HTMLInputElement>) => void
}

export function DatasetImportStep({
  datasetType,
  hasImportedData = false,
  uploadStatus,
  uploadMessage,
  uploadPercent,
  uploadTotalFiles,
  uploadedCount,
  currentFile,
  rawPrefix,
  datasetCreated = true,
  onFilesChange,
}: DatasetImportStepProps) {
  if (datasetType === 'duckdb') {
    return (
      <div className='space-y-6'>
        <div>
          <Label className='text-base font-semibold'>导入数据（DuckDB）</Label>
          <p className='text-muted-foreground mt-1 text-sm'>
            选择一个本地文件夹，将其中的所有文件上传到 RustFS 的 datasets 桶中，对应当前团队与数据集的目录
            （例如：team-&lt;teamId&gt;/dataset-&lt;datasetId&gt;/&lt;statDate&gt;/&lt;相对路径&gt;）。
          </p>
        </div>

        <Card>
          <CardContent className='flex flex-col gap-4 py-6'>
            {hasImportedData || uploadStatus === 'success' ? (
              <div className='flex flex-col items-start gap-3 rounded-lg border border-emerald-500/50 bg-emerald-50/50 p-4 text-sm dark:bg-emerald-900/20'>
                <div className='flex items-center gap-2 font-medium text-emerald-700 dark:text-emerald-400'>
                  <Check className='h-4 w-4' />
                  数据已导入
                </div>
                <p className='text-muted-foreground text-xs'>
                  {uploadMessage || '该数据集已经导入过数据，可以直接进入下一步选择处理模式。'}
                </p>
                {rawPrefix && (
                  <p className='text-xs text-muted-foreground'>
                    存储路径：{rawPrefix}
                  </p>
                )}
              </div>
            ) : (
              <div className='flex flex-col items-start gap-3 rounded-lg border border-dashed bg-muted/40 p-4 text-sm'>
                <div className='font-medium'>上传文件到对象存储</div>
                <p className='text-muted-foreground text-xs'>
                  支持一次选择整个文件夹，系统会保留文件在文件夹中的相对路径，并按照当前日期（statDate）归档到
                  RustFS 的 datasets 桶下对应的团队 / 数据集目录中。
                </p>
                <div>
                  <Input
                    type='file'
                    multiple
                    disabled={!datasetCreated || uploadStatus === 'uploading'}
                    onChange={onFilesChange}
                    {...({ webkitdirectory: '' } as unknown as React.InputHTMLAttributes<HTMLInputElement>)}
                  />
                </div>
                {!datasetCreated && (
                  <p className='text-xs text-amber-600'>
                    请先完成前几步并成功创建数据集后，再进行文件上传。
                  </p>
                )}
                {uploadStatus === 'error' && (
                  <p className='text-xs text-red-600'>{uploadMessage}</p>
                )}
                {uploadStatus === 'uploading' && (
                  <p className='text-xs text-muted-foreground'>
                    {uploadTotalFiles > 0
                      ? `正在同步第 ${uploadedCount + 1} / ${uploadTotalFiles} 个文件：${
                          currentFile || '...'
                        }（${uploadPercent}%）`
                      : `正在上传，请稍候……（${uploadPercent}%）`}
                  </p>
                )}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    )
  }

  return (
    <div className='space-y-6'>
      <div>
        <Label className='text-base font-semibold'>导入数据</Label>
        <p className='text-muted-foreground mt-1 text-sm'>
          当前数据库类型暂未接入统一导入向导，可在后续版本中补充针对 {datasetType} 的导入能力。
        </p>
      </div>

      <Card className='border-dashed'>
        <CardContent className='py-10 text-center text-sm text-muted-foreground'>
          目前仅 DuckDB 支持在此步骤直接上传文件，其它数据库类型请稍后通过专用导入流程完成数据准备。
        </CardContent>
      </Card>
    </div>
  )
}

