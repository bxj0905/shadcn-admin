import React from 'react'
import { Label } from '@/components/ui/label'
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group'
import { cn } from '@/lib/utils'
import { type DatasetType } from '@/services/datasets'

type DatasetTypeStepProps = {
  type: DatasetType
  onTypeChange: (type: DatasetType) => void
}

export function DatasetTypeStep({ type, onTypeChange }: DatasetTypeStepProps) {
  return (
    <div className='space-y-6'>
      <div>
        <Label className='text-base font-semibold'>选择数据库类型</Label>
        <p className='text-muted-foreground mt-1 text-sm'>
          选择数据集的底层存储类型，不同类型会影响后续导入方式与查询能力。
        </p>
      </div>

      <div className='space-y-4'>
        <div className='mx-auto w-full max-w-[1120px] px-1'>
          <RadioGroup
            value={type}
            onValueChange={(v) => onTypeChange(v as DatasetType)}
            className='grid gap-4 sm:grid-cols-1 md:grid-cols-2 xl:grid-cols-3'
          >
            {/* DuckDB */}
            <Label
              htmlFor='dataset-type-duckdb'
              className={cn(
                'border-input flex min-h-[120px] cursor-pointer items-center rounded-lg border p-4 text-left text-sm transition-colors',
                type === 'duckdb' ? 'bg-muted ring-2 ring-primary' : 'hover:bg-muted/40',
              )}
            >
              <div className='flex w-full items-center gap-3'>
                <RadioGroupItem id='dataset-type-duckdb' value='duckdb' />
                <div className='flex items-center gap-4'>
                  <div className='flex h-14 items-center justify-center rounded-md bg-primary/10 px-4'>
                    <img
                      src='/images/DuckDB_icon-lightmode.svg'
                      alt='DuckDB'
                      className='block h-11 w-auto dark:hidden'
                    />
                    <img
                      src='/images/DuckDB_icon-darkmode.svg'
                      alt='DuckDB'
                      className='hidden h-11 w-auto dark:block'
                    />
                  </div>
                  <div className='min-w-0'>
                    <div className='text-sm font-medium'>DuckDB</div>
                  </div>
                </div>
              </div>
            </Label>

            {/* PostgreSQL */}
            <Label
              htmlFor='dataset-type-pgsql'
              className={cn(
                'border-input flex min-h-[120px] cursor-pointer items-center rounded-lg border p-4 text-left text-sm transition-colors',
                type === 'pgsql' ? 'bg-muted ring-2 ring-primary' : 'hover:bg-muted/40',
              )}
            >
              <div className='flex w-full items-center gap-3'>
                <RadioGroupItem id='dataset-type-pgsql' value='pgsql' />
                <div className='flex items-center gap-4'>
                  <div className='flex h-14 items-center justify-center rounded-md bg-primary/10 px-4'>
                    <img
                      src='/images/postgresql-icon.svg'
                      alt='PostgreSQL'
                      className='h-11 w-auto'
                    />
                  </div>
                  <div className='min-w-0'>
                    <div className='text-sm font-medium'>PostgreSQL</div>
                  </div>
                </div>
              </div>
            </Label>

            {/* MySQL */}
            <Label
              htmlFor='dataset-type-mysql'
              className={cn(
                'border-input flex min-h-[120px] cursor-pointer items-center rounded-lg border p-4 text-left text-sm transition-colors',
                type === 'mysql' ? 'bg-muted ring-2 ring-primary' : 'hover:bg-muted/40',
              )}
            >
              <div className='flex w-full items-center gap-3'>
                <RadioGroupItem id='dataset-type-mysql' value='mysql' />
                <div className='flex items-center gap-4'>
                  <div className='flex h-14 items-center justify-center rounded-md bg-primary/10 px-4'>
                    <img
                      src='/images/mysql.png'
                      alt='MySQL'
                      className='h-11 w-auto'
                    />
                  </div>
                  <div className='min-w-0'>
                    <div className='text-sm font-medium'>MySQL</div>
                  </div>
                </div>
              </div>
            </Label>

            {/* OceanBase */}
            <Label
              htmlFor='dataset-type-oceanbase'
              className={cn(
                'border-input flex min-h-[120px] cursor-pointer items-center rounded-lg border p-4 text-left text-sm transition-colors',
                type === 'oceanbase' ? 'bg-muted ring-2 ring-primary' : 'hover:bg-muted/40',
              )}
            >
              <div className='flex w-full items-center gap-3'>
                <RadioGroupItem id='dataset-type-oceanbase' value='oceanbase' />
                <div className='flex items-center gap-4'>
                  <div className='flex h-14 items-center justify-center rounded-md bg-primary/10 px-4'>
                    <img
                      src='/images/oceanbase.png'
                      alt='OceanBase'
                      className='h-11 w-auto'
                    />
                  </div>
                  <div className='min-w-0'>
                    <div className='text-sm font-medium'>OceanBase</div>
                  </div>
                </div>
              </div>
            </Label>

            {/* MSSQL */}
            <Label
              htmlFor='dataset-type-mssql'
              className={cn(
                'border-input flex min-h-[120px] cursor-pointer items-center rounded-lg border p-4 text-left text-sm transition-colors',
                type === 'mssql' ? 'bg-muted ring-2 ring-primary' : 'hover:bg-muted/40',
              )}
            >
              <div className='flex w-full items-center gap-3'>
                <RadioGroupItem id='dataset-type-mssql' value='mssql' />
                <div className='flex items-center gap-4'>
                  <div className='flex h-14 w-14 items-center justify-center rounded-md bg-primary/10 text-xs font-semibold text-primary'>
                    MS
                  </div>
                  <div className='min-w-0'>
                    <div className='text-sm font-medium'>MSSQL</div>
                  </div>
                </div>
              </div>
            </Label>

            {/* TiDB */}
            <Label
              htmlFor='dataset-type-tidb'
              className={cn(
                'border-input flex min-h-[120px] cursor-pointer items-center rounded-lg border p-4 text-left text-sm transition-colors',
                type === 'tidb' ? 'bg-muted ring-2 ring-primary' : 'hover:bg-muted/40',
              )}
            >
              <div className='flex w-full items-center gap-3'>
                <RadioGroupItem id='dataset-type-tidb' value='tidb' />
                <div className='flex items-center gap-4'>
                  <div className='flex h-14 items-center justify-center rounded-md bg-primary/10 px-4'>
                    <img
                      src='/images/tidb.png'
                      alt='TiDB'
                      className='h-11 w-auto'
                    />
                  </div>
                  <div className='min-w-0'>
                    <div className='text-sm font-medium'>TiDB</div>
                  </div>
                </div>
              </div>
            </Label>
          </RadioGroup>
        </div>
      </div>
    </div>
  )
}

