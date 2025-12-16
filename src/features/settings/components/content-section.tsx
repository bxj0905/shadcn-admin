import { Separator } from '@/components/ui/separator'
import { cn } from '@/lib/utils'

type ContentSectionProps = {
  title: string
  desc: string
  children: React.JSX.Element
  fullWidth?: boolean
  scrollable?: boolean
}

export function ContentSection({
  title,
  desc,
  children,
  fullWidth = false,
  scrollable = true,
}: ContentSectionProps) {
  return (
    <div className='flex flex-1 flex-col'>
      <div className='flex-none'>
        <h3 className='text-lg font-medium'>{title}</h3>
        <p className='text-muted-foreground text-sm'>{desc}</p>
      </div>
      <Separator className='my-4 flex-none' />
      <div
        className={cn(
          'pe-4 pb-12',
          scrollable ? 'faded-bottom h-full w-full overflow-y-auto scroll-smooth' : 'w-full',
        )}
      >
        <div className={cn('-mx-1 px-1.5', fullWidth ? 'w-full lg:max-w-none' : 'lg:max-w-xl')}>
          {children}
        </div>
      </div>
    </div>
  )
}
