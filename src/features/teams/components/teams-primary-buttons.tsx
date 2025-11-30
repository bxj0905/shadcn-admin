import { Button } from '@/components/ui/button'
import { useTeams } from './teams-provider'

export function TeamsPrimaryButtons() {
  const { setOpen } = useTeams()

  return (
    <div className='flex gap-2'>
      <Button
        size='sm'
        onClick={() => {
          setOpen('add')
        }}
      >
        新增团队
      </Button>
    </div>
  )
}
