import * as React from 'react'
import { ChevronsUpDown, Plus } from 'lucide-react'
import { Link } from '@tanstack/react-router'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuShortcut,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import {
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  useSidebar,
} from '@/components/ui/sidebar'
import { useAuthStore } from '@/stores/auth-store'
import { useQuery } from '@tanstack/react-query'
import { fetchTeams, type Team } from '@/services/teams'
import { useTeamStore } from '@/stores/team-store'

export function TeamSwitcher() {
  const { isMobile } = useSidebar()
  const { auth } = useAuthStore()
  const { currentTeam, setCurrentTeam } = useTeamStore()

  const { data: teams = [] } = useQuery<Team[]>({
    queryKey: ['teams'],
    queryFn: fetchTeams,
  })

  const currentUser = auth.user
  const isAdmin = !!currentUser &&
    (currentUser.roles.includes('admin') || currentUser.roles.includes('super_admin'))
  const [activeTeam, setActiveTeam] = React.useState<Team | null>(currentTeam)

  React.useEffect(() => {
    if (!teams.length) return

    // 如果当前 store 里已经有团队，保持不变
    if (currentTeam) {
      setActiveTeam(currentTeam)
      return
    }

    // 从 localStorage 恢复上次选择的团队
    if (typeof window !== 'undefined') {
      const savedId = window.localStorage.getItem('current_team_id')
      if (savedId) {
        const matched = teams.find((t) => t.id === savedId)
        if (matched) {
          setCurrentTeam(matched)
          setActiveTeam(matched)
          return
        }
      }
    }

    // 没有保存或找不到匹配时，退回到第一个团队
    if (teams.length > 0) {
      setCurrentTeam(teams[0])
      setActiveTeam(teams[0])
      if (typeof window !== 'undefined') {
        window.localStorage.setItem('current_team_id', teams[0].id)
      }
    }
  }, [currentTeam, setCurrentTeam, teams])

  return (
    <SidebarMenu>
      <SidebarMenuItem>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <SidebarMenuButton
              size='lg'
              className='data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground'
            >
              <div className='bg-sidebar-primary text-sidebar-primary-foreground flex aspect-square size-8 items-center justify-center rounded-lg'>
                <span className='text-xs font-semibold'>
                  {activeTeam?.name?.charAt(0) || 'T'}
                </span>
              </div>
              <div className='grid flex-1 text-start text-sm leading-tight'>
                <span className='truncate font-semibold'>
                  {activeTeam?.name || '选择团队'}
                </span>
                <span className='truncate text-xs'>Team</span>
              </div>
              <ChevronsUpDown className='ms-auto' />
            </SidebarMenuButton>
          </DropdownMenuTrigger>
          <DropdownMenuContent
            className='w-(--radix-dropdown-menu-trigger-width) min-w-56 rounded-lg'
            align='start'
            side={isMobile ? 'bottom' : 'right'}
            sideOffset={4}
          >
            <DropdownMenuLabel className='text-muted-foreground text-xs'>Teams</DropdownMenuLabel>
            {teams.map((team, index) => (
              <DropdownMenuItem
                key={team.id}
                onClick={() => {
                  setActiveTeam(team)
                  setCurrentTeam(team)
                  if (typeof window !== 'undefined') {
                    window.localStorage.setItem('current_team_id', team.id)
                  }
                }}
                className='gap-2 p-2'
              >
                <div className='flex size-6 items-center justify-center rounded-sm border'>
                  <span className='text-xs font-semibold'>{team.name.charAt(0)}</span>
                </div>
                {team.name}
                <DropdownMenuShortcut>⌘{index + 1}</DropdownMenuShortcut>
              </DropdownMenuItem>
            ))}
            <DropdownMenuSeparator />
            {isAdmin && (
              <DropdownMenuItem asChild className='gap-2 p-2'>
                <Link to='/teams'>
                  <div className='bg-background flex size-6 items-center justify-center rounded-md border'>
                    <Plus className='size-4' />
                  </div>
                  <div className='text-muted-foreground font-medium'>Add team</div>
                </Link>
              </DropdownMenuItem>
            )}
          </DropdownMenuContent>
        </DropdownMenu>
      </SidebarMenuItem>
    </SidebarMenu>
  )
}
