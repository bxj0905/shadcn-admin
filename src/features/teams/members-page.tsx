import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Button } from '@/components/ui/button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Checkbox } from '@/components/ui/checkbox'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import type { TeamMember } from '@/services/teams'
import { addTeamMember, deleteTeamMember, fetchTeamMembers, updateTeamMember } from '@/services/teams'
import { fetchUsers } from '@/services/users'
import type { User } from '@/features/users/data/schema'
import { useState } from 'react'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { ConfigDrawer } from '@/components/config-drawer'
import { ProfileDropdown } from '@/components/profile-dropdown'
import { TeamMembersTable } from '@/features/teams/components/team-members-table'

export function TeamMembersPage({ teamId }: { teamId: string }) {
  const queryClient = useQueryClient()
  const [selectedUserIds, setSelectedUserIds] = useState<string[]>([])
  const [selectedRole, setSelectedRole] = useState<'owner' | 'maintainer' | 'member'>('member')
  const [submitting, setSubmitting] = useState(false)

  const { data: members = [] } = useQuery<TeamMember[]>({
    queryKey: ['team-members', teamId],
    queryFn: () => fetchTeamMembers(teamId),
  })

  const { data: users = [] } = useQuery<User[]>({
    queryKey: ['users'],
    queryFn: fetchUsers,
  })
  const memberUserIds = new Set(members.map((m) => String(m.userId)))
  const selectableUsers = users.filter((u) => !memberUserIds.has(u.id))
  const allSelected =
    selectableUsers.length > 0 &&
    selectableUsers.every((u) => selectedUserIds.includes(u.id))

  const onInvite = async () => {
    if (!selectedUserIds.length) return
    try {
      setSubmitting(true)
      await Promise.all(
        selectedUserIds.map((userId) =>
          addTeamMember(teamId, { userId, role: selectedRole }),
        ),
      )
      await queryClient.invalidateQueries({ queryKey: ['team-members', teamId] })
    } finally {
      setSubmitting(false)
    }
  }

  const onRoleChange = async (member: TeamMember, role: 'owner' | 'maintainer' | 'member') => {
    await updateTeamMember(teamId, member.id, { role })
    await queryClient.invalidateQueries({ queryKey: ['team-members', teamId] })
  }

  const onRemove = async (member: TeamMember) => {
    await deleteTeamMember(teamId, member.id)
    await queryClient.invalidateQueries({ queryKey: ['team-members', teamId] })
  }

  return (
    <>
      <Header fixed>
        <Search />
        <div className='ms-auto flex items-center space-x-4'>
          <ThemeSwitch />
          <ConfigDrawer />
          <ProfileDropdown />
        </div>
      </Header>

      <Main className='flex flex-1 flex-col gap-4 sm:gap-6'>
        <div className='flex flex-wrap items-end justify-between gap-2'>
          <div>
            <h2 className='text-2xl font-bold tracking-tight'>团队成员</h2>
          </div>
        </div>

        <div className='space-y-4'>
          <div className='flex flex-wrap items-center gap-4'>
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button
                  variant='outline'
                  size='sm'
                  className='bg-muted/25 text-muted-foreground hover:bg-accent h-8 w-[150px] justify-start rounded-md text-sm font-normal shadow-none lg:w-[250px]'
                >
                  {selectedUserIds.length ? `已选 ${selectedUserIds.length} 人` : '选择用户'}
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent className='w-[--radix-dropdown-menu-trigger-width]'>
                <DropdownMenuLabel>邀请用户</DropdownMenuLabel>
                <DropdownMenuItem
                  onSelect={(event) => {
                    event.preventDefault()
                    if (allSelected) {
                      setSelectedUserIds([])
                    } else {
                      setSelectedUserIds(selectableUsers.map((u) => u.id))
                    }
                  }}
                  className='gap-2'
                >
                  <Checkbox checked={allSelected} />
                  <span className='text-sm'>全选（仅未加入成员）</span>
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                {users.map((user) => {
                  const isMember = memberUserIds.has(user.id)
                  const checked = isMember || selectedUserIds.includes(user.id)
                  return (
                    <DropdownMenuItem
                      key={user.id}
                      onSelect={(event) => {
                        event.preventDefault()
                        if (isMember) return
                        setSelectedUserIds((prev) =>
                          checked
                            ? prev.filter((id) => id !== user.id)
                            : [...prev, user.id],
                        )
                      }}
                      className='gap-2'
                    >
                      <Checkbox checked={checked} disabled={isMember} />
                      <span className='text-xs'>
                        {user.username}
                        {user.firstName ? ` (${user.firstName})` : ''}
                      </span>
                    </DropdownMenuItem>
                  )
                })}
              </DropdownMenuContent>
            </DropdownMenu>

        <Select
          value={selectedRole}
          onValueChange={(value) =>
            setSelectedRole(value as 'owner' | 'maintainer' | 'member')
          }
        >
          <SelectTrigger className='w-32'>
            <SelectValue placeholder='角色' />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value='owner'>拥有者</SelectItem>
            <SelectItem value='maintainer'>维护者</SelectItem>
            <SelectItem value='member'>成员</SelectItem>
          </SelectContent>
        </Select>

        <Button size='sm' onClick={onInvite} disabled={submitting || !selectedUserIds.length}>
          邀请加入
        </Button>
      </div>

      <TeamMembersTable
        data={members.map((m) => ({
          ...m,
          teamRoleCode: m.teamRoleCode || 'member',
        }))}
        onRemove={onRemove}
      />
        </div>
      </Main>
    </>
  )
}
