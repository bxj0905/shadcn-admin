import { useEffect, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
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
import type { Team } from '../data/schema'
import type { TeamMember } from '@/services/teams'
import { addTeamMember, deleteTeamMember, fetchTeamMembers, updateTeamMember } from '@/services/teams'
import { fetchUsers } from '@/services/users'
import type { User } from '@/features/users/data/schema'

type TeamMembersDialogProps = {
  team: Team
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function TeamMembersDialog({ team, open, onOpenChange }: TeamMembersDialogProps) {
  const queryClient = useQueryClient()
  const [selectedUserIds, setSelectedUserIds] = useState<string[]>([])
  const [selectedRole, setSelectedRole] = useState<'owner' | 'maintainer' | 'member'>('member')
  const [submitting, setSubmitting] = useState(false)

  const { data: members = [] } = useQuery<TeamMember[]>({
    queryKey: ['team-members', team.id],
    queryFn: () => fetchTeamMembers(team.id),
    enabled: open,
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

  useEffect(() => {
    if (!open) {
      setSelectedUserIds([])
      setSelectedRole('member')
    }
  }, [open])

  const onInvite = async () => {
    if (!selectedUserIds.length) return
    try {
      setSubmitting(true)
      await Promise.all(
        selectedUserIds.map((userId) =>
          addTeamMember(team.id, { userId, role: selectedRole }),
        ),
      )
      await queryClient.invalidateQueries({ queryKey: ['team-members', team.id] })
    } finally {
      setSubmitting(false)
    }
  }

  const onRoleChange = async (member: TeamMember, role: 'owner' | 'maintainer' | 'member') => {
    await updateTeamMember(team.id, member.id, { role })
    await queryClient.invalidateQueries({ queryKey: ['team-members', team.id] })
  }

  const onRemove = async (member: TeamMember) => {
    await deleteTeamMember(team.id, member.id)
    await queryClient.invalidateQueries({ queryKey: ['team-members', team.id] })
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className='max-w-3xl'>
        <DialogHeader className='text-start'>
          <DialogTitle>团队成员 - {team.name}</DialogTitle>
          <DialogDescription>在这里管理团队成员及其角色。</DialogDescription>
        </DialogHeader>
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
                <SelectItem value='owner'>所有者</SelectItem>
                <SelectItem value='maintainer'>维护者</SelectItem>
                <SelectItem value='member'>成员</SelectItem>
              </SelectContent>
            </Select>

            <Button size='sm' onClick={onInvite} disabled={submitting || !selectedUserIds.length}>
              邀请加入
            </Button>
          </div>

          <div className='overflow-hidden rounded-md border'>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>用户名</TableHead>
                  <TableHead>姓名</TableHead>
                  <TableHead>角色</TableHead>
                  <TableHead className='w-24 text-right'>操作</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {members.length ? (
                  members.map((member) => (
                    <TableRow key={member.id}>
                      <TableCell>{member.username}</TableCell>
                      <TableCell>{member.name || '-'}</TableCell>
                      <TableCell>
                        <Select
                          value={member.teamRoleCode || 'member'}
                          onValueChange={(value) =>
                            onRoleChange(
                              member,
                              value as 'owner' | 'maintainer' | 'member'
                            )
                          }
                        >
                          <SelectTrigger className='w-32'>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value='owner'>所有者</SelectItem>
                            <SelectItem value='maintainer'>维护者</SelectItem>
                            <SelectItem value='member'>成员</SelectItem>
                          </SelectContent>
                        </Select>
                      </TableCell>
                      <TableCell className='text-right'>
                        <Button
                          variant='ghost'
                          size='sm'
                          onClick={() => onRemove(member)}
                          className='text-muted-foreground hover:text-red-500 hover:bg-red-500/10'
                        >
                          移除
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))
                ) : (
                  <TableRow>
                    <TableCell colSpan={4} className='h-24 text-center'>
                      暂无成员。
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
        </div>
        <DialogFooter>
          <Button variant='outline' onClick={() => onOpenChange(false)}>
            关闭
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
