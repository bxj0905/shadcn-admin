import { createFileRoute } from '@tanstack/react-router'
import { TeamMembersPage } from '@/features/teams/members-page'
import { useTeamStore } from '@/stores/team-store'

function TeamMembersWrapper() {
  const { currentTeam } = useTeamStore()

  if (!currentTeam) {
    return <div className='p-4 text-sm text-muted-foreground'>请先在左上角选择一个团队。</div>
  }

  return <TeamMembersPage teamId={currentTeam.id} />
}

export const Route = createFileRoute('/_authenticated/team-members')({
  component: TeamMembersWrapper,
})
