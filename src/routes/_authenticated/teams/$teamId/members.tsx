import { createFileRoute } from '@tanstack/react-router'
import { TeamMembersPage } from '@/features/teams/members-page'

export const Route = createFileRoute('/_authenticated/teams/$teamId/members')({
  component: function TeamMembersRouteComponent() {
    const { teamId } = Route.useParams()
    return <TeamMembersPage teamId={teamId} />
  },
})
