import { TeamActionDialog } from './teams-action-dialog'
import { TeamMembersDialog } from './teams-members-dialog'
import { useTeams } from './teams-provider'

export function TeamsDialogs() {
  const { open, setOpen, currentTeam, setCurrentTeam } = useTeams()

  return (
    <>
      <TeamActionDialog
        key='team-add'
        open={open === 'add'}
        onOpenChange={() => setOpen('add')}
      />

      {currentTeam && (
        <TeamMembersDialog
          key={`team-members-${currentTeam.id}`}
          open={open === 'members'}
          onOpenChange={() => {
            setOpen('members')
            setTimeout(() => {
              setCurrentTeam(null)
            }, 500)
          }}
          team={currentTeam}
        />
      )}
    </>
  )
}
