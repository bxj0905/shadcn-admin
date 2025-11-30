import React, { useState } from 'react'
import useDialogState from '@/hooks/use-dialog-state'
import type { Team } from '../data/schema'

export type TeamsDialogType = 'add' | 'members'

type TeamsContextType = {
  open: TeamsDialogType | null
  setOpen: (str: TeamsDialogType | null) => void
  currentTeam: Team | null
  setCurrentTeam: React.Dispatch<React.SetStateAction<Team | null>>
}

const TeamsContext = React.createContext<TeamsContextType | null>(null)

export function TeamsProvider({ children }: { children: React.ReactNode }) {
  const [open, setOpen] = useDialogState<TeamsDialogType>(null)
  const [currentTeam, setCurrentTeam] = useState<Team | null>(null)

  return (
    <TeamsContext value={{ open, setOpen, currentTeam, setCurrentTeam }}>
      {children}
    </TeamsContext>
  )
}

// eslint-disable-next-line react-refresh/only-export-components
export const useTeams = () => {
  const teamsContext = React.useContext(TeamsContext)

  if (!teamsContext) {
    throw new Error('useTeams has to be used within <TeamsProvider>')
  }

  return teamsContext
}
