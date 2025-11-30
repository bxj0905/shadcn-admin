import { create } from 'zustand'
import type { Team } from '@/services/teams'

type TeamState = {
  currentTeam: Team | null
  setCurrentTeam: (team: Team | null) => void
}

export const useTeamStore = create<TeamState>((set) => ({
  currentTeam: null,
  setCurrentTeam: (team) => set({ currentTeam: team }),
}))
