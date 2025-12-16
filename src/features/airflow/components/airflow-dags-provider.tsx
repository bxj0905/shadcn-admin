import { createContext, useContext, useState, type ReactNode } from 'react'
import type { AirflowDag } from '@/services/airflow'

export type AirflowDagsContextValue = {
  selectedDag: AirflowDag | null
  setSelectedDag: (dag: AirflowDag | null) => void
  open: boolean
  openDrawer: (dag: AirflowDag) => void
  closeDrawer: () => void
}

const AirflowDagsContext = createContext<AirflowDagsContextValue | undefined>(
  undefined
)

export function AirflowDagsProvider({ children }: { children: ReactNode }) {
  const [selectedDag, setSelectedDag] = useState<AirflowDag | null>(null)
  const [open, setOpen] = useState(false)

  const openDrawer = (dag: AirflowDag) => {
    setSelectedDag(dag)
    setOpen(true)
  }

  const closeDrawer = () => {
    setOpen(false)
  }

  return (
    <AirflowDagsContext.Provider
      value={{ selectedDag, setSelectedDag, open, openDrawer, closeDrawer }}
    >
      {children}
    </AirflowDagsContext.Provider>
  )
}

export function useAirflowDags() {
  const ctx = useContext(AirflowDagsContext)
  if (!ctx) {
    throw new Error('useAirflowDags must be used within AirflowDagsProvider')
  }
  return ctx
}
