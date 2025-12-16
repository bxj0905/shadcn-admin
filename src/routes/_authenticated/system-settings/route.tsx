import { createFileRoute } from '@tanstack/react-router'
import { SystemSettingsPage } from '@/features/system-settings'

export const Route = createFileRoute('/_authenticated/system-settings')({
  component: SystemSettingsPage,
})
