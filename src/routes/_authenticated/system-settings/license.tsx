import { createFileRoute } from '@tanstack/react-router'
import { SystemLicenseSection } from '@/features/system-settings/license'

export const Route = createFileRoute('/_authenticated/system-settings/license')({
  component: SystemLicenseSection,
})
