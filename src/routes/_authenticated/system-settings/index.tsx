import { createFileRoute } from '@tanstack/react-router'
import { SystemBrandingConfigSection } from '@/features/system-settings/config'

export const Route = createFileRoute('/_authenticated/system-settings/')({
  component: SystemBrandingConfigSection,
})
