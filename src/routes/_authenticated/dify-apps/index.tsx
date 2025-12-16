import { createFileRoute } from '@tanstack/react-router'
import { DifyAppsPage } from '@/features/dify-apps'

export const Route = createFileRoute('/_authenticated/dify-apps/')({
  component: DifyAppsPage,
})
