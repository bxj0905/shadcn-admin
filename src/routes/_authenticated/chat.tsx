import { createFileRoute } from '@tanstack/react-router'
import Page from '@/features/chat'

// This route file corresponds to the authenticated "/chat" page.
// Building Blocks for AI and the sidebar should navigate to "/chat".
export const Route = createFileRoute('/_authenticated/chat')({
  component: Page,
})
