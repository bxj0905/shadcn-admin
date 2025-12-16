import { createFileRoute } from '@tanstack/react-router'
import Page from '@/features/chat'

// File-based id stays '/_authenticated/ai-chat' (from the file path)
// We expose it at URL '/chat' via the `path` option, matching the Introduction docs.
export const Route = createFileRoute('/_authenticated/ai-chat')({
  path: '/chat',
  component: Page,
})
