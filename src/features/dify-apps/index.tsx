import { FormEvent, useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import axios from 'axios'
import { SlidersHorizontal, ArrowUpAZ, ArrowDownAZ } from 'lucide-react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Separator } from '@/components/ui/separator'
import { ConfigDrawer } from '@/components/config-drawer'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { ProfileDropdown } from '@/components/profile-dropdown'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from '@/components/ui/sheet'
import { Textarea } from '@/components/ui/textarea'

export type DifyAppVisibility = 'personal' | 'team'

export type DifyAppDto = {
  id: number
  code: string
  name: string
  description?: string
  icon?: string
  visibility: DifyAppVisibility | string
  isEnabled: boolean
}

export function DifyAppsPage() {
  const [open, setOpen] = useState(false)
  const [activeApp, setActiveApp] = useState<DifyAppDto | null>(null)
  const [conversationId, setConversationId] = useState<string | undefined>()
  const [input, setInput] = useState('')
  const [messages, setMessages] = useState<
    { id: string; role: 'user' | 'assistant'; content: string }[]
  >([])
  const [loading, setLoading] = useState(false)
  const { data: apps = [], isLoading } = useQuery<DifyAppDto[]>({
    queryKey: ['dify-apps'],
    queryFn: async () => {
      const res = await axios.get('/api/difyai/apps')
      return res.data ?? []
    },
  })

  // 简单按名称排序，后续可以加筛选
  const sortedApps = [...apps].sort((a, b) =>
    a.name.localeCompare(b.name),
  )

  // 每次切换应用或关闭抽屉时，重置会话
  useEffect(() => {
    if (!open) {
      setConversationId(undefined)
      setMessages([])
      setInput('')
    }
  }, [open])

  const handleSend = async (e: FormEvent) => {
    e.preventDefault()
    const value = input.trim()
    if (!value || !activeApp) return

    const userMessage = {
      id: `${Date.now()}-user`,
      role: 'user' as const,
      content: value,
    }
    const nextMessages = [...messages, userMessage]
    setMessages(nextMessages)
    setInput('')
    setLoading(true)

    try {
      const assistantId = `${Date.now()}-assistant`
      const assistantMessage = {
        id: assistantId,
        role: 'assistant' as const,
        content: '',
      }
      setMessages([...nextMessages, assistantMessage])

      const resp = await fetch('/api/difyai/chat-messages/stream', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: value,
          conversationId,
          appCode: activeApp.code,
        }),
      })

      if (!resp.body) {
        throw new Error('No response body')
      }

      const reader = resp.body.getReader()
      const decoder = new TextDecoder('utf-8')
      let buffer = ''

      // 读取 SSE 流
      // eslint-disable-next-line no-constant-condition
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        if (!value) continue
        buffer += decoder.decode(value, { stream: true })

        const events = buffer.split('\n\n')
        buffer = events.pop() ?? ''

        for (const evt of events) {
          const line = evt
            .split('\n')
            .find((l) => l.startsWith('data:'))
          if (!line) continue

          const dataStr = line.slice('data:'.length).trim()
          if (!dataStr || dataStr === '[DONE]') continue

          let parsed: any
          try {
            parsed = JSON.parse(dataStr)
          } catch {
            continue
          }

          const delta: string =
            parsed.answer_delta ??
            parsed.answer ??
            parsed.data?.answer_delta ??
            parsed.data?.answer ??
            ''

          const newConvId: string | undefined =
            parsed.conversation_id ?? parsed.data?.conversation_id
          if (newConvId && newConvId !== conversationId) {
            setConversationId(newConvId)
          }

          if (delta) {
            setMessages((prev) =>
              prev.map((m) =>
                m.id === assistantId
                  ? { ...m, content: m.content + delta }
                  : m,
              ),
            )
          }
        }
      }
    } catch (error) {
      const aiMessage = {
        id: `${Date.now()}-assistant-error`,
        role: 'assistant' as const,
        content: '调用 Dify 失败，请稍后重试。',
      }
      setMessages([...nextMessages, aiMessage])
    } finally {
      setLoading(false)
    }
  }

  return (
    <>
      {/* 顶部与 Apps 页面一致 */}
      <Header>
        <Search />
        <div className='ms-auto flex items-center gap-4'>
          <ThemeSwitch />
          <ConfigDrawer />
          <ProfileDropdown />
        </div>
      </Header>

      <Main fixed>
        <div>
          <h1 className='text-2xl font-bold tracking-tight'>知识库应用</h1>
          <p className='text-muted-foreground'>
            管理和使用接入 Dify 的 AI 应用，点击卡片可以打开右侧对话抽屉（后续接入）。
          </p>
        </div>

        <div className='my-4 flex items-end justify-between sm:my-0 sm:items-center'>
          <div className='flex flex-col gap-4 sm:my-4 sm:flex-row'>
            <Input
              placeholder='暂不支持筛选，后续可按名称/权限过滤...'
              className='h-9 w-40 lg:w-[250px]'
              disabled
            />
          </div>

          <Select defaultValue='asc' disabled>
            <SelectTrigger className='w-16'>
              <SelectValue>
                <SlidersHorizontal size={18} />
              </SelectValue>
            </SelectTrigger>
            <SelectContent align='end'>
              <SelectItem value='asc'>
                <div className='flex items-center gap-4'>
                  <ArrowUpAZ size={16} />
                  <span>升序</span>
                </div>
              </SelectItem>
              <SelectItem value='desc'>
                <div className='flex items-center gap-4'>
                  <ArrowDownAZ size={16} />
                  <span>降序</span>
                </div>
              </SelectItem>
            </SelectContent>
          </Select>
        </div>

        <Separator className='shadow-sm' />

        <ul className='faded-bottom no-scrollbar grid gap-4 overflow-auto pt-4 pb-16 md:grid-cols-2 lg:grid-cols-3'>
          {isLoading ? (
            <li className='text-muted-foreground text-sm'>加载中...</li>
          ) : sortedApps.length === 0 ? (
            <li className='text-muted-foreground text-sm'>暂无知识库应用</li>
          ) : (
            sortedApps.map((app) => (
              <li
                key={app.id}
                className='cursor-pointer rounded-lg border p-4 hover:shadow-md'
                onClick={() => {
                  setActiveApp(app)
                  setOpen(true)
                }}
              >
                <div className='mb-8 flex items-center justify-between'>
                  <div className='bg-muted flex size-10 items-center justify-center rounded-lg p-2'>
                    <span className='text-sm font-semibold'>
                      {app.name.charAt(0).toUpperCase()}
                    </span>
                  </div>
                  <Button
                    variant='outline'
                    size='sm'
                    className={app.isEnabled ? '' : 'opacity-60'}
                  >
                    {app.visibility === 'team' ? '团队应用' : '个人应用'}
                  </Button>
                </div>
                <div>
                  <h2 className='mb-1 font-semibold'>{app.name}</h2>
                  <p className='line-clamp-2 text-gray-500'>{app.description}</p>
                </div>
              </li>
            ))
          )}
        </ul>

        <Sheet open={open} onOpenChange={setOpen}>
          <SheetContent side='right' className='flex w-full flex-col gap-0 p-0 sm:max-w-xl'>
            <SheetHeader className='border-b px-6 py-4 text-left'>
              <SheetTitle>
                {activeApp?.name ?? 'Dify Chat'}
              </SheetTitle>
              <p className='text-muted-foreground text-xs'>
                与 Dify 应用进行对话，当前 AppCode：{activeApp?.code ?? '-'}
              </p>
            </SheetHeader>
            <div className='flex h-full flex-1 flex-col'>
              <div className='flex-1 space-y-3 overflow-y-auto px-6 py-4 text-sm'>
                {messages.length === 0 ? (
                  <p className='text-muted-foreground text-xs'>
                    还没有消息，开始向该 Dify 应用提问吧。
                  </p>
                ) : (
                  messages.map((m) => (
                    <div
                      key={m.id}
                      className={
                        m.role === 'user'
                          ? 'ml-auto max-w-[80%] rounded-lg bg-primary px-3 py-2 text-primary-foreground'
                          : 'mr-auto max-w-[80%] rounded-lg bg-muted px-3 py-2'
                      }
                    >
                      <ReactMarkdown
                        remarkPlugins={[remarkGfm]}
                        className='prose prose-sm max-w-none break-words dark:prose-invert'
                        components={{
                          code({ inline, className, children, ...props }) {
                            const match = /language-(\w+)/.exec(className || '')
                            return !inline ? (
                              <pre
                                className={
                                  'mt-2 max-h-80 overflow-auto rounded-md bg-black/80 p-3 text-xs text-white'
                                }
                              >
                                <code className={className} {...props}>
                                  {children}
                                </code>
                              </pre>
                            ) : (
                              <code
                                className={
                                  'rounded bg-black/10 px-1 py-0.5 text-[0.8rem] ' +
                                  (className || '')
                                }
                                {...props}
                              >
                                {children}
                              </code>
                            )
                          },
                          img({ alt, ...props }) {
                            return (
                              <img
                                alt={alt ?? ''}
                                className='my-1 max-h-64 max-w-full rounded border'
                                {...props}
                              />
                            )
                          },
                        }}
                      >
                        {m.content}
                      </ReactMarkdown>
                    </div>
                  ))
                )}
              </div>

              <form
                onSubmit={handleSend}
                className='border-t bg-background px-4 py-3'
              >
                <div className='flex items-end gap-2'>
                  <Textarea
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter' && !e.shiftKey) {
                        e.preventDefault()
                        // 按 Enter 直接发送消息，Shift+Enter 才换行
                        void handleSend(e as unknown as FormEvent)
                      }
                    }}
                    placeholder={
                      activeApp
                        ? `向 ${activeApp.name} 提问...`
                        : '请选择一个应用后开始对话'
                    }
                    className='min-h-[48px] max-h-32 flex-1 resize-none'
                    disabled={loading || !activeApp}
                  />
                  <Button
                    type='submit'
                    size='sm'
                    disabled={loading || !input.trim() || !activeApp}
                  >
                    发送
                  </Button>
                </div>
              </form>
            </div>
          </SheetContent>
        </Sheet>
      </Main>
    </>
  )
}
