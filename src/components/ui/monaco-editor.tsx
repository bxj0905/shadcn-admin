'use client'

import * as React from 'react'
import Editor, { loader } from '@monaco-editor/react'
import { useTheme } from 'next-themes'
import { cn } from '@/lib/utils'

// 配置 Monaco Editor 使用本地资源而不是 CDN
// 这需要在组件加载前执行
if (typeof window !== 'undefined') {
  // 检查是否已经配置过
  if (!(window as any).__MONACO_LOADER_CONFIGURED__) {
    loader.config({ paths: { vs: '/monaco-editor/vs' } })
    ;(window as any).__MONACO_LOADER_CONFIGURED__ = true
  }
}

export type MonacoEditorProps = {
  value?: string
  defaultValue?: string
  onChange?: (value: string | undefined) => void
  language?: string
  height?: number | string
  placeholder?: string
  className?: string
  options?: {
    fontSize?: number
    minimap?: { enabled: boolean }
    wordWrap?: 'on' | 'off' | 'wordWrapColumn' | 'bounded'
    lineNumbers?: 'on' | 'off' | 'relative' | 'interval'
    tabSize?: number
    automaticLayout?: boolean
    scrollBeyondLastLine?: boolean
    formatOnPaste?: boolean
    formatOnType?: boolean
  }
}

export function MonacoEditor({
  value,
  defaultValue = '',
  onChange,
  language = 'python',
  height = '100%',
  placeholder,
  className,
  options = {},
}: MonacoEditorProps) {
  const { theme } = useTheme()
  const [mounted, setMounted] = React.useState(false)

  React.useEffect(() => {
    setMounted(true)
  }, [])

  const editorHeight = typeof height === 'number' ? `${height}px` : height
  const isDark = theme === 'dark'

  const editorOptions = {
    fontSize: 14,
    minimap: { enabled: false },
    wordWrap: 'on' as const,
    lineNumbers: 'on' as const,
    tabSize: 4,
    automaticLayout: true,
    scrollBeyondLastLine: false,
    formatOnPaste: true,
    formatOnType: true,
    ...options,
  }

  if (!mounted) {
    return (
      <div
        className={cn(
          'relative rounded-md border bg-muted/40 flex items-center justify-center',
          className,
        )}
        style={{
          width: '100%',
          height: editorHeight,
        }}
      >
        <p className='text-sm text-muted-foreground'>加载编辑器...</p>
      </div>
    )
  }

  return (
    <div
      className={cn(
        'relative rounded-md border overflow-hidden',
        isDark ? 'bg-[#1e1e1e]' : 'bg-white',
        className,
      )}
      style={{
        width: '100%',
        height: editorHeight,
      }}
    >
      <Editor
        height={editorHeight}
        language={language}
        value={value ?? defaultValue}
        onChange={onChange}
        theme={isDark ? 'vs-dark' : 'light'}
        options={editorOptions}
        loading={
          <div className='flex items-center justify-center h-full'>
            <p className='text-sm text-muted-foreground'>加载编辑器...</p>
          </div>
        }
      />
    </div>
  )
}

