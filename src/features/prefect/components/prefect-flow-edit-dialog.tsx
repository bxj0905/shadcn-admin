'use client'

import { useEffect, useState } from 'react'
import {
  createOnDropHandler,
  dragAndDropFeature,
  hotkeysCoreFeature,
  keyboardDragAndDropFeature,
  renamingFeature,
  type TreeState,
  selectionFeature,
  syncDataLoaderFeature,
} from '@headless-tree/core'
import { AssistiveTreeDescription, useTree } from '@headless-tree/react'
import {
  RiBracesLine,
  RiCodeSSlashLine,
  RiFileLine,
  RiFileTextLine,
  RiImageLine,
  RiReactjsLine,
} from '@remixicon/react'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Tree, TreeItem, TreeItemLabel } from '@/components/ui/tree'
import { MonacoEditor } from '@/components/ui/monaco-editor'
import { cn } from '@/lib/utils'
import { FolderPlus, FilePlus } from 'lucide-react'
import { Input } from '@/components/ui/input'
import { toast } from 'sonner'
import {
  fetchPrefectFlowFiles,
  updatePrefectFlowCode,
  uploadPrefectFlowFiles,
  type PrefectFlow,
} from '@/services/prefect'

interface PrefectFlowEditDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  flow: PrefectFlow
}

interface Item {
  name: string
  children?: string[]
  fileExtension?: string
}

const indent = 20

const DEFAULT_CODE = [
  'from prefect import flow',
  '',
  '',
  '@flow',
  'def my_flow():',
  "    '''Flow description'''",
  '    pass',
  '',
].join('\n')

function sanitizePrefix(prefixRaw?: string) {
  if (!prefixRaw) return ''
  let prefix = prefixRaw.trim().replace(/^[\\/]+/, '').replace(/[\\/]+$/, '')
  prefix = prefix.replace(/[^\w\s\-_.\\/]/g, '_')
  if (!prefix) return ''
  return `${prefix}/`
}

function getFileIcon(extension: string | undefined, className: string) {
  switch (extension) {
    case 'py':
      return <RiCodeSSlashLine className={className} />
    case 'tsx':
    case 'jsx':
    case 'ts':
    case 'js':
    case 'mjs':
      return <RiReactjsLine className={className} />
    case 'yml':
    case 'yaml':
    case 'json':
      return <RiBracesLine className={className} />
    case 'sql':
      return <RiCodeSSlashLine className={className} />
    case 'svg':
    case 'ico':
    case 'png':
    case 'jpg':
      return <RiImageLine className={className} />
    case 'md':
    case 'markdown':
      return <RiFileTextLine className={className} />
    case 'env':
      return <RiFileLine className={className} />
    default:
      return <RiFileLine className={className} />
  }
}

function inferExtension(name?: string) {
  if (!name) return ''
  const idx = name.lastIndexOf('.')
  if (idx === -1) return ''
  return name.slice(idx + 1).toLowerCase()
}

function languageFromExt(ext?: string) {
  switch (ext) {
    case 'py':
      return 'python'
    case 'json':
      return 'json'
    case 'yaml':
    case 'yml':
      return 'yaml'
    case 'sql':
      return 'sql'
    case 'env':
      return 'shell'
    case 'md':
    case 'markdown':
      return 'markdown'
    default:
      return 'plaintext'
  }
}

interface FlowFileTreeProps {
  items: Record<string, Item>
  onItemsChange: (updater: (prev: Record<string, Item>) => Record<string, Item>) => void
  onSelectionChange?: (id: string, isFolder: boolean) => void
  selectedId: string
  onRenameApply: (id: string, newName: string) => boolean
  treeState: Partial<TreeState<Item>>
  setTreeState: React.Dispatch<React.SetStateAction<Partial<TreeState<Item>>>>
}

function FlowFileTree({
  items,
  onItemsChange,
  onSelectionChange,
  selectedId,
  onRenameApply,
  treeState,
  setTreeState,
}: FlowFileTreeProps) {
  const tree = useTree<Item>({
    canReorder: false,
    dataLoader: {
      getChildren: (itemId) => items[itemId]?.children ?? [],
      getItem: (itemId) => {
        const item = items[itemId]
        if (item) return item
        return { name: itemId.split('/').pop() ?? 'Unknown', children: [] }
      },
    },
    features: [
      syncDataLoaderFeature,
      selectionFeature,
      hotkeysCoreFeature,
      dragAndDropFeature,
      keyboardDragAndDropFeature,
      renamingFeature,
    ],
    getItemName: (item) => item.getItemData()?.name ?? 'Unknown',
    indent,
    onRename: (item, name: string) => onRenameApply(item.getId(), name),
    isItemFolder: (item) => Array.isArray(item.getItemData()?.children),
    onDrop: createOnDropHandler((parentItem, newChildrenIds) => {
      onItemsChange((prevItems) => {
        const sortedChildren = [...newChildrenIds].sort((a, b) => {
          const itemA = prevItems[a]
          const itemB = prevItems[b]

          const isAFolder = Array.isArray(itemA?.children)
          const isBFolder = Array.isArray(itemB?.children)

          if (isAFolder && !isBFolder) return -1
          if (!isAFolder && isBFolder) return 1

          return (itemA?.name ?? '').localeCompare(itemB?.name ?? '')
        })

        return {
          ...prevItems,
          [parentItem.getId()]: {
            ...prevItems[parentItem.getId()],
            children: sortedChildren,
          },
        }
      })
    }),
    rootItemId: 'root',
    state: treeState,
    setState: (updater) => {
      setTreeState((prev) => {
        const next =
          typeof updater === 'function'
            ? { ...prev, ...(updater(prev as TreeState<Item>)) }
            : { ...prev, ...updater }

        const nextSelected = next.selectedItems?.[0]
        if (nextSelected && nextSelected !== selectedId) {
          const isFolder = Array.isArray(items[nextSelected]?.children)
          onSelectionChange?.(nextSelected, isFolder)
        }
        return next
      })
    },
  })

  return (
    <Tree className='relative h-full overflow-auto' indent={indent} tree={tree}>
      <AssistiveTreeDescription tree={tree} />
      {tree.getItems().map((item) => {
        const handleSelect = () => {
          const isFolder = Array.isArray(item.getItemData()?.children)
          onSelectionChange?.(item.getId(), isFolder)
        }

        return (
          <TreeItem className='pb-0! border-none shadow-none outline-none ring-0' item={item} key={item.getId()}>
            <TreeItemLabel
              className={cn(
                'rounded-none py-1 border-0 shadow-none outline-none ring-0 focus:outline-none focus:ring-0 focus-visible:!outline-none focus-visible:!ring-0 focus-visible:!ring-offset-0 in-focus-visible:!ring-0 in-focus-visible:!ring-offset-0',
                selectedId === item.getId() ? '!bg-[#3ECF8E] !text-white' : 'bg-transparent text-foreground',
                'cursor-pointer',
              )}
              onClick={handleSelect}
            >
              <div className='flex items-center gap-2 w-full'>
                {!item.isFolder() &&
                  getFileIcon(
                    item.getItemData()?.fileExtension,
                    'text-muted-foreground pointer-events-none size-4',
                  )}
                {item.isRenaming() ? (
                  <Input
                    autoFocus
                    className='h-7 w-full px-2 text-sm'
                    {...(item.getRenameInputProps ? item.getRenameInputProps() : {})}
                  />
                ) : (
                  <span className='whitespace-nowrap'>{item.getItemName()}</span>
                )}
              </div>
            </TreeItemLabel>
          </TreeItem>
        )
      })}
    </Tree>
  )
}

function buildTreeFromFiles(files: Array<{ path: string; code: string }>) {
  const items: Record<string, Item> = {
    root: { name: 'Project Root', children: [] },
  }
  const fileCodes: Record<string, string> = {}

  const ensureFolder = (id: string, name: string, parentId: string) => {
    if (!items[id]) {
      items[id] = { name, children: [] }
    }
    if (!items[parentId]?.children?.includes(id)) {
      items[parentId] = items[parentId] ?? { name: parentId, children: [] }
      items[parentId].children = items[parentId].children ?? []
      items[parentId].children?.push(id)
    }
  }

  files.forEach(({ path, code }) => {
    const cleanPath = path.replace(/^\/+/, '').replace(/\/+/g, '/')
    if (!cleanPath) return

    const segments = cleanPath.split('/').filter(Boolean)
    let parentId = 'root'
    let currentPath = ''

    segments.forEach((seg, idx) => {
      const isLast = idx === segments.length - 1
      currentPath = currentPath ? `${currentPath}/${seg}` : seg
      const id = currentPath

      if (isLast) {
        const ext = inferExtension(seg)
        items[id] = { name: seg, fileExtension: ext }
        items[parentId] = items[parentId] ?? { name: parentId, children: [] }
        items[parentId].children = items[parentId].children ?? []
        if (!items[parentId].children?.includes(id)) {
          items[parentId].children?.push(id)
        }
        fileCodes[id] = code ?? ''
      } else {
        ensureFolder(id, seg, parentId)
      }

      parentId = id
    })
  })

  return { items, fileCodes }
}

export function PrefectFlowEditDialog({ open, onOpenChange, flow }: PrefectFlowEditDialogProps) {
  const [items, setItems] = useState<Record<string, Item>>({ root: { name: 'Project Root', children: [] } })
  const [treeVersion, setTreeVersion] = useState(0)
  const [treeState, setTreeState] = useState<Partial<TreeState<Item>>>({
    expandedItems: ['root'],
    selectedItems: ['root'],
  })
  const [flowName, setFlowName] = useState<string>(flow.name)
  const [code, setCode] = useState<string>(DEFAULT_CODE)
  const [language, setLanguage] = useState<string>('python')
  const [selectedIsFolder, setSelectedIsFolder] = useState<boolean>(true)
  const [nextId, setNextId] = useState(1)
  const [selectedId, setSelectedId] = useState<string>('root')
  const [fileCodes, setFileCodes] = useState<Record<string, string>>({})
  const [saving, setSaving] = useState(false)
  const [loading, setLoading] = useState(false)
  const [basePrefix, setBasePrefix] = useState<string>('')
  const [mainRelativePath, setMainRelativePath] = useState<string | undefined>(undefined)

  const resolveParentId = () => {
    const current = items[selectedId]
    if (current && Array.isArray(current.children)) {
      return selectedId
    }
    const slash = selectedId.lastIndexOf('/')
    if (slash > 0) return selectedId.slice(0, slash)
    return 'root'
  }

  const handleAddFolder = () => {
    const parentId = resolveParentId()
    const newId = `${parentId}/folder_${nextId}`
    const parent = items[parentId]
    if (!parent || !Array.isArray(parent.children)) return

    setItems((prev) => ({
      ...prev,
      [newId]: { name: `folder_${nextId}`, children: [] },
      [parentId]: {
        ...prev[parentId],
        children: [...(prev[parentId].children ?? []), newId],
      },
    }))
    setTreeState((prev) => ({
      ...prev,
      expandedItems: Array.from(new Set([...(prev.expandedItems ?? []), parentId])),
      selectedItems: [newId],
    }))
    setSelectedId(newId)
    setSelectedIsFolder(true)
    setNextId((v) => v + 1)
    setTreeVersion((v) => v + 1)
  }

  const handleAddFile = () => {
    const parentId = resolveParentId()
    const newId = `${parentId}/file_${nextId}.py`
    const parent = items[parentId]
    if (!parent || !Array.isArray(parent.children)) return

    setItems((prev) => ({
      ...prev,
      [newId]: { name: `file_${nextId}.py`, fileExtension: 'py' },
      [parentId]: {
        ...prev[parentId],
        children: [...(prev[parentId].children ?? []), newId],
      },
    }))
    setFileCodes((prev) => ({ ...prev, [newId]: DEFAULT_CODE }))
    setTreeState((prev) => ({
      ...prev,
      expandedItems: Array.from(new Set([...(prev.expandedItems ?? []), parentId])),
      selectedItems: [newId],
    }))
    setSelectedId(newId)
    setSelectedIsFolder(false)
    setCode(DEFAULT_CODE)
    setLanguage('python')
    setNextId((v) => v + 1)
    setTreeVersion((v) => v + 1)
  }

  const renameSubtree = (
    oldId: string,
    newId: string,
    source: Record<string, Item>,
  ): Record<string, Item> => {
    const updated: Record<string, Item> = {}
    const oldItem = source[oldId]
    if (!oldItem) return updated

    const isFolder = Array.isArray(oldItem.children)
    const newChildren = isFolder
      ? (oldItem.children ?? []).map((cid) => cid.replace(`${oldId}/`, `${newId}/`))
      : oldItem.children

    const newBaseName = newId.split('/').pop() ?? oldItem.name

    updated[newId] = {
      ...oldItem,
      name: newBaseName,
      children: isFolder ? newChildren : undefined,
      fileExtension: !isFolder ? inferExtension(newBaseName) || oldItem.fileExtension : undefined,
    }

    if (isFolder) {
      for (const childId of oldItem.children ?? []) {
        Object.assign(
          updated,
          renameSubtree(childId, childId.replace(`${oldId}/`, `${newId}/`), source),
        )
      }
    }

    return updated
  }

  const applyRename = (id: string, newName: string) => {
    const trimmed = newName.trim()
    if (!trimmed || trimmed.includes('/')) return false
    if (id === 'root') return false

    const parentSlash = id.lastIndexOf('/')
    const parentId = parentSlash > 0 ? id.slice(0, parentSlash) : 'root'
    const newId = parentId === 'root' ? trimmed : `${parentId}/${trimmed}`

    setItems((prev) => {
      const prevParent = prev[parentId]
      const parent: Item = prevParent ?? { name: parentId, children: [] }
      const parentChildren = Array.isArray(parent.children)
        ? parent.children
        : []

      const withoutOld: Record<string, Item> = {}
      Object.keys(prev).forEach((key) => {
        if (!key.startsWith(`${id}/`) && key !== id) {
          withoutOld[key] = prev[key]
        }
      })

      const cloned = renameSubtree(id, newId, prev)
      const newChildren = parentChildren.map((cid) => (cid === id ? newId : cid))
      if (!newChildren.includes(newId)) {
        newChildren.push(newId)
      }
      return {
        ...withoutOld,
        ...cloned,
        [parentId]: { ...parent, children: newChildren },
      }
    })
    setFileCodes((prev) => {
      const next: Record<string, string> = {}
      Object.entries(prev).forEach(([key, val]) => {
        if (key === id) {
          next[newId] = val
        } else if (key.startsWith(`${id}/`)) {
          next[key.replace(`${id}/`, `${newId}/`)] = val
        } else {
          next[key] = val
        }
      })
      return next
    })

    setTreeState((prev) => {
      const expanded = new Set(prev.expandedItems ?? [])
      if (expanded.has(id)) {
        expanded.delete(id)
        expanded.add(newId)
      }
      const selectedItems =
        prev.selectedItems?.map((sid) => (sid === id || sid.startsWith(`${id}/`) ? sid.replace(id, newId) : sid)) ?? []
      return { ...prev, expandedItems: Array.from(expanded), selectedItems }
    })
    setSelectedId((prevSelected) => {
      if (prevSelected === id || prevSelected.startsWith(`${id}/`)) {
        return prevSelected.replace(id, newId)
      }
      return prevSelected
    })
    setCode((prev) => {
      if (fileCodes[newId] !== undefined) return fileCodes[newId]
      if (fileCodes[id] !== undefined) return fileCodes[id]
      return prev
    })
    setLanguage(languageFromExt(inferExtension(trimmed)))
    setTreeVersion((v) => v + 1)
    return true
  }

  const collectFilesPayload = () => {
    const files: Array<{
      path: string
      code: string
      flowType?: 'main' | 'feature' | 'subflow'
      name?: string
    }> = []
    Object.entries(items).forEach(([id, item]) => {
      const path = id.startsWith('/') ? id.slice(1) : id

      if (Array.isArray(item.children)) {
        if (id === 'root') return
        files.push({
          path: `${path}/`,
          code: '',
          name: item.name,
        })
        return
      }

      const codeContent = fileCodes[id] ?? DEFAULT_CODE
      const ext = item.fileExtension
      const flowType: 'main' | 'feature' | 'subflow' | undefined =
        path === 'main.py'
          ? 'main'
          : path.startsWith('feature_flows/')
            ? 'subflow'
            : ext
              ? 'feature'
              : undefined
      files.push({
        path,
        code: codeContent,
        flowType,
        name: item.name,
      })
    })
    return files
  }

  const pickMainCode = () => {
    if (mainRelativePath && fileCodes[mainRelativePath] !== undefined) {
      return fileCodes[mainRelativePath]
    }
    if (fileCodes['flow/main.py'] !== undefined) return fileCodes['flow/main.py']
    const pyEntry = Object.entries(fileCodes).find(([key]) => key.endsWith('.py'))
    if (pyEntry) return pyEntry[1]
    const first = Object.values(fileCodes)[0]
    return first ?? DEFAULT_CODE
  }

  const handleSave = async () => {
    if (saving) return
    setSaving(true)
    const trimmedName = flowName.trim() || flow.name
    const prefix = basePrefix || sanitizePrefix(trimmedName)
    try {
      const files = collectFilesPayload()
      if (files.length === 0) {
        toast.error('没有可上传的文件')
        return
      }
      await uploadPrefectFlowFiles(files, { basePrefix: prefix })

      const mainCode = pickMainCode()
      await updatePrefectFlowCode(flow.id, mainCode)

      toast.success('保存成功', { description: '已上传文件并更新 Flow' })
      onOpenChange(false)
    } catch (error) {
      toast.error('保存失败', {
        description: error instanceof Error ? error.message : String(error),
      })
    } finally {
      setSaving(false)
    }
  }

  const handleSelectionChange = (id: string, isFolder: boolean) => {
    setSelectedId(id)
    setSelectedIsFolder(isFolder)
    if (isFolder) {
      setCode('')
      return
    }
    const item = items[id]
    const ext = item?.fileExtension ?? inferExtension(item?.name)
    setCode(fileCodes[id] ?? '')
    setLanguage(languageFromExt(ext))
  }

  // 打开时拉取 RustFS 文件
  useEffect(() => {
    if (!open) return
    let cancelled = false
    setLoading(true)
    ;(async () => {
      try {
        const res = await fetchPrefectFlowFiles(flow.id)
        if (cancelled) return
        const { items: loadedItems, fileCodes: loadedCodes } = buildTreeFromFiles(res.files)
        const folderIds = Object.keys(loadedItems).filter((id) => Array.isArray(loadedItems[id].children))
        const firstFileId = Object.keys(loadedItems).find((id) => !Array.isArray(loadedItems[id].children))
        const preferredFileId =
          res.mainRelativePath && loadedItems[res.mainRelativePath] && !Array.isArray(loadedItems[res.mainRelativePath].children)
            ? res.mainRelativePath
            : firstFileId
        setItems(loadedItems)
        setFileCodes(loadedCodes)
        setBasePrefix(res.prefix)
        setMainRelativePath(res.mainRelativePath)
        setFlowName(flow.name)
        setTreeState({
          expandedItems: folderIds,
          selectedItems: preferredFileId ? [preferredFileId] : ['root'],
        })
        setSelectedId(preferredFileId ?? 'root')
        setSelectedIsFolder(preferredFileId ? false : true)
        if (preferredFileId) {
          const item = loadedItems[preferredFileId]
          const ext = item?.fileExtension ?? inferExtension(item?.name)
          setCode(loadedCodes[preferredFileId] ?? '')
          setLanguage(languageFromExt(ext))
        } else {
          setCode('')
          setLanguage('plaintext')
        }
        setTreeVersion((v) => v + 1)
      } catch (error) {
        if (!cancelled) {
          toast.error('加载 RustFS 文件失败', {
            description: error instanceof Error ? error.message : String(error),
          })
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()
    return () => {
      cancelled = true
    }
  }, [open, flow.id, flow.name])

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className='flex h-screen max-h-screen w-screen max-w-none sm:max-w-none md:max-w-none lg:max-w-none xl:max-w-none 2xl:max-w-none flex-col gap-4 p-0 rounded-none border-none top-0 left-0 translate-x-0 translate-y-0'>
        <DialogHeader className='border-b px-6 py-4'>
          <DialogTitle className='text-lg font-semibold'>编辑 Prefect 工作流</DialogTitle>
          <DialogDescription className='sr-only'>从 RustFS 加载文件并编辑</DialogDescription>
          <div className='mt-2 flex items-center gap-3'>
            <span className='text-xs font-medium text-muted-foreground'>Flow 名称</span>
            <Input
              className='h-8 w-64'
              value={flowName}
              onChange={(e) => setFlowName(e.target.value)}
              placeholder='请输入 Flow 名称'
            />
          </div>
        </DialogHeader>

        <div className='flex flex-1 min-h-0 gap-4 px-6 pb-6 pt-4'>
          <div className='flex h-full w-72 flex-col gap-2 border-r pr-4'>
            <div className='flex items-center justify-between gap-2'>
              <span className='text-xs font-semibold text-muted-foreground'>FILES</span>
              <div className='flex items-center gap-1'>
                <Button
                  size='icon'
                  variant='ghost'
                  className='h-7 w-7'
                  onClick={handleAddFolder}
                  aria-label='新建文件夹'
                  disabled={loading}
                >
                  <FolderPlus className='size-4' />
                </Button>
                <Button
                  size='icon'
                  variant='ghost'
                  className='h-7 w-7'
                  onClick={handleAddFile}
                  aria-label='新建文件'
                  disabled={loading}
                >
                  <FilePlus className='size-4' />
                </Button>
              </div>
            </div>

            <div className='flex-1 min-h-0 overflow-auto rounded-md border'>
              {loading ? (
                <div className='flex h-full items-center justify-center text-xs text-muted-foreground'>加载中...</div>
              ) : (
                <FlowFileTree
                  key={treeVersion}
                  items={items}
                  onItemsChange={setItems}
                  onSelectionChange={handleSelectionChange}
                  selectedId={selectedId}
                  onRenameApply={applyRename}
                  treeState={treeState}
                  setTreeState={setTreeState}
                />
              )}
            </div>
          </div>

          <div className='flex min-w-0 flex-1 flex-col gap-3'>
            <div className='flex items-center justify-between'>
              <div className='flex items-center gap-3'>
                <span className='text-xs font-semibold text-muted-foreground'>
                  {selectedIsFolder ? '目录' : '文件'}
                </span>
                <span className='text-xs text-muted-foreground'>{selectedId}</span>
              </div>
            </div>
            <div className='flex-1 min-h-0'>
              {selectedIsFolder ? (
                <div className='flex h-full items-center justify-center rounded-md border border-dashed text-sm text-muted-foreground'>
                  请选择文件以编辑
                </div>
              ) : (
                <MonacoEditor
                  height='100%'
                  language={language}
                  value={code}
                  onChange={(value) => {
                    setCode(value ?? '')
                    setFileCodes((prev) => ({ ...prev, [selectedId]: value ?? '' }))
                  }}
                  options={{
                    minimap: { enabled: false },
                    fontSize: 13,
                  }}
                />
              )}
            </div>
            <div className='flex justify-end gap-2 pt-2'>
              <Button variant='outline' size='sm' onClick={() => onOpenChange(false)} disabled={saving}>
                取消
              </Button>
              <Button size='sm' onClick={handleSave} disabled={saving || loading}>
                {saving ? '保存中...' : '保存'}
              </Button>
            </div>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  )
}

