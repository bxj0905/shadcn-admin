'use client'

import { useState } from 'react'
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
import { createPrefectFlow, uploadPrefectFlowFiles } from '@/services/prefect'

interface PrefectFlowNewDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
}

interface Item {
  name: string
  children?: string[]
  fileExtension?: string
}

const initialItems: Record<string, Item> = {
  flow: {
    children: ['flow/main.py', 'flow/tasks', 'flow/feature_flows', 'flow/utils', 'flow/config'],
    name: 'flow',
  },
  'flow/main.py': { fileExtension: 'py', name: 'main.py' },
  'flow/tasks': { children: [], name: 'tasks' },
  'flow/feature_flows': { children: [], name: 'feature_flows' },
  'flow/utils': { children: [], name: 'utils' },
  'flow/config': { children: [], name: 'config' },
  root: {
    children: ['flow'],
    name: 'Project Root',
  },
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
        // 防御：避免 headless-tree 在选中已被重命名/删除的节点时报 undefined
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

        // 将树的 selectedItems 同步到外部
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
    <Tree
      className='relative h-full overflow-auto'
      indent={indent}
      tree={tree}
    >
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
                selectedId === item.getId()
                  ? '!bg-[#3ECF8E] !text-white'
                  : 'bg-transparent text-foreground',
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
                  <span className='truncate'>{item.getItemName()}</span>
                )}
              </div>
            </TreeItemLabel>
          </TreeItem>
        )
      })}
    </Tree>
  )
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

export function PrefectFlowNewDialog({ open, onOpenChange }: PrefectFlowNewDialogProps) {
  const [items, setItems] = useState(initialItems)
  const [treeVersion, setTreeVersion] = useState(0)
  const [treeState, setTreeState] = useState<Partial<TreeState<Item>>>({
    expandedItems: ['root', 'flow'],
    selectedItems: ['flow'],
  })
  const [flowName, setFlowName] = useState<string>('my_flow')
  const [code, setCode] = useState<string>(DEFAULT_CODE)
  const [language, setLanguage] = useState<string>('python')
  const [selectedIsFolder, setSelectedIsFolder] = useState<boolean>(true)
  const [nextId, setNextId] = useState(1)
  const [selectedId, setSelectedId] = useState<string>('flow')
  const [fileCodes, setFileCodes] = useState<Record<string, string>>({
    'flow/main.py': DEFAULT_CODE,
  })
  const [saving, setSaving] = useState(false)

  // 当选中文件切换时，更新代码区；选中目录则清空
  const resolveParentId = () => {
    if (Array.isArray(items[selectedId]?.children)) {
      return selectedId
    }
    const lastSlash = selectedId.lastIndexOf('/')
    if (lastSlash > 0) {
      return selectedId.slice(0, lastSlash)
    }
    return 'flow'
  }

  const addChildToParent = (parentId: string, childId: string, child: Item) => {
    setItems((prev) => {
      const parent = prev[parentId]
      const children = parent?.children ? [...parent.children, childId] : [childId]
      return {
        ...prev,
        [childId]: child,
        [parentId]: {
          ...parent,
          children,
        },
      }
    })
    setTreeState((prev) => {
      const expanded = new Set(prev.expandedItems ?? [])
      expanded.add(parentId)
      return { ...prev, expandedItems: Array.from(expanded) }
    })
    setTreeVersion((v) => v + 1)
  }

  const handleAddFolder = () => {
    const parentId = resolveParentId()
    const name = `folder_${nextId}`
    const id = `${parentId}/${name}`
    setNextId((v) => v + 1)
    addChildToParent(parentId, id, { name, children: [] })
  }

  const handleAddFile = () => {
    const parentId = resolveParentId()
    const name = `file_${nextId}.py`
    const id = `${parentId}/${name}`
    setNextId((v) => v + 1)
    addChildToParent(parentId, id, { name, fileExtension: 'py' })
    setFileCodes((prev) => ({
      ...prev,
      [id]: DEFAULT_CODE,
    }))
  }

  const inferExtension = (name: string): string | undefined => {
    const ext = name.split('.').pop()?.toLowerCase()
    if (!ext || ext === name) return undefined
    return ext
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
      fileExtension: !isFolder ? inferExtension(newBaseName) ?? oldItem.fileExtension : undefined,
    }

    if (isFolder) {
      for (const childId of oldItem.children ?? []) {
        Object.assign(updated, renameSubtree(childId, childId.replace(`${oldId}/`, `${newId}/`), source))
      }
    }
    return updated
  }

  const languageFromExt = (ext?: string): string => {
    switch (ext) {
      case 'py':
        return 'python'
      case 'sql':
        return 'sql'
      case 'json':
        return 'json'
      case 'yml':
      case 'yaml':
        return 'yaml'
      case 'env':
        return 'shell'
      case 'ts':
      case 'tsx':
        return 'typescript'
      case 'js':
      case 'jsx':
        return 'javascript'
      case 'md':
        return 'markdown'
      default:
        return 'plaintext'
    }
  }

  const applyRename = (id: string, newName: string) => {
    const trimmed = newName.trim()
    if (!trimmed || trimmed.includes('/')) return false
    if (id === 'flow') return false

    const parentSlash = id.lastIndexOf('/')
    const parentId = parentSlash > 0 ? id.slice(0, parentSlash) : 'flow'
    const newId = parentId === 'flow' ? `flow/${trimmed}` : `${parentId}/${trimmed}`

    setItems((prev) => {
      const parent = prev[parentId]
      if (!parent || !Array.isArray(parent.children)) return prev

      const withoutOld: Record<string, Item> = {}
      Object.keys(prev).forEach((key) => {
        if (!key.startsWith(`${id}/`) && key !== id) {
          withoutOld[key] = prev[key]
        }
      })

      const cloned = renameSubtree(id, newId, prev)
      const newChildren = parent.children.map((cid) => (cid === id ? newId : cid))
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
        prev.selectedItems?.map((sid) =>
          sid === id || sid.startsWith(`${id}/`) ? sid.replace(id, newId) : sid,
        ) ?? []
      return { ...prev, expandedItems: Array.from(expanded), selectedItems }
    })
    // 更新选中项与代码区
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
    // 强制重建树实例，确保最新的文件/文件夹属性与图标生效
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
      if (Array.isArray(item.children)) return
      const codeContent = fileCodes[id] ?? DEFAULT_CODE
      const path = id.startsWith('/') ? id.slice(1) : id
      const ext = item.fileExtension
      const flowType: 'main' | 'feature' | 'subflow' | undefined =
        id === 'flow/main.py' ? 'main' : ext ? 'feature' : undefined
      files.push({
        path,
        code: codeContent,
        flowType,
        name: item.name,
      })
    })
    return files
  }

  const resetAllStates = () => {
    setItems(initialItems)
    setTreeState({
      expandedItems: ['root', 'flow'],
      selectedItems: ['flow'],
    })
    setTreeVersion((v) => v + 1)
    setFlowName('my_flow')
    setCode(DEFAULT_CODE)
    setLanguage('python')
    setSelectedIsFolder(true)
    setNextId(1)
    setSelectedId('flow')
    setFileCodes({
      'flow/main.py': DEFAULT_CODE,
    })
  }

  const handleSave = async () => {
    if (saving) return
    const trimmedName = flowName.trim()
    if (!trimmedName) {
      toast.error('请输入 Flow 名称')
      return
    }
    setSaving(true)
    try {
      const files = collectFilesPayload()
      if (files.length === 0) {
        toast.error('没有可上传的文件')
        return
      }
      await uploadPrefectFlowFiles(files)

      const mainCode = fileCodes['flow/main.py'] ?? DEFAULT_CODE
      await createPrefectFlow({
        name: trimmedName,
        code: mainCode,
      })

      toast.success('保存成功', { description: '已上传文件并创建 Flow 与 Deployment' })
      resetAllStates()
      onOpenChange(false)
    } catch (error) {
      toast.error('保存失败', {
        description: error instanceof Error ? error.message : String(error),
      })
    } finally {
      setSaving(false)
    }
  }

  // 同步树状态变化（键盘选择等）到右侧代码区和选中状态
  // 依赖 treeState 的 setState 回调同步选中，无需额外 effect


  return (
    <Dialog
      open={open}
      onOpenChange={onOpenChange}
    >
      <DialogContent
        className='flex h-screen max-h-screen w-screen max-w-none sm:max-w-none md:max-w-none lg:max-w-none xl:max-w-none 2xl:max-w-none flex-col gap-4 p-0 rounded-none border-none top-0 left-0 translate-x-0 translate-y-0'
      >
        <DialogHeader className='border-b px-6 py-4'>
          <DialogTitle className='text-lg font-semibold'>新建 Prefect 工作流</DialogTitle>
          <DialogDescription className='sr-only'>
            在左侧文件树中添加文件或文件夹，并在右侧编辑 Python 代码。
          </DialogDescription>
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

        <div className='flex flex-1 gap-4 px-6 pb-6 pt-4'>
          {/* 左侧文件 Tree */}
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
                >
                  <FolderPlus className='size-4' />
                </Button>
                <Button
                  size='icon'
                  variant='ghost'
                  className='h-7 w-7'
                  onClick={handleAddFile}
                  aria-label='新建文件'
                >
                  <FilePlus className='size-4' />
                </Button>
              </div>
            </div>
            <div className='flex-1 min-h-0'>
              <FlowFileTree
                key={treeVersion}
                items={items}
                onItemsChange={(updater) => {
                  setItems((prev) => updater(prev))
                  setTreeVersion((v) => v + 1)
                }}
                onSelectionChange={(id, isFolder) => {
                  setSelectedId(id)
                  setTreeState((prev) => ({ ...prev, selectedItems: [id] }))
                  setSelectedIsFolder(isFolder)
                  if (!isFolder) {
                    const ext = items[id]?.fileExtension
                    setLanguage(languageFromExt(ext))
                    setFileCodes((prev) => {
                      if (prev[id] !== undefined) return prev
                      return { ...prev, [id]: DEFAULT_CODE }
                    })
                    setCode((prev) => (prev === fileCodes[id] ? prev : fileCodes[id] ?? DEFAULT_CODE))
                  } else {
                    setLanguage('plaintext')
                    setCode('')
                  }
                }}
                selectedId={selectedId}
                onRenameApply={applyRename}
                treeState={treeState}
                setTreeState={setTreeState}
              />
            </div>
            <p className='mt-1 text-muted-foreground text-[11px]'>
              文件树示例，可拖拽排序。右侧编辑区用于编写 Prefect Flow 代码。
            </p>
          </div>

          {/* 右侧代码编辑器 */}
          <div className='flex min-w-0 flex-1 flex-col gap-2'>
            <div className='flex items-center justify-between'>
              <span className='text-xs font-semibold text-muted-foreground'>
                FLOW CODE ({language.toUpperCase()})
              </span>
            </div>
            <div className='flex-1 min-h-0'>
              {selectedIsFolder ? (
                <div className='flex h-full items-center justify-center rounded-md border border-dashed text-sm text-muted-foreground'>
                  请选择文件以编辑
                </div>
              ) : (
                <MonacoEditor
                  value={code}
                  onChange={(v) => {
                    const next = v ?? ''
                    setCode(next)
                    // 同步当前文件的内容缓存
                    const isFolder = Array.isArray(items[selectedId]?.children)
                    if (!isFolder) {
                      setFileCodes((prev) => {
                        if (prev[selectedId] === next) return prev
                        return { ...prev, [selectedId]: next }
                      })
                    }
                  }}
                  language={language}
                  height='100%'
                />
              )}
            </div>
            <div className='flex justify-end gap-2 pt-2'>
              <Button variant='outline' size='sm' onClick={() => onOpenChange(false)}>
                取消
              </Button>
              <Button size='sm' onClick={handleSave} disabled={saving}>
                {saving ? '保存中...' : '保存'}
              </Button>
            </div>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  )
}


