import { z } from 'zod'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { useEffect, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import {
  Form,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form'
import { Input } from '@/components/ui/input'
import { createTeam, updateTeam } from '@/services/teams'
import { useTeams } from './teams-provider'

const formSchema = z.object({
  name: z.string().min(1, '团队名称必填'),
  slug: z.string().optional(),
  description: z.string().optional(),
})

type TeamForm = z.infer<typeof formSchema>

type TeamActionDialogProps = {
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function TeamActionDialog({ open, onOpenChange }: TeamActionDialogProps) {
  const [submitting, setSubmitting] = useState(false)
  const queryClient = useQueryClient()
  const { currentTeam, setCurrentTeam } = useTeams()

  const isEdit = !!currentTeam

  const form = useForm<TeamForm>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      name: '',
      slug: '',
      description: '',
    },
  })

  useEffect(() => {
    if (open && currentTeam) {
      form.reset({
        name: currentTeam.name,
        slug: currentTeam.slug || '',
        description: '',
      })
    }
    if (open && !currentTeam) {
      form.reset({ name: '', slug: '', description: '' })
    }
  }, [open, currentTeam, form])

  const onSubmit = async (values: TeamForm) => {
    try {
      setSubmitting(true)
      if (isEdit && currentTeam) {
        await updateTeam(currentTeam.id, {
          name: values.name,
          slug: values.slug || undefined,
          description: values.description || undefined,
        })
      } else {
        await createTeam({
          name: values.name,
          slug: values.slug || undefined,
          description: values.description || undefined,
        })
      }
      await queryClient.invalidateQueries({ queryKey: ['teams'] })
      form.reset()
      setCurrentTeam(null)
      onOpenChange(false)
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(state) => {
        form.reset()
        onOpenChange(state)
      }}
    >
      <DialogContent className='sm:max-w-lg'>
        <DialogHeader className='text-start'>
          <DialogTitle>{isEdit ? '编辑团队' : '新增团队'}</DialogTitle>
          <DialogDescription>
            {isEdit ? '在这里修改团队信息。' : '在这里创建一个新的团队。'}
          </DialogDescription>
        </DialogHeader>
        <div className='w-[calc(100%+0.75rem)] overflow-y-auto py-1 pe-3'>
          <Form {...form}>
            <form
              id='team-form'
              onSubmit={form.handleSubmit(onSubmit)}
              className='space-y-4 px-0.5'
            >
              <FormField
                control={form.control}
                name='name'
                render={({ field }) => (
                  <FormItem className='grid grid-cols-6 items-center space-y-0 gap-x-4 gap-y-1'>
                    <FormLabel className='col-span-2 text-end'>团队名称</FormLabel>
                    <FormControl>
                      <Input
                        placeholder='研发团队'
                        className='col-span-4'
                        autoComplete='off'
                        {...field}
                      />
                    </FormControl>
                    <FormMessage className='col-span-4 col-start-3' />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name='slug'
                render={({ field }) => (
                  <FormItem className='grid grid-cols-6 items-center space-y-0 gap-x-4 gap-y-1'>
                    <FormLabel className='col-span-2 text-end'>标识</FormLabel>
                    <FormControl>
                      <Input
                        placeholder='例如: rd-team'
                        className='col-span-4'
                        autoComplete='off'
                        {...field}
                      />
                    </FormControl>
                    <FormMessage className='col-span-4 col-start-3' />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name='description'
                render={({ field }) => (
                  <FormItem className='grid grid-cols-6 items-center space-y-0 gap-x-4 gap-y-1'>
                    <FormLabel className='col-span-2 text-end'>描述</FormLabel>
                    <FormControl>
                      <Input
                        placeholder='可选的团队说明'
                        className='col-span-4'
                        autoComplete='off'
                        {...field}
                      />
                    </FormControl>
                    <FormMessage className='col-span-4 col-start-3' />
                  </FormItem>
                )}
              />
            </form>
          </Form>
        </div>
        <DialogFooter>
          <Button type='submit' form='team-form' disabled={submitting}>
            {submitting ? '保存中...' : '保存'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
