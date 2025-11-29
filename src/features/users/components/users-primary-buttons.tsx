import { RefreshCw, UserPlus } from 'lucide-react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import { syncLdapUsers } from '@/services/ldap'
import { useUsers } from './users-provider'

export function UsersPrimaryButtons() {
  const { setOpen } = useUsers()
  const queryClient = useQueryClient()
  const mutation = useMutation({
    mutationFn: () => syncLdapUsers(),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['users'] })
    },
  })
  return (
    <div className='flex gap-2'>
      <Button
        className='space-x-1'
        variant='outline'
        disabled={mutation.isPending}
        onClick={() => {
          toast.promise(mutation.mutateAsync(), {
            loading: '正在从 LDAP 同步用户...',
            success: (data) =>
              (data && data.message) || 'LDAP 用户同步完成（如有变更请查看列表）',
            error: 'LDAP 用户同步失败，请检查服务器日志。',
          })
        }}
      >
        <span>同步 LDAP 用户</span> <RefreshCw size={18} />
      </Button>
      <Button className='space-x-1' onClick={() => setOpen('add')}>
        <span>新增用户</span> <UserPlus size={18} />
      </Button>
    </div>
  )
}
