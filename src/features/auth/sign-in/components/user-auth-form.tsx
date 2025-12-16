import { useState } from 'react'
import { z } from 'zod'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { Link, useNavigate } from '@tanstack/react-router'
import { Loader2, LogIn } from 'lucide-react'
import { toast } from 'sonner'
import { useTranslation } from 'react-i18next'
import i18n from '@/lib/i18n'
import { IconFacebook, IconGithub } from '@/assets/brand-icons'
import { useAuthStore } from '@/stores/auth-store'
import { login, type AuthProvider } from '@/services/auth'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import {
  Form,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form'
import { Input } from '@/components/ui/input'
import { PasswordInput } from '@/components/password-input'

const formSchema = z.object({
  email: z
    .string()
    .min(1, i18n.t('auth.form.validation.emailOrUsername.required'))
    .email(i18n.t('auth.form.validation.email.invalid'))
    .or(
      z
        .string()
        .min(1, i18n.t('auth.form.validation.emailOrUsername.required')),
    ),
  password: z
    .string()
    .min(1, i18n.t('auth.form.validation.password.required'))
    .min(7, i18n.t('auth.form.validation.password.min')),
})

interface UserAuthFormProps extends React.HTMLAttributes<HTMLFormElement> {
  redirectTo?: string
}

export function UserAuthForm({
  className,
  redirectTo,
  ...props
}: UserAuthFormProps) {
  const [isLoading, setIsLoading] = useState(false)
  const navigate = useNavigate()
  const { auth } = useAuthStore()
  const { t } = useTranslation()

  const form = useForm<z.infer<typeof formSchema>>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      email: '',
      password: '',
    },
  })

  async function onSubmit(data: z.infer<typeof formSchema>) {
    setIsLoading(true)

    await toast.promise(
      // provider 使用 auto，由后端自动判断是本地账号还是 LDAP 账号
      login('auto' as AuthProvider, data.email, data.password),
      {
        loading: i18n.t('auth.toast.signIn.loading'),
        success: (res) => {
          const exp = Date.now() + res.expiresIn * 1000

          auth.setUser({
            ...res.user,
            exp,
          })
          auth.setAccessToken(res.accessToken)

          const targetPath = redirectTo || '/_authenticated/'
          navigate({ to: targetPath, replace: true })

          return i18n.t('auth.toast.signIn.success', {
            email: res.user.email,
          })
        },
        error: (err) => {
          // eslint-disable-next-line no-console
          console.error(err)
          return i18n.t('auth.toast.signIn.error')
        },
      }
    )

    setIsLoading(false)
  }

  return (
    <Form {...form}>
      <form
        onSubmit={form.handleSubmit(onSubmit)}
        className={cn('grid gap-3', className)}
        {...props}
      >
        <FormField
          control={form.control}
          name='email'
          render={({ field }) => (
            <FormItem>
              <FormLabel>{t('auth.form.emailOrUsername')}</FormLabel>
              <FormControl>
                <Input
                  placeholder={t('auth.form.emailOrUsername.placeholder')}
                  {...field}
                />
              </FormControl>
              <FormMessage />
            </FormItem>
          )}
        />
        <FormField
          control={form.control}
          name='password'
          render={({ field }) => (
            <FormItem className='relative'>
              <FormLabel>{t('auth.form.password')}</FormLabel>
              <FormControl>
                <PasswordInput placeholder='********' {...field} />
              </FormControl>
              <FormMessage />
              <Link
                to='/forgot-password'
                className='text-muted-foreground absolute end-0 -top-0.5 text-sm font-medium hover:opacity-75'
              >
                {t('auth.form.forgotPassword')}
              </Link>
            </FormItem>
          )}
        />
        <Button className='mt-2' disabled={isLoading}>
          {isLoading ? <Loader2 className='animate-spin' /> : <LogIn />}
          {t('auth.signIn.submit')}
        </Button>

        <div className='relative my-2'>
          <div className='absolute inset-0 flex items-center'>
            <span className='w-full border-t' />
          </div>
          <div className='relative flex justify-center text-xs uppercase'>
            <span className='bg-background text-muted-foreground px-2'>
              {t('auth.form.orContinueWith')}
            </span>
          </div>
        </div>

        <div className='grid grid-cols-2 gap-2'>
          <Button variant='outline' type='button' disabled={isLoading}>
            <IconGithub className='h-4 w-4' /> GitHub
          </Button>
          <Button variant='outline' type='button' disabled={isLoading}>
            <IconFacebook className='h-4 w-4' /> Facebook
          </Button>
        </div>
      </form>
    </Form>
  )
}
