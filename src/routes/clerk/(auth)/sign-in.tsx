import { createFileRoute, useSearch } from '@tanstack/react-router'
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { UserAuthForm } from '@/features/auth/sign-in/components/user-auth-form'

export const Route = createFileRoute('/clerk/(auth)/sign-in')({
  component: ClerkLocalSignIn,
})

function ClerkLocalSignIn() {
  const { redirect } = useSearch({ from: '/clerk/(auth)/sign-in' })

  return (
    <Card className='gap-4 w-full max-w-md'>
      <CardHeader>
        <CardTitle className='text-lg tracking-tight'>登录</CardTitle>
        <CardDescription>
          请输入邮箱和密码以
          <br />
          登录到你的账户
        </CardDescription>
      </CardHeader>
      <CardContent>
        <UserAuthForm redirectTo={redirect} />
      </CardContent>
      <CardFooter>
        <p className='text-muted-foreground px-8 text-center text-sm'>
          点击“登录”即表示你同意我们的{' '}
          <a
            href='/terms'
            className='hover:text-primary underline underline-offset-4'
          >
            服务条款
          </a>{' '}
          及{' '}
          <a
            href='/privacy'
            className='hover:text-primary underline underline-offset-4'
          >
            隐私政策
          </a>
          .
        </p>
      </CardFooter>
    </Card>
  )
}
