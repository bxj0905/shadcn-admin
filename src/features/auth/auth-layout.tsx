import { SystemBranding } from '@/components/system-branding'

type AuthLayoutProps = {
  children: React.ReactNode
}

export function AuthLayout({ children }: AuthLayoutProps) {
  return (
    <div className='container grid h-svh max-w-none items-center justify-center'>
      <div className='mx-auto flex w-full flex-col justify-center space-y-2 py-8 sm:w-[480px] sm:p-8'>
        <SystemBranding
          orientation='column'
          showTagline
          size='lg'
          className='items-center justify-center text-center'
        />
        {children}
      </div>
    </div>
  )
}
