import {
  Construction,
  LayoutDashboard,
  Monitor,
  Bug,
  ListTodo,
  FileX,
  HelpCircle,
  Lock,
  Bell,
  Package,
  Palette,
  ServerOff,
  Settings,
  Wrench,
  UserCog,
  UserX,
  Users,
  MessagesSquare,
  ShieldCheck,
  AudioWaveform,
  Command,
  GalleryVerticalEnd,
} from 'lucide-react'
import { type SidebarData } from '../types'

export const sidebarData: SidebarData = {
  user: {
    name: 'satnaing',
    email: 'satnaingdev@gmail.com',
    avatar: '/avatars/shadcn.jpg',
  },
  teams: [
    {
      name: 'Shadcn Admin',
      logo: Command,
      plan: 'Vite + ShadcnUI',
    },
    {
      name: 'Acme Inc',
      logo: GalleryVerticalEnd,
      plan: 'Enterprise',
    },
    {
      name: 'Acme Corp.',
      logo: AudioWaveform,
      plan: 'Startup',
    },
  ],
  navGroups: [
    {
      title: 'General',
      i18nKey: 'sidebar.group.general',
      items: [
        {
          title: 'Dashboard',
          i18nKey: 'sidebar.nav.dashboard',
          url: '/',
          icon: LayoutDashboard,
        },
        {
          title: 'Tasks',
          i18nKey: 'sidebar.nav.tasks',
          url: '/tasks',
          icon: ListTodo,
        },
        {
          title: 'Apps',
          i18nKey: 'sidebar.nav.apps',
          url: '/apps',
          icon: Package,
        },
        {
          title: 'Chats',
          i18nKey: 'sidebar.nav.chats',
          url: '/chats',
          badge: '3',
          icon: MessagesSquare,
        },
        {
          title: 'Users',
          i18nKey: 'sidebar.nav.users',
          url: '/users',
          icon: Users,
        },
        // LDAP 相关演示页面入口已去掉，实际同步入口在 /users 页面的“同步 LDAP 用户”按钮
      ],
    },
    {
      title: 'Pages',
      i18nKey: 'sidebar.group.pages',
      items: [
        {
          title: 'Auth',
          i18nKey: 'sidebar.nav.auth.group',
          icon: ShieldCheck,
          items: [
            {
              title: 'Sign In',
              i18nKey: 'sidebar.nav.auth.signIn',
              url: '/sign-in',
            },
            {
              title: 'Sign In (2 Col)',
              i18nKey: 'sidebar.nav.auth.signIn2Col',
              url: '/sign-in-2',
            },
            {
              title: 'Sign Up',
              i18nKey: 'sidebar.nav.auth.signUp',
              url: '/sign-up',
            },
            {
              title: 'Forgot Password',
              i18nKey: 'sidebar.nav.auth.forgotPassword',
              url: '/forgot-password',
            },
            {
              title: 'OTP',
              i18nKey: 'sidebar.nav.auth.otp',
              url: '/otp',
            },
          ],
        },
        {
          title: 'Errors',
          i18nKey: 'sidebar.nav.errors.group',
          icon: Bug,
          items: [
            {
              title: 'Unauthorized',
              i18nKey: 'sidebar.nav.errors.unauthorized',
              url: '/errors/unauthorized',
              icon: Lock,
            },
            {
              title: 'Forbidden',
              i18nKey: 'sidebar.nav.errors.forbidden',
              url: '/errors/forbidden',
              icon: UserX,
            },
            {
              title: 'Not Found',
              i18nKey: 'sidebar.nav.errors.notFound',
              url: '/errors/not-found',
              icon: FileX,
            },
            {
              title: 'Internal Server Error',
              i18nKey: 'sidebar.nav.errors.internalServerError',
              url: '/errors/internal-server-error',
              icon: ServerOff,
            },
            {
              title: 'Maintenance Error',
              i18nKey: 'sidebar.nav.errors.maintenanceError',
              url: '/errors/maintenance-error',
              icon: Construction,
            },
          ],
        },
      ],
    },
    {
      title: 'Other',
      i18nKey: 'sidebar.group.other',
      items: [
        {
          title: 'Settings',
          i18nKey: 'sidebar.nav.settings.group',
          icon: Settings,
          items: [
            {
              title: 'Profile',
              i18nKey: 'sidebar.nav.settings.profile',
              url: '/settings',
              icon: UserCog,
            },
            {
              title: 'Account',
              i18nKey: 'sidebar.nav.settings.account',
              url: '/settings/account',
              icon: Wrench,
            },
            {
              title: 'Appearance',
              i18nKey: 'sidebar.nav.settings.appearance',
              url: '/settings/appearance',
              icon: Palette,
            },
            {
              title: 'Notifications',
              i18nKey: 'sidebar.nav.settings.notifications',
              url: '/settings/notifications',
              icon: Bell,
            },
            {
              title: 'Display',
              i18nKey: 'sidebar.nav.settings.display',
              url: '/settings/display',
              icon: Monitor,
            },
          ],
        },
        {
          title: 'Help Center',
          i18nKey: 'sidebar.nav.helpCenter',
          url: '/help-center',
          icon: HelpCircle,
        },
      ],
    },
  ],
}
