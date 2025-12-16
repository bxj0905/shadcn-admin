import { useLayout } from '@/context/layout-provider'
import { useAuthStore } from '@/stores/auth-store'
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarRail,
  useSidebar,
} from '@/components/ui/sidebar'
import { sidebarData } from './data/sidebar-data'
import { NavGroup } from './nav-group'
import { NavUser } from './nav-user'
import { TeamSwitcher } from './team-switcher'
import { SystemBranding } from '@/components/system-branding'

export function AppSidebar() {
  const { collapsible, variant } = useLayout()
  const { auth } = useAuthStore()
  const { state } = useSidebar()

  const currentUser = auth.user
  const isAdmin = !!currentUser &&
    (currentUser.roles.includes('admin') || currentUser.roles.includes('super_admin'))

  const navGroups = isAdmin
    ? sidebarData.navGroups
    : sidebarData.navGroups.map((group) => ({
        ...group,
        items: group.items.filter(
          (item) => item.url !== '/users' && item.url !== '/teams' && item.url !== '/team-members',
        ) as typeof group.items,
      }))
  const navUser = currentUser
    ? {
        name: currentUser.name || currentUser.username,
        email: currentUser.email,
        avatar: currentUser.avatarUrl || sidebarData.user.avatar,
      }
    : sidebarData.user

  return (
    <Sidebar collapsible={collapsible} variant={variant}>
      <SidebarHeader>
        {state === 'collapsed' && (
          <SystemBranding
            orientation='column'
            showName={false}
            showTagline={false}
            size='sm'
            className='justify-center'
          />
        )}

        <TeamSwitcher />
      </SidebarHeader>
      <SidebarContent>
        {navGroups.map((props) => (
          <NavGroup key={props.title} {...props} />
        ))}
      </SidebarContent>
      <SidebarFooter>
        <NavUser user={navUser} />
      </SidebarFooter>
      <SidebarRail />
    </Sidebar>
  )
}
