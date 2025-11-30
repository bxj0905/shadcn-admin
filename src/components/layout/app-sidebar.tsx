import { useLayout } from '@/context/layout-provider'
import { useAuthStore } from '@/stores/auth-store'
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarRail,
} from '@/components/ui/sidebar'
// import { AppTitle } from './app-title'
import { sidebarData } from './data/sidebar-data'
import { NavGroup } from './nav-group'
import { NavUser } from './nav-user'
import { TeamSwitcher } from './team-switcher'

export function AppSidebar() {
  const { collapsible, variant } = useLayout()
  const { auth } = useAuthStore()

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
        <TeamSwitcher />

        {/* Replace <TeamSwitch /> with the following <AppTitle />
         /* if you want to use the normal app title instead of TeamSwitch dropdown */}
        {/* <AppTitle /> */}
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
