import axios from 'axios'

export async function syncLdapUsers() {
  const res = await axios.post('/api/ldap/sync-users')
  return res.data
}
