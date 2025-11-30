import { z } from 'zod'

export const teamSchema = z.object({
  id: z.string(),
  name: z.string(),
  slug: z.string().nullable(),
  status: z.number(),
  ownerUsername: z.string(),
  ownerName: z.string().nullable().optional(),
  createdAt: z.coerce.date(),
  updatedAt: z.coerce.date(),
})

export type Team = z.infer<typeof teamSchema>

export const teamListSchema = z.array(teamSchema)

export const teamMemberSchema = z.object({
  id: z.string(),
  userId: z.string(),
  username: z.string(),
  name: z.string().nullable().optional(),
  teamRoleCode: z.union([z.literal('owner'), z.literal('maintainer'), z.literal('member')]).nullable().optional(),
  teamRoleName: z.string().nullable().optional(),
})

export type TeamMember = z.infer<typeof teamMemberSchema>

export const teamMemberListSchema = z.array(teamMemberSchema)
