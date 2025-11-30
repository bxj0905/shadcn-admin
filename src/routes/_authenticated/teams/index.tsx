import z from 'zod'
import { createFileRoute } from '@tanstack/react-router'
import { Teams } from '@/features/teams'

const teamsSearchSchema = z.object({
  page: z.number().optional().catch(1),
  pageSize: z.number().optional().catch(10),
})

export const Route = createFileRoute('/_authenticated/teams/')({
  validateSearch: teamsSearchSchema,
  component: Teams,
})
