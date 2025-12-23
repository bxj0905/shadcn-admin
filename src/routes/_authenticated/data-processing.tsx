import { createFileRoute } from '@tanstack/react-router';
import { PrefectFlowsPage } from '@/features/prefect/page';

export const Route = createFileRoute('/_authenticated/data-processing')({
  component: PrefectFlowsPage,
});
