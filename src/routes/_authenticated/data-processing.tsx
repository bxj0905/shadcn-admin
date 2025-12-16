import { createFileRoute } from '@tanstack/react-router';
import { AirflowDagsPage } from '@/features/airflow/page';

export const Route = createFileRoute('/_authenticated/data-processing')({
  component: AirflowDagsPage,
});
