import axios from 'axios';

export type AirflowDag = {
  dag_id: string;
  description?: string | null;
  is_paused?: boolean;
  tags?: { name: string }[];
  next_dagrun?: string | null;
  next_dagrun_create_after?: string | null;
};

export type AirflowListDagsResponse = {
  dags: AirflowDag[];
  total_entries: number;
};

export type AirflowDagSourceResponse = {
  dag_id: string;
  file_token?: string;
  source_code?: string;
};

export async function fetchAirflowDags(params?: { limit?: number; offset?: number }) {
  const res = await axios.get<AirflowListDagsResponse>('/api/airflow/dags', {
    params,
  });
  return res.data;
}

export async function fetchAirflowDagSource(dagId: string) {
  const res = await axios.get<AirflowDagSourceResponse>(`/api/airflow/dags/${encodeURIComponent(dagId)}/source`);
  return res.data;
}
