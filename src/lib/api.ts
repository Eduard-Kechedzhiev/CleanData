/**
 * API client for the CleanData backend.
 * All calls go through the Vite proxy (/api -> localhost:8000).
 */

export interface UploadResponse {
  job_id: string;
  file_name: string;
  row_count: number;
  columns: string[];
}

export interface StageCounts {
  completed: number;
  total: number;
}

export interface FailureInfo {
  stage: string | null;
  message: string;
  retryable: boolean;
  occurred_at: string;
}

export interface StageProgress {
  name: string;
  state: "pending" | "running" | "completed" | "skipped" | "failed";
  percent: number;
  counts: StageCounts;
  started_at: string | null;
  completed_at: string | null;
  message: string | null;
  error: string | null;
}

export interface JobPipeline {
  current_stage: string | null;
  percent: number;
  stage_order: string[];
  stages: StageProgress[];
}

export interface JobSummaryMeta {
  row_count: number;
  input_filename: string;
}

export interface JobStatus {
  job_id: string;
  state: "queued" | "running" | "completed" | "completed_with_warnings" | "failed";
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  summary: JobSummaryMeta;
  pipeline: JobPipeline;
  failure: FailureInfo | null;
  warnings: FailureInfo[];
}

export interface JobUpdatedEnvelope {
  type: "job.updated";
  sequence: number;
  job_id: string;
  job: JobStatus;
}

export interface JobDeletedEnvelope {
  type: "job.deleted";
  sequence: number;
  job_id: string;
  job: null;
}

export type JobEventEnvelope = JobUpdatedEnvelope | JobDeletedEnvelope;

export interface SampleRow {
  original: string;
  cleaned: string;
  brand: string;
  pack: string;
  category: string;
  score: number;
}

export interface JobSummary {
  job_id: string;
  row_count: number;
  column_count: number;
  avg_quality_score: number | null;
  quality_distribution: { score: string; count: number }[];
  brands_extracted: number;
  top_brands: { name: string; count: number }[];
  gtins_found: number;
  gtins_total: number;
  category_breakdown: { name: string; count: number }[];
  sample_rows: SampleRow[];
}

export interface EmailSubmitResponse {
  ok: boolean;
  email: string;
}

export interface PublicConfig {
  contact_email: string;
}

export interface ApiErrorPayload {
  code: string;
  message: string;
  retryable: boolean;
  details?: unknown;
}

export class ApiRequestError extends Error {
  status: number;
  code: string;
  retryable: boolean;
  details?: unknown;

  constructor(status: number, payload: ApiErrorPayload) {
    super(payload.message);
    this.name = "ApiRequestError";
    this.status = status;
    this.code = payload.code;
    this.retryable = payload.retryable;
    this.details = payload.details;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object";
}

async function readJson(response: Response): Promise<unknown> {
  const text = await response.text();
  if (!text) return null;

  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function buildApiErrorPayload(body: unknown, status: number, fallbackMessage: string): ApiErrorPayload {
  if (isRecord(body) && isRecord(body.error)) {
    const error = body.error;
    return {
      code: typeof error.code === "string" && error.code ? error.code : "request_failed",
      message: typeof error.message === "string" && error.message ? error.message : fallbackMessage,
      retryable: typeof error.retryable === "boolean" ? error.retryable : status >= 500 || status === 429,
      details: error.details,
    };
  }

  if (isRecord(body) && typeof body.detail === "string" && body.detail) {
    return {
      code: status === 422 ? "validation_error" : "request_failed",
      message: body.detail,
      retryable: status >= 500 || status === 429,
    };
  }

  return {
    code: status === 422 ? "validation_error" : "request_failed",
    message: fallbackMessage,
    retryable: status >= 500 || status === 429,
  };
}

async function requestJson<T>(input: RequestInfo | URL, init: RequestInit, fallbackMessage: string): Promise<T> {
  const response = await fetch(input, init);
  const body = await readJson(response);

  if (!response.ok) {
    throw new ApiRequestError(
      response.status,
      buildApiErrorPayload(
        body,
        response.status,
        fallbackMessage || response.statusText || "Request failed",
      ),
    );
  }

  return body as T;
}

export async function uploadFile(file: File): Promise<UploadResponse> {
  const form = new FormData();
  form.append("file", file);

  return requestJson<UploadResponse>("/api/upload", { method: "POST", body: form }, "Upload failed");
}

export async function getJobStatus(jobId: string): Promise<JobStatus> {
  return requestJson<JobStatus>(`/api/jobs/${jobId}/status`, {}, "Failed to get status");
}

export async function getJobResults(jobId: string): Promise<JobSummary> {
  return requestJson<JobSummary>(`/api/jobs/${jobId}/results`, {}, "Failed to get results");
}

export async function submitEmail(jobId: string, email: string, company: string, distributorType: string = ""): Promise<EmailSubmitResponse> {
  return requestJson<EmailSubmitResponse>(
    `/api/jobs/${jobId}/email`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, company, distributor_type: distributorType }),
    },
    "Email submit failed",
  );
}

export async function getPublicConfig(): Promise<PublicConfig> {
  return requestJson<PublicConfig>("/api/config", {}, "Failed to get config");
}

export async function getSampleResults(): Promise<JobSummary> {
  return requestJson<JobSummary>("/api/sample", {}, "Failed to get sample");
}
