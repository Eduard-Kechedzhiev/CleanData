/**
 * API client for the CleanData backend.
 * All calls go through the Vite proxy (/api -> localhost:8000).
 */

export interface UploadUrlResponse {
  upload_url: string;
  s3_key: string;
  queue_position: number;
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
      buildApiErrorPayload(body, response.status, fallbackMessage || response.statusText || "Request failed"),
    );
  }

  return body as T;
}

/**
 * Full upload flow:
 * 1. Get a presigned S3 PUT URL from the backend
 * 2. Upload the file directly to S3
 * 3. Notify the team via backend
 * Returns the queue position for display.
 */
export async function uploadFile(file: File): Promise<{ queue_position: number }> {
  const contentType = file.type || "application/octet-stream";

  // 1. Get presigned URL
  const { upload_url, s3_key, queue_position } = await requestJson<UploadUrlResponse>(
    "/api/upload-url",
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filename: file.name, content_type: contentType }),
    },
    "Failed to prepare upload",
  );

  // 2. Upload directly to S3
  const s3Response = await fetch(upload_url, {
    method: "PUT",
    body: file,
    headers: { "Content-Type": contentType },
  });

  if (!s3Response.ok) {
    throw new ApiRequestError(s3Response.status, {
      code: "s3_upload_failed",
      message: "Failed to upload file. Please try again.",
      retryable: true,
    });
  }

  // 3. Notify team (best-effort — don't fail the user if this errors)
  try {
    await requestJson("/api/notify", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ s3_key, filename: file.name }),
    }, "Notification failed");
  } catch (err) {
    console.warn("Team notification failed, but upload succeeded:", err);
  }

  return { queue_position };
}
