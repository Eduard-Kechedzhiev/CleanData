import { useState, useEffect, useRef, useCallback } from "react";
import { Link, useParams, useNavigate } from "react-router-dom";
import {
  Check, Loader2, Mail, AlertCircle, Download, Lock, MessageSquare, Link2,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import Stepper from "@/components/Stepper";
import { useContactEmail } from "@/hooks/use-contact-email";
import { motion } from "framer-motion";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts";
import { ApiRequestError, downloadResults, getJobResults, getJobStatus, submitEmail } from "@/lib/api";
import type { ApiErrorPayload, JobEventEnvelope, JobStatus, JobSummary } from "@/lib/api";

/* ------------------------------------------------------------------ */
/*  Download token helpers (per-requester, stored in localStorage)      */
/* ------------------------------------------------------------------ */

function getStoredToken(jobId: string): string | null {
  try {
    return localStorage.getItem(`cleandata_dl_${jobId}`);
  } catch {
    return null;
  }
}

function storeToken(jobId: string, token: string): void {
  try {
    localStorage.setItem(`cleandata_dl_${jobId}`, token);
  } catch {
    // localStorage may be unavailable (private browsing, etc.)
  }
}

function clearStoredToken(jobId: string): void {
  try {
    localStorage.removeItem(`cleandata_dl_${jobId}`);
  } catch {
    // localStorage may be unavailable (private browsing, etc.)
  }
}

/* ------------------------------------------------------------------ */
/*  Stage definitions & progress computation                          */
/* ------------------------------------------------------------------ */

const UI_STAGES = [
  { label: "Reading your catalog...", showCount: false },
  { label: "Cleaning names, brands & quality scores...", showCount: true },
  { label: "Assigning product categories...", showCount: false },
  { label: "Validating GTINs...", showCount: false },
];

function computeVisualStage(status: JobStatus) {
  if (status.state === "queued") {
    return { stageIndex: 0, rowsDone: 0, rowsTotal: status.summary.row_count };
  }
  if (status.state === "completed" || status.state === "completed_with_warnings") {
    return {
      stageIndex: UI_STAGES.length,
      rowsDone: status.summary.row_count,
      rowsTotal: status.summary.row_count,
    };
  }

  const stageMap: Record<string, number> = {
    reading: 0,
    cleaning: 1,
    taxonomy: 2,
    gtin: 3,
  };

  let stageIndex = 0;
  let rowsDone = 0;
  let rowsTotal = status.summary.row_count;

  for (const stage of status.pipeline.stages) {
    const mapped = stageMap[stage.name];
    if (mapped === undefined) continue;

    if (stage.state === "running" || stage.state === "failed") {
      if (mapped > stageIndex) stageIndex = mapped;
    }
    if (stage.state === "completed" || stage.state === "skipped") {
      if (mapped + 1 > stageIndex) stageIndex = mapped + 1;
    }
    if (stage.name === "cleaning" && stage.state === "running") {
      rowsDone = stage.counts.completed;
      rowsTotal = stage.counts.total || status.summary.row_count;
    }
  }

  return { stageIndex: Math.min(stageIndex, UI_STAGES.length), rowsDone, rowsTotal };
}

function computeProgress(status: JobStatus | null): number {
  if (!status) return 0;
  if (status.state === "completed" || status.state === "completed_with_warnings") return 100;
  return status.pipeline.percent;
}

function getJobFailureMessage(status: JobStatus | null): string {
  if (!status) return "Job not found";
  if (status.failure?.message) return status.failure.message;
  const failedStage = status.pipeline.stages.find((stage) => stage.state === "failed");
  if (failedStage?.error) return failedStage.error;
  return "Pipeline failed";
}

function isCompletedState(state: JobStatus["state"]) {
  return state === "completed" || state === "completed_with_warnings";
}

function isSseEnvelope(value: unknown): value is JobEventEnvelope {
  return Boolean(value && typeof value === "object" && "type" in value && "job" in value);
}

function getStatusFromSsePayload(payload: unknown): JobStatus | null {
  if (isSseEnvelope(payload) && payload.type === "job.updated") return payload.job;
  if (payload && typeof payload === "object" && "state" in payload) {
    return payload as JobStatus;
  }
  return null;
}

function isDeletedEnvelope(payload: unknown): payload is Extract<JobEventEnvelope, { type: "job.deleted" }> {
  return isSseEnvelope(payload) && payload.type === "job.deleted";
}

function getSseErrorMessage(payload: unknown): string | null {
  if (!payload || typeof payload !== "object" || !("error" in payload)) return null;
  const error = (payload as { error: string | ApiErrorPayload }).error;
  if (typeof error === "string") return error;
  if (error && typeof error === "object" && "message" in error && typeof error.message === "string") {
    return error.message;
  }
  return "Request failed";
}

/* ------------------------------------------------------------------ */
/*  Chart colors                                                       */
/* ------------------------------------------------------------------ */

const SCORE_COLORS: Record<string, string> = {
  "1-2": "hsl(var(--score-poor))",
  "3-4": "hsl(var(--score-poor))",
  "5-6": "hsl(var(--score-good))",
  "7-8": "hsl(var(--score-excellent))",
  "9-10": "hsl(var(--score-excellent))",
};

/* ------------------------------------------------------------------ */
/*  JobPage — unified processing + results view                        */
/* ------------------------------------------------------------------ */

type Phase = "loading" | "processing" | "complete" | "failed";
type FailureKind = "job_not_found" | "processing_failed";

const POLL_INTERVAL_MS = 5000;
const MAX_POLL_ERRORS = 30; // ~2.5 min of consecutive failures

const JobPage = () => {
  const { jobId } = useParams<{ jobId: string }>();
  const navigate = useNavigate();
  const contactEmail = useContactEmail();

  const [phase, setPhase] = useState<Phase>("loading");
  const [status, setStatus] = useState<JobStatus | null>(null);
  const [summary, setSummary] = useState<JobSummary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [failureKind, setFailureKind] = useState<FailureKind>("processing_failed");
  const [isPollingFallback, setIsPollingFallback] = useState(false);

  // Per-requester download token (from email submission)
  const [downloadToken, setDownloadToken] = useState<string | null>(() =>
    jobId ? getStoredToken(jobId) : null
  );

  // Email capture during processing
  const [notifyEmail, setNotifyEmail] = useState("");
  const [notifySubmitted, setNotifySubmitted] = useState(false);
  const [notifyError, setNotifyError] = useState<string | null>(null);

  // Download gate (results phase)
  const [gateEmail, setGateEmail] = useState("");
  const [gateCompany, setGateCompany] = useState("");
  const [gateSubmitting, setGateSubmitting] = useState(false);
  const [gateError, setGateError] = useState<string | null>(null);
  const [downloadSubmitting, setDownloadSubmitting] = useState(false);
  const [downloadError, setDownloadError] = useState<string | null>(null);

  const eventSourceRef = useRef<EventSource | null>(null);
  const pollingTimeoutRef = useRef<number | null>(null);
  const pollErrorCountRef = useRef(0);
  const activeRunRef = useRef(0);

  const closeEventSource = useCallback(() => {
    eventSourceRef.current?.close();
    eventSourceRef.current = null;
  }, []);

  const stopPolling = useCallback(() => {
    if (pollingTimeoutRef.current !== null) {
      window.clearTimeout(pollingTimeoutRef.current);
      pollingTimeoutRef.current = null;
    }
  }, []);

  const isActiveRun = useCallback((runId: number) => activeRunRef.current === runId, []);

  const clearDownloadAccess = useCallback((targetJobId: string) => {
    clearStoredToken(targetJobId);
    setDownloadToken(null);
  }, []);

  const markResultsExpired = useCallback((targetJobId: string) => {
    clearDownloadAccess(targetJobId);
    setGateError(null);
    setDownloadError(null);
    setStatus((current) => {
      if (!current || current.job_id !== targetJobId) return current;
      return {
        ...current,
        download: {
          ...current.download,
          state: "expired",
        },
      };
    });
    setPhase("complete");
  }, [clearDownloadAccess]);

  const failJobFlow = useCallback((kind: FailureKind, message: string) => {
    closeEventSource();
    stopPolling();
    setIsPollingFallback(false);
    setFailureKind(kind);
    setError(message);
    setPhase("failed");
  }, [closeEventSource, stopPolling]);

  const fetchResults = useCallback(async (targetJobId: string, runId: number) => {
    try {
      const data = await getJobResults(targetJobId);
      if (!isActiveRun(runId)) return false;
      setSummary(data);
      setPhase("complete");
      setIsPollingFallback(false);
      return true;
    } catch (err: unknown) {
      if (!isActiveRun(runId)) return false;
      if (err instanceof ApiRequestError && err.code === "results_expired") {
        markResultsExpired(targetJobId);
        return true;
      }
      const msg = err instanceof Error ? err.message : "Failed to load results";
      const kind: FailureKind = err instanceof ApiRequestError && err.code === "job_not_found"
        ? "job_not_found"
        : "processing_failed";
      failJobFlow(kind, msg);
      return false;
    }
  }, [failJobFlow, isActiveRun, markResultsExpired]);

  const handleStatusUpdate = useCallback(async (nextStatus: JobStatus, runId: number) => {
    if (!isActiveRun(runId)) return;

    setStatus(nextStatus);

    if (nextStatus.download.state === "expired") {
      markResultsExpired(nextStatus.job_id);
    }

    if (isCompletedState(nextStatus.state)) {
      closeEventSource();
      stopPolling();
      if (nextStatus.download.state === "expired") {
        setPhase("complete");
        return;
      }
      await fetchResults(nextStatus.job_id, runId);
      return;
    }

    if (nextStatus.state === "failed") {
      failJobFlow("processing_failed", getJobFailureMessage(nextStatus));
      return;
    }

    setPhase("processing");
  }, [closeEventSource, failJobFlow, fetchResults, isActiveRun, markResultsExpired, stopPolling]);

  // Open SSE for live progress updates
  const startPolling = useCallback((targetJobId: string, runId: number, delayMs = 0) => {
    if (!isActiveRun(runId)) return;

    closeEventSource();
    stopPolling();
    setIsPollingFallback(true);

    const pollOnce = async () => {
      if (!isActiveRun(runId)) return;
      try {
        const polledStatus = await getJobStatus(targetJobId);
        if (!isActiveRun(runId)) return;
        pollErrorCountRef.current = 0;
        await handleStatusUpdate(polledStatus, runId);
        if (!isActiveRun(runId)) return;
        if (!isCompletedState(polledStatus.state) && polledStatus.state !== "failed") {
          pollingTimeoutRef.current = window.setTimeout(pollOnce, POLL_INTERVAL_MS);
        }
      } catch (err: unknown) {
        if (!isActiveRun(runId)) return;
        if (err instanceof ApiRequestError && err.code === "job_not_found") {
          failJobFlow("job_not_found", err.message);
          return;
        }
        if (err instanceof ApiRequestError && !err.retryable) {
          failJobFlow("processing_failed", err.message);
          return;
        }
        pollErrorCountRef.current += 1;
        if (pollErrorCountRef.current >= MAX_POLL_ERRORS) {
          failJobFlow("processing_failed", "Lost connection to server. Please refresh the page.");
          return;
        }
        pollingTimeoutRef.current = window.setTimeout(pollOnce, POLL_INTERVAL_MS);
      }
    };

    pollingTimeoutRef.current = window.setTimeout(pollOnce, delayMs);
  }, [closeEventSource, failJobFlow, handleStatusUpdate, isActiveRun, stopPolling]);

  const openSSE = useCallback((targetJobId: string, runId: number) => {
    if (!isActiveRun(runId)) return;

    closeEventSource();
    setIsPollingFallback(false);

    const es = new EventSource(`/api/jobs/${targetJobId}/stream`);
    eventSourceRef.current = es;

    es.onopen = () => {
      if (!isActiveRun(runId)) return;
      setIsPollingFallback(false);
    };

    es.onmessage = async (event) => {
      if (!isActiveRun(runId)) return;
      try {
        const parsed = JSON.parse(event.data);
        if (isDeletedEnvelope(parsed)) {
          failJobFlow("job_not_found", "Job not found");
          closeEventSource();
          return;
        }
        const data = getStatusFromSsePayload(parsed);
        if (!data) {
          const message = getSseErrorMessage(parsed);
          if (message) {
            failJobFlow("processing_failed", message);
            closeEventSource();
          }
          return;
        }
        await handleStatusUpdate(data, runId);
      } catch {
        // Ignore malformed SSE payloads and keep the stream open.
      }
    };

    es.onerror = () => {
      if (!isActiveRun(runId)) return;
      closeEventSource();
      startPolling(targetJobId, runId);
    };
  }, [closeEventSource, failJobFlow, handleStatusUpdate, isActiveRun, startPolling]);

  useEffect(() => {
    if (!jobId) return;
    activeRunRef.current += 1;
    const runId = activeRunRef.current;

    // Check localStorage for existing download token
    const stored = getStoredToken(jobId);
    closeEventSource();
    stopPolling();
    setIsPollingFallback(false);
    setPhase("loading");
    setStatus(null);
    setSummary(null);
    setError(null);
    setFailureKind("processing_failed");
    setNotifyEmail("");
    setNotifySubmitted(false);
    setNotifyError(null);
    setGateEmail("");
    setGateCompany("");
    setGateError(null);
    setDownloadError(null);
    setDownloadSubmitting(false);
    setGateSubmitting(false);
    setDownloadToken(stored ?? null);

    const init = async () => {
      try {
        // Fast path: REST check first (instant for completed/failed jobs)
        const initialStatus = await getJobStatus(jobId);
        if (!isActiveRun(runId)) return;

        setStatus(initialStatus);
        if (initialStatus.download.state === "expired") {
          clearDownloadAccess(jobId);
        }
        if (stored && initialStatus.download.state !== "expired") {
          setNotifySubmitted(true);
        }

        if (isCompletedState(initialStatus.state)) {
          if (initialStatus.download.state === "expired") {
            setPhase("complete");
            return;
          }
          await fetchResults(jobId, runId);
          return;
        }
        if (initialStatus.state === "failed") {
          failJobFlow("processing_failed", getJobFailureMessage(initialStatus));
          return;
        }

        // Job is in progress — open SSE for live updates
        setPhase("processing");
        openSSE(jobId, runId);
      } catch (err: unknown) {
        if (!isActiveRun(runId)) return;
        const msg = err instanceof Error ? err.message : "Job not found";
        const kind: FailureKind = err instanceof ApiRequestError && err.code === "job_not_found"
          ? "job_not_found"
          : "processing_failed";
        failJobFlow(kind, msg);
      }
    };

    init();
    return () => {
      activeRunRef.current += 1;
      closeEventSource();
      stopPolling();
    };
  }, [clearDownloadAccess, closeEventSource, failJobFlow, fetchResults, isActiveRun, jobId, openSSE, stopPolling]);

  /* -- Handlers ---------------------------------------------------- */

  const handleNotifySubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!notifyEmail.includes("@") || !jobId) return;
    const runId = activeRunRef.current;
    setNotifyError(null);
    try {
      const res = await submitEmail(jobId, notifyEmail, "");
      if (!isActiveRun(runId)) return;
      setNotifySubmitted(true);
      if (res.download_token) {
        setDownloadToken(res.download_token);
        storeToken(jobId, res.download_token);
      }
    } catch (err: unknown) {
      if (!isActiveRun(runId)) return;
      const msg = err instanceof Error ? err.message : "Failed to submit";
      setNotifyError(msg);
    }
  };

  const handleGateSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!gateEmail.includes("@") || !gateCompany.trim() || !jobId) {
      setGateError("Email and company name are required.");
      return;
    }
    const runId = activeRunRef.current;
    setGateSubmitting(true);
    setGateError(null);
    try {
      const res = await submitEmail(jobId, gateEmail, gateCompany);
      if (!isActiveRun(runId)) return;
      if (res.download_token) {
        setDownloadToken(res.download_token);
        storeToken(jobId, res.download_token);
      } else {
        setGateError("Download access is not available yet. Try again once processing completes.");
      }
    } catch (err: unknown) {
      if (!isActiveRun(runId)) return;
      const msg = err instanceof Error ? err.message : "Failed to submit. Please try again.";
      setGateError(msg);
    } finally {
      if (isActiveRun(runId)) {
        setGateSubmitting(false);
      }
    }
  };

  const handleDownload = useCallback(async () => {
    if (!jobId || !downloadToken) return;
    const runId = activeRunRef.current;

    setDownloadSubmitting(true);
    setDownloadError(null);
    setGateError(null);

    try {
      const { blob, filename } = await downloadResults(jobId, downloadToken);
      if (!isActiveRun(runId)) return;
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.setTimeout(() => window.URL.revokeObjectURL(url), 0);
    } catch (err: unknown) {
      if (!isActiveRun(runId)) return;
      if (err instanceof ApiRequestError) {
        if (err.code === "download_forbidden") {
          clearDownloadAccess(jobId);
          setGateError(err.message);
        } else if (err.code === "results_expired") {
          markResultsExpired(jobId);
          return;
        } else if (err.code === "job_not_found") {
          failJobFlow("job_not_found", err.message);
          return;
        }
      }
      const msg = err instanceof Error ? err.message : "Download failed";
      setDownloadError(msg);
    } finally {
      if (isActiveRun(runId)) {
        setDownloadSubmitting(false);
      }
    }
  }, [clearDownloadAccess, downloadToken, failJobFlow, isActiveRun, jobId, markResultsExpired]);

  /* -- Computed ---------------------------------------------------- */

  const visual = status
    ? computeVisualStage(status)
    : { stageIndex: 0, rowsDone: 0, rowsTotal: 0 };
  const progress = computeProgress(status);
  const scoreColor = (s: number) =>
    s >= 8 ? "text-score-excellent" : s >= 5 ? "text-score-good" : "text-score-poor";
  const isExpired = status?.download.state === "expired";
  const hasDownloadAccess = Boolean(downloadToken) && !isExpired;
  const stepperStep = phase === "complete" ? (hasDownloadAccess ? 4 : 3) : 2;

  /* -- Shared header ----------------------------------------------- */

  const header = (
    <>
      <div className="container mx-auto px-4 py-6 flex items-center justify-between">
        <Link to="/" className="font-heading text-xl font-bold text-foreground">
          <span className="text-primary">Clean</span>Data
        </Link>
        {phase === "complete" && (
          <a href={`mailto:${contactEmail}`}>
            <Button variant="default" size="sm">
              <MessageSquare className="w-4 h-4" />
              Talk to Our Team
            </Button>
          </a>
        )}
      </div>
      <div className="container mx-auto px-4 py-2">
        <Stepper currentStep={stepperStep} />
      </div>
    </>
  );

  /* ================================================================ */
  /*  LOADING                                                          */
  /* ================================================================ */

  if (phase === "loading") {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <Loader2 className="w-8 h-8 animate-spin text-primary" />
      </div>
    );
  }

  /* ================================================================ */
  /*  FAILED                                                           */
  /* ================================================================ */

  if (phase === "failed") {
    return (
      <div className="min-h-screen bg-background flex flex-col">
        <div className="container mx-auto px-4 py-6">
          <Link to="/" className="font-heading text-xl font-bold text-foreground">
            <span className="text-primary">Clean</span>Data
          </Link>
        </div>
        <div className="flex-1 flex items-center justify-center px-4 pb-20">
          <div className="max-w-md w-full text-center">
            <AlertCircle className="w-12 h-12 text-destructive mx-auto mb-4" />
            <h1 className="font-heading text-2xl font-bold text-foreground mb-2">
              {failureKind === "job_not_found" ? "Job Not Found" : "Processing Failed"}
            </h1>
            <p className="text-muted-foreground mb-6">{error}</p>
            <Button variant="default" onClick={() => navigate("/")}>
              Try Again
            </Button>
          </div>
        </div>
      </div>
    );
  }

  /* ================================================================ */
  /*  PROCESSING                                                       */
  /* ================================================================ */

  if (phase === "processing") {
    return (
      <div className="min-h-screen bg-background flex flex-col">
        {header}
        <div className="flex-1 flex items-center justify-center px-4 pb-20">
          <div className="max-w-md w-full">
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              className="text-center"
            >
              <div className="mb-8">
                <Loader2 className="w-12 h-12 text-primary mx-auto animate-spin" />
              </div>

              <h1 className="font-heading text-2xl font-bold text-foreground mb-2">
                Processing your catalog
              </h1>

              {status && (
                <p className="text-sm text-muted-foreground mb-4">
                  {status.summary.input_filename} &middot; {status.summary.row_count.toLocaleString()} rows
                </p>
              )}

              {/* Progress bar */}
              <div className="w-full bg-muted rounded-full h-2 mb-6 overflow-hidden">
                <motion.div
                  className="h-full bg-primary rounded-full"
                  initial={{ width: 0 }}
                  animate={{ width: `${progress}%` }}
                  transition={{ ease: "linear", duration: 0.5 }}
                />
              </div>

              {/* Stages */}
              <div className="space-y-2 mb-10 text-left">
                {UI_STAGES.map((stage, i) => (
                  <div
                    key={i}
                    className={`flex items-center gap-2 text-sm transition-opacity ${
                      i < visual.stageIndex
                        ? "text-primary"
                        : i === visual.stageIndex
                        ? "text-foreground font-medium"
                        : "text-muted-foreground/40"
                    }`}
                  >
                    {i < visual.stageIndex ? (
                      <Check className="w-4 h-4 text-primary flex-shrink-0" />
                    ) : i === visual.stageIndex ? (
                      <Loader2 className="w-4 h-4 animate-spin flex-shrink-0" />
                    ) : (
                      <div className="w-4 h-4 rounded-full border border-current flex-shrink-0" />
                    )}
                    <span>{stage.label}</span>
                    {stage.showCount && i === visual.stageIndex && visual.rowsTotal > 0 && (
                      <span className="text-xs text-muted-foreground ml-auto">
                        {visual.rowsDone.toLocaleString()} / {visual.rowsTotal.toLocaleString()}
                      </span>
                    )}
                  </div>
                ))}
              </div>

              {isPollingFallback && (
                <p className="text-xs text-muted-foreground mb-6">
                  Live updates dropped. Checking status every few seconds instead.
                </p>
              )}

              {/* Email capture + bookmark hint */}
              {!notifySubmitted ? (
                <motion.div
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 1 }}
                  className="bg-card border border-border rounded-xl p-6"
                >
                  <Mail className="w-8 h-8 text-primary mx-auto mb-3" />
                  <p className="font-medium text-foreground mb-1">
                    Processing takes up to 40 minutes
                  </p>
                  <p className="text-sm text-muted-foreground mb-3">
                    Leave your email and we'll follow up with tips for your catalog.
                  </p>
                  <form onSubmit={handleNotifySubmit} className="flex gap-2">
                    <Input
                      type="email"
                      placeholder="you@company.com"
                      value={notifyEmail}
                      onChange={(e) => setNotifyEmail(e.target.value)}
                      className="flex-1"
                    />
                    <Button type="submit" variant="default" size="default">
                      Submit
                    </Button>
                  </form>
                  {notifyError && (
                    <p className="text-destructive text-sm mt-2">{notifyError}</p>
                  )}
                  <div className="flex items-center justify-center gap-1.5 mt-4 text-xs text-muted-foreground">
                    <Link2 className="w-3.5 h-3.5" />
                    <span>Bookmark this page — results will appear here when ready.</span>
                  </div>
                </motion.div>
              ) : (
                <motion.div
                  initial={{ opacity: 0, scale: 0.95 }}
                  animate={{ opacity: 1, scale: 1 }}
                  className="bg-accent border border-primary/20 rounded-xl p-6 text-center"
                >
                  <Check className="w-8 h-8 text-primary mx-auto mb-2" />
                  <p className="font-medium text-foreground">Thanks! We'll be in touch.</p>
                  <div className="flex items-center justify-center gap-1.5 mt-3 text-xs text-muted-foreground">
                    <Link2 className="w-3.5 h-3.5" />
                    <span>Bookmark this page — results will appear here when ready.</span>
                  </div>
                </motion.div>
              )}
            </motion.div>
          </div>
        </div>
      </div>
    );
  }

  /* ================================================================ */
  /*  COMPLETE (Results)                                               */
  /* ================================================================ */

  if (isExpired && !summary) {
    return (
      <div className="min-h-screen bg-background flex flex-col">
        {header}
        <div className="flex-1 flex items-center justify-center px-4 pb-20">
          <div className="max-w-md w-full text-center">
            <AlertCircle className="w-12 h-12 text-amber-600 mx-auto mb-4" />
            <h1 className="font-heading text-2xl font-bold text-foreground mb-2">
              Results Expired
            </h1>
            <p className="text-muted-foreground mb-6">
              This report is no longer available. Upload the catalog again to regenerate fresh results.
            </p>
            <Button variant="default" onClick={() => navigate("/")}>
              Upload Again
            </Button>
          </div>
        </div>
      </div>
    );
  }

  if (!summary) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <Loader2 className="w-8 h-8 animate-spin text-primary" />
      </div>
    );
  }

  const gtinPercent = summary.gtins_total > 0
    ? Math.round((summary.gtins_found / summary.gtins_total) * 100)
    : 0;
  const resultWarnings = status?.warnings ?? [];

  const summaryStats = [
    { label: "Products processed", value: summary.row_count.toLocaleString(), sub: null, highlight: false },
    { label: "Avg quality score", value: summary.avg_quality_score?.toFixed(1) ?? "N/A", sub: summary.avg_quality_score != null ? "/ 10" : null, highlight: true },
    { label: "Brands extracted", value: summary.brands_extracted.toLocaleString(), sub: "unique", highlight: false },
    { label: "GTINs validated", value: `${summary.gtins_found.toLocaleString()} / ${summary.gtins_total.toLocaleString()}`, sub: `${gtinPercent}%`, highlight: false },
  ];

  const scoreDistribution = summary.quality_distribution.map((d) => ({
    ...d,
    color: SCORE_COLORS[d.score] || "hsl(var(--primary))",
  }));

  return (
    <div className="min-h-screen bg-background">
      {header}

      <div className="container mx-auto px-4 py-8 max-w-6xl">
        {resultWarnings.length > 0 && (
          <motion.div
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            className="mb-6 rounded-xl border border-amber-300/40 bg-amber-50 px-4 py-3 text-sm text-amber-900"
          >
            Some pipeline steps completed with warnings: {resultWarnings.map((warning) => warning.stage ?? "job").join(", ")}.
          </motion.div>
        )}

        {/* Summary stats */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-10"
        >
          {summaryStats.map((stat) => (
            <div
              key={stat.label}
              className={`bg-card border rounded-xl p-6 text-center ${
                stat.highlight ? "border-primary/30 shadow-md" : "border-border"
              }`}
            >
              <p className={`font-heading text-3xl md:text-4xl font-bold ${
                stat.highlight ? "text-score-excellent" : "text-foreground"
              }`}>
                {stat.value}
                {stat.sub && (
                  <span className="text-base font-normal text-muted-foreground ml-1">{stat.sub}</span>
                )}
              </p>
              <p className="text-sm text-muted-foreground mt-1">{stat.label}</p>
            </div>
          ))}
        </motion.div>

        {/* Before / After preview */}
        {summary.sample_rows.length > 0 && (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.1 }}
            className="mb-10"
          >
            <h2 className="font-heading text-xl font-semibold text-foreground mb-4">
              Before &rarr; After Preview
            </h2>
            <div className="overflow-x-auto rounded-xl border border-border bg-card">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border bg-muted/50">
                    <th className="text-left px-4 py-3 font-medium text-muted-foreground">Original</th>
                    <th className="text-left px-4 py-3 font-medium text-muted-foreground">Cleaned Name</th>
                    <th className="text-left px-4 py-3 font-medium text-muted-foreground">Brand</th>
                    <th className="text-left px-4 py-3 font-medium text-muted-foreground">Pack/Size</th>
                    <th className="text-left px-4 py-3 font-medium text-muted-foreground">Category</th>
                    <th className="text-center px-4 py-3 font-medium text-muted-foreground">Score</th>
                  </tr>
                </thead>
                <tbody>
                  {summary.sample_rows.map((row, i) => (
                    <tr key={i} className="border-b border-border/50 last:border-0">
                      <td className="px-4 py-3 text-muted-foreground font-mono text-xs max-w-[180px] truncate">{row.original}</td>
                      <td className="px-4 py-3 text-foreground font-medium">{row.cleaned}</td>
                      <td className="px-4 py-3 text-foreground">{row.brand}</td>
                      <td className="px-4 py-3 text-foreground">{row.pack}</td>
                      <td className="px-4 py-3 text-foreground text-xs">{row.category}</td>
                      <td className={`px-4 py-3 text-center font-bold ${scoreColor(row.score)}`}>{row.score}/10</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </motion.div>
        )}

        {/* Charts */}
        <div className="grid md:grid-cols-2 gap-6 mb-10">
          {scoreDistribution.length > 0 && (
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.2 }}
              className="bg-card border border-border rounded-xl p-6"
            >
              <h3 className="font-heading font-semibold text-foreground mb-4">Quality Score Distribution</h3>
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={scoreDistribution}>
                  <XAxis dataKey="score" tick={{ fontSize: 12, fill: "hsl(var(--muted-foreground))" }} axisLine={false} tickLine={false} />
                  <YAxis tick={{ fontSize: 12, fill: "hsl(var(--muted-foreground))" }} axisLine={false} tickLine={false} />
                  <Tooltip contentStyle={{ background: "hsl(var(--card))", border: "1px solid hsl(var(--border))", borderRadius: 8, fontSize: 13 }} />
                  <Bar dataKey="count" radius={[6, 6, 0, 0]}>
                    {scoreDistribution.map((entry, i) => (
                      <Cell key={i} fill={entry.color} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </motion.div>
          )}

          {summary.top_brands.length > 0 && (
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.3 }}
              className="bg-card border border-border rounded-xl p-6"
            >
              <h3 className="font-heading font-semibold text-foreground mb-4">Top Brands Extracted</h3>
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={summary.top_brands} layout="vertical">
                  <XAxis type="number" tick={{ fontSize: 12, fill: "hsl(var(--muted-foreground))" }} axisLine={false} tickLine={false} />
                  <YAxis type="category" dataKey="name" tick={{ fontSize: 12, fill: "hsl(var(--muted-foreground))" }} axisLine={false} tickLine={false} width={70} />
                  <Tooltip contentStyle={{ background: "hsl(var(--card))", border: "1px solid hsl(var(--border))", borderRadius: 8, fontSize: 13 }} />
                  <Bar dataKey="count" fill="hsl(var(--primary))" radius={[0, 6, 6, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </motion.div>
          )}
        </div>

        {/* Category breakdown */}
        {summary.category_breakdown.length > 0 && (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.35 }}
            className="bg-card border border-border rounded-xl p-6 mb-10"
          >
            <h3 className="font-heading font-semibold text-foreground mb-4">Category Breakdown</h3>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              {summary.category_breakdown.map((cat) => (
                <div key={cat.name} className="flex items-center justify-between text-sm px-3 py-2 rounded-lg bg-muted/50">
                  <span className="text-foreground truncate mr-2">{cat.name}</span>
                  <span className="text-muted-foreground font-medium whitespace-nowrap">{cat.count.toLocaleString()}</span>
                </div>
              ))}
            </div>
          </motion.div>
        )}

        {/* Pepper CTA */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.4 }}
          className="bg-hero rounded-xl p-8 text-center mb-10"
        >
          <p className="text-hero-foreground font-heading text-xl font-semibold mb-2">
            Your catalog has {summary.row_count.toLocaleString()} products ready for image sourcing.
          </p>
          <p className="text-hero-muted mb-5">Want us to help?</p>
          <a href={`mailto:${contactEmail}`}>
            <Button variant="cta-dark" size="lg">
              <MessageSquare className="w-5 h-5" />
              Talk to Our Team
            </Button>
          </a>
        </motion.div>

        {/* Download / Gate */}
        {isExpired ? (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="bg-amber-50 border border-amber-300/40 rounded-xl p-8 text-center"
          >
            <AlertCircle className="w-10 h-10 text-amber-700 mx-auto mb-3" />
            <h3 className="font-heading text-xl font-semibold text-foreground mb-2">This report has expired</h3>
            <p className="text-sm text-muted-foreground mb-5">
              Upload the catalog again to regenerate the downloadable CSV and results preview.
            </p>
            <Button variant="default" size="lg" onClick={() => navigate("/")}>
              Upload Again
            </Button>
          </motion.div>
        ) : hasDownloadAccess ? (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="bg-accent border border-primary/20 rounded-xl p-8 text-center"
          >
            <Download className="w-10 h-10 text-primary mx-auto mb-3" />
            <h3 className="font-heading text-xl font-semibold text-foreground mb-2">Your full report is ready</h3>
            <p className="text-sm text-muted-foreground mb-5">
              Includes cleaned names, brands, pack sizes, categories, quality scores, and GTIN status for all {summary.row_count.toLocaleString()} products.
            </p>
            <Button variant="cta" size="lg" onClick={handleDownload} disabled={downloadSubmitting}>
              {downloadSubmitting ? <Loader2 className="w-5 h-5 animate-spin" /> : <Download className="w-5 h-5" />}
              {downloadSubmitting ? "Preparing download..." : "Download Full CSV"}
            </Button>
            {downloadError && <p className="text-destructive text-sm mt-3">{downloadError}</p>}
            <p className="text-xs text-muted-foreground mt-4">Results expire in 24 hours</p>
          </motion.div>
        ) : (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="bg-card border border-border rounded-xl p-8 text-center relative overflow-hidden"
          >
            <div className="absolute inset-0 bg-gradient-to-t from-card via-card/80 to-transparent pointer-events-none" />
            <div className="relative z-10">
              <Lock className="w-10 h-10 text-muted-foreground mx-auto mb-3" />
              <h3 className="font-heading text-xl font-semibold text-foreground mb-2">
                Enter your email to download your full report
              </h3>
              <p className="text-sm text-muted-foreground mb-6 max-w-md mx-auto">
                Get the complete enriched CSV with cleaned names, brands, categories, quality scores, and GTIN validation for all {summary.row_count.toLocaleString()} products.
              </p>
              <form onSubmit={handleGateSubmit} className="flex flex-col sm:flex-row gap-3 max-w-md mx-auto">
                <Input
                  type="email"
                  placeholder="you@company.com"
                  value={gateEmail}
                  onChange={(e) => setGateEmail(e.target.value)}
                  required
                />
                <Input
                  type="text"
                  placeholder="Company name"
                  value={gateCompany}
                  onChange={(e) => setGateCompany(e.target.value)}
                  required
                />
                <Button type="submit" variant="cta" size="default" className="whitespace-nowrap" disabled={gateSubmitting}>
                  {gateSubmitting ? <Loader2 className="w-4 h-4 animate-spin" /> : "Download Report"}
                </Button>
              </form>
              {gateError && <p className="text-destructive text-sm mt-2">{gateError}</p>}
            </div>
          </motion.div>
        )}
      </div>
    </div>
  );
};

export default JobPage;
