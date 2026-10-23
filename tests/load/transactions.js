import http from "k6/http";
import encoding from "k6/encoding";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";
import exec from "k6/execution";

const scenario = __ENV.SCENARIO || "sustained";
const baseUrl = (__ENV.BASE_URL || "http://localhost:8000").replace(/\/$/, "");
const rate = integerEnv("RATE", 100, 1);
const duration = __ENV.DURATION || "60s";
const burstCount = integerEnv("BURST_COUNT", 1000, 1);
const burstVUs = integerEnv("BURST_VUS", 100, 1);
const completionTimeoutSeconds = integerEnv("COMPLETION_TIMEOUT_SECONDS", 30, 1);
const completionPollingSeconds = numberEnv("COMPLETION_POLL_SECONDS", 0.2, 0.01);
const runId = __ENV.LOAD_RUN_ID || `load-${Date.now()}`;
const userId = __ENV.USER_ID || `${runId}-user`;
const managementUrl = __ENV.RABBITMQ_MANAGEMENT_URL;
const managementUser = __ENV.RABBITMQ_USERNAME || "async_processing";
const managementPassword = __ENV.RABBITMQ_PASSWORD || "async_processing";
const managementVhost = __ENV.RABBITMQ_VHOST_ENCODED || "%2F";
const recordAcceptedIds = __ENV.RECORD_ACCEPTED_IDS === "1";

if (!["sustained", "burst"].includes(scenario)) {
  throw new Error("SCENARIO must be sustained or burst");
}

export const acceptedSubmissions = new Counter("transaction_acceptance_confirmed_total");
export const rejectedSubmissions = new Counter("transaction_acceptance_rejected_total");
export const unavailableSubmissions = new Counter("transaction_acceptance_unavailable_total");
export const completionObserved = new Rate("transaction_processing_completion_observed");
export const completionLatency = new Trend("transaction_processing_completion_latency_ms", true);
export const duplicateRecords = new Counter("transaction_duplicate_records_observed_total");
export const workQueueDepth = new Trend("transaction_work_queue_depth", true);
export const processingFailureMetric = new Trend("transaction_processing_failures_metric", true);
// This is intentionally test-only. It is emitted only to the local k6 raw JSON
// output when explicit ID reconciliation is requested; it is not an application
// metric and is never exposed by the service's Prometheus endpoint.
export const acceptedIdLedger = new Counter("transaction_acceptance_id_ledger");

export const options = {
  scenarios:
    scenario === "sustained"
      ? {
          sustained: {
            executor: "constant-arrival-rate",
            rate,
            timeUnit: "1s",
            duration,
            preAllocatedVUs: Math.max(10, Math.ceil(rate / 2)),
            maxVUs: Math.max(100, rate * 2),
          },
        }
      : {
          burst: {
            executor: "shared-iterations",
            vus: Math.min(burstVUs, burstCount),
            iterations: burstCount,
            maxDuration: __ENV.BURST_MAX_DURATION || "2m",
          },
        },
};

export function setup() {
  const response = http.get(`${baseUrl}/metrics`);
  if (response.status !== 200) {
    throw new Error(`BASE_URL did not expose /metrics successfully (status ${response.status})`);
  }
  return { startedAt: new Date().toISOString() };
}

export default function (data) {
  const transactionId = `${runId}-${exec.vu.idInTest}-${exec.vu.iterationInScenario}`;
  const transactionUserId = `${userId}-${exec.vu.idInTest}-${exec.vu.iterationInScenario}`;
  const timestamp = new Date().toISOString();
  const submittedAt = Date.now();
  const response = http.post(
    `${baseUrl}/transactions`,
    JSON.stringify({
      id: transactionId,
      user_id: transactionUserId,
      amount: "1.00000000",
      currency: "USD",
      timestamp,
    }),
    { headers: { "Content-Type": "application/json" }, tags: { endpoint: "transactions" } },
  );

  const accepted = check(response, {
    "accepted only after broker confirmation": (value) => value.status === 202,
  });
  if (!accepted) {
    if (response.status === 422) {
      rejectedSubmissions.add(1);
    } else if (response.status === 503) {
      unavailableSubmissions.add(1);
    }
    return;
  }
  acceptedSubmissions.add(1);
  if (recordAcceptedIds) {
    acceptedIdLedger.add(1, { transaction_id: transactionId });
  }
  awaitCompletion(transactionId, transactionUserId, submittedAt, data.startedAt);
}

export function teardown() {
  const metrics = http.get(`${baseUrl}/metrics`);
  if (metrics.status === 200) {
    processingFailureMetric.add(sumFailureMetric(metrics.body));
  }
  if (managementUrl) {
    const credentials = encoding.b64encode(`${managementUser}:${managementPassword}`);
    const queue = http.get(`${managementUrl}/api/queues/${managementVhost}/transactions.work`, {
      headers: { Authorization: `Basic ${credentials}` },
    });
    if (queue.status === 200) {
      const body = queue.json();
      workQueueDepth.add((body.messages_ready || 0) + (body.messages_unacknowledged || 0));
    }
  }
}

function awaitCompletion(transactionId, transactionUserId, submittedAt, startedAt) {
  const from = encodeURIComponent(startedAt);
  const to = encodeURIComponent(new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString());
  const deadline = Date.now() + completionTimeoutSeconds * 1000;
  while (Date.now() < deadline) {
    const response = http.get(
      `${baseUrl}/users/${encodeURIComponent(transactionUserId)}/transactions?from=${from}&to=${to}&limit=100`,
      { tags: { endpoint: "transaction-history" } },
    );
    if (response.status === 200) {
      const matches = response.json("items")?.filter((item) => item.id === transactionId) || [];
      if (matches.length > 0) {
        completionObserved.add(matches.length === 1);
        duplicateRecords.add(Math.max(0, matches.length - 1));
        completionLatency.add(Date.now() - submittedAt);
        return;
      }
    }
    sleep(completionPollingSeconds);
  }
  completionObserved.add(false);
}

function sumFailureMetric(body) {
  return body
    .split("\n")
    .filter((line) => line.startsWith("transaction_processing_failures_total{"))
    .reduce((total, line) => {
      const fields = line.trim().split(" ");
      return total + Number(fields[fields.length - 1]);
    }, 0);
}

function integerEnv(name, fallback, minimum) {
  const value = __ENV[name] === undefined ? fallback : Number(__ENV[name]);
  if (!Number.isInteger(value) || value < minimum) {
    throw new Error(`${name} must be an integer >= ${minimum}`);
  }
  return value;
}

function numberEnv(name, fallback, minimum) {
  const value = __ENV[name] === undefined ? fallback : Number(__ENV[name]);
  if (!Number.isFinite(value) || value < minimum) {
    throw new Error(`${name} must be a number >= ${minimum}`);
  }
  return value;
}
