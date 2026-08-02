import { createHash } from "node:crypto";
import { getRuntimeConfig } from "../config/config.js";
import { resolveAgentIdFromSessionKey, resolveStorePath } from "../config/sessions.js";
import { loadSessionEntry, patchSessionEntry } from "../config/sessions/session-accessor.js";
import type { GatewayRecoveryRuntime } from "../gateway/server-instance-runtime.types.js";
import {
  extractMessageRole,
  extractMessageText,
  readSessionMessagesAsync,
} from "../gateway/session-transcript-readers.js";
import { getAgentEventLifecycleGeneration } from "../infra/agent-events.js";
import { formatErrorMessage } from "../infra/errors.js";
import {
  beginSessionWorkAdmission,
  cancelSessionWorkAdmissionHandoff,
} from "../sessions/session-lifecycle-admission.js";
import { truncateUtf16Safe } from "../utils.js";
import { resolveInternalSessionEffectsTarget } from "./internal-session-effects.js";
import {
  formatSubagentRecoveryWedgedReason,
  isSubagentRecoveryWedgedEntry,
} from "./subagent-recovery-state.js";
import type { createSubagentRunManager } from "./subagent-registry-run-manager.js";
import type {
  SubagentRestartRecoveryReceipt,
  SubagentRunRecord,
} from "./subagent-registry.types.js";
import { isStaleUnendedSubagentRun } from "./subagent-run-liveness.js";
import { getSubagentSessionStartedAt } from "./subagent-session-metrics.js";

const MAX_RECOVERY_ATTEMPTS = 2;
const RECOVERY_ATTEMPT_WINDOW_MS = 2 * 60_000;

type RestartRecoveryResult =
  | { status: "ignored" }
  | { status: "handled" }
  | { status: "deferred" }
  | { status: "accepted" }
  | { status: "retry"; error: string }
  | { status: "terminal"; error: string; endedAt?: number };

type Params = {
  runId: string;
  entry: SubagentRunRecord;
  now: number;
  gatewayRuntime: GatewayRecoveryRuntime | undefined;
  isCurrent: () => boolean;
  abandonLaunch: ReturnType<
    typeof createSubagentRunManager
  >["abandonSubagentRestartRecoveryLaunch"];
  replaceRun: ReturnType<typeof createSubagentRunManager>["replaceSubagentRunAfterSteer"];
  markLaunchAttempted: ReturnType<
    typeof createSubagentRunManager
  >["markSubagentRestartRecoveryLaunchAttempted"];
  markLaunchAccepted: ReturnType<
    typeof createSubagentRunManager
  >["markSubagentRestartRecoveryLaunchAccepted"];
  markLaunchConsumed: ReturnType<
    typeof createSubagentRunManager
  >["markSubagentRestartRecoveryLaunchConsumed"];
  resetLaunchAttempt: ReturnType<
    typeof createSubagentRunManager
  >["resetSubagentRestartRecoveryLaunchAttempt"];
  reserveLaunch: ReturnType<
    typeof createSubagentRunManager
  >["reserveSubagentRestartRecoveryLaunch"];
  warn: (message: string, meta?: Record<string, unknown>) => void;
};

type RecoveryRetry = {
  entry: SubagentRunRecord;
  attempts: number;
  at: number;
  error: string;
  endedAt?: number;
  terminal?: true;
};

type RecoveryCoordinatorParams = Pick<
  Params,
  | "abandonLaunch"
  | "markLaunchAccepted"
  | "markLaunchAttempted"
  | "markLaunchConsumed"
  | "replaceRun"
  | "reserveLaunch"
  | "resetLaunchAttempt"
  | "warn"
> & {
  runs: Map<string, SubagentRunRecord>;
  getGatewayRuntime: () => GatewayRecoveryRuntime | undefined;
  finalizeRun: (params: { runId: string; error: string; endedAt?: number }) => Promise<number>;
  recoverRow?: typeof recoverInterruptedSubagentRow;
  schedule: (delayMs: number) => void;
};

function resumeMessage(task: string, lastHumanMessage?: string): string {
  const original = task.length > 2_000 ? `${truncateUtf16Safe(task, 2_000)}...` : task;
  return (
    `[System] Your previous turn was interrupted by a gateway reload. ` +
    `Your original task was:\n\n${original}\n\n` +
    (lastHumanMessage
      ? `The last message from the user before the interruption was:\n\n${lastHumanMessage}\n\n`
      : "") +
    `Please continue where you left off.`
  );
}

function buildRestartRecoveryIdempotencyKey(runId: string, sessionMarker: string): string {
  return `subagent-recovery:${createHash("sha256")
    .update(runId)
    .update("\0")
    .update(sessionMarker)
    .digest("hex")}`;
}

function assertRestartRecoverySnapshotCurrent(params: {
  childSessionKey: string;
  sessionId: string;
  storePath: string;
  updatedAt: number;
}): void {
  const current = loadSessionEntry({
    storePath: params.storePath,
    sessionKey: params.childSessionKey,
    clone: false,
  });
  if (
    current?.sessionId !== params.sessionId ||
    current.updatedAt !== params.updatedAt ||
    current.abortedLastRun !== true
  ) {
    throw new Error("subagent restart recovery session snapshot changed before dispatch");
  }
}

function replayError(entry: SubagentRunRecord): string | undefined {
  return entry.terminalOwner !== "interrupted-recovery" ||
    entry.pauseReason === "sessions_yield" ||
    entry.execution.status !== "terminal" ||
    typeof entry.execution.endedAt !== "number" ||
    entry.execution.outcome?.status !== "error" ||
    entry.endedReason !== "subagent-error"
    ? undefined
    : (entry.execution.outcome.error ?? "subagent run interrupted by gateway restart");
}

async function settleAcceptedRecoverySession(params: {
  attempts: number;
  childSessionKey: string;
  marker: string;
  now: number;
  runId: string;
  storePath: string;
}): Promise<boolean> {
  let settled = false;
  await patchSessionEntry(
    { storePath: params.storePath, sessionKey: params.childSessionKey },
    (current) => {
      const currentMarker = `${current.sessionId ?? ""}:${current.updatedAt ?? ""}`;
      if (current.abortedLastRun !== true || currentMarker !== params.marker) {
        return current;
      }
      current.abortedLastRun = false;
      current.subagentRecovery = {
        automaticAttempts: Math.max(
          current.subagentRecovery?.automaticAttempts ?? 0,
          params.attempts + 1,
        ),
        lastAttemptAt: params.now,
        lastRunId: params.runId,
      };
      current.updatedAt = params.now;
      settled = true;
      return current;
    },
    { replaceEntry: true, skipMaintenance: true },
  );
  return settled;
}

async function reconcileAcceptedRecovery(params: {
  agentId: string;
  attempts: number;
  childSessionKey: string;
  currentSessionAborted: boolean;
  currentSessionMarker?: string;
  entry: SubagentRunRecord;
  isCurrent: () => boolean;
  now: number;
  receipt: SubagentRestartRecoveryReceipt;
  replaceRun: Params["replaceRun"];
  runId: string;
  storePath: string;
  warn: Params["warn"];
}): Promise<RestartRecoveryResult> {
  if (
    params.currentSessionAborted &&
    params.currentSessionMarker === params.receipt.sessionMarker
  ) {
    try {
      if (
        !(await settleAcceptedRecoverySession({
          attempts: params.attempts,
          childSessionKey: params.childSessionKey,
          marker: params.receipt.sessionMarker,
          now: params.now,
          runId: params.runId,
          storePath: params.storePath,
        }))
      ) {
        params.warn("accepted subagent restart recovery session changed during settlement", {
          runId: params.runId,
          childSessionKey: params.childSessionKey,
        });
        return {
          status: "deferred",
        };
      }
    } catch (error) {
      params.warn("accepted subagent restart recovery could not clear its abort marker", {
        runId: params.runId,
        childSessionKey: params.childSessionKey,
        error,
      });
      return { status: "deferred" };
    }
  }

  let remapped = false;
  try {
    remapped =
      params.isCurrent() &&
      params.replaceRun({
        previousRunId: params.runId,
        nextRunId: params.receipt.idempotencyKey,
        fallback: params.entry,
        expected: params.entry,
        transcriptTarget: resolveInternalSessionEffectsTarget({
          agentId: params.agentId,
          runId: params.receipt.idempotencyKey,
          storePath: params.storePath,
        }),
        task: params.entry.task,
        requirePersistence: true,
      });
  } catch {
    // The accepted source receipt remains durable until replacement succeeds.
  }
  if (!remapped) {
    params.warn("accepted subagent restart recovery could not remap its exact row", {
      runId: params.runId,
      childSessionKey: params.childSessionKey,
    });
    return {
      status: "deferred",
    };
  }
  return { status: "accepted" };
}

export async function recoverInterruptedSubagentRow(
  params: Params,
): Promise<RestartRecoveryResult> {
  const terminalError = replayError(params.entry);
  if (terminalError) {
    return { status: "terminal", error: terminalError, endedAt: params.entry.execution.endedAt };
  }
  if (!params.isCurrent() || params.entry.pauseReason === "sessions_yield") {
    return { status: "ignored" };
  }

  const childSessionKey = params.entry.childSessionKey.trim();
  if (!childSessionKey) {
    return { status: "ignored" };
  }
  try {
    const agentId = resolveAgentIdFromSessionKey(childSessionKey);
    const storePath = resolveStorePath(getRuntimeConfig().session?.store, { agentId });
    const sessionEntry = loadSessionEntry({
      storePath,
      sessionKey: childSessionKey,
      clone: false,
    });
    const loadedMarker = sessionEntry
      ? `${sessionEntry.sessionId ?? ""}:${sessionEntry.updatedAt ?? ""}`
      : undefined;
    const recovery = sessionEntry?.subagentRecovery;
    const attempts =
      typeof recovery?.lastAttemptAt === "number" &&
      Number.isFinite(recovery.lastAttemptAt) &&
      params.now - recovery.lastAttemptAt <= RECOVERY_ATTEMPT_WINDOW_MS &&
      typeof recovery.automaticAttempts === "number" &&
      Number.isFinite(recovery.automaticAttempts) &&
      recovery.automaticAttempts > 0
        ? Math.floor(recovery.automaticAttempts)
        : 0;
    const currentRecoveryReceipt = params.entry.execution.restartRecovery;
    const abandonedError =
      "subagent restart recovery was abandoned after an ambiguous Gateway restart; " +
      "automatic replay was suppressed to avoid duplicate side effects";
    if (currentRecoveryReceipt?.phase === "accepted") {
      return await reconcileAcceptedRecovery({
        agentId,
        attempts,
        childSessionKey,
        currentSessionAborted: sessionEntry?.abortedLastRun === true,
        currentSessionMarker: loadedMarker,
        entry: params.entry,
        isCurrent: params.isCurrent,
        now: params.now,
        receipt: currentRecoveryReceipt,
        replaceRun: params.replaceRun,
        runId: params.runId,
        storePath,
        warn: params.warn,
      });
    }
    if (currentRecoveryReceipt?.phase === "abandoned") {
      return { status: "terminal", error: abandonedError };
    }
    if (
      currentRecoveryReceipt?.phase === "attempted" ||
      currentRecoveryReceipt?.phase === "consumed"
    ) {
      if (
        !params.abandonLaunch({
          runId: params.runId,
          expected: params.entry,
          sessionMarker: currentRecoveryReceipt.sessionMarker,
          idempotencyKey: currentRecoveryReceipt.idempotencyKey,
        })
      ) {
        return {
          status: "retry",
          error: "ambiguous subagent restart recovery could not persist its terminal fence",
        };
      }
      return { status: "terminal", error: abandonedError };
    }
    if (!sessionEntry?.abortedLastRun) {
      return { status: "ignored" };
    }
    const marker = `${sessionEntry.sessionId ?? ""}:${sessionEntry.updatedAt ?? ""}`;
    const legacyTimeout =
      params.entry.execution.outcome?.status === "timeout" &&
      typeof params.entry.execution.endedAt === "number";
    if (typeof params.entry.execution.endedAt === "number" && !legacyTimeout) {
      return { status: "ignored" };
    }
    if (legacyTimeout) {
      const interruptedAt = params.entry.execution.endedAt;
      params.entry.execution = {
        ...params.entry.execution,
        status: "interrupted",
        interruptedAt,
        interruptionReason: "gateway-restart",
        endedAt: undefined,
        outcome: undefined,
      };
      params.entry.endedReason = undefined;
      params.entry.terminalOwner = undefined;
    }
    if (isStaleUnendedSubagentRun(params.entry, params.now)) {
      const age = Math.round(
        (params.now - (getSubagentSessionStartedAt(params.entry) ?? params.now)) / 1_000,
      );
      return {
        status: "terminal",
        error: `stale aborted subagent run not resumed (${age}s old, exceeds stale-run window)`,
      };
    }

    const alreadyWedged = isSubagentRecoveryWedgedEntry(sessionEntry);
    const blockedReason = alreadyWedged
      ? formatSubagentRecoveryWedgedReason(sessionEntry)
      : attempts >= MAX_RECOVERY_ATTEMPTS
        ? `subagent orphan recovery blocked after ${attempts} rapid accepted resume attempts; ` +
          `run "openclaw tasks maintenance --apply" or "openclaw doctor --fix" to reconcile it`
        : undefined;
    if (blockedReason) {
      if (!alreadyWedged) {
        await patchSessionEntry(
          { storePath, sessionKey: childSessionKey },
          (current) => {
            current.abortedLastRun = false;
            current.subagentRecovery = {
              ...current.subagentRecovery,
              automaticAttempts: Math.max(
                current.subagentRecovery?.automaticAttempts ?? 0,
                MAX_RECOVERY_ATTEMPTS,
              ),
              lastAttemptAt: current.subagentRecovery?.lastAttemptAt ?? params.now,
              lastRunId: params.runId,
              wedgedAt: params.now,
              wedgedReason: blockedReason,
            };
            current.updatedAt = params.now;
            return current;
          },
          { replaceEntry: true, skipMaintenance: true },
        ).catch((error: unknown) =>
          params.warn("failed to persist wedged subagent recovery marker", {
            runId: params.runId,
            childSessionKey,
            error,
          }),
        );
      }
      params.warn("subagent restart recovery is blocked", {
        runId: params.runId,
        childSessionKey,
        reason: blockedReason,
      });
      return { status: "handled" };
    }
    if (!params.gatewayRuntime) {
      return { status: "deferred" };
    }

    const messages = await readSessionMessagesAsync(
      {
        agentId,
        sessionEntry,
        sessionId: sessionEntry.sessionId,
        sessionKey: childSessionKey,
        storePath,
      },
      { mode: "recent", maxMessages: 200, maxBytes: 1024 * 1024 },
    );
    if (!params.isCurrent()) {
      return { status: "handled" };
    }
    const lastHumanMessage = extractMessageText(
      [...messages].toReversed().find((message) => extractMessageRole(message) === "user"),
    );
    const configChanged = messages.some(
      (message) =>
        extractMessageRole(message) === "assistant" &&
        /openclaw\.json|openclaw gateway restart|config\.patch/i.test(
          extractMessageText(message) ?? "",
        ),
    );
    const sessionId = sessionEntry.sessionId;
    const updatedAt = sessionEntry.updatedAt;
    if (!sessionId || typeof updatedAt !== "number") {
      return {
        status: "retry",
        error: "subagent restart recovery session snapshot is incomplete",
      };
    }
    const assertSnapshotCurrent = () =>
      assertRestartRecoverySnapshotCurrent({
        childSessionKey,
        sessionId,
        storePath,
        updatedAt,
      });
    const admission = await beginSessionWorkAdmission({
      scope: storePath,
      identities: [childSessionKey, sessionId],
      assertAllowed: assertSnapshotCurrent,
      revalidateAllowed: assertSnapshotCurrent,
    });
    const handoffId = admission.createHandoff();
    let idempotencyKey = "";
    let dispatched: { runId: string; status: unknown };
    let launchAttempted = false;
    let dispatchReturned = false;
    let handoffConsumed = false;
    try {
      idempotencyKey =
        params.reserveLaunch({
          runId: params.runId,
          expected: params.entry,
          sessionMarker: marker,
          idempotencyKey: buildRestartRecoveryIdempotencyKey(params.runId, marker),
        }) ?? "";
      if (!idempotencyKey) {
        return { status: "handled" };
      }
      const attempted = params.markLaunchAttempted({
        runId: params.runId,
        expected: params.entry,
        sessionMarker: marker,
        idempotencyKey,
        lifecycleGeneration: getAgentEventLifecycleGeneration(),
      });
      if (!attempted || attempted.phase === "accepted") {
        return { status: "handled" };
      }
      launchAttempted = true;
      dispatched = await admission.run(() =>
        params.gatewayRuntime!.dispatchAgent<{ runId: string; status: unknown }>({
          message:
            resumeMessage(params.entry.task, lastHumanMessage ?? undefined) +
            (configChanged
              ? "\n\n[config changes from your previous run were already applied — do not re-modify openclaw.json or restart the gateway]"
              : ""),
          sessionKey: childSessionKey,
          expectedExistingSessionId: sessionId,
          internalRuntimeHandoffId: handoffId,
          idempotencyKey,
          deliver: false,
          lane: "subagent",
          ...(params.entry.collect
            ? { swarmCollector: true, swarmOutputSchema: params.entry.outputSchema }
            : {}),
          inputProvenance: {
            kind: "inter_session",
            sourceSessionKey: params.entry.requesterSessionKey,
            sourceChannel: "internal",
            sourceTool: "subagent_interrupted_resume",
          },
          sessionEffects: "internal",
          suppressPromptPersistence: true,
        }),
      );
      dispatchReturned = true;
    } finally {
      const handoffCanceled = cancelSessionWorkAdmissionHandoff(handoffId);
      handoffConsumed = !handoffCanceled;
      if (launchAttempted) {
        if (handoffCanceled) {
          if (
            !params.resetLaunchAttempt({
              runId: params.runId,
              expected: params.entry,
              sessionMarker: marker,
              idempotencyKey,
            })
          ) {
            throw new Error("failed to reset unconsumed subagent restart recovery attempt");
          }
        } else {
          try {
            const consumed = params.markLaunchConsumed({
              runId: params.runId,
              expected: params.entry,
              sessionMarker: marker,
              idempotencyKey,
            });
            if (!consumed || consumed.phase === "reserved" || consumed.phase === "attempted") {
              throw new Error("failed to persist consumed subagent restart recovery attempt");
            }
          } catch (error) {
            if (!dispatchReturned) {
              throw error;
            }
            // The definitive response below can advance the in-memory consumed
            // fact directly to accepted or abandoned and persist that state.
            params.warn(
              "subagent restart recovery could not persist its intermediate consumed receipt",
              {
                runId: params.runId,
                childSessionKey,
                error,
              },
            );
          }
        }
      }
    }
    if (!handoffConsumed) {
      return {
        status: "retry",
        error: "Gateway did not consume the subagent restart recovery admission",
      };
    }
    if (
      dispatched.runId !== idempotencyKey ||
      (dispatched.status !== "accepted" && dispatched.status !== "in_flight")
    ) {
      if (
        !params.abandonLaunch({
          runId: params.runId,
          expected: params.entry,
          sessionMarker: marker,
          idempotencyKey,
        })
      ) {
        return {
          status: "retry",
          error: "rejected subagent restart recovery could not persist its terminal fence",
        };
      }
      return {
        status: "terminal",
        error:
          "Gateway did not accept the subagent restart recovery run; " +
          "automatic replay was suppressed to avoid duplicate side effects",
      };
    }
    const restartRecovery = params.markLaunchAccepted({
      runId: params.runId,
      expected: params.entry,
      sessionMarker: marker,
      idempotencyKey,
    });
    if (!restartRecovery || restartRecovery.phase !== "accepted") {
      return {
        status: "retry",
        error: "accepted subagent restart recovery could not persist its acceptance receipt",
      };
    }
    return await reconcileAcceptedRecovery({
      agentId,
      attempts,
      childSessionKey,
      currentSessionAborted: sessionEntry.abortedLastRun === true,
      currentSessionMarker: marker,
      entry: params.entry,
      isCurrent: params.isCurrent,
      now: Date.now(),
      receipt: restartRecovery,
      replaceRun: params.replaceRun,
      runId: params.runId,
      storePath,
      warn: params.warn,
    });
  } catch (error) {
    return { status: "retry", error: formatErrorMessage(error) };
  }
}

export function createInterruptedRecoveryCoordinator(params: RecoveryCoordinatorParams) {
  const retries = new Map<string, RecoveryRetry>();

  function defer(runId: string, retry: Omit<RecoveryRetry, "at">, delayMs: number) {
    retries.set(runId, { ...retry, at: Date.now() + delayMs });
    params.schedule(delayMs);
  }

  async function projectTerminal(runId: string, pending: RecoveryRetry) {
    let updated = 0;
    try {
      updated = await params.finalizeRun({
        runId,
        error: pending.error,
        endedAt: pending.endedAt,
      });
    } catch (error) {
      params.warn("subagent interrupted terminal projection failed", { runId, error });
    }
    const attempts = pending.attempts + 1;
    if (updated === 0 && attempts < 3) {
      defer(runId, { ...pending, attempts }, 1_000);
      return;
    }
    if (updated === 0) {
      params.warn("subagent interrupted terminal projection remains incomplete", { runId });
    }
    retries.delete(runId);
  }

  async function recover(runId: string, entry: SubagentRunRecord, now: number): Promise<boolean> {
    let pending = retries.get(runId);
    if (pending?.entry !== entry) {
      retries.delete(runId);
      pending = undefined;
    }
    if (pending && pending.at > now) {
      params.schedule(pending.at - now);
      return true;
    }
    if (pending?.terminal) {
      await projectTerminal(runId, pending);
      return true;
    }
    const result = await (params.recoverRow ?? recoverInterruptedSubagentRow)({
      runId,
      entry,
      now,
      gatewayRuntime: params.getGatewayRuntime(),
      isCurrent: () => params.runs.get(runId) === entry,
      abandonLaunch: params.abandonLaunch,
      replaceRun: params.replaceRun,
      markLaunchAttempted: params.markLaunchAttempted,
      markLaunchAccepted: params.markLaunchAccepted,
      markLaunchConsumed: params.markLaunchConsumed,
      reserveLaunch: params.reserveLaunch,
      resetLaunchAttempt: params.resetLaunchAttempt,
      warn: params.warn,
    });
    if (result.status === "deferred") {
      params.schedule(1_000);
      return true;
    }
    if (
      result.status === "ignored" ||
      result.status === "handled" ||
      result.status === "accepted"
    ) {
      retries.delete(runId);
      return result.status !== "ignored";
    }
    if (result.status === "terminal") {
      await projectTerminal(runId, {
        entry,
        attempts: 0,
        at: now,
        error: result.error,
        endedAt: result.endedAt,
        terminal: true,
      });
      return true;
    }

    const attempts = (pending?.attempts ?? 0) + 1;
    if (attempts < 4) {
      defer(runId, { entry, attempts, error: result.error }, 1_000 * 2 ** (attempts - 1));
      return true;
    }
    const error =
      `Subagent run was interrupted by a gateway restart or connection loss. ` +
      `Automatic recovery failed after ${attempts} attempts. Please retry.` +
      (result.error.trim() ? ` (${result.error.trim()})` : "");
    await projectTerminal(runId, { entry, attempts: 0, at: now, error, terminal: true });
    return true;
  }

  return {
    recover,
    prune() {
      for (const [runId, retry] of retries) {
        if (params.runs.get(runId) !== retry.entry) {
          retries.delete(runId);
        }
      }
    },
    reset() {
      retries.clear();
    },
  };
}
