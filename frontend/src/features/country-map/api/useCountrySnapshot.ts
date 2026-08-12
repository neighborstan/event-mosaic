import { useEffect, useReducer, useRef } from "react";

import type { CountryGeometry } from "./countryGeometry";
import {
  isCountrySnapshotAbortError,
  joinCountrySnapshot,
  loadCountrySnapshot,
  type CountrySnapshot,
  type JoinedCountrySnapshot,
} from "./countrySnapshot";

export const COUNTRY_SNAPSHOT_REFRESH_INTERVAL_MILLISECONDS = 15 * 60 * 1_000;

export type CountrySnapshotLoader = (
  signal: AbortSignal,
) => Promise<CountrySnapshot>;

export type CountrySnapshotClock = () => Date;

export type CountrySnapshotRequestState =
  | Readonly<{ status: "loading" }>
  | Readonly<{ status: "error" }>
  | Readonly<{
      status: "accepted";
      joinedSnapshot: JoinedCountrySnapshot;
      acceptedAt: string;
      refreshStatus:
        "idle" | "refreshing" | "warning" | "refreshing-with-warning";
    }>;

type UseCountrySnapshotOptions = Readonly<{
  geometry: CountryGeometry | null;
  loader?: CountrySnapshotLoader | undefined;
  clock?: CountrySnapshotClock | undefined;
  refreshIntervalMilliseconds?: number | undefined;
}>;

type SnapshotCandidate = Readonly<{
  requestId: number;
  snapshot: CountrySnapshot;
}>;

type AcceptedSnapshot = Readonly<{
  joinedSnapshot: JoinedCountrySnapshot;
  acceptedAt: string;
}>;

type SnapshotOwnerState = Readonly<{
  accepted: AcceptedSnapshot | null;
  candidate: SnapshotCandidate | null;
  requestStatus: "idle" | "loading";
  failureStatus: "none" | "initial" | "refresh";
}>;

type SnapshotOwnerAction =
  | Readonly<{ type: "request-started" }>
  | Readonly<{
      type: "request-succeeded";
      requestId: number;
      snapshot: CountrySnapshot;
    }>
  | Readonly<{ type: "request-failed" }>
  | Readonly<{
      type: "join-accepted";
      requestId: number;
      joinedSnapshot: JoinedCountrySnapshot;
      acceptedAt: string;
    }>
  | Readonly<{ type: "join-rejected"; requestId: number }>;

const INITIAL_OWNER_STATE: SnapshotOwnerState = {
  accepted: null,
  candidate: null,
  requestStatus: "loading",
  failureStatus: "none",
};

const DEFAULT_SNAPSHOT_LOADER: CountrySnapshotLoader = (signal) =>
  loadCountrySnapshot({ signal });

const DEFAULT_CLOCK: CountrySnapshotClock = () => new Date();

/**
 * Владеет последовательной загрузкой snapshot и принимает данные только после
 * полного соединения с уже проверенной геометрией.
 */
export function useCountrySnapshot({
  geometry,
  loader = DEFAULT_SNAPSHOT_LOADER,
  clock = DEFAULT_CLOCK,
  refreshIntervalMilliseconds = COUNTRY_SNAPSHOT_REFRESH_INTERVAL_MILLISECONDS,
}: UseCountrySnapshotOptions): CountrySnapshotRequestState {
  const [ownerState, dispatch] = useReducer(
    reduceSnapshotOwnerState,
    INITIAL_OWNER_STATE,
  );
  const lifecycleGeneration = useRef(0);

  useEffect(() => {
    const generation = lifecycleGeneration.current + 1;
    lifecycleGeneration.current = generation;
    let isOwned = true;
    let requestSequence = 0;
    let refreshTimer: ReturnType<typeof setTimeout> | undefined;
    let activeController: AbortController | undefined;

    const scheduleRefresh = (): void => {
      refreshTimer = setTimeout(() => {
        void performRequest();
      }, refreshIntervalMilliseconds);
    };

    const performRequest = async (): Promise<void> => {
      const requestId = ++requestSequence;
      const controller = new AbortController();
      activeController = controller;
      dispatch({ type: "request-started" });

      try {
        const snapshot = await loader(controller.signal);
        if (
          isOwned &&
          lifecycleGeneration.current === generation &&
          requestId === requestSequence
        ) {
          dispatch({ type: "request-succeeded", requestId, snapshot });
        }
      } catch (error: unknown) {
        if (
          isOwned &&
          lifecycleGeneration.current === generation &&
          requestId === requestSequence &&
          !controller.signal.aborted &&
          !isCountrySnapshotAbortError(error)
        ) {
          dispatch({ type: "request-failed" });
        }
      } finally {
        if (
          isOwned &&
          lifecycleGeneration.current === generation &&
          requestId === requestSequence
        ) {
          activeController = undefined;
          scheduleRefresh();
        }
      }
    };

    queueMicrotask(() => {
      if (isOwned && lifecycleGeneration.current === generation) {
        void performRequest();
      }
    });

    return () => {
      isOwned = false;
      lifecycleGeneration.current += 1;
      if (refreshTimer !== undefined) {
        clearTimeout(refreshTimer);
      }
      activeController?.abort();
    };
  }, [loader, refreshIntervalMilliseconds]);

  useEffect(() => {
    const candidate = ownerState.candidate;
    if (candidate === null || geometry === null) {
      return;
    }

    try {
      const joinedSnapshot = joinCountrySnapshot(candidate.snapshot, geometry);
      dispatch({
        type: "join-accepted",
        requestId: candidate.requestId,
        joinedSnapshot,
        acceptedAt: clock().toISOString(),
      });
    } catch {
      dispatch({ type: "join-rejected", requestId: candidate.requestId });
    }
  }, [clock, geometry, ownerState.candidate]);

  return toRequestState(ownerState);
}

function reduceSnapshotOwnerState(
  state: SnapshotOwnerState,
  action: SnapshotOwnerAction,
): SnapshotOwnerState {
  switch (action.type) {
    case "request-started":
      return {
        ...state,
        requestStatus: "loading",
      };
    case "request-succeeded":
      return {
        ...state,
        candidate: {
          requestId: action.requestId,
          snapshot: action.snapshot,
        },
        requestStatus: "loading",
      };
    case "request-failed":
      if (state.accepted !== null) {
        return {
          ...state,
          requestStatus: "idle",
          failureStatus: "refresh",
        };
      }
      if (state.candidate !== null) {
        return state;
      }
      return {
        ...state,
        requestStatus: "idle",
        failureStatus: "initial",
      };
    case "join-accepted":
      if (state.candidate?.requestId !== action.requestId) {
        return state;
      }
      return {
        accepted: {
          joinedSnapshot: action.joinedSnapshot,
          acceptedAt: action.acceptedAt,
        },
        candidate: null,
        requestStatus: "idle",
        failureStatus: "none",
      };
    case "join-rejected":
      if (state.candidate?.requestId !== action.requestId) {
        return state;
      }
      return {
        ...state,
        candidate: null,
        requestStatus: "idle",
        failureStatus: state.accepted === null ? "initial" : "refresh",
      };
  }
}

function toRequestState(
  ownerState: SnapshotOwnerState,
): CountrySnapshotRequestState {
  if (ownerState.accepted !== null) {
    const refreshStatus =
      ownerState.failureStatus === "refresh"
        ? ownerState.requestStatus === "loading"
          ? "refreshing-with-warning"
          : "warning"
        : ownerState.requestStatus === "loading"
          ? "refreshing"
          : "idle";
    return {
      status: "accepted",
      joinedSnapshot: ownerState.accepted.joinedSnapshot,
      acceptedAt: ownerState.accepted.acceptedAt,
      refreshStatus,
    };
  }

  return ownerState.failureStatus === "initial"
    ? { status: "error" }
    : { status: "loading" };
}
