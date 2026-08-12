import { act, render, renderHook, screen } from "@testing-library/react";
import { StrictMode, type JSX } from "react";
import { afterEach, describe, expect, test, vi } from "vitest";

import { createValidatedCountryGeometry } from "../../../test/countryGeometryFixtures";
import {
  createCountrySnapshotDocument,
  createValidatedCountrySnapshot,
} from "../../../test/countrySnapshotFixtures";
import type { CountryGeometry } from "./countryGeometry";
import {
  validateCountrySnapshot,
  type CountryCoverageStatus,
  type CountrySnapshot,
} from "./countrySnapshot";
import {
  COUNTRY_SNAPSHOT_REFRESH_INTERVAL_MILLISECONDS,
  useCountrySnapshot,
  type CountrySnapshotLoader,
} from "./useCountrySnapshot";

interface ControlledSnapshotRequest {
  signal: AbortSignal;
  resolve(snapshot: CountrySnapshot): void;
  reject(error: unknown): void;
}

interface ControlledSnapshotLoader {
  loader: CountrySnapshotLoader;
  requests: ControlledSnapshotRequest[];
}

afterEach(() => {
  vi.useRealTimers();
});

describe("Владелец country snapshot", () => {
  test("Начинает запрос сразу и принимает ответ только после полного соединения с geometry", async () => {
    const fixture = createControlledSnapshotLoader();
    const clock = () => new Date("2026-08-11T12:16:30Z");
    const initialProps: { geometry: CountryGeometry | null } = {
      geometry: null,
    };
    const view = renderHook(
      ({ geometry }: { geometry: CountryGeometry | null }) =>
        useCountrySnapshot({ geometry, loader: fixture.loader, clock }),
      { initialProps },
    );

    await flushSnapshotStart();
    expect(fixture.requests).toHaveLength(1);
    expect(view.result.current).toEqual({ status: "loading" });

    await act(async () => {
      requireSnapshotRequest(fixture).resolve(createValidatedCountrySnapshot());
      await Promise.resolve();
    });
    expect(view.result.current).toEqual({ status: "loading" });

    view.rerender({ geometry: createValidatedCountryGeometry() });

    expect(view.result.current).toMatchObject({
      status: "accepted",
      acceptedAt: "2026-08-11T12:16:30.000Z",
      refreshStatus: "idle",
    });
  });

  test("Планирует следующий запрос через 15 минут после завершения и не допускает overlap", async () => {
    vi.useFakeTimers();
    const fixture = createControlledSnapshotLoader();
    const view = renderHook(() =>
      useCountrySnapshot({
        geometry: createValidatedCountryGeometry(),
        loader: fixture.loader,
      }),
    );

    await act(async () => {
      await vi.advanceTimersByTimeAsync(
        COUNTRY_SNAPSHOT_REFRESH_INTERVAL_MILLISECONDS,
      );
    });
    expect(fixture.requests).toHaveLength(1);

    await act(async () => {
      requireSnapshotRequest(fixture).resolve(createValidatedCountrySnapshot());
      await Promise.resolve();
    });
    expect(view.result.current).toMatchObject({
      status: "accepted",
      refreshStatus: "idle",
    });

    await act(async () => {
      await vi.advanceTimersByTimeAsync(
        COUNTRY_SNAPSHOT_REFRESH_INTERVAL_MILLISECONDS - 1,
      );
    });
    expect(fixture.requests).toHaveLength(1);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1);
    });
    expect(fixture.requests).toHaveLength(2);
    expect(view.result.current).toMatchObject({
      status: "accepted",
      refreshStatus: "refreshing",
    });

    await act(async () => {
      await vi.advanceTimersByTimeAsync(
        COUNTRY_SNAPSHOT_REFRESH_INTERVAL_MILLISECONDS,
      );
    });
    expect(fixture.requests).toHaveLength(2);
  });

  test.each(["PARTIAL", "UNKNOWN"] satisfies CountryCoverageStatus[])(
    "Принимает успешный %s как factual snapshot, а не transport error",
    async (coverageStatus) => {
      const fixture = createControlledSnapshotLoader();
      const view = renderHook(() =>
        useCountrySnapshot({
          geometry: createValidatedCountryGeometry(),
          loader: fixture.loader,
        }),
      );

      await flushSnapshotStart();
      await act(async () => {
        requireSnapshotRequest(fixture).resolve(
          createSnapshotWithCoverage(coverageStatus),
        );
        await Promise.resolve();
      });

      expect(view.result.current.status).toBe("accepted");
      if (view.result.current.status === "accepted") {
        expect(
          view.result.current.joinedSnapshot.snapshot.coverage.status,
        ).toBe(coverageStatus);
      }
    },
  );

  test("Сохраняет последнюю принятую модель при refresh failure и заменяет ее следующим полным успехом", async () => {
    vi.useFakeTimers();
    const fixture = createControlledSnapshotLoader();
    const acceptedTimes = [
      new Date("2026-08-11T12:16:30Z"),
      new Date("2026-08-11T12:31:30Z"),
    ];
    const clock = () =>
      acceptedTimes.shift() ?? new Date("2026-08-11T12:31:30Z");
    const view = renderHook(() =>
      useCountrySnapshot({
        geometry: createValidatedCountryGeometry(),
        loader: fixture.loader,
        clock,
      }),
    );

    await flushSnapshotStart();
    await act(async () => {
      requireSnapshotRequest(fixture, 0).resolve(
        createValidatedCountrySnapshot(),
      );
      await Promise.resolve();
    });

    await act(async () => {
      await vi.advanceTimersByTimeAsync(
        COUNTRY_SNAPSHOT_REFRESH_INTERVAL_MILLISECONDS,
      );
      requireSnapshotRequest(fixture, 1).reject(new Error("503 detail"));
      await Promise.resolve();
    });

    expect(view.result.current).toMatchObject({
      status: "accepted",
      acceptedAt: "2026-08-11T12:16:30.000Z",
      refreshStatus: "warning",
    });

    await act(async () => {
      await vi.advanceTimersByTimeAsync(
        COUNTRY_SNAPSHOT_REFRESH_INTERVAL_MILLISECONDS,
      );
    });
    expect(view.result.current).toMatchObject({
      status: "accepted",
      acceptedAt: "2026-08-11T12:16:30.000Z",
      refreshStatus: "refreshing-with-warning",
    });

    await act(async () => {
      requireSnapshotRequest(fixture, 2).reject(new Error("second 503"));
      await Promise.resolve();
    });
    expect(view.result.current).toMatchObject({
      status: "accepted",
      acceptedAt: "2026-08-11T12:16:30.000Z",
      refreshStatus: "warning",
    });

    await act(async () => {
      await vi.advanceTimersByTimeAsync(
        COUNTRY_SNAPSHOT_REFRESH_INTERVAL_MILLISECONDS,
      );
      requireSnapshotRequest(fixture, 3).resolve(
        createSnapshotWithCoverage("UNKNOWN"),
      );
      await Promise.resolve();
    });

    expect(view.result.current).toMatchObject({
      status: "accepted",
      acceptedAt: "2026-08-11T12:31:30.000Z",
      refreshStatus: "idle",
    });
    if (view.result.current.status === "accepted") {
      expect(view.result.current.joinedSnapshot.snapshot.coverage.status).toBe(
        "UNKNOWN",
      );
    }
  });

  test("Первый полный отказ показывает error без подмены нулевым snapshot", async () => {
    const fixture = createControlledSnapshotLoader();
    const view = renderHook(() =>
      useCountrySnapshot({
        geometry: createValidatedCountryGeometry(),
        loader: fixture.loader,
      }),
    );

    await flushSnapshotStart();
    await act(async () => {
      requireSnapshotRequest(fixture).reject(new Error("raw backend reason"));
      await Promise.resolve();
    });

    expect(view.result.current).toEqual({ status: "error" });
  });

  test("Strict Mode гасит пробный generation до HTTP и оставляет один initial request", async () => {
    const fixture = createControlledSnapshotLoader();
    render(
      <StrictMode>
        <SnapshotStateProbe fixture={fixture} />
      </StrictMode>,
    );

    await act(async () => {
      await Promise.resolve();
    });
    expect(fixture.requests).toHaveLength(1);
    const request = requireSnapshotRequest(fixture);
    expect(request.signal.aborted).toBe(false);

    await act(async () => {
      request.resolve(createSnapshotWithCoverage("UNKNOWN"));
      await Promise.resolve();
    });
    expect(screen.getByTestId("snapshot-state")).toHaveTextContent(
      "accepted UNKNOWN",
    );
  });

  test("Abort и generation token подавляют запоздалый ответ уже начатого lifecycle", async () => {
    const firstFixture = createControlledSnapshotLoader();
    const secondFixture = createControlledSnapshotLoader();
    const geometry = createValidatedCountryGeometry();
    const view = renderHook(
      ({ loader }: { loader: CountrySnapshotLoader }) =>
        useCountrySnapshot({ geometry, loader }),
      { initialProps: { loader: firstFixture.loader } },
    );

    await act(async () => {
      await Promise.resolve();
    });
    const staleRequest = requireSnapshotRequest(firstFixture);

    view.rerender({ loader: secondFixture.loader });
    await act(async () => {
      await Promise.resolve();
    });
    const currentRequest = requireSnapshotRequest(secondFixture);
    expect(staleRequest.signal.aborted).toBe(true);
    expect(currentRequest.signal.aborted).toBe(false);

    await act(async () => {
      staleRequest.resolve(createSnapshotWithCoverage("PARTIAL"));
      await Promise.resolve();
    });
    expect(view.result.current).toEqual({ status: "loading" });

    await act(async () => {
      currentRequest.resolve(createSnapshotWithCoverage("UNKNOWN"));
      await Promise.resolve();
    });
    expect(view.result.current.status).toBe("accepted");
    if (view.result.current.status === "accepted") {
      expect(view.result.current.joinedSnapshot.snapshot.coverage.status).toBe(
        "UNKNOWN",
      );
    }
  });
});

function createControlledSnapshotLoader(): ControlledSnapshotLoader {
  const requests: ControlledSnapshotRequest[] = [];
  const loader: CountrySnapshotLoader = (signal) =>
    new Promise((resolve, reject) => {
      requests.push({ signal, resolve, reject });
    });
  return { loader, requests };
}

function requireSnapshotRequest(
  fixture: ControlledSnapshotLoader,
  index = 0,
): ControlledSnapshotRequest {
  const request = fixture.requests[index];
  if (request === undefined) {
    throw new Error(`Не найден тестовый snapshot request с индексом ${index}`);
  }
  return request;
}

function createSnapshotWithCoverage(
  coverageStatus: CountryCoverageStatus,
): CountrySnapshot {
  const document = createCountrySnapshotDocument();
  document.coverage.status = coverageStatus;
  document.coverage.missingIntervals =
    coverageStatus === "UNKNOWN"
      ? null
      : coverageStatus === "PARTIAL"
        ? [
            {
              from: "2026-08-10T12:15:00Z",
              to: "2026-08-10T12:30:00Z",
            },
          ]
        : [];
  return validateCountrySnapshot(document);
}

function SnapshotStateProbe({
  fixture,
}: Readonly<{ fixture: ControlledSnapshotLoader }>): JSX.Element {
  const state = useCountrySnapshot({
    geometry: createValidatedCountryGeometry(),
    loader: fixture.loader,
  });
  const coverage =
    state.status === "accepted"
      ? ` ${state.joinedSnapshot.snapshot.coverage.status}`
      : "";
  return <p data-testid="snapshot-state">{`${state.status}${coverage}`}</p>;
}

async function flushSnapshotStart(): Promise<void> {
  await act(async () => {
    await Promise.resolve();
  });
}
