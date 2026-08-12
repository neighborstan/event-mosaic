import { useEffect, useState } from "react";

import {
  isAbortError,
  loadCountryGeometry,
  type CountryGeometry,
} from "./countryGeometry";

export type CountryGeometryLoader = (
  signal: AbortSignal,
) => Promise<CountryGeometry>;

export type CountryGeometryRequestState =
  | Readonly<{ status: "loading" }>
  | Readonly<{ status: "loaded"; geometry: CountryGeometry }>
  | Readonly<{ status: "error" }>;

type UseCountryGeometryOptions = Readonly<{
  loader?: CountryGeometryLoader | undefined;
}>;

const LOADING_STATE = {
  status: "loading",
} as const satisfies CountryGeometryRequestState;

const ERROR_STATE = {
  status: "error",
} as const satisfies CountryGeometryRequestState;

const DEFAULT_GEOMETRY_LOADER: CountryGeometryLoader = (signal) =>
  loadCountryGeometry({ signal });

/**
 * Владеет одним запросом геометрии на время mount и отменяет его при cleanup,
 * не публикуя запоздалый результат в удаленный компонент.
 */
export function useCountryGeometry({
  loader = DEFAULT_GEOMETRY_LOADER,
}: UseCountryGeometryOptions = {}): CountryGeometryRequestState {
  const [state, setState] =
    useState<CountryGeometryRequestState>(LOADING_STATE);

  useEffect(() => {
    const controller = new AbortController();
    let isOwned = true;

    void loader(controller.signal).then(
      (geometry) => {
        if (isOwned) {
          setState({ status: "loaded", geometry });
        }
      },
      (error: unknown) => {
        if (isOwned && !controller.signal.aborted && !isAbortError(error)) {
          setState(ERROR_STATE);
        }
      },
    );

    return () => {
      isOwned = false;
      controller.abort();
    };
  }, [loader]);

  return state;
}
