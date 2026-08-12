export type BasemapProvider = "openfreemap" | "stadia";

export type BasemapConfiguration = Readonly<{
  provider: BasemapProvider;
  styleUrl: string;
  attributionControl: Readonly<{
    compact: false;
  }>;
}>;

export type BasemapProviderResolution =
  | Readonly<{
      status: "resolved";
      configuration: BasemapConfiguration;
    }>
  | Readonly<{
      status: "invalid";
      error: Readonly<{
        code: "unsupported-basemap-provider";
        message: string;
      }>;
    }>;

const ATTRIBUTION_CONTROL = {
  compact: false,
} as const;

const BASEMAP_CONFIGURATIONS = {
  openfreemap: {
    provider: "openfreemap",
    styleUrl: "https://tiles.openfreemap.org/styles/liberty",
    attributionControl: ATTRIBUTION_CONTROL,
  },
  stadia: {
    provider: "stadia",
    styleUrl: "https://tiles.stadiamaps.com/styles/alidade_smooth.json",
    attributionControl: ATTRIBUTION_CONTROL,
  },
} as const satisfies Record<BasemapProvider, BasemapConfiguration>;

const INVALID_PROVIDER_RESOLUTION = {
  status: "invalid",
  error: {
    code: "unsupported-basemap-provider",
    message:
      "Источник фоновой карты настроен неверно. Доступны openfreemap и stadia.",
  },
} as const satisfies BasemapProviderResolution;

/**
 * Выбирает заранее разрешенный источник фоновой карты до создания MapLibre.
 */
export function resolveBasemapProvider(
  value: string | undefined,
): BasemapProviderResolution {
  const provider = value ?? "openfreemap";

  switch (provider) {
    case "openfreemap":
    case "stadia":
      return {
        status: "resolved",
        configuration: BASEMAP_CONFIGURATIONS[provider],
      };
    default:
      return INVALID_PROVIDER_RESOLUTION;
  }
}
