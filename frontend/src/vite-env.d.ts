/// <reference types="vite/client" />

interface ViteTypeOptions {
  strictImportMetaEnv: unknown;
}

interface ImportMetaEnv {
  readonly VITE_MAP_BASEMAP_PROVIDER?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
