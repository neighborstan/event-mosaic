import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv } from "vite";

const DEFAULT_BACKEND_ORIGIN = "http://localhost:8080";

function resolveBackendOrigin(value: string | undefined): string {
  const candidate = value ?? DEFAULT_BACKEND_ORIGIN;
  let url: URL;

  try {
    url = new URL(candidate);
  } catch {
    throw new Error(
      "MAP_BACKEND_ORIGIN должен быть полным HTTP(S)-адресом backend",
    );
  }

  const hasOnlyOrigin =
    url.pathname === "/" &&
    url.search === "" &&
    url.hash === "" &&
    url.username === "" &&
    url.password === "";
  const hasSupportedProtocol =
    url.protocol === "http:" || url.protocol === "https:";

  if (!hasOnlyOrigin || !hasSupportedProtocol) {
    throw new Error(
      "MAP_BACKEND_ORIGIN должен содержать только HTTP(S) origin без пути, параметров и credentials",
    );
  }

  return url.origin;
}

export default defineConfig(({ mode }) => {
  const environment = loadEnv(mode, process.cwd(), "MAP_");
  const backendOrigin = resolveBackendOrigin(environment["MAP_BACKEND_ORIGIN"]);

  return {
    plugins: [react()],
    server: {
      proxy: {
        "/map": {
          target: backendOrigin,
          changeOrigin: true,
        },
      },
    },
  };
});
