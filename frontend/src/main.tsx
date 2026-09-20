import { setWorkerUrl } from "maplibre-gl";
import mapWorkerUrl from "maplibre-gl/dist/maplibre-gl-worker.mjs?worker&url";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "maplibre-gl/dist/maplibre-gl.css";

import { App } from "./app/App";
import "./app/App.css";

// Vite собирает worker вместе с его зависимостями и дает адрес внутри того же приложения.
setWorkerUrl(mapWorkerUrl);

const rootElement = document.getElementById("root");

if (rootElement === null) {
  throw new Error("Не найден корневой элемент frontend");
}

createRoot(rootElement).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
