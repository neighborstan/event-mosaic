import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "maplibre-gl/dist/maplibre-gl.css";

import { App } from "./app/App";
import "./app/App.css";

const rootElement = document.getElementById("root");

if (rootElement === null) {
  throw new Error("Не найден корневой элемент frontend");
}

createRoot(rootElement).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
