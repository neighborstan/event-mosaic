import type { JSX } from "react";

export function CountryMapScreen(): JSX.Element {
  return (
    <main className="app-shell">
      <article
        className="country-map-shell"
        aria-labelledby="country-map-title"
      >
        <p className="country-map-shell__eyebrow">Event Mosaic</p>
        <h1 id="country-map-title">Карта событий по странам</h1>
        <p className="country-map-shell__intro">
          Первый экран готовит отдельную и проверяемую основу для будущей карты
          событий GDELT.
        </p>
        <section
          className="country-map-shell__placeholder"
          aria-label="Область карты стран"
        >
          <p role="status">
            Карта пока не загружена. Сейчас доступен только каркас интерфейса.
          </p>
        </section>
      </article>
    </main>
  );
}
