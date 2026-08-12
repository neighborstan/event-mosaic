import { act, render, screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import {
  createControlledMapLibreFactory,
  requireControlledMap,
} from "../../../test/controlledMapLibre";
import { CountryMapScreen } from "./CountryMapScreen";

describe("Начальный экран фоновой карты", () => {
  test("Показывает доступную карту и не объявляет готовность до слоя стран", () => {
    const fixture = createControlledMapLibreFactory();
    render(<CountryMapScreen mapFactory={fixture.factory} />);

    expect(screen.getByRole("main")).toBeInTheDocument();
    expect(
      screen.getByRole("heading", {
        level: 1,
        name: "Карта событий по странам",
      }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("region", { name: "Область карты стран" }),
    ).toHaveAttribute("aria-busy", "true");
    expect(
      screen.getByRole("region", { name: "Фоновая карта мира" }),
    ).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent(
      "Фоновая карта загружается. Данные по странам пока не подключены.",
    );
    expect(fixture.options).toEqual([
      expect.objectContaining({
        style: "https://tiles.openfreemap.org/styles/liberty",
        attributionControl: {
          compact: false,
        },
      }),
    ]);

    act(() => {
      requireControlledMap(fixture).emit("load");
    });

    expect(screen.getByRole("status")).toHaveTextContent(
      "Фоновая карта загружается",
    );
    expect(screen.queryByText(/карта готова/i)).not.toBeInTheDocument();
  });

  test("Неизвестный provider показывает безопасную ошибку до создания карты", () => {
    const fixture = createControlledMapLibreFactory();
    render(
      <CountryMapScreen
        providerValue="private-provider-value"
        mapFactory={fixture.factory}
      />,
    );

    expect(screen.getByRole("alert")).toHaveTextContent(
      "Источник фоновой карты настроен неверно",
    );
    expect(screen.getByRole("alert")).not.toHaveTextContent(
      "private-provider-value",
    );
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
    expect(fixture.maps).toHaveLength(0);
    expect(fixture.options).toHaveLength(0);
  });

  test("Явный Stadia provider использует тот же владеющий adapter", () => {
    const fixture = createControlledMapLibreFactory();
    render(
      <CountryMapScreen providerValue="stadia" mapFactory={fixture.factory} />,
    );

    expect(fixture.maps).toHaveLength(1);
    expect(fixture.options).toEqual([
      expect.objectContaining({
        style: "https://tiles.stadiamaps.com/styles/alidade_smooth.json",
        attributionControl: {
          compact: false,
        },
      }),
    ]);
    expect(screen.getByRole("status")).toHaveTextContent(
      "Фоновая карта загружается",
    );
  });

  test("Ошибка OpenFreeMap видна текстом и не запускает автоматический fallback", () => {
    const fixture = createControlledMapLibreFactory();
    render(<CountryMapScreen mapFactory={fixture.factory} />);

    act(() => {
      requireControlledMap(fixture).emit("error");
    });

    expect(screen.getByRole("alert")).toHaveTextContent(
      "Не удалось загрузить выбранную фоновую карту",
    );
    expect(screen.getByRole("alert")).toHaveTextContent(
      "Автоматическое переключение источника не выполняется",
    );
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
    expect(fixture.maps).toHaveLength(1);
    expect(fixture.options).toEqual([
      expect.objectContaining({
        style: "https://tiles.openfreemap.org/styles/liberty",
      }),
    ]);
  });
});
