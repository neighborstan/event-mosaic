import { render, screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { App } from "./App";

describe("Начальный экран карты стран", () => {
  test("Показывает доступный каркас и честно сообщает, что карта еще не загружена", () => {
    render(<App />);

    expect(screen.getByRole("main")).toBeInTheDocument();
    expect(
      screen.getByRole("heading", {
        level: 1,
        name: "Карта событий по странам",
      }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("region", { name: "Область карты стран" }),
    ).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent(
      "Карта пока не загружена",
    );
  });
});
