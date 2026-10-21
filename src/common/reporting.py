"""Вывод для отчёта: дублирует всё в stdout и в текстовый файл (UTF-8)."""

from __future__ import annotations

from pathlib import Path


def human(n: float) -> str:
    """Размер в байтах по-человечески: 808 МБ, 2.7 КБ."""
    for unit in ("Б", "КБ", "МБ", "ГБ"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}" if unit != "Б" else f"{int(n)} Б"
        n /= 1024
    return f"{n:.1f} ТБ"


def money(value: float | None) -> str:
    """Денежное значение с пробелами-разделителями тысяч."""
    if value is None:
        return "-"
    return f"{value:,.0f}".replace(",", " ")


def pct(value: float | None) -> str:
    """Знаковое изменение в процентах: +12.3% / -4.5%."""
    if value is None:
        return "-"
    return f"{value:+.1f}%"


class Transcript:
    """Печатает строки в консоль и одновременно пишет их в файл отчёта."""

    def __init__(self, path: str | Path, echo: bool = True):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("w", encoding="utf-8", newline="\n")
        self.echo = echo

    def log(self, *args, sep: str = " ") -> None:
        line = sep.join(str(a) for a in args)
        if self.echo:
            print(line)
        self._fh.write(line + "\n")
        self._fh.flush()

    def rule(self, title: str = "", char: str = "=") -> None:
        self.log(char * 78)
        if title:
            self.log(title)
            self.log(char * 78)

    def table(self, headers: list[str], rows: list[list[str]]) -> None:
        """Печатает таблицу с выравниванием колонок по ширине содержимого."""
        widths = [len(h) for h in headers]
        for row in rows:
            for i, cell in enumerate(row):
                widths[i] = max(widths[i], len(str(cell)))
        self.log("  " + "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers)))
        self.log("  " + "  ".join("-" * widths[i] for i in range(len(headers))))
        for row in rows:
            self.log("  " + "  ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)))

    def close(self) -> None:
        if not self._fh.closed:
            self._fh.close()

    def __enter__(self) -> "Transcript":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

