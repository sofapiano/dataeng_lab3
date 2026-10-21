"""Вывод для отчёта: дублирует всё в stdout и в текстовый файл (UTF-8)."""

from __future__ import annotations

from pathlib import Path


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

    def close(self) -> None:
        if not self._fh.closed:
            self._fh.close()

    def __enter__(self) -> "Transcript":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
