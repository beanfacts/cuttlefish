"""
Copyright (c) 2024-2026 Tim Dettmar

Cuttlefish I/O utilities.

This module provides simple async I/O wrappers for reading and writing
files. These functions are great if you have a shitty network filesystem
(which will remain unnamed here) with high latency.
"""

import asyncio
import pathlib
from typing import Union


async def mkdir(path: pathlib.Path) -> None:
    """
    Create a directory asynchronously.
    """

    def _mkdir():
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _mkdir)


async def write(folder: pathlib.Path, file: str, content: Union[str, bytes]) -> None:
    """
    Wrapper to write a file asynchronously.
    """

    def _write_file():
        if not folder.exists():
            folder.mkdir(parents=True, exist_ok=True)
        mode = "w" if isinstance(content, str) else "wb"
        with open(folder / file, mode) as f:
            f.write(content)

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _write_file)


async def read(file: pathlib.Path) -> str:
    """
    Wrapper to read a file asynchronously.
    """

    def _read_file() -> str:
        with open(file, "r") as f:
            return f.read()

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _read_file)
