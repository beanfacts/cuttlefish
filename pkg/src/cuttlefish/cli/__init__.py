"""
Cuttlefish
Copyright (c) 2024-2026 Tim Dettmar

Cuttlefish CLI entry point.
"""

import argparse
from typing import Optional, Sequence

from . import controller, keyring, manifest, shell, worker


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        prog="cuttle",
        description="Cuttlefish orchestrator CLI",
    )
    subparsers = parser.add_subparsers(dest="command")

    controller.register(subparsers)
    worker.register(subparsers)
    manifest.register(subparsers)
    shell.register(subparsers)
    keyring.register(subparsers)

    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return

    args.func(args)
