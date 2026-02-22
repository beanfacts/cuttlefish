"""
Cuttlefish
Copyright (c) 2024-2026 Tim Dettmar

Ceph/SSH shell helper script.
"""

import argparse
import asyncio
import configparser
import logging
import os
import pty
import subprocess
import tempfile

import cuttlefish.schema as sch
import cuttlefish.util as util


def register(subparsers: argparse._SubParsersAction) -> None:
    """Attach shell-related subcommands to the CLI."""
    parser = subparsers.add_parser(
        "shell",
        help="Launch a Ceph shell, run a script, or SSH to a daemon host",
        description="Ceph shell wrapper",
    )
    parser.add_argument(
        "-s",
        "--script",
        type=str,
        help="Path to a script to execute instead of an interactive shell",
        default=None,
    )
    parser.add_argument(
        "--ssh",
        type=str,
        help="Use SSH to connect to the node hosting the specified daemon",
    )
    parser.add_argument("--export", action="store_true", help="Export ceph.conf")
    parser.add_argument(
        "--native",
        action="store_true",
        help="Use host's native Ceph installation instead of the Cuttlefish container",
    )
    parser.add_argument(
        "ceph_args",
        nargs=argparse.REMAINDER,
        help="Arguments to pass to ceph command",
    )
    parser.set_defaults(func=_run_shell)


def _run_shell(args: argparse.Namespace) -> None:
    asyncio.run(_shell(args))


async def _shell(args: argparse.Namespace) -> None:
    with open("manifest.json", "r") as f:
        manifest = sch.Manifest.model_validate_json(f.read())

    if args.ssh:
        for node, info in manifest.nodes.items():
            for daemon in info.daemons:
                if daemon.name == args.ssh:
                    ssh_cmd = ["ssh", node]
                    pty.spawn(ssh_cmd)
                    return

    with tempfile.TemporaryDirectory() as tmpdir:
        conf = configparser.ConfigParser()
        conf.read_string(manifest.ceph_config)

        kr = configparser.ConfigParser()
        kr.read_string(manifest.keyring)

        try:
            conf.add_section("client.admin")
        except configparser.DuplicateSectionError:
            pass

        for key, value in kr["client.admin"].items():
            conf.set("client.admin", key, value)

        if args.export:
            dest = "ceph.conf"
        else:
            dest = f"{tmpdir}/ceph.conf"

        with open(dest, "w") as conf_file:
            conf.write(conf_file)

        if args.export:
            print(f"Exported ceph.conf to {dest}")
            return

        logging.basicConfig(level=logging.INFO)

        if args.native:
            cmd = ["ceph", "--conf", f"{tmpdir}/ceph.conf"]
        else:
            if (
                manifest.bench_image is None
                or manifest.image.type.value.lower() != "sif"
            ):
                raise RuntimeError(
                    "Container image not found in manifest or not a SIF image"
                )
            cmd = [
                "apptainer",
                "exec",
                "--bind",
                f"{tmpdir}/ceph.conf:/etc/ceph/ceph.conf:ro",
                manifest.image.name,
                "ceph",
            ]

        if args.ceph_args:
            cmd.extend(args.ceph_args)
            subprocess.run(cmd, check=True)
        else:
            if args.script:
                script = os.path.abspath(args.script)
                os.chdir(tmpdir)
                subprocess.run(script, check=True, shell=True)
            else:
                pty.spawn(cmd)
