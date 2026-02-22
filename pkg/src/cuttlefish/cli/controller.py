"""
Cuttlefish
Copyright (c) 2024-2026 Tim Dettmar
"""

import argparse
import asyncio
import enum
import json
import logging
import os
import pathlib
from typing import List

import cuttlefish.container.apptainer as cfat
import cuttlefish.schema as sch
import cuttlefish.shell as cfs

try:
    import enlighten
except ImportError:  # pragma: no cover - optional dependency
    enlighten = None


class TColor(enum.Enum):
    RESET = "\033[0m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"


def register(subparsers: argparse._SubParsersAction) -> None:
    """Attach controller-related subcommands to the CLI."""

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "-m",
        "--manifest",
        type=str,
        default="manifest.json",
        help="Path to the placement manifest file",
    )
    common.add_argument(
        "-w",
        "--workdir",
        type=str,
        default=f"/tmp/{os.getuid()}/cuttlefish",
        help="Working directory for deployment",
    )
    common.add_argument(
        "--repodir",
        type=str,
        default=pathlib.Path(".").resolve().as_posix(),
        help="Path to the cuttlefish repository directory",
    )
    common.add_argument(
        "--max-workers",
        type=int,
        default=200,
        help="Maximum number of parallel SSH workers",
    )
    common.add_argument(
        "--no-progress", action="store_true", help="Disable progress bars"
    )
    common.add_argument("--debug", action="store_true", help="Enable debug logging")

    deploy_parser = subparsers.add_parser(
        "deploy",
        parents=[common],
        help="Deploy ceph daemons across the cluster",
        description="Deploy Ceph daemons on all nodes in the manifest",
    )
    deploy_parser.add_argument(
        "--prepare",
        action="store_true",
        help="Perform initial setup actions before deployment",
    )
    deploy_parser.add_argument(
        "--only",
        type=str,
        choices=["mon", "osd", "mgr", "mds", "rgw", "node_exporter"],
        help="Only deploy the specified daemon type",
        default=None,
    )
    deploy_parser.add_argument(
        "--restart", action="store_true", help="Restart the selected daemons"
    )
    deploy_parser.add_argument(
        "exec",
        nargs=argparse.REMAINDER,
        help="Execute an ad-hoc command on all nodes in the manifest",
    )
    deploy_parser.set_defaults(func=_run_deploy)

    daemons_parser = subparsers.add_parser(
        "daemons",
        parents=[common],
        help="Show daemon status on all nodes",
        description="Display the currently running Ceph daemon containers",
    )
    daemons_parser.add_argument(
        "--json",
        action="store_true",
        help="Output in JSON format (also disables progress bars)",
    )
    daemons_parser.set_defaults(func=_run_daemons)

    teardown_parser = subparsers.add_parser(
        "teardown",
        parents=[common],
        help="Stop and clean all deployed daemons",
        description="Stop all Cuttlefish containers and clean working directories",
    )
    teardown_parser.set_defaults(func=_run_teardown)


def _configure_logging(args: argparse.Namespace) -> None:
    if getattr(args, "json", False):
        level = logging.FATAL
    elif args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")
    if getattr(args, "json", False):
        args.no_progress = True


async def _execute(
    args: argparse.Namespace,
    executor: cfs.ParallelRemoteExecutor,
    cmds: List[List[str]],
    timeout: float = 60.0,
):
    if enlighten is not None and not args.no_progress:
        logging.info("Executing commands (with progress)")
        with enlighten.Manager() as manager:
            return await executor.execute(cmds, timeout=timeout, manager=manager)
    logging.info("Executing commands")
    return await executor.execute(cmds, timeout=timeout, manager=None)


def _resolve_repodir(path_str: str) -> pathlib.Path:
    return pathlib.Path(path_str).expanduser().resolve()


def _build_worker_command(
    wrapper_path: str, args: argparse.Namespace, repodir: pathlib.Path
) -> List[str]:
    cmd = [wrapper_path, "cuttle", "worker", "-m", args.manifest]
    cmd += ["--repodir", repodir.as_posix()]
    if args.workdir:
        cmd += ["--workdir", args.workdir]
    return cmd


async def _controller(
    args: argparse.Namespace,
    mode: str,
) -> None:
    args.manifest = pathlib.Path(args.manifest).expanduser().resolve().as_posix()
    with open(args.manifest, "r", encoding="utf-8") as handle:
        manifest = sch.Manifest.model_validate_json(handle.read())

    hosts = list(manifest.nodes.keys())
    if not hosts:
        logging.info("Manifest does not define any nodes")
        return

    repodir = _resolve_repodir(args.repodir)
    executor = cfs.ParallelRemoteExecutor(hosts, max_workers=args.max_workers)
    wrapper_path = str((repodir / "scripts" / "wrapper.sh").resolve())

    if mode == "deploy":
        if args.prepare:
            logging.warning(
                "Preparing hosts for deployment. No output will be shown unless an error occurs."
            )
            setup_script = str((repodir / "scripts" / "setup.sh").resolve())
            cmds = [["/bin/bash", setup_script] for _ in hosts]
            ret = await _execute(args, executor, cmds, timeout=600.0)
            for host, output in zip(hosts, ret):
                if output is not None and output.retcode != 0:
                    logging.error(
                        "Preparation on host %s failed with rc=%d: %s",
                        host,
                        output.retcode,
                        output.stderr,
                    )
                    raise RuntimeError(f"Preparation failed on host {host}")
            return

        cmds: List[List[str]] = []
        if args.exec:
            cmds = [[wrapper_path] + args.exec for _ in hosts]
        elif args.only == "node_exporter":
            base_cmd = _build_worker_command(wrapper_path, args, repodir)
            cmds = [base_cmd + ["--node-exporter"] for _ in hosts]
        else:
            base_cmd = _build_worker_command(wrapper_path, args, repodir)
            for host, node_manif in manifest.nodes.items():
                tmp = base_cmd.copy()
                if args.only:
                    daemons = [
                        i for i in node_manif.get_daemons() if i.startswith(args.only)
                    ]
                else:
                    daemons = node_manif.get_daemons()
                if daemons:
                    if args.restart:
                        tmp += ["-r", " ".join(daemons)]
                    else:
                        tmp += ["-u", " ".join(daemons)]
                cmds.append(tmp)

        if not cmds:
            logging.info("No actions specified, exiting")
            return

        await _execute(args, executor, cmds, timeout=60.0)
        return

    if mode == "teardown":
        base_cmd = _build_worker_command(wrapper_path, args, repodir)
        cmds = [base_cmd + ["--cleanall"] for _ in hosts]
        await _execute(args, executor, cmds, timeout=60.0)
        return

    if mode == "daemons":
        cmds = [
            [wrapper_path, "apptainer", "instance", "list", "--json"] for _ in hosts
        ]
        ret = await _execute(args, executor, cmds, timeout=60.0)

        ttl_daemons = 0
        ttl_nodes = 0
        ttl_type = {"mon": 0, "osd": 0, "mgr": 0, "mds": 0, "rgw": 0}
        out = {"nodes": 0, "daemons": 0, "layout": {}}

        for node, output in zip(hosts, ret):
            if output is None:
                continue
            logging.debug(
                "Node: %s, stdout: %s, stderr: %s",
                node,
                output.stdout,
                output.stderr,
            )
            data = json.loads(output.stdout)
            prev = ttl_daemons
            node_daemons: List[str] = []
            for instance in data.get("instances", []):
                info = cfat.ApptainerInstanceInfo(**instance)
                if info.name.startswith("cuttlefish."):
                    name = info.name.split(".")
                    for typ in ttl_type.keys():
                        if name[1] == typ:
                            ttl_type[typ] += 1
                    if name[1] in ("mon", "osd", "mgr", "mds", "rgw"):
                        node_daemons.append(info.name.removeprefix("cuttlefish."))
                        ttl_daemons += 1
            if ttl_daemons > prev:
                out["nodes"] += 1
                out["layout"][node] = node_daemons
                out["daemons"] += len(node_daemons)

        if args.json:
            print(json.dumps(out, indent=4))
            return

        if ttl_daemons:
            for node, daemons in out["layout"].items():
                daemon_types = {}
                print(f"{node}: ", end="")
                for daemon in daemons:
                    typ = daemon.split(".")[0]
                    daemon_types.setdefault(typ, []).append(daemon.split(".", 1)[1])
                for i, (typ, dlist) in enumerate(daemon_types.items()):
                    dlist.sort(key=lambda x: int(x) if x.isdigit() else x)
                    PFX = TColor.GREEN.value
                    SFX = TColor.RESET.value
                    for j, itm in enumerate(dlist):
                        dlist[j] = f"{PFX}{itm}{SFX}"
                    if len(dlist) == 1:
                        print(
                            f"{TColor.CYAN.value}{typ}{TColor.RESET.value}.{dlist[0]}",
                            end=", ",
                        )
                    else:
                        members = ", ".join(dlist)
                        print(
                            f"{TColor.CYAN.value}{typ}{TColor.RESET.value}.[{members}]",
                            end=", ",
                        )
                    if i == len(daemon_types) - 1:
                        print("\b\b ")
            print(
                f"Total: {ttl_daemons} daemons on {out['nodes']} nodes:",
                ", ".join([f"{cnt} {typ}" for typ, cnt in ttl_type.items() if cnt]),
            )
        else:
            print("No daemons running.")
        return

    raise ValueError(f"Unsupported controller mode: {mode}")


def _run_deploy(args: argparse.Namespace) -> None:
    _configure_logging(args)
    asyncio.run(_controller(args, "deploy"))


def _run_daemons(args: argparse.Namespace) -> None:
    _configure_logging(args)
    asyncio.run(_controller(args, "daemons"))


def _run_teardown(args: argparse.Namespace) -> None:
    _configure_logging(args)
    asyncio.run(_controller(args, "teardown"))
