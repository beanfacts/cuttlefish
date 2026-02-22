"""
Cuttlefish
Copyright (c) 2024-2026 Tim Dettmar

Cuttlefish Ceph daemon worker script.
"""

import argparse
import asyncio
import base64
import configparser
import io
import logging
import os
import pathlib
import socket
import uuid
from typing import List, Optional, Union

import cuttlefish.container as cct
import cuttlefish.io as cfio
import cuttlefish.schema as sch


def register(subparsers: argparse._SubParsersAction) -> None:
    """Attach the worker subcommand."""
    parser = subparsers.add_parser(
        "worker",
        help="Execute node-level deployment tasks",
        description="Start, restart, or stop Ceph daemons on the local node",
    )
    _setup_parser(parser)
    parser.set_defaults(func=_run_worker)


def _setup_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "-m",
        "--manifest",
        type=str,
        default="manifest.json",
        help="Path to the manifest file",
    )
    parser.add_argument(
        "-u",
        "--up",
        nargs="+",
        default=[],
        help="List of daemons to start e.g. 'mon', 'osd.0'",
    )
    parser.add_argument(
        "-d",
        "--down",
        nargs="+",
        default=[],
        help="List of daemons to stop",
    )
    parser.add_argument(
        "-r",
        "--restart",
        nargs="+",
        default=[],
        help="List of daemons to restart",
    )
    parser.add_argument(
        "--no-delete",
        action="store_true",
        help="Do not delete the daemon directories after stopping",
    )
    parser.add_argument(
        "-s",
        "--signal",
        type=str,
        default="SIGTERM",
        help="Signal to send to instances in --down argument",
    )
    parser.add_argument(
        "-w",
        "--workdir",
        type=str,
        default=None,
        help="Working directory for the orchestrator. Default /tmp/<user>/cuttlefish",
    )
    parser.add_argument(
        "--repodir",
        type=str,
        default=pathlib.Path.cwd().as_posix(),
        help="Path to the cuttlefish repository",
    )
    parser.add_argument(
        "--cleanall",
        action="store_true",
        help="Clean all daemon directories",
    )


def get_daemon_type(daemon: str) -> str:
    return daemon.split(".")[0]


def get_daemon_name(daemon: str) -> str:
    return daemon.split(".")[1] if "." in daemon else daemon


def get_daemon_data_args(daemon: str, helper: bool = False) -> List[str]:
    dt = get_daemon_type(daemon)
    args = [
        f"--{dt}-data",
        f"/data/workdir/{daemon}/data",
        "--conf",
        f"/data/workdir/config/ceph.conf",
        "--keyring",
        f"/data/workdir/{daemon}/keyring",
    ]
    if dt == "mon":
        args += [
            "--monmap",
            f"/data/workdir/config/monmap",
            "--mon-host",
            get_daemon_name(daemon),
        ]
    elif dt == "osd":
        args += ["--osd-journal", f"/data/workdir/{daemon}/journal", "--no-mon-config"]
    return args


class Deployer:
    def __init__(
        self,
        manifest: sch.Manifest,
        workdir: Union[str, pathlib.Path],
        repodir: Union[str, pathlib.Path],
    ) -> None:
        self.hostname = socket.gethostname()
        self.manifest = manifest
        self.workdir = pathlib.Path(workdir)
        self.repodir = pathlib.Path(repodir)
        self.runtime = cct.get_runtime(manifest.runtime)
        self.helper: Optional[cct.Instance] = None
        self.session_id: Optional[str] = None

    async def _write_session(self) -> None:
        sid = str(uuid.uuid4())
        await cfio.write(self.workdir, f".session-{sid}", sid)
        self.session_id = sid

    async def _write_config_files(self) -> None:
        tasks = []
        if self.manifest.ceph_config:
            tasks.append(
                asyncio.create_task(
                    cfio.write(
                        self.workdir / "config", "ceph.conf", self.manifest.ceph_config
                    )
                )
            )
        if self.manifest.monmap:
            monmap = base64.b64decode(self.manifest.monmap)
            tasks.append(
                asyncio.create_task(
                    cfio.write(self.workdir / "config", "monmap", monmap)
                )
            )
        tasks.append(asyncio.create_task(self._write_session()))

        logging.debug("Creating configuration files")
        await asyncio.gather(*tasks)

    async def _get_or_create_helper_container(self) -> None:
        logging.debug("Waiting for helper container")
        try:
            self.helper = self.runtime.get_instance("cuttlefish.helper")
            try:
                await self._validate_mount()
            except RuntimeError:
                logging.info("Recreating helper container due to mount point change")
                await self.helper.stop()
                raise RuntimeError("Recreating helper container")
        except (KeyError, RuntimeError):
            await self.runtime.create_instance_async(
                "cuttlefish.helper",
                self.manifest.image,
                [cct.Mount(self.workdir, "/data/workdir")],
                compat=True,
            )
            self.helper = self.runtime.get_instance("cuttlefish.helper")

    async def _validate_mount(self) -> None:
        assert self.helper is not None
        if self.session_id is None:
            await self._write_session()
            assert self.session_id is not None
        ret = await self.helper.exec(
            ["ls", "-la", f"/data/workdir/.session-{self.session_id}"]
        )
        if ret.returncode == 0 and ret.stdout:
            if self.session_id not in ret.stdout:
                logging.error(
                    "Session ID mismatch: expected %s, got %s",
                    self.session_id,
                    ret.stdout,
                )
                raise RuntimeError("Session ID mismatch in helper container")

    async def setup_node_exporter(
        self, image: Optional[cct.ContainerImage] = None
    ) -> None:
        logging.info("Setting up node_exporter")
        mounts = [
            cct.Mount("/", "/host", readonly=True),
            cct.Mount(self.repodir / "overlay", "/data/cfoverlay", readonly=True),
        ]
        if not image:
            image = self.manifest.image
        try:
            self.runtime.get_instance("cuttlefish.node_exporter")
            logging.info("node_exporter instance already exists")
        except KeyError:
            await self.runtime.create_instance_async(
                "cuttlefish.node_exporter",
                image,
                mounts,
                compat=True,
                args=["/data/cfoverlay/node_exporter.sh"],
            )
            logging.info("node_exporter instance started")

    async def write_keyrings(self) -> None:
        keyring_config = configparser.ConfigParser()
        keyring_config.read_string(self.manifest.keyring)

        tasks = []

        for daemon in self.manifest.nodes[self.hostname].daemons:
            daemon_type = daemon.get_type()
            daemon_dir = self.workdir / daemon.name

            if daemon_type == "mon":
                keyring_content = self.manifest.keyring
            else:
                section_name = daemon.name

                if not keyring_config.has_section(section_name):
                    logging.warning("No keyring section for daemon %s", daemon.name)
                    continue

                daemon_config = configparser.ConfigParser()
                daemon_config.add_section(section_name)

                for option in keyring_config.options(section_name):
                    value = keyring_config.get(section_name, option)
                    daemon_config.set(section_name, option, value)

                keyring_buffer = io.StringIO()
                daemon_config.write(keyring_buffer)
                keyring_content = keyring_buffer.getvalue()

            tasks.append(cfio.write(daemon_dir, "keyring", keyring_content + "\n"))
            logging.debug("Prepared keyring for daemon %s", daemon.name)

        if tasks:
            await asyncio.gather(*tasks)
            logging.debug("Keyring files written for %d daemons", len(tasks))

    async def cleanall(self) -> None:
        if self.helper is None:
            await self._get_or_create_helper_container()
        assert self.helper is not None
        logging.info("Cleaning up workdir")
        await self.helper.exec(["rm", "-rf", f"/data/workdir"])
        logging.info("Stopping Cuttlefish containers")
        tasks = []
        instances = await self.runtime.list_instances_async()
        for inst in instances:
            if inst.name.startswith("cuttlefish."):
                instance = self.runtime.get_instance(inst.name)
                tasks.append(asyncio.create_task(instance.stop()))
        await asyncio.gather(*tasks)
        logging.info("Cuttlefish containers (%s) stopped", len(tasks))

    async def init(self) -> None:
        conf_task = asyncio.create_task(self._write_config_files())
        ct_task = asyncio.create_task(self._get_or_create_helper_container())
        await asyncio.gather(conf_task, ct_task)
        await self._validate_mount()

    async def setup_daemon(self, daemon: str, timeout: float = 30.0) -> None:
        assert self.helper is not None
        logging.info("Setting up daemon %s", daemon)
        daemon_dir = self.workdir / daemon
        var_log_dir = self.workdir / "var" / "log"
        (var_log_dir / "ceph").mkdir(parents=True, exist_ok=True)
        mounts = [
            cct.Mount(daemon_dir, f"/data/workdir/{daemon}"),
            cct.Mount(self.workdir / "config", "/data/workdir/config", readonly=True),
            cct.Mount(var_log_dir, "/var/log"),
            cct.Mount(self.repodir / "overlay", "/data/cfoverlay", readonly=True),
        ]

        ib_paths = [
            "/sys/class/infiniband",
            "/sys/class/infiniband_mad",
            "/sys/class/infiniband_verbs",
            "/dev/infiniband",
        ]
        if all(pathlib.Path(p).exists() for p in ib_paths):
            for path in ib_paths:
                mounts.append(cct.Mount(pathlib.Path(path), path))

        binary = f"ceph-{get_daemon_type(daemon)}"
        data_dir = daemon_dir / "data"

        if any(daemon.startswith(i) for i in ("osd", "mon")):
            needs_mkfs = False
            if not data_dir.exists():
                logging.debug("Creating data directory for %s -> %s", daemon, data_dir)
                data_dir.mkdir(parents=True)
                needs_mkfs = True
            elif not any(data_dir.iterdir()):
                logging.debug("Data directory for %s is empty, mkfs required", daemon)
                needs_mkfs = True
            else:
                logging.debug(
                    "Data directory for %s exists and is non-empty, skipping mkfs",
                    daemon,
                )

            if needs_mkfs:
                cmd = [binary, "-n", daemon, "--mkfs"] + get_daemon_data_args(daemon)
                logging.debug(
                    "Running mkfs for %s with command: %s", daemon, " ".join(cmd)
                )
                result = await self.helper.exec(cmd)
                if result.returncode != 0:
                    stderr = result.stderr or b""
                    logging.error("mkfs failed for %s: %s", daemon, stderr)
                    raise RuntimeError(f"Failed to initialize {daemon}")
                logging.debug("mkfs completed successfully for %s", daemon)

        try:
            self.runtime.get_instance(f"cuttlefish.{daemon}")
            logging.debug("Daemon %s already exists, skipping setup", daemon)
        except KeyError:
            conf = self.manifest.get_daemon_conf(daemon, self.hostname)
            cmd = [
                "/bin/bash",
                "/data/cfoverlay/autorestart.sh",
                str(conf.px_port),
                binary,
                "-d",
                "-n",
                daemon,
            ] + get_daemon_data_args(daemon)
            logging.debug("Starting daemon %s with command: %s", daemon, " ".join(cmd))
            await self.runtime.create_instance_async(
                f"cuttlefish.{daemon}",
                self.manifest.image,
                mounts,
                compat=True,
                args=cmd,
            )
            logging.debug("Daemon %s started", daemon)

    async def restart_daemon(self, daemon: str, px_port: int = 0) -> None:
        try:
            instance = self.runtime.get_instance(f"cuttlefish.{daemon}")
            await instance.stop()
        except KeyError:
            logging.warning("Daemon %s not found for restart", daemon)
        await self.setup_daemon(daemon)

    async def teardown_daemon(
        self, daemon: str, signal: str = "SIGTERM", delete: bool = False
    ) -> None:
        assert self.helper is not None
        try:
            instance = self.runtime.get_instance(f"cuttlefish.{daemon}")
            await instance.stop(signal)
            if delete:
                await self.helper.exec(["rm", "-rf", f"/data/workdir/{daemon}"])
        except KeyError:
            logging.warning("Daemon %s not found", daemon)
        except Exception as exc:
            logging.error("Failed to stop daemon %s: %s", daemon, exc)


def _configure_logging(hostname: str) -> pathlib.Path:
    log_dir = pathlib.Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_filename = log_dir / f"cuttlefish-deploy-{hostname}.log"
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_filename),
        ],
    )
    logging.info("Logging to file: %s", log_filename)
    return log_filename


async def _worker_main(args: argparse.Namespace) -> None:
    hostname = socket.gethostname()
    with open(args.manifest, "r", encoding="utf-8") as handle:
        manifest = sch.Manifest.model_validate_json(handle.read())

    workdir = pathlib.Path(args.workdir or f"/tmp/{os.getuid()}/cuttlefish")
    (workdir / "config").mkdir(parents=True, exist_ok=True)
    repodir = pathlib.Path(args.repodir).expanduser().resolve()

    deployer = Deployer(manifest, workdir, repodir)

    if args.cleanall:
        logging.debug("Cleaning all Cuttlefish containers and workdirs")
        await deployer.cleanall()
        return

    logging.debug("Initializing deployer")
    await deployer.init()

    if args.up:
        await deployer.write_keyrings()

    if manifest.nx_port:
        await deployer.setup_node_exporter()

    node_conf = manifest.nodes.get(deployer.hostname)
    if not node_conf:
        logging.warning(
            "No node configuration found for this host (%s)", deployer.hostname
        )
        return

    if not args.up and not args.down and not args.restart:
        logging.info("No actions specified, exiting")
        return

    tasks = []
    tasks += [deployer.setup_daemon(daemon) for daemon in args.up]
    tasks += [deployer.restart_daemon(daemon) for daemon in args.restart]
    tasks += [
        deployer.teardown_daemon(daemon, args.signal, not args.no_delete)
        for daemon in args.down
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            logging.error("Error during daemon operation: %s", result)
            raise result
    logging.info("Daemons set up successfully")


def _run_worker(args: argparse.Namespace) -> None:
    hostname = socket.gethostname()
    _configure_logging(hostname)
    asyncio.run(_worker_main(args))
