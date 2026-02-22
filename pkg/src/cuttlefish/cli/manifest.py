"""
Cuttlefish
Copyright (c) 2024-2026 Tim Dettmar

Manifest generator.
"""

import argparse
import asyncio
import configparser
import json
import logging
import os
from io import StringIO
from typing import Any, Dict, List, Optional, Sequence, cast

import pydantic

import cuttlefish.cluster as cct
import cuttlefish.cluster.slurm as slurm
import cuttlefish.container as cfc
import cuttlefish.deploy as cfd
import cuttlefish.placement as cfp
import cuttlefish.schema as sch
import cuttlefish.shell as cfs
import cuttlefish.util as cfu


def register(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "manifest",
        help="Generate or update the cluster manifest",
        description="Generate the placement manifest for the cluster",
    )
    _configure_parser(parser)
    parser.set_defaults(func=_run_manifest)


async def gather_network_interfaces(
    nodes: List[str], domain: Optional[str] = None, min_speed: int = 10000
) -> Dict[str, List[sch.NetworkInterface]]:
    logging.info("Gathering network interface information from nodes...")
    ssh_hosts = [f"{node}.{domain}" for node in nodes] if domain else nodes
    executor = cfs.ParallelRemoteExecutor(ssh_hosts, max_workers=len(ssh_hosts))
    ip_cmds = [["ip", "--json", "addr"] for _ in ssh_hosts]
    speed_cmd_str = (
        "for f in /sys/class/net/*/speed; do "
        '[ -r "$f" ] && echo "$f:$(cat $f 2>/dev/null || echo 0)"; '
        "done"
    )
    speed_cmds = [[speed_cmd_str] for _ in ssh_hosts]
    logging.debug("Running 'ip --json addr' on all nodes")
    ip_results = await executor.execute(ip_cmds, timeout=30.0)
    logging.debug("Running 'grep . /sys/class/net/*/speed' on all nodes")
    speed_results = await executor.execute(speed_cmds, timeout=30.0)

    node_netifs: Dict[str, List[sch.NetworkInterface]] = {}
    for idx, node in enumerate(nodes):
        netifs: List[sch.NetworkInterface] = []

        ip_data: Dict[str, Dict[str, Optional[str]]] = {}
        ip_result = ip_results[idx]
        if ip_result is not None and ip_result.retcode == 0:
            try:
                for iface in json.loads(ip_result.stdout):
                    name = iface.get("ifname")
                    if not name:
                        continue
                    ipv4 = None
                    ipv6 = None
                    for addr_info in iface.get("addr_info", []):
                        family = addr_info.get("family")
                        address = addr_info.get("local")
                        prefixlen = addr_info.get("prefixlen")
                        if family == "inet" and not ipv4 and address and prefixlen:
                            ipv4 = f"{address}/{prefixlen}"
                        elif family == "inet6" and not ipv6 and address and prefixlen:
                            ipv6 = f"{address}/{prefixlen}"
                    ip_data[name] = {"ipv4": ipv4, "ipv6": ipv6}
            except (json.JSONDecodeError, KeyError) as err:
                logging.warning("Failed to parse IP data for %s: %s", node, err)
        else:
            logging.warning("Failed to get IP addresses for node %s", node)

        speed_data: Dict[str, int] = {}
        speed_result = speed_results[idx]
        logging.debug(
            "Speed result for %s: retcode=%s, stdout_len=%s",
            node,
            speed_result.retcode if speed_result else "None",
            len(speed_result.stdout) if speed_result else 0,
        )
        if speed_result is not None and speed_result.stdout.strip():
            logging.debug("Speed output for %s: %s", node, speed_result.stdout.strip())
            for line in speed_result.stdout.strip().split("\n"):
                if ":" not in line:
                    continue
                path, speed_str = line.split(":", 1)
                parts = path.split("/")
                if len(parts) < 5:
                    continue
                iface_name = parts[4]
                try:
                    speed_mbps = int(speed_str.strip())
                    if speed_mbps < 0:
                        speed_mbps = 0
                    speed_data[iface_name] = speed_mbps
                    logging.debug(
                        "Parsed speed for %s on %s: %s Mbps",
                        iface_name,
                        node,
                        speed_mbps,
                    )
                except ValueError:
                    logging.debug(
                        "Could not parse speed for %s on %s: %s",
                        iface_name,
                        node,
                        speed_str,
                    )
        else:
            logging.warning(
                "Failed to get interface speeds for node %s, no output from command",
                node,
            )

        seen = set(list(ip_data.keys()) + list(speed_data.keys()))
        for iface_name in seen:
            if iface_name == "lo":
                continue
            ip_info = ip_data.get(iface_name, {})
            speed = speed_data.get(iface_name, 0)
            ipv4 = ip_info.get("ipv4")
            ipv6 = ip_info.get("ipv6")
            if speed == 0 and ipv4 is None and ipv6 is None:
                logging.debug(
                    "Skipping interface %s on %s: no speed or address info",
                    iface_name,
                    node,
                )
                continue
            if speed < min_speed:
                logging.debug(
                    "Skipping interface %s on %s: speed %s < %s Mbps",
                    iface_name,
                    node,
                    speed,
                    min_speed,
                )
                continue
            netifs.append(
                sch.NetworkInterface(
                    name=iface_name, speed_mbps=speed, ipv4=ipv4, ipv6=ipv6
                )
            )
        netifs.sort(key=lambda entry: entry.speed_mbps, reverse=True)
        node_netifs[node] = netifs
        logging.debug("Found %s interfaces on %s", len(netifs), node)

    logging.info(
        "Successfully gathered network information from %s nodes",
        len(node_netifs),
    )
    return node_netifs


def apply_daemon_overlap_settings(
    daemon_configs: List[cfp.DaemonTypeConfig], args: argparse.Namespace
) -> List[cfp.DaemonTypeConfig]:
    for dconf in daemon_configs:
        if not dconf.disallow_overlap:
            continue
        for dtype in dconf.disallow_overlap:
            for dconf_target in daemon_configs:
                if dconf_target.name != dtype:
                    continue
                if dconf_target.disallow_overlap is None:
                    dconf_target.disallow_overlap = []
                if dconf.name not in dconf_target.disallow_overlap:
                    dconf_target.disallow_overlap.append(dconf.name)
    return daemon_configs


def get_daemon_config(
    name: str, configs: List[cfp.DaemonTypeConfig]
) -> Optional[cfp.DaemonTypeConfig]:
    for config in configs:
        if config.name == name:
            return config
    return None


async def execute(args: argparse.Namespace) -> None:
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    node_allocator = cct.get_provider(cct.NodeProviderType.SLURM)
    if args.nodelist:
        all_nodes = slurm.from_slurm_nodelist(args.nodelist)
    elif args.jobid:
        job = node_allocator.get_allocation(args.jobid)
        all_nodes = job.nodes
    elif os.environ.get("SLURM_JOB_ID"):
        job_id = os.environ["SLURM_JOB_ID"]
        job = node_allocator.get_allocation(job_id)
        all_nodes = job.nodes
    else:
        jobs = node_allocator.get_allocation("cuttlefish")
        if not jobs:
            raise ValueError(
                "No SLURM jobs found. Specify --nodelist or --jobid explicitly."
            )
        all_nodes = jobs.nodes

    if not all_nodes:
        logging.warning("No nodes available for placement. Proceeding anyway.")

    logging.info("Available nodes: %s", ", ".join(all_nodes))
    nodes = all_nodes
    unused: List[str] = []
    if args.nodecount > 0:
        nodes = all_nodes[: args.nodecount]
        unused = all_nodes[args.nodecount :]
    logging.info("Using nodes:     %s", ", ".join(nodes))
    if unused:
        logging.info("Unused nodes:    %s", ", ".join(unused))

    daemon_configs: List[cfp.DaemonTypeConfig] = []
    if args.osd > 0:
        daemon_configs.append(
            cfp.DaemonTypeConfig(
                name="osd",
                count=args.osd,
                placement=cfp.PlacementPolicy.SPREAD,
                density=1000,
            )
        )
    if args.mon > 0:
        daemon_configs.append(
            cfp.DaemonTypeConfig(
                name="mon",
                count=args.mon,
                placement=cfp.PlacementPolicy.SPREAD,
                density=1,
            )
        )
    if args.mgr > 0:
        daemon_configs.append(
            cfp.DaemonTypeConfig(
                name="mgr",
                count=args.mgr,
                placement=cfp.PlacementPolicy.SPREAD,
                density=1,
                require_overlap=["mon"],
            )
        )
    if args.mds > 0:
        daemon_configs.append(
            cfp.DaemonTypeConfig(
                name="mds",
                count=args.mds,
                placement=cfp.PlacementPolicy.SPREAD,
                density=1000,
            )
        )

    for dconf in daemon_configs:
        if dconf.name == "mon" and args.mon_overlap:
            allowed = [d.strip() for d in args.mon_overlap.split(",")]
            dconf.disallow_overlap = [
                d for d in cfp.all_daemons_except("mon") if d not in allowed
            ]
        elif dconf.name == "mgr" and args.mgr_overlap:
            allowed = [d.strip() for d in args.mgr_overlap.split(",")]
            dconf.disallow_overlap = [
                d for d in cfp.all_daemons_except("mgr") if d not in allowed
            ]
        elif dconf.name == "mds" and args.mds_overlap:
            allowed = [d.strip() for d in args.mds_overlap.split(",")]
            dconf.disallow_overlap = [
                d for d in cfp.all_daemons_except("mds") if d not in allowed
            ]

    if args.mds_require_overlap:
        reqd = [d.strip() for d in args.mds_require_overlap.split(",")]
        mds_conf = get_daemon_config("mds", daemon_configs)
        if mds_conf:
            mds_conf.require_overlap = reqd

    daemon_configs = apply_daemon_overlap_settings(daemon_configs, args)

    out_container_image: Optional[cfc.ContainerImage] = None
    out_bench_image: Optional[cfc.ContainerImage] = None
    out_config: Optional[str] = None
    out_keyring: Optional[str] = None
    out_domain = args.domain

    if args.no_overwrite:
        try:
            with open(args.output, "r") as handle:
                old_manifest = sch.Manifest.model_validate_json(handle.read())
            out_container_image = old_manifest.image
            out_bench_image = old_manifest.bench_image
            out_config = old_manifest.ceph_config
            out_keyring = old_manifest.keyring
            out_domain = old_manifest.domain
        except FileNotFoundError:
            logging.info(
                "No existing manifest found at %s, creating new one.", args.output
            )
        except pydantic.ValidationError as exc:
            logging.error(
                "Existing manifest at %s is invalid: %s, creating new one",
                args.output,
                exc,
            )
            return

    if not args.no_overwrite:
        image_format = args.image_format
        if image_format == "auto":
            if args.image.endswith(".sif"):
                image_format = "sif"
            elif args.image.endswith(".tar"):
                logging.warning(
                    "Assuming docker-archive format for .tar image, use -f to specify if incorrect!"
                )
                image_format = "docker-archive"
            else:
                image_format = "docker"
        out_container_image = cfc.ContainerImage(
            name=args.image, type=cfc.ContainerImageType(image_format)
        )
        if out_container_image.type == cfc.ContainerImageType.SIF:
            out_container_image.name = os.path.abspath(out_container_image.name)

        if args.bench_image:
            bench_format = args.bench_image_format
            if bench_format == "auto":
                if args.bench_image.endswith(".sif"):
                    bench_format = "sif"
                elif args.bench_image.endswith(".tar"):
                    logging.warning(
                        "Assuming docker-archive format for .tar bench image, use --bench-image-format to specify if incorrect"
                    )
                    bench_format = "docker-archive"
                else:
                    bench_format = "docker"
            out_bench_image = cfc.ContainerImage(
                name=args.bench_image, type=cfc.ContainerImageType(bench_format)
            )
            if out_bench_image.type == cfc.ContainerImageType.SIF:
                out_bench_image.name = os.path.abspath(out_bench_image.name)
        else:
            out_bench_image = out_container_image

        with open(args.config, "r") as handle:
            out_config = handle.read()
        with open(args.keyring, "r") as handle:
            out_keyring = handle.read()
        out_domain = args.domain

    if not daemon_configs:
        logging.error("No daemons specified for placement")
        return

    assert out_container_image is not None
    assert out_config is not None
    assert out_keyring is not None
    if out_bench_image is None:
        out_bench_image = out_container_image

    resolver = cfu.DNSResolver()
    lookups = (
        [f"{node}.{out_domain}" for node in all_nodes] if out_domain else all_nodes
    )
    addrs = await resolver.resolve_bulk_async(lookups)
    addrs = cast(Dict[str, str], addrs)
    addrs = {node.split(".")[0]: ip for node, ip in addrs.items()}
    logging.debug("Resolving hostnames: %s -> %s", lookups, addrs)

    node_netifs: Dict[str, List[sch.NetworkInterface]] = {}
    if not args.skip_netif and nodes:
        try:
            node_netifs = await gather_network_interfaces(nodes, None, args.min_speed)
        except Exception as exc:
            logging.warning("Failed to gather network interfaces: %s", exc)
            logging.warning("Continuing without network interface information...")

    placed_nodes = cfp.place_daemons(nodes, daemon_configs, allow_empty=True)
    out_nodes: Dict[str, sch.NodeManifest] = {}
    out_nx_port = args.nx_port
    mon_nodes: List[str] = []
    for host, daemons in placed_nodes.items():
        daemon_list: List[sch.Daemon] = []
        for name in daemons.export_daemons():
            daemon_list.append(sch.Daemon(name=name, px_port=args.px_base_port))
            if args.px_base_port:
                args.px_base_port += 1
            if name.startswith("mon"):
                mon_nodes.append(host)
        out_nodes[host] = sch.NodeManifest(
            addr=addrs[host], daemons=daemon_list, netifs=node_netifs.get(host, [])
        )

    max_osd = 0
    for data in out_nodes.values():
        max_osd = max(max_osd, len(data.get_daemons("osd")))

    if args.mem_target > 0 and max_osd > 0:
        mem_bytes = args.mem_target * 1024**3
        per_osd = mem_bytes // max_osd
        logging.info(
            "Set memstore_device_bytes to %s GB, target %s GB/node, %s OSDs on busiest node",
            per_osd // (1024**3),
            args.mem_target,
            max_osd,
        )
        conf = configparser.ConfigParser()
        conf.read_string(out_config)
        if not conf.has_section("osd"):
            conf.add_section("osd")
        conf.set("osd", "memstore_device_bytes", str(per_osd))
        with StringIO() as buffer:
            conf.write(buffer)
            out_config = buffer.getvalue()

    manifest = sch.Manifest(
        nodes=out_nodes,
        image=out_container_image,
        bench_image=out_bench_image,
        ceph_config=out_config,
        keyring=out_keyring,
        domain=out_domain,
        nx_port=out_nx_port,
        monmap="",
        runtime=cfc.ContainerRuntimeType(args.runtime),
    )

    runtime = cfc.get_runtime(manifest.runtime)
    try:
        deployer = runtime.get_instance(manifest.deployer_name)
        if deployer.info.img != out_container_image.name:
            logging.info(
                "Deployer container image mismatch. Manifest: %s, Found: %s",
                out_container_image.name,
                deployer.info.img,
            )
            await deployer.stop()
            raise KeyError("Deployer container image mismatch, recreating")
    except KeyError:
        logging.info("Creating deployer container for monmap generation")
        deployer = await runtime.create_instance_async(
            manifest.deployer_name, out_container_image, compat=True
        )
    await cfd.setup_mon_config(deployer, manifest)

    with open(args.output, "w") as handle:
        handle.write(manifest.model_dump_json(indent=4))

    logging.info("Node placement written to manifest at %s", args.output)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="cuttle manifest",
        description="Generate the placement manifest for the cluster",
    )
    _configure_parser(parser)
    return parser.parse_args(argv)


def run(argv: Optional[Sequence[str]] = None) -> None:
    asyncio.run(execute(parse_args(argv)))


def _configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--osd", type=int, default=3, help="Number of OSDs to place")
    parser.add_argument(
        "--mon", type=int, default=1, help="Number of monitors to place"
    )
    parser.add_argument(
        "--mgr", type=int, default=1, help="Number of manager daemons to place"
    )
    parser.add_argument(
        "--mds", type=int, default=0, help="Number of MDS daemons to place"
    )
    parser.add_argument(
        "--nodelist",
        type=str,
        default=None,
        help="Nodelist in slurm format. Overrides -J/--jobid and SLURM_JOB_ID",
    )
    parser.add_argument(
        "-J",
        "--jobid",
        type=str,
        default=None,
        help="Slurm job ID for placement",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default="manifest.json",
        help="Output file for the placement manifest",
    )
    parser.add_argument(
        "-N",
        "--nodecount",
        type=int,
        default=-1,
        help="Number of nodes to place daemons on (default: all available nodes)",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--px-base-port",
        type=int,
        required=False,
        help="Base port number for prometheus-process-exporter (default: disabled)",
    )
    parser.add_argument(
        "--nx-port",
        type=int,
        default=None,
        help="Port number for node exporter (default: disabled)",
    )
    parser.add_argument(
        "-i",
        "--image",
        type=str,
        default="quay.io/ceph/ceph:v19.2",
        help="Container image to use for the executor container",
    )
    parser.add_argument(
        "-f",
        "--image-format",
        type=str,
        choices=["docker", "oci-archive", "docker-archive", "sif", "auto"],
        default="auto",
        help="Format of the container image specified",
    )
    parser.add_argument(
        "--bench-image",
        type=str,
        default=None,
        help="Container image to use for benchmarking (default: use main image)",
    )
    parser.add_argument(
        "--bench-image-format",
        type=str,
        choices=["docker", "oci-archive", "docker-archive", "sif", "auto"],
        default="auto",
        help="Format of the benchmarking container image",
    )
    parser.add_argument(
        "-d",
        "--domain",
        type=str,
        help="Domain to use for DNS resolution of cluster hosts",
    )
    parser.add_argument(
        "-k",
        "--keyring",
        type=str,
        default="keyring",
        help="Path to the Ceph keyring file",
    )
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        default="ceph.conf",
        help="Path to the base Ceph configuration file",
    )
    parser.add_argument(
        "-w",
        "--no-overwrite",
        action="store_true",
        help="Do not overwrite existing output manifest file parameters",
    )
    parser.add_argument(
        "-r",
        "--runtime",
        type=str,
        choices=["apptainer", "docker", "podman"],
        default="apptainer",
        help="Container runtime to use for deployment",
    )
    parser.add_argument(
        "--mon-overlap",
        type=str,
        default="mgr,mds",
        help="Allow monitor daemons to overlap with specific daemon types",
    )
    parser.add_argument(
        "--mgr-overlap",
        type=str,
        default="mon,mds",
        help="Allow manager daemons to overlap with specific daemon types",
    )
    parser.add_argument(
        "--mds-overlap",
        type=str,
        default="mon,mgr",
        help="Allow MDS daemons to overlap with other daemon types",
    )
    parser.add_argument(
        "--mds-require-overlap",
        type=str,
        default="",
        help="Require MDS daemons to overlap with specific daemon types",
    )
    parser.add_argument(
        "--mem-target",
        type=int,
        default=0,
        help="Amount of memory available per node for OSD placement in GB",
    )
    parser.add_argument(
        "--skip-netif",
        action="store_true",
        help="Skip gathering network interface information from nodes",
    )
    parser.add_argument(
        "--min-speed",
        type=int,
        default=10000,
        help="Minimum interface speed in Mbps to include in netifs",
    )


def _run_manifest(args: argparse.Namespace) -> None:
    asyncio.run(execute(args))
