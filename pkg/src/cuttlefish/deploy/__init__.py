"""
Copyright (c) 2024-2026 Tim Dettmar

Cuttlefish deployment module

This module contains functions necessary for the deployment of a Ceph
cluster.
"""

import asyncio
import configparser
import io
import ipaddress
import logging
from typing import Any, Dict, List, Optional, Set

import cuttlefish.container as cfc
import cuttlefish.schema as sch


def find_monitor_ips(manifest: sch.Manifest) -> Dict[str, str]:
    """
    For each monitor node, select the slowest interface IP that is on a
    network reachable by all other active nodes.

    Returns a dictionary mapping monitor node names to their selected IP addresses.
    """
    mons = manifest.get_nodes_with("mon")
    active_nodes = [node for node, info in manifest.nodes.items() if info.daemons]

    monitor_ips = {}

    for mon_node in mons:
        mon_info = manifest.nodes[mon_node]

        if not mon_info.netifs:
            # No network interface info, use fallback addr
            monitor_ips[mon_node] = mon_info.addr
            logging.warning(
                f"No netifs for monitor {mon_node}, using addr {mon_info.addr}"
            )
            continue

        # Sort interfaces by speed (ascending) to get slowest first
        # For same speed, sort by name (descending) to get last one alphabetically
        sorted_netifs = sorted(
            mon_info.netifs,
            key=lambda x: (x.speed_mbps, tuple(-ord(c) for c in x.name)),
        )

        selected_ip = None
        for netif in sorted_netifs:
            # Try both IPv4 and IPv6
            for ip_str in [netif.ipv4, netif.ipv6]:
                if ip_str is None:
                    continue

                try:
                    ip = ipaddress.ip_interface(ip_str)
                    network = ip.network

                    # Check if all other active nodes have an interface on this network
                    all_reachable = True
                    for other_node in active_nodes:
                        if other_node == mon_node:
                            continue

                        other_info = manifest.nodes[other_node]
                        has_interface_on_network = False

                        for other_netif in other_info.netifs:
                            for other_ip_str in [other_netif.ipv4, other_netif.ipv6]:
                                if other_ip_str is None:
                                    continue
                                try:
                                    other_ip = ipaddress.ip_interface(other_ip_str)
                                    if other_ip.network == network:
                                        has_interface_on_network = True
                                        break
                                except (ValueError, ipaddress.AddressValueError):
                                    continue

                            if has_interface_on_network:
                                break

                        if not has_interface_on_network:
                            all_reachable = False
                            break

                    if all_reachable:
                        selected_ip = ip_str
                        logging.info(
                            f"Selected IP {selected_ip} on network {network} (speed {netif.speed_mbps} Mbps) for monitor {mon_node}"
                        )
                        break

                except (ValueError, ipaddress.AddressValueError) as e:
                    logging.debug(f"Could not parse IP {ip_str}: {e}")
                    continue

            if selected_ip:
                break

        if selected_ip:
            # Strip CIDR notation to get just the IP address
            monitor_ips[mon_node] = selected_ip.split("/")[0]
        else:
            # Fallback to addr if no common network found
            monitor_ips[mon_node] = mon_info.addr
            logging.warning(
                f"No common network found for monitor {mon_node}, using addr {mon_info.addr}"
            )

    return monitor_ips


async def setup_mon_config(inst: cfc.Instance, manifest: sch.Manifest) -> None:
    """
    Set up the monitors for the Ceph cluster based on the provided manifest.
    The monmap will be generated and mon_initial_members will be set in the
    manifest Ceph config.

    .. warning::

        This function modifies the manifest in place and is therefore
        not thread-safe.

    :param inst: The deployer instance where the monitors will be set up.
    :param manifest: The deployment manifest containing cluster information.
    """
    mons = manifest.get_nodes_with("mon")

    # Find IPs for each monitor on networks reachable by all nodes
    monitor_ips = find_monitor_ips(manifest)

    mon_initial_members = list()
    mon_host = list()

    cmd = [
        "monmaptool",
        "--enable-all-features",
        "--create",
        "--clobber",
        "--set-min-mon-release",
        "19",
    ]
    for node in mons:
        ip = monitor_ips.get(node, manifest.nodes[node].addr)

        logging.debug("Adding monitor %s at %s", node, ip)
        mon_initial_members.append(node)
        mon_host.append(ip)
        cmd += ["--add", node, ip]
    cmd.append("/tmp/monmap")
    logging.debug("Monmap creation command: %s", " ".join(cmd))
    ret = await inst.exec(cmd)
    logging.debug("Extracting monmap from container")
    ret = await inst.exec(["base64", "-w", "0", "/tmp/monmap"])
    if ret.returncode != 0 or ret.stdout is None:
        logging.error("Failed to create monmap (rc=%d): %s", ret.returncode, ret.stderr)
        raise RuntimeError("Failed to create monmap")
    monmap = ret.stdout.strip()
    manifest.monmap = monmap

    # Set Ceph config monitor settings
    conf = configparser.ConfigParser()
    conf.read_string(manifest.ceph_config)
    conf.set("global", "mon_initial_members", ",".join(mon_initial_members))
    conf.set("global", "mon_host", ",".join(mon_host))

    # Set public_addr for each monitor
    for node in mons:
        ip = monitor_ips.get(node, manifest.nodes[node].addr)
        section = f"mon.{node}"
        if not conf.has_section(section):
            conf.add_section(section)
        conf.set(section, "public_addr", ip)

    logging.debug("Writing new config")
    x = io.StringIO()
    conf.write(x)
    x.seek(0)
    manifest.ceph_config = x.read()
