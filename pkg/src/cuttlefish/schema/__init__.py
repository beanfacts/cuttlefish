"""
Copyright (c) 2024-2026 Tim Dettmar

Cuttlefish Pydantic schema definitions.

Cuttlefish creates manifests that describe the desired state of a Ceph
cluster, including all configuration necessary for deployment. These
manifests are represented using the Pydantic models defined in this
module.
"""

import enum
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field

import cuttlefish.container as cfc
import cuttlefish.util as cfu


class NetworkInterface(BaseModel):
    name: str  # Name of the network interface, e.g., ib0
    speed_mbps: int = 0
    ipv4: Optional[str] = None  # IPv4 address with CIDR notation, e.g., "10.0.0.1/24"
    ipv6: Optional[str] = None  # IPv6 address with CIDR notation, e.g., "fe80::1/64"


class Daemon(BaseModel):
    name: str  # Name of the daemon, e.g., "osd.0"
    px_port: Optional[int] = None  # Port number for prometheus-process-exporter

    def get_type(self) -> str:
        return self.name.split(".")[0] if "." in self.name else self.name


class NodeManifest(BaseModel):
    addr: str
    """
    IP address of the management interface of the node.
    This address is used for SSH connections during deployment.
    """
    netifs: List[NetworkInterface] = Field(default_factory=list)
    """
    Detected network interfaces on the node. When provided, Cuttlefish
    will explicitly set the public network address inside ceph.conf.
    """
    daemons: List[Daemon] = Field(default_factory=list)
    """
    Daemons assigned to this node.
    """

    def get_daemons(self, prefix: Optional[str] = None) -> List[str]:
        if prefix:
            return [d.name for d in self.daemons if d.name.startswith(prefix)]
        return [d.name for d in self.daemons]


class NodeConfig(BaseModel):
    daemon_limit: int  # Max number of daemons per node


class PlacementPolicy(enum.Enum):
    SPREAD = "spread"  # Spread across nodes, wrapping around up to density
    GROUP = "group"  # Group together on the same node up to density, then move to next nodes


class DaemonTypeConfig(BaseModel):
    name: str  # Name of the daemon type (mon/mgr/osd etc)
    count: int  # The desired number of daemons of this type
    placement: PlacementPolicy = PlacementPolicy.SPREAD  # Placement policy
    density: int = 1  # Maximum number of these daemons on a single node
    caps: Optional[Dict[str, str]] = (
        None  # Capabilities for this daemon type (e.g. "mon": "allow *")
    )
    disallow_overlap: Optional[List[str]] = (
        None  # Daemon types that cannot overlap with this daemon type
    )
    require_overlap: Optional[List[str]] = None


class PlacementConfig(BaseModel):
    mon: int = 1
    mgr: int = 1
    osd: int = 3
    mds: int = 0
    nodelist: List[str]


class PlacementManifest(BaseModel):
    nodes: Dict[str, NodeManifest]
    """
    Mapping of node hostnames to their respective manifest.
    """


class Manifest(BaseModel):
    """
    The manifest contains all information required to deploy a Ceph cluster
    from scratch.
    """

    nodes: Dict[str, NodeManifest]
    """
    Mapping of node hostnames to their respective manifest.
    """
    monmap: str
    """
    Base64-encoded monmap for the cluster. This is generated during
    deployment and is empty in the initial manifest.
    """
    ceph_config: str
    """
    ceph.conf file shared across all nodes in the cluster.
    """
    image: cfc.ContainerImage
    """
    Information about the container image to use for deployment.
    """
    bench_image: cfc.ContainerImage
    """
    Information about the container image to use for benchmarking.
    Defaults to the main image if not explicitly specified during placement.
    """
    keyring: str
    """
    Content of the keyring containing all necessary keys for deployment.
    The content of the keyring will be tailored to the requirements of
    each node during deployment (e.g., monitor has all keys, OSD only
    the key for that specific daemon ID).
    """
    nx_port: Optional[int]
    """
    Port number for Prometheus Node Exporter, if used.
    """
    domain: Optional[str] = None
    """
    Domain for DNS resolution.
    
    Cuttlefish always requires properly set up DNS in order to resolve
    the IP addresses of all nodes in the manifest. If no domain is
    specified, the hostname will directly be used for resolution, and if
    a domain is provided, it will be appended to the hostname.
    
    The DNS and domain setup should be configured such that the IP
    addresses returned by the resolution match the high-speed network
    interfaces used for Ceph traffic, and not e.g., management networks.
    """
    deployer_name: str = "cuttlefish.deployer"
    """
    Name of the container instance which will be used to run deployment
    commands. Must have a unique name in the container runtime, as well
    as a Ceph installation.
    """
    runtime: cfc.ContainerRuntimeType = cfc.ContainerRuntimeType.APPTAINER
    """
    Container runtime to use for deployment.
    """

    def get_daemon_conf(self, name: str, only_node: Optional[str] = None) -> Daemon:
        """
        Get the config for the daemon with the specified name from the manifest.

        :param name: Name of the daemon to get (e.g., "osd.0").
        :param only_node: If specified, only search for the daemon on
            this node, and raise a KeyError if it is not found there.

        :raises KeyError: If the daemon is not found in the manifest.
        :return: The Daemon object with the specified name.
        """
        for node, info in self.nodes.items():
            if only_node is not None and node != only_node:
                continue
            for daemon in info.daemons:
                if daemon.name == name:
                    return daemon
        raise KeyError(f"Daemon {name} not found in manifest")

    def get_nodes_with(self, dtype: str) -> List[str]:
        """
        Get a list of node names hosting a specific daemon type.

        :param dtype: Daemon type to look for (e.g., "mon", "osd").
            Performs a prefix match.
        """
        out = list()
        for node, info in self.nodes.items():
            for daemon in info.daemons:
                if daemon.name.startswith(dtype):
                    out.append(node)
        return out

    def find_node_with(self, daemon: str) -> str:
        """
        Find a node hosting a specific daemon by its type and ID.

        :param daemon: Daemon to find. To find e.g., osd.0, pass "osd.0"
            as the daemon parameter. If, however, you just want all
            OSDs, use get_nodes_with("osd") instead.
        """
        for node, info in self.nodes.items():
            for d in info.daemons:
                if d.name == daemon:
                    return node
        raise KeyError(f"No node found hosting daemon {daemon}")

    async def resolve_all(
        self, ignore_exceptions: bool = False
    ) -> Dict[str, Optional[str]]:
        """
        Resolve all node hostnames to their IP addresses.

        :param ignore_exceptions: If True, nodes that cannot be resolved
            will have a value of None in the returned dictionary. If
            False, an exception will be raised if any node cannot be
            resolved.
        """
        hostnames = list()
        for node in self.nodes.keys():
            hostnames.append(node)
            if self.domain is not None:
                hostnames.append(f"{node}.{self.domain}")
        dr = cfu.DNSResolver(max_workers=len(hostnames))
        return await dr.resolve_bulk_async(
            hostnames, ignore_exceptions=ignore_exceptions
        )

    async def get_deployment_container(self) -> cfc.Instance:
        """
        Return the deployer container instance, creating it if it
        does not exist.
        """
        pt = cfc.get_runtime(self.runtime)
        try:
            inst = pt.get_instance(self.deployer_name)
            return inst
        except KeyError:
            return await pt.create_instance_async(
                self.deployer_name, self.image, compat=True
            )

    def get_used_nodes(self) -> List[str]:
        """
        Get a list of nodes which have daemons assigned to them in
        the manifest.
        """
        out = list()
        for node, info in self.nodes.items():
            if info.daemons:
                out.append(node)
        return out

    def get_unused_nodes(self) -> List[str]:
        """
        Get a list of nodes which are included in the manifest but do
        not have any daemons assigned to them. This can be from e.g.,
        a Slurm allocation that provides extra nodes for client use.
        """
        out = list()
        for node, info in self.nodes.items():
            if not info.daemons:
                out.append(node)
        return out
