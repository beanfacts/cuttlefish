"""
Copyright (c) 2024-2026 Tim Dettmar

Cuttlefish daemon placement module

This module provides the implementation of the daemon placement
algorithm used to assign daemons to nodes based on various constraints
and policies. This is not a complex CSP solver, just a simple load
balancer with some constraints.
"""

import copy
import enum
import logging
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Union, cast

from pydantic import BaseModel

from cuttlefish.schema import DaemonTypeConfig, NodeConfig, PlacementPolicy
from cuttlefish.util import get_daemon_type

# There are four placement policies that can be implemented:
# Strict spread: Daemons must be on different nodes, error if not possible
#                PlacementPolicy.SPREAD + density = 1
# Prefer spread: Daemons should be on different nodes, but can be on the same node
#                if no other nodes are available, up to the density limit
#                PlacementPolicy.SPREAD + density > 1
# Prefer group:  Daemons should be on the same node, but can be on different nodes
#                if the density limit is reached
#                PlacementPolicy.GROUP + density > 1
# Strict group:  Daemons must be on the same node, error if not possible
#                PlacementPolicy.GROUP + density = number of daemons of type

# Nodelist ["node1", "node2", "node3", etc.]
# Daemons ["mon", "mgr", etc.]


class Node(BaseModel):
    name: str
    conf: NodeConfig = NodeConfig(daemon_limit=-1)
    current: Dict[str, List[Union[int, str]]] = defaultdict(list)

    def count(self, dtype: Optional[str] = None) -> int:
        """
        Count the number of daemons of a given type on this node.

        :param dtype: The daemon type, e.g. "mon", "osd", etc. If not specified,
            returns the total number of daemons on this node.

        :return: int
        """
        if dtype is None:
            return sum(len(ids) for ids in self.current.values())
        return len(self.current.get(dtype, []))

    def export_daemons(self) -> Iterable[str]:
        """
        Export the list of daemons on this node.
        """
        for i, (dtype, ids) in enumerate(self.current.items()):
            for daemon_id in ids:
                yield f"{dtype}.{daemon_id}"

    def place(self, daemon: str, test: bool = False) -> bool:
        """
        Place a daemon on the node.
        This function only checks the node-specific constraints.

        :param daemon: The daemon to place, e.g. "mon.0", "osd.1", etc.
        :param test: Only test the placement, do not place it on the node.

        :return: Whether the daemon can be placed on the node
        """
        dtype = get_daemon_type(daemon)
        if dtype == "mon":
            if len(self.current.get(dtype, [])) >= 1:
                return False
            elif not test:
                logging.debug("Placing %s on %s", daemon, self.name)
                self.current[dtype] = [self.name]
            return True

        if (
            self.conf.daemon_limit > 0
            and len(self.current[dtype]) >= self.conf.daemon_limit
        ):
            return False
        else:
            logging.debug("Placing %s on %s", daemon, self.name)
            if not test:
                if dtype == "mds":
                    self.current[dtype].append(f"x{daemon.split('.')[1]}")
                else:
                    self.current[dtype].append(int(daemon.split(".")[1]))
            return True


def place_daemons(
    node_list: List[str],
    daemon_conf: List[DaemonTypeConfig],
    node_conf: Optional[NodeConfig] = None,
    allow_empty: bool = False,
) -> Dict[str, Node]:
    """
    Place daemons on nodes according to the provided constraints.

    Placement is evaluated in the order of daemon types:

    highest | mon, mgr, mds, osd, rgw, client | lowest priority

    The parameters can/must_overlap are only evaluated from
    lower->higher priority daemon types, i.e.:

    - setting mon.can_overlap -> mgr will not work
    - mgr.can_overlap -> mon will work

    For each daemon type, the placement policy can be either group or
    spread.

    - Grouping tries to place as many daemons of the same type on the
      same node as possible, until either the density limit or node
      limit is reached.

    - Spreading tries to place daemons of the same type on different
      nodes. If there are no more available empty nodes, it will wrap
      around to the least used node and place the daemon there.

    :param node_list: List of node names for placement.
    :param daemon_conf: List of DaemonTypeConfig objects defining the
        daemon types to place and their constraints.
    :param node_conf: Optional NodeConfig object defining node-wide
        constraints. If not provided, no node-wide constraints are
        applied.
    :param allow_empty: If True, allows placement with zero daemons.
        If False, raises an error if no daemons are to be placed.
    """
    DAEMON_PRIORITY = ("mon", "mgr", "mds", "osd", "rgw")

    dconfs = {x.name: x for x in daemon_conf}

    if node_conf is None:
        node_conf = NodeConfig(daemon_limit=-1)

    nodes = {node: Node(name=node, conf=node_conf) for node in node_list}

    # Get the actually used daemon types
    daemon_types = {d.name for d in daemon_conf}

    if not dconfs:
        if allow_empty:
            return nodes
        raise ValueError("No daemon configurations provided")

    # Process daemon types in specified order
    for dtype in DAEMON_PRIORITY:

        logging.debug("Processing daemon type %s", dtype)

        # Ignore if caller didn't specify this daemon type
        if dtype not in daemon_types:
            logging.debug("Daemon type %s not in daemons list", dtype)
            continue

        if dtype not in dconfs:
            raise ValueError(f"Daemon type {dtype} not found in daemon_conf")

        if dconfs[dtype].count <= 0:
            logging.debug("Daemon type %s has no daemons to place", dtype)
            continue

        logging.debug("Placing %d %s daemons", dconfs[dtype].count, dtype)

        for d_id in range(dconfs[dtype].count):

            # Check placement satisfies the constraints
            d_name = f"{dtype}.{d_id}"
            candidates = list()  # type: List[Node]
            for node in nodes.values():
                if node.count(dtype) >= dconfs[dtype].density:
                    logging.debug(
                        "Node %s already has %d daemons of type %s, " "skipping",
                        node.name,
                        node.count(dtype),
                        dtype,
                    )
                    continue
                if not node.place(f"{dtype}.{d_id}", test=True):
                    logging.debug(
                        "Node %s cannot place %s, skipping", node.name, d_name
                    )
                    continue
                bad = False
                for dt in dconfs[dtype].disallow_overlap or []:
                    count = node.count(dt)
                    if count:
                        logging.debug(
                            "Node %s has %d daemons of type %s, "
                            "disallowing placement of %s",
                            node.name,
                            node.count(dt),
                            dt,
                            d_name,
                        )
                        bad = True
                for dt in dconfs[dtype].require_overlap or []:
                    if not node.count(dt):
                        logging.debug(
                            "Node %s has no daemons of type %s, "
                            "disallowing placement of %s",
                            node.name,
                            dt,
                            d_name,
                        )
                        bad = True
                if not bad:
                    candidates.append(node)

            if not len(candidates):
                raise ValueError(
                    f"Could not find a suitable node for {dtype}.{d_id} "
                    f"with the current configuration. Please check the "
                    "daemon configuration and node limits."
                )

            # Choose the best candidate based on the placement policy
            logging.debug("Candidates for %s: %s", d_name, str(candidates))
            sorted_peers = sorted(candidates, key=lambda n: n.count(dtype))
            for p in sorted_peers:
                logging.debug(
                    "Node %s has %d daemons of type %s, total: %d",
                    p.name,
                    p.count(dtype),
                    dtype,
                    p.count(),
                )

            # Spread: node with the least number of daemons of this type
            # Group:  ditto, but most
            if dconfs[dtype].placement == PlacementPolicy.SPREAD:
                best = sorted_peers[0]
            elif dconfs[dtype].placement == PlacementPolicy.GROUP:
                best = sorted_peers[-1]
            else:
                raise ValueError(
                    f"Unknown placement policy "
                    f"{dconfs[dtype].placement} for daemon type {dtype}"
                )

            # Tie-breaking: if there are multiple nodes with the same
            # daemon type count, select the one with the fewest total
            # daemons
            dtype_count = best.count(dtype)
            tied_nodes = [n for n in sorted_peers if n.count(dtype) == dtype_count]
            if len(tied_nodes) > 1:
                best = min(tied_nodes, key=lambda n: n.count())

            best.place(d_name)
            logging.debug("Placed %s on %s", d_name, best.name)

    return nodes


ALL_DAEMONS = ("mon", "mgr", "mds", "osd", "rgw")


def all_daemons_except(d: str) -> List[str]:
    return [daemon for daemon in ALL_DAEMONS if daemon != d]
