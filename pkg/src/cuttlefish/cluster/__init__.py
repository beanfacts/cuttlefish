"""
Copyright (c) 2024-2026 Tim Dettmar

Cuttlefish cluster node provider module

This module defines an abstraction for cluster node providers, allowing
the orchestrator to allocate and manage nodes from different backends
like Slurm.
"""

import enum
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, TypeVar, Union

from pydantic import BaseModel


class NodeAllocationTimeoutError(TimeoutError):
    """
    Raised when a node allocation request times out.
    """

    pass


class NodeAllocationError(ResourceWarning):
    """
    Raised when an attempt to allocate nodes fails. If the allocation
    times out, NodeAllocationTimeoutError is raised instead.
    """


class NodeProviderType(enum.Enum):
    SLURM = "slurm"


class NodeAllocation(BaseModel, ABC):
    name: str
    nodes: List[str]
    active: bool

    @abstractmethod
    def release(self) -> None:
        """
        Release the node allocation. If the provider doesn't need to
        release resources, this function must still be implemented but
        can be a no-op.
        """
        pass

    @abstractmethod
    async def run(
        self,
        cmd: List[str],
        nodes: Optional[List[str]] = None,
        ppn: int = 1,
        stream: bool = False,
        timeout: Optional[float] = None,
    ) -> subprocess.CompletedProcess:
        """
        Run a command on the allocated nodes.
        """
        pass


class NodeProvider(ABC):
    """
    This interface defines method for getting and manipulating lists of
    cluster nodes for use in the orchestrator. For instance, the slurm
    provider abstracts the slurm CLI.
    """

    @abstractmethod
    def get_allocation(self, identifier: Union[str, int]) -> NodeAllocation:
        """
        Get an existing node allocation.

        :param identifier: The name or ID of the allocation.
        """
        pass

    @abstractmethod
    def create_allocation(
        self,
        count: int,
        name: Optional[str] = None,
        node_type: Optional[str] = None,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> NodeAllocation:
        """
        Create a new node allocation.

        :param count: The number of nodes to allocate.
        :param name: An optional name for the allocation. If not
            specified, a default name will be generated to identify the
            allocation.
        :param node_type: The type of nodes to allocate. How this
            parameter is used depends on the provider. For Slurm, this
            is used to specify a partition.
        :param timeout: The maximum time to wait for an allocation in
            seconds. If None, wait indefinitely. Warning: you must
            ensure that the allocation e.g. uses the correct partition,
            as this may lead to indefinite waits.
        :param kwargs: Additional provider-specific parameters.
        """
        pass


TNodeProvider = TypeVar("TNodeProvider", bound=NodeProvider)


def get_provider(prov_type: NodeProviderType) -> NodeProvider:
    if prov_type == NodeProviderType.SLURM:
        from cuttlefish.cluster.slurm import SlurmClusterNodeProvider

        return SlurmClusterNodeProvider()
    else:
        raise ValueError(f"Unsupported provider type: {prov_type}")
