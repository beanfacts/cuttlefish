"""
Copyright (c) 2024-2026 Tim Dettmar

Cuttlefish container abstraction module

This module is designed to provide a generic abstraction for container
runtimes. In practice, only Apptainer is currently supported, and
because this was only for my thesis this probably won't be extended.
"""

import enum
import pathlib
import subprocess
from abc import ABC, abstractmethod
from typing import Generic, List, Optional, TypeVar, Union

from pydantic import BaseModel


class Mount:
    """
    A generic abstraction for a container mount point.
    """

    def __init__(
        self, source: Union[str, pathlib.Path], target: str, readonly: bool = False
    ):
        self.source = str(source)
        self.target = target
        self.readonly = readonly

    def __repr__(self):
        return f"Mount(source={self.source}, target={self.target}, readonly={self.readonly})"


class ContainerImageType(str, enum.Enum):
    GENERIC = "generic"
    DOCKER = "docker"
    DOCKER_ARCHIVE = "docker-archive"
    OCI_ARCHIVE = "oci-archive"
    SIF = "sif"


class ContainerRuntimeType(str, enum.Enum):
    APPTAINER = "apptainer"
    DOCKER = "docker"
    PODMAN = "podman"


class ContainerImage(BaseModel):
    """
    The type of image must be specified when referring to container images, as
    different runtimes (especially Apptainer) have special handling of
    non-native image formats. An error will also be raised if the
    specified image type is not supported by the runtime in use.
    """

    name: str
    type: ContainerImageType = ContainerImageType.GENERIC


class InstanceInfo(BaseModel):
    """
    Instance information.
    """

    name: str
    """
    The name of the container instance. Provided by all runtimes.
    """
    pid: Optional[int]
    """
    PID of the container process, if available. Usually this is the PID
    of the init process inside the container.
    """
    img: str
    """
    Name of the image used to launch the container.
    """
    ip: Optional[str]
    """
    IP address assigned to the container, if network isolation is used.
    """


TInstanceInfo = TypeVar("TInstanceInfo", bound=InstanceInfo)


class Instance(ABC, Generic[TInstanceInfo]):
    info: TInstanceInfo
    """
    Information about this container instance.
    """

    def get_stdout(self) -> bytes:
        """
        Get the standard output of the container instance.

        :raises NotImplementedError: If the method is not implemented
            by the subclass.
        :return: Standard output as bytes.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__}.get_stdout not implemented"
        )

    def get_stderr(self) -> bytes:
        """
        Get the standard error output of the container instance.

        :raises NotImplementedError: If the method is not implemented
            by the subclass.
        :return: Standard error output as bytes.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__}.get_stderr not implemented"
        )

    async def exec(
        self, command: List[str], timeout: Optional[float] = None
    ) -> subprocess.CompletedProcess:
        """
        Execute a command inside the container instance using asyncio.

        :param command: The command to execute as a list of strings.
        :param timeout: An optional timeout in seconds for the command.

        :return: subprocess.CompletedProcess

        :raises NotImplementedError: If the container runtime does not
            implement this method.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__}.exec_async not implemented"
        )

    async def stop(
        self, signal: str = "SIGTERM", timeout: Optional[float] = None
    ) -> None:
        """
        Stop the container instance asynchronously.

        :param signal: The signal to send to the container process.
        :param timeout: An optional timeout in seconds to wait for the
            container to stop.

        :raises NotImplementedError: If the container runtime does not
            implement this method.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__}.stop_async not implemented"
        )


class ContainerRuntime(ABC):
    """
    Abstract base class for container runtimes.

    This class provides a very basic abstraction for running containers
    for the Cuttlefish orchestrator.
    """

    @abstractmethod
    def available(cls) -> bool:
        """
        Check if the container runtime is available.
        """

    @abstractmethod
    def list_instances(self) -> List[InstanceInfo]:
        """
        List all running container instances.
        """

    @abstractmethod
    async def list_instances_async(self) -> List[InstanceInfo]:
        """
        List all running container instances (async).
        """

    @abstractmethod
    def get_instance(self, name: str) -> Instance:
        """
        Get a container instance by name.

        .. note::

            A new instance will not be created if the specified instance
            does not exist.

        :param name: The name of the container instance. This refers to
            the instance name and not the image name.

        """

    @abstractmethod
    def create_instance(
        self,
        name: str,
        image: ContainerImage,
        mounts: Optional[List[Mount]] = None,
        compat: bool = False,
        args: List[str] = [],
    ) -> Instance:
        """
        Create a new container instance.

        :param name: The name of the container instance.
        :param image: The container image to use.
        :param mounts: Optional list of mount points for the container.
        :param compat: Whether to enable OCI compatibility mode for the
            container runtime.
        :param args: Arguments to pass to the container instance
            entrypoint.

        :return: The created container instance.
        """

    @abstractmethod
    async def create_instance_async(
        self,
        name: str,
        image: ContainerImage,
        mounts: Optional[List[Mount]] = None,
        compat: bool = False,
        args: List[str] = [],
    ) -> Instance:
        """
        Create a new container instance (async).

        :param name: The name of the container instance.
        :param image: The container image to use.
        :param mounts: Optional list of mount points for the container.
        :param compat: Whether to enable OCI compatibility mode for the
            container runtime.
        :param args: Arguments to pass to the container instance
            entrypoint.

        :return: The created container instance.
        """


def get_runtime(type: ContainerRuntimeType) -> ContainerRuntime:
    """
    Get a container runtime instance based on the specified type.

    :param type: The type of container runtime to get.

    :raises ValueError: If the specified container runtime type is
        not supported.

    :return: An instance of the specified container runtime.
    """
    if type == ContainerRuntimeType.APPTAINER:
        from cuttlefish.container.apptainer import ApptainerRuntime

        if ApptainerRuntime.available():
            return ApptainerRuntime()
        raise RuntimeError("Apptainer runtime not available")
    # elif type == ContainerRuntimeType.DOCKER:
    #     from cuttlefish.container.docker import DockerRuntime
    #     return DockerRuntime()
    # elif type == ContainerRuntimeType.PODMAN:
    #     from cuttlefish.container.podman import PodmanRuntime
    #     return PodmanRuntime()
    else:
        raise ValueError(f"Unsupported container runtime type: {type}")
