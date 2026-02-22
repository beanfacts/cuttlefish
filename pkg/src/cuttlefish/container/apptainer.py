"""
Copyright (c) 2024-2026 Tim Dettmar

Cuttlefish Apptainer container runtime module

This module provides an implementation of the ContainerRuntime
abstraction for the Apptainer container runtime.
"""

import asyncio
import itertools
import json
import logging
import pathlib
import shlex
import subprocess
import time
from contextlib import contextmanager
from typing import Dict, List, Optional, Union, cast

from pydantic import BaseModel, Field

import cuttlefish.container as cfc


class InternalError(Exception):
    """Custom exception for internal errors in this module."""

    pass


class ApptainerError(Exception):
    """Custom exception for errors related to Apptainer operations."""

    pass


class ApptainerInstanceInfo(cfc.InstanceInfo):
    name: str = Field(alias="instance")
    pid: Optional[int]
    img: str
    ip: Optional[str]
    logErrPath: Optional[str]
    logOutPath: Optional[str]


class ApptainerInstance(cfc.Instance[ApptainerInstanceInfo]):
    info: ApptainerInstanceInfo

    def __init__(self, info: ApptainerInstanceInfo):
        self.info = info

    async def _run_async(
        self, command: List[str], timeout: Optional[float] = None
    ) -> subprocess.CompletedProcess:
        """
        Executes a command using asyncio and transforms the output into
        a subprocess.CompletedProcess object.
        """
        logging.debug("Executing command in Apptainer instance: %s", " ".join(command))
        process = await asyncio.create_subprocess_exec(
            *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        ret = await process.wait()
        stdout = None
        stderr = None
        if process.stdout is not None:
            stdout = (await process.stdout.read()).decode("utf-8")
        if process.stderr is not None:
            stderr = (await process.stderr.read()).decode("utf-8")
        return subprocess.CompletedProcess(
            args=command, returncode=ret, stdout=stdout, stderr=stderr
        )

    async def exec(
        self, command: List[str], timeout: Optional[float] = None
    ) -> subprocess.CompletedProcess:
        """
        Execute a command in the instance asynchronously.
        """
        if not self.info:
            raise InternalError("Instance info unavailable")

        cmd = [
            "apptainer",
            "--silent",
            "exec",
            f"instance://{self.info.name}",
        ] + command
        return await self._run_async(cmd, timeout)

    async def stop(
        self, signal: str = "SIGTERM", timeout: Optional[float] = None
    ) -> None:
        """Stop the instance."""
        await self._run_async(
            ["apptainer", "--silent", "instance", "stop", "-s", signal, self.info.name],
            timeout,
        )

    def discard(self, sig: str = "SIGTERM") -> None:
        """Stop the instance. If it does not exist, do nothing."""
        try:
            self.stop(sig)
        except ApptainerError as e:
            logging.debug("Instance discard not completed: %s", e)
            pass

    async def discard_async(self, sig: str = "SIGTERM") -> None:
        """Stop the instance. If it does not exist, do nothing."""
        try:
            await self.stop_async(sig)
        except ApptainerError as e:
            logging.debug("Instance discard not completed: %s", e)
            pass


class ApptainerRuntime(cfc.ContainerRuntime):

    @classmethod
    def _convert_mount(cls, mount: cfc.Mount) -> List[str]:
        """
        Converts the abstract Mount object to Apptainer CLI arguments.
        """
        readonly_flag = ",ro" if mount.readonly else ""
        return [
            "--mount",
            f"type=bind,src={mount.source},dst={mount.target}{readonly_flag}",
        ]

    @classmethod
    def _get_instance_cmd(
        cls,
        name: str,
        image: str,
        mounts: Optional[List[cfc.Mount]] = None,
        compat: bool = False,
        using: str = "start",
        args: List[str] = [],
    ) -> List[str]:
        """
        Construct the command to create a new Apptainer instance.

        :param name: Name of the instance to be created.
        :param image: Path to the image to be used for the instance. This is
            passed directly to Apptainer; you can use
            DockerImage/DockerArchive/OCIArchive to import foreign images or
            simply use the Apptainer syntax directly.
        :param mounts: Optional list of mount points.
        :param compat: If True, use the --compat flag to enable the OCI
            container compatibility mode.
        :param using: The command used to start the instance, either "start"
            or "run" to execute the startscript/runscript respectively. Note that
            "run" is not available in older Apptainer versions.
        :param args: Additional arguments to pass to the instance command.
        :return: A list of command line arguments to be used with subprocess.

        """
        mount_args = list(
            itertools.chain.from_iterable(
                (cls._convert_mount(mount) for mount in mounts) if mounts else []
            )
        )
        cmd = (
            [
                "apptainer",
                "--silent",
                "instance",
                using,
            ]
            + (["--compat", "--fakeroot", "--containall"] if compat else [])
            + mount_args
            + [image, name]
            + args
        )
        logging.debug(f"Constructed instance command: {' '.join(cmd)}")
        return cmd

    @classmethod
    def _convert_ct(cls, ct: cfc.ContainerImage) -> str:
        """
        Allows non-native image formats to be used with Apptainer by
        signaling the format in the image name.
        """
        if ct.type == cfc.ContainerImageType.DOCKER:
            return f"docker://{ct.name}"
        elif ct.type == cfc.ContainerImageType.DOCKER_ARCHIVE:
            return f"docker-archive:{ct.name}"
        elif ct.type == cfc.ContainerImageType.OCI_ARCHIVE:
            return f"oci-archive:{ct.name}"
        elif ct.type == cfc.ContainerImageType.SIF:
            return ct.name
        else:
            raise ApptainerError(f"Unsupported container locator type: {type(ct)}")

    def _new_instance(
        self,
        name: str,
        image: cfc.ContainerImage,
        mounts: Optional[List[cfc.Mount]] = None,
        compat: bool = False,
        using: str = "run",
        args: List[str] = [],
        timeout: float = 5.0,
    ) -> ApptainerInstance:
        """
        Create a new Apptainer instance.

        :param name: Name of the instance to be created.
        :param image: The container image to use.
        :param mounts: Optional list of mount points.
        :param compat: Use the --compat flag to enable Apptainer's OCI
            compatibility mode.
        :param using: The entrypoint to use when starting the instance,
            either "start" or "run". Note the following idiosyncrasies:

            - Using "run" with instances is only supported in Apptainer
              1.2.0 or later.
            - Images converted from Docker/OCI images often have an
              empty startscript, necessitating the use of "run".
              Therefore, if using Apptainer 1.2.0 or earlier with
              Docker/OCI containers, it may be necessary to create a
              shim build script that copies the runscript contents to
              the startscript, then use "start" on the converted SIF
              image - not the original!
            - Apptainer instances may still be created even if the
              startscript/runscript is empty, but they will not have any
              default behaviour. Executing commands in such instances
              should still work.

        :param args: The arguments to pass to the startscript/runscript.
        :param timeout: Time in seconds to wait for the instance to
            start after creation.
        """
        start = time.time()
        try:
            subprocess.check_call(
                self._get_instance_cmd(
                    name, self._convert_ct(image), mounts, compat, using, args
                )
            )
        except subprocess.CalledProcessError as e:
            raise ApptainerError(f"Failed to create instance '{name}': {e}")

        while time.time() - start < timeout:
            try:
                return self.get_instance(name)
            except KeyError:
                time.sleep(0.1)

        raise ApptainerError(f"Instance '{name}' failed to start after creation")

    async def _new_instance_async(
        self,
        name: str,
        image: cfc.ContainerImage,
        mounts: Optional[List[cfc.Mount]] = None,
        compat: bool = False,
        using: str = "start",
        args: List[str] = [],
        timeout: float = 5.0,
    ) -> ApptainerInstance:
        """Create a new Apptainer instance."""
        try:
            cmd = self._get_instance_cmd(
                name, self._convert_ct(image), mounts, compat, using, args
            )
            logging.debug("Creating Apptainer instance with cmd: %s", " ".join(cmd))
            process = await asyncio.create_subprocess_exec(*cmd)
            await process.wait()
            if process.returncode != 0:
                logging.error(
                    "Apptainer instance creation failed with rc=%d, "
                    "stdout: %s; stderr: %s",
                    process.returncode,
                    (
                        (await process.stdout.read()).decode("utf-8")
                        if process.stdout
                        else ""
                    ),
                    (
                        (await process.stderr.read()).decode("utf-8")
                        if process.stderr
                        else ""
                    ),
                )
                raise ApptainerError(
                    f"Failed to create instance '{name}': {process.returncode}"
                )
            return await self.get_instance_async(name)
        except Exception as e:
            raise ApptainerError(f"Failed to create instance '{name}': {e}")

    @classmethod
    def available(cls) -> bool:
        """Check if Apptainer is available on this host."""
        try:
            subprocess.check_output(["apptainer", "--version"], stderr=subprocess.PIPE)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False

    def list_instances(self) -> List[cfc.InstanceInfo]:
        """List all running Apptainer instances on this host."""
        try:
            result = subprocess.check_output(
                ["apptainer", "instance", "list", "--json"], text=True
            )
            instances = json.loads(result)
            return [ApptainerInstanceInfo(**inst) for inst in instances["instances"]]
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to list instances: {e}")
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse JSON output: {e}")

    async def list_instances_async(self) -> List[cfc.InstanceInfo]:
        """List all running Apptainer instances on this host (async)."""
        try:
            process = await asyncio.create_subprocess_exec(
                "apptainer",
                "instance",
                "list",
                "--json",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            if process.returncode != 0:
                raise RuntimeError(f"Failed to list instances: {stderr.decode()}")

            instances = json.loads(stdout.decode())
            out = list()
            for inst in instances["instances"]:
                out.append(ApptainerInstanceInfo(**inst))
            return out
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse JSON output: {e}")

    def get_instance(self, name: str) -> ApptainerInstance:
        """Get an Apptainer instance by name."""
        instances = self.list_instances()
        for inst in instances:
            if inst.name == name:
                info = cast(ApptainerInstanceInfo, inst)
                return ApptainerInstance(info)
        raise KeyError(f"Instance '{name}' not found")

    async def get_instance_async(self, name: str) -> ApptainerInstance:
        """Get an Apptainer instance by name (async)."""
        instances = await self.list_instances_async()
        for inst in instances:
            if inst.name == name:
                info = cast(ApptainerInstanceInfo, inst)
                return ApptainerInstance(info)
        raise KeyError(f"Instance '{name}' not found")

    def create_instance(
        self,
        name: str,
        image: cfc.ContainerImage,
        mounts: Optional[List[cfc.Mount]] = None,
        compat: bool = False,
        args: List[str] = [],
    ) -> cfc.Instance:
        """
        Create a new Apptainer instance.
        See ContainerRuntime.create_instance for details.
        """
        return self._new_instance(name, image, mounts, compat, "run", args)

    async def create_instance_async(
        self,
        name: str,
        image: cfc.ContainerImage,
        mounts: Optional[List[cfc.Mount]] = None,
        compat: bool = False,
        args: List[str] = [],
    ) -> cfc.Instance:
        """
        Create a new Apptainer instance (async).
        See ContainerRuntime.create_instance_async for details.
        """
        return await self._new_instance_async(name, image, mounts, compat, "run", args)
