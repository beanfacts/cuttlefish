"""
Cuttlefish
Copyright (c) 2024-2026 Tim Dettmar

Node provider interface for Slurm clusters.
"""

import asyncio
import json
import logging
import re
import subprocess
import sys
import time
from typing import Generic, List, Optional, TypeVar, Union

from cuttlefish.cluster import NodeAllocation, NodeAllocationError, NodeProvider

TNodeProvider = TypeVar("TNodeProvider", bound=NodeProvider)


class SlurmJob(NodeAllocation):
    name: str
    nodes: List[str]
    active: bool
    job_id: int

    def release(self) -> None:
        """
        Release the Slurm job allocation via scancel.
        """
        try:
            subprocess.check_output(
                ["scancel", str(self.job_id)], stderr=subprocess.STDOUT
            )
        except subprocess.CalledProcessError as e:
            err = e.stderr.decode()
            logging.debug("scancel failed, output: %s", err)
            raise NodeAllocationError(
                f"Failed to release allocation {self.job_id}: {err}"
            ) from e

    async def run(
        self,
        cmd: List[str],
        nodes: Optional[List[str]] = None,
        ppn: int = 1,
        stream: bool = False,
        timeout: Optional[float] = None,
    ) -> subprocess.CompletedProcess:
        """
        Run a command on the allocated nodes via srun.

        :param cmd: The command to run as a list of arguments.
        :param nodes: Optional list of nodes to run the command on. Must
            be a subset of the nodes in this allocation. By default, the
            command is run on all nodes in the allocation.
        :param ppn: Processes per node. Defaults to 1.
        :param stream: Whether to stream output in real-time.
        :return: A CompletedProcess instance with the command result.
        """
        n = nodes or self.nodes
        if not set(n).issubset(set(self.nodes)):
            raise ValueError(
                f"Specified nodes ({', '.join(n)}) are not a "
                f"subset of the allocated nodes {', '.join(self.nodes)} "
                f"for job {self.job_id}"
            )

        base_cmd = [
            "srun",
            "--mpi=pmix",
            f"--jobid={self.job_id}",
            "-N",
            str(len(n)),
            "-n",
            str(len(n) * ppn),
            "--nodelist",
            ",".join(n),
        ]
        # There is no clean way to stream output from
        # asyncio.create_subprocess_exec currently, so we fall back to
        # subprocess.Popen with polling + sleeping.
        logging.debug("Running command: %s", " ".join([*base_cmd, *cmd]))
        proc = subprocess.Popen(
            [*base_cmd, *cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        stdout = ""
        stderr = ""
        deadline = 1e99
        if timeout is not None:
            deadline = time.time() + timeout

        while timeout is None or time.time() < deadline:
            rc = proc.poll()
            try:
                out, err = proc.communicate(timeout=0.001)
                stdout += out
                stderr += err
                if stream:
                    if out:
                        print(out, end="")
                    if err:
                        print(err, end="", file=sys.stderr)
            except subprocess.TimeoutExpired:
                pass
            if rc is not None:
                return subprocess.CompletedProcess(
                    args=cmd, returncode=rc, stdout=stdout, stderr=stderr
                )
            await asyncio.sleep(0.1)
        raise TimeoutError(
            f"Command '{' '.join(cmd)}' timed out after {timeout} seconds"
        )


class SlurmClusterNodeProvider(NodeProvider, Generic[TNodeProvider]):

    def __init__(self):
        super().__init__()

    @classmethod
    def _process_nodelist(cls, nl: str) -> list[str]:
        in_br = 0
        nodes = list()

        # Get the nodes with unique prefixes
        filt = "[a-z]+[0-9]+"
        n = re.findall(filt, nl)
        if n:
            nodes += n

        # Get all the nodes in square brackets
        filt = r"([a-z]+)(\[[0-9,-]*\])"
        in_br = re.findall(filt, nl)
        for itm in in_br:
            grp, nums = itm  # abc [123,456-789]
            if len(nums) == 0:
                continue
            # Remove the square brackets and split by comma
            ranges = re.sub(r"[\[\]]", "", itm[1]).split(",")
            for rng in ranges:
                if "-" in rng:
                    start_str, end_str = rng.split("-")
                    s, e = int(start_str), int(end_str)
                    # Detect zero-padding in the range
                    width = max(len(start_str), len(end_str))
                    nodes += [f"{itm[0]}{i:0{width}d}" for i in range(s, e + 1)]
                else:
                    nodes.append(f"{itm[0]}{rng}")

        # Remove the entries in square brackets and process the individual results
        return nodes

    @classmethod
    def _get_jobs(cls) -> list[SlurmJob]:
        nl = json.loads(subprocess.check_output(["squeue", "--me", "--json"]))
        out = list()
        for job in nl["jobs"]:
            try:
                out.append(
                    SlurmJob(
                        name=job["name"],
                        job_id=job["job_id"],
                        active=("RUNNING" in job["job_state"]),
                        nodes=cls._process_nodelist(
                            job["job_resources"]["nodes"]["list"]
                        ),
                    )
                )
            except (TypeError, KeyError, ValueError, IndexError):
                continue
        return out

    @classmethod
    def _find_job(cls, job_id: Union[int, str]) -> SlurmJob:
        """
        Find a job by its ID or name. If multiple jobs match, the first
        active one is returned. The name in slurm does not need to be
        unique, so it is recommended to use the job ID where possible.
        """
        jobs = cls._get_jobs()
        for job in jobs:
            if not job.active:
                continue
            if job.job_id == job_id or job.name == job_id:
                return job
        raise ValueError(f"No job with ID or name '{job_id}' found")

    def get_allocation(self, identifier: Union[str, int]) -> SlurmJob:
        return self._find_job(identifier)

    def create_allocation(
        self,
        count: int,
        name: Optional[str] = "cuttlefish",
        node_type: Optional[str] = None,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> SlurmJob:
        """
        Create a Slurm allocation via salloc.

        :param count: The number of nodes to allocate.
        :param name: An optional name for the allocation.
        :param node_type: The partition to use for the allocation.
        :param timeout: The maximum time to wait for an allocation in
            seconds. If None, wait indefinitely.
        :param kwargs: Additional salloc parameters. Currently supported:
            - time_limit: The time limit for the job (str, e.g. "01:00:00")
            - exclusive: Whether to request exclusive node access (bool)
            - qos: The QoS queue to use for the job (str)
            - account: The account to charge for the job (str)
        """
        cmd = ["salloc", f"--job-name={name}", f"--nodes={count}", "--no-shell"]
        if kwargs.get("time_limit"):
            cmd.append(f"--time={kwargs['time_limit']}")
        if kwargs.get("exclusive", False):
            cmd.append("--exclusive")
        if kwargs.get("qos"):
            cmd.append(f"--qos={kwargs['qos']}")
        if kwargs.get("account"):
            cmd.append(f"--account={kwargs['account']}")

        if node_type is not None:
            cmd.append(f"--partition={node_type}")

        try:
            out = subprocess.check_output(
                cmd, timeout=timeout, stderr=subprocess.STDOUT
            )
            data = out.decode("utf-8")
            m = re.search(r"job allocation (\d+)", data)
            if m:
                job_id = int(m.group(1))
                start = time.time()
                # The job sometimes doesn't change to the active state
                # immediately after the allocation is created, so wait
                # 30 seconds for squeue to return what we expect.
                # Otherwise, something failed.
                while time.time() - start < 30:
                    try:
                        return self._find_job(job_id)
                    except ValueError:
                        time.sleep(2)
                raise NodeAllocationError(
                    "Allocation created but job not found after waiting!"
                )

            logging.error("Unrecognized salloc output!")
            logging.error("salloc output: %s", "; ".join(data))
            raise NodeAllocationError(
                "Failed to parse job ID from salloc output; job may "
                "or may not be running!"
            )
        except subprocess.CalledProcessError as e:
            err = e.stderr.decode()
            logging.debug("salloc failed, output: %s", err)
            raise NodeAllocationError(f"Failed to create allocation: {err}") from e


def from_slurm_nodelist(nodelist: str) -> list[str]:
    """
    Utility function to convert a SLURM nodelist string into a list
    of node hostnames.

    :param nodelist: The SLURM nodelist string.
    :return: A list of node hostnames.
    """
    return SlurmClusterNodeProvider._process_nodelist(nodelist)
