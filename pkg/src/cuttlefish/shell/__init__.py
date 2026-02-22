"""
Copyright (c) 2024-2026 Tim Dettmar

Cuttlefish remote execution module

This module provides utilities to run commands on remote nodes via SSH,
with high parallelism (tested up to 128 nodes) and progress tracking.
"""

import asyncio
import concurrent.futures
import errno
import logging
import os
import subprocess
import time
from typing import Any, List, Optional

import enlighten


class ExecResult:
    """
    Class containing information about a remote command execution.
    """

    node: str
    """
    Node the command was executed on.
    """
    retcode: int
    """
    Return code of the command. Note that this is the return code
    provided by ssh, and may not reflect the return code of the remote
    command itself if e.g., ssh failed to connect.
    """
    stdout: str
    """
    Standard output of the command.
    """
    stderr: str
    """
    Standard error output of the command.
    """
    ref: Any
    """
    User-defined reference object of arbitrary type.
    """

    def __init__(
        self, node: str, retcode: int, stdout: str, stderr: str, ref: Any = None
    ):
        self.node = node
        self.retcode = retcode
        self.stdout = stdout
        self.stderr = stderr
        self.ref = ref


def exec_remote(
    node: str, command: List[str], timeout: Optional[float] = None, ref: Any = None
) -> ExecResult:
    """
    SSH execution function optimized for performance.

    As this function calls the ssh command directly, it will use the SSH
    config set in the user's environment. Therefore this command can be
    sped up further by adding additional options to the SSH config file,
    notably ControlMaster, ControlPath, and ControlPersist.

    :param node: The target node to SSH into.
    :param command: The command to execute as a list of strings.
    :param timeout: An optional timeout in seconds for the command.
    :param ref: An optional reference object to include in the result.
        Useful when gathering results in order of completion rather than
        submission.

    :return: ExecResult
    """
    try:
        logging.debug("Running command on %s: %s", node, " ".join(command))

        ssh_opts = [
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "ServerAliveInterval=5",
            "-o",
            "ServerAliveCountMax=3",
            "-o",
            "BatchMode=yes",
            "-T",
        ]

        cmd = ["ssh"] + ssh_opts + [node] + command

        # Use a reasonable connection timeout if none specified
        # This prevents hung SSH connections from blocking workers indefinitely
        exec_timeout = timeout if timeout is not None else 30.0

        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=exec_timeout,
            preexec_fn=os.setsid,
        )

        if result.returncode != 0:
            logging.error(f"Command failed on {node}, stdout: {result.stdout.strip()}")
            logging.error(f"Command failed on {node}, stderr: {result.stderr.strip()}")
        else:
            logging.debug("Command output on %s: %s", node, result.stdout.strip())
        return ExecResult(node, result.returncode, result.stdout, result.stderr, ref)

    except subprocess.TimeoutExpired:
        logging.error(f"Command timed out on {node}")
        return ExecResult(node, errno.ETIMEDOUT, "", "Command timed out", ref)
    except Exception as e:
        logging.error(f"SSH execution failed on {node}: {e}")
        return ExecResult(node, errno.EIO, "", str(e), ref)


class ParallelRemoteExecutor:
    """
    Class for executing remote commands in parallel over SSH with
    support for progress tracking.

    To prevent overwhelming network infrastructure when connecting to many
    nodes simultaneously, use the 'stagger' parameter to introduce a delay
    between initiating connections.
    """

    def __init__(
        self, nodes: List[str], max_workers: int = 200, stagger: float = 0.0
    ) -> None:
        self.nodes = nodes
        self.max_workers = max_workers
        self.stagger = stagger
        self.tpe = concurrent.futures.ThreadPoolExecutor(
            max_workers=min(len(nodes), max_workers),
            thread_name_prefix=f"cf-executor-{id(self)}",
        )

    async def execute(
        self,
        commands: List[List[str]],
        timeout: float = 86400.0,
        manager: Optional[enlighten.Manager] = None,
        nodelist: Optional[List[str]] = None,
        output: bool = True,
    ) -> List[Optional[ExecResult]]:
        """
        Execute commands in parallel on multiple remote nodes using SSH.

        The level of concurrency depends on the max_workers parameter
        provided at class instantiation.

        :param commands: List of commands to execute. Each list entry is
            treated as a separate command for each node.
        :param timeout: Optional timeout in seconds for completion of
            all commands. The default is 86400 seconds (24 hours). Note
            that since there is connection setup overhead, the expected
            runtime of the commands themselves should be significantly
            lower than this timeout.
        :param manager: Optional enlighten.Manager for progress
            feedback.
        :param nodelist: Optional list of nodes to execute commands on.
            This overrides the nodelist provided at class instantiation.
        """
        deadline = time.time() + timeout
        start = time.time()
        loop = asyncio.get_event_loop()

        # Create futures with optional staggering to prevent connection storms
        futures = []
        queue_pbar = None
        if self.stagger > 0 and manager is not None:
            queue_pbar = manager.counter(
                total=len(self.nodes), desc="Queueing", unit="nodes"
            )

        for i, node in enumerate(self.nodes):
            if self.stagger > 0 and i > 0:
                await asyncio.sleep(self.stagger)
            futures.append(
                loop.run_in_executor(self.tpe, exec_remote, node, commands[i], timeout)
            )
            if queue_pbar is not None:
                queue_pbar.update()

        if queue_pbar is not None:
            queue_pbar.close()

        results = [
            None for _ in range(len(futures))
        ]  # type: List[Optional[ExecResult]]

        pbar = None
        if manager is not None:
            pbar = manager.counter(total=len(futures), desc="Executing", unit="nodes")
            pbar.refresh()

        if nodelist is None:
            nodelist = self.nodes

        for f in asyncio.as_completed(futures, timeout=timeout):
            if pbar is not None:
                pbar.update(1)
            ret = await f
            logging.debug(
                "Completed on node %s in %.2f sec, rc: %d",
                ret.node,
                time.time() - start,
                ret.retcode,
            )
            idx = nodelist.index(ret.node)
            results[idx] = ret

        return results
