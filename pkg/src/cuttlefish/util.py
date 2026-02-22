import asyncio
import concurrent.futures
import configparser
import logging
import pathlib
import pickle
import socket
import subprocess
import sys
from typing import (
    Dict,
    Iterable,
    Optional,
)


class DNSResolver:

    def _resolve(self, hostname: str):
        try:
            gai = socket.getaddrinfo(hostname, 3300, proto=socket.IPPROTO_TCP)
            out = list()
            for result in gai:
                out.append(result[4][0])
            self.results[hostname] = out
        except Exception as e:
            self.results[hostname] = e

    def __init__(self, timeout: float = 3.0, max_workers: int = 10):
        """
        Class to resolve DNS names to IP addresses. Internally this calls the
        socket.gethostbyname() function. As the function does not have the
        ability to set a timeout, this class simulates the timeout by running
        the function in a thread and waiting for the result.
        """
        self.timeout = timeout
        self.resolvers = dict()
        self.results = dict()
        self.tpe = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix=f"cf-dnsresolver-{id(self)}"
        )

    def resolve(
        self, hostname: str, timeout: Optional[float] = None, bypass_cache: bool = False
    ) -> list:
        """
        Resolve a hostname to a list of IP addresses.
        """
        if hostname in self.results:
            return self.results[hostname]

    @classmethod
    async def resolve_async(cls, hostname: str) -> str:
        """
        Resolve a hostname to an IP address asynchronously.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, socket.gethostbyname, hostname)

    async def resolve_bulk_async(
        self, hostnames: Iterable[str], ignore_exceptions: bool = False
    ) -> Dict[str, Optional[str]]:
        """
        Resolve multiple hostnames to IP addresses asynchronously.

        :param hostnames: Iterable of hostnames to resolve.
        :param ignore_exceptions: If True, exceptions during resolution
            will be ignored and the corresponding hostname will have a
            value of None in the returned dictionary. If False,
            exceptions will be raised.

        :return: Dictionary mapping hostnames to their resolved IP
            addresses. If ignore_exceptions is True, hostnames that
            could not be resolved will have a value of None.
        """
        loop = asyncio.get_event_loop()
        tasks = {
            hostname: loop.run_in_executor(self.tpe, socket.gethostbyname, hostname)
            for hostname in hostnames
        }
        results = dict()
        for hostname, task in tasks.items():
            results[hostname] = None
            try:
                ip = await task
                results[hostname] = ip
            except Exception as e:
                if not ignore_exceptions:
                    raise e
                results[hostname] = None
        return results


def check_commands_exist(tools: list):
    for tool in tools:
        try:
            logging.debug("Checking for command %s", tool)
            sys.stdout.flush()
            subprocess.check_output([tool, "--help"], stderr=subprocess.PIPE)
            logging.debug("Command %s found", tool)
        except subprocess.CalledProcessError as e:
            if e.returncode != 1:
                logging.debug("Running %s failed, cannot continue: %s", tool, str(e))
                return False
            logging.debug("Command %s found", tool)
        except FileNotFoundError:
            logging.debug("Could not find %s, cannot continue", tool)
            return False
    return True


def conv_to_ini(json_conf: dict):
    out = ""
    for sect in json_conf.keys():
        out += f"[{sect}]\n"
        for k, v in json_conf[sect].items():
            if isinstance(v, bool):
                v = str(v).lower()
            out += f"    {k} = {v}\n"
        out += "\n"
    return out


# Deepcopy ConfigParser object
# https://stackoverflow.com/questions/23416370/manually-building-a-deep-copy-of-a-configparser-in-python-2-7
def copy_config(conf: configparser.ConfigParser) -> configparser.ConfigParser:
    return pickle.loads(pickle.dumps(conf))


def get_daemon_type(daemon: str) -> str:
    return daemon.split(".")[0]


def get_daemon_data_args(daemon: str, base_path: pathlib.PurePath) -> list[str]:
    dt = get_daemon_type(daemon)
    args = [
        f"--{dt}-data",
        str(base_path / daemon / "data"),
        "--conf",
        str(base_path / daemon / "ceph.conf"),
        "--keyring",
        str(base_path / daemon / "keyring"),
    ]
    if dt == "mon":
        args += ["--monmap", str(base_path / "shared" / "monmap")]
    elif dt == "osd":
        args += ["--osd-journal", str(base_path / daemon / "journal")]
    return args
