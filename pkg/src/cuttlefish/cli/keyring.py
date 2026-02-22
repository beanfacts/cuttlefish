"""
Cuttlefish
Copyright (c) 2024-2026 Tim Dettmar

Keyring generator.
"""

import argparse
import asyncio
import configparser
import logging
import sys
from typing import Dict, Optional

DESCRIPTION = "Generate keyrings for Ceph daemons"


def register(subparsers: argparse._SubParsersAction) -> None:
    """Attach the keyring subcommand to the CLI."""
    parser = subparsers.add_parser(
        "keyring",
        help=DESCRIPTION,
        description=DESCRIPTION,
    )
    setup_parser(parser)
    parser.set_defaults(func=run)


async def _gen_ceph_key() -> str:
    """Generate a single Ceph authentication key via ceph-authtool."""
    proc = await asyncio.create_subprocess_exec(
        "ceph-authtool",
        "--gen-print-key",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"Failed to generate print key: {stderr.decode().strip()}")
    return stdout.decode().strip()


async def gen_ceph_keys(num: int) -> list[str]:
    """Generate *num* keys in parallel."""
    tasks = [_gen_ceph_key() for _ in range(num)]
    return await asyncio.gather(*tasks)


def _add_or_replace_section(
    config: configparser.ConfigParser,
    section: str,
    data: Dict[str, str],
    rekey: bool = False,
) -> None:
    """Insert a keyring section, optionally replacing an existing key."""
    if not config.has_section(section):
        config.add_section(section)
    for key, value in data.items():
        if key == "key" and config.has_option(section, "key") and not rekey:
            continue
        config.set(section, key, value)


async def gen_master_keyring(
    path: str,
    daemons: Dict[str, int],
    clients: Optional[Dict[str, Dict[str, str]]] = None,
    rekey: bool = False,
) -> None:
    """
    Generate the master keyring containing all daemon keys.

    :param path:    Path to the keyring file.
    :param daemons: Dictionary of daemon types and counts.
    :param clients: Dictionary of client names and permissions.
    :param rekey:   If True, overwrite existing keys.
    """
    total_keys = sum(daemons.values()) + 3  # +3 for mon, client.admin, mgr.0

    keys = await gen_ceph_keys(total_keys)
    config = configparser.ConfigParser()
    try:
        with open(path, "r") as f:
            config.read_file(f)
    except FileNotFoundError:
        pass

    # Capability templates per daemon type
    perms: Dict[str, Dict[str, str]] = {
        "osd": {
            "caps mon": "allow profile osd",
            "caps mgr": "allow profile osd",
            "caps osd": "allow *",
        },
        "mgr": {
            "caps mon": "allow profile mgr",
            "caps mds": "allow *",
            "caps osd": "allow *",
        },
        "mds": {
            "caps mon": "allow profile mds",
            "caps mgr": "allow profile mds",
            "caps mds": "allow *",
            "caps osd": "allow rwx",
        },
        "rgw": {
            "caps mon": "allow rwx",
            "caps osd": "allow rwx",
        },
    }

    # Always-present sections: mon., client.admin, mgr.0
    _add_or_replace_section(
        config, "mon.", {"key": keys[-1], "caps mon": "allow *"}, rekey
    )
    _add_or_replace_section(
        config,
        "client.admin",
        {
            "key": keys[-2],
            "caps mon": "allow *",
            "caps mgr": "allow *",
            "caps osd": "allow *",
            "caps mds": "allow *",
        },
        rekey,
    )
    _add_or_replace_section(config, "mgr.0", {"key": keys[-3], **perms["mgr"]}, rekey)

    key_index = 0
    for dtype, count in daemons.items():
        if count <= 0:
            continue

        if dtype not in ("osd", "mgr", "mds", "rgw"):
            raise ValueError(
                f"Unsupported daemon type: {dtype}. "
                "If creating clients, use the clients parameter."
            )

        for j in range(count):
            if dtype == "mds":
                dname = f"{dtype}.x{j}"
            elif dtype == "rgw":
                dname = f"client.radosgw.{j}"
            else:
                dname = f"{dtype}.{j}"
            _add_or_replace_section(
                config, dname, {"key": keys[key_index], **perms[dtype]}, rekey
            )
            key_index += 1

    with open(path, "w") as f:
        config.write(f)

    logging.info("Written keyring with %d sections to %s", len(config.sections()), path)


def setup_parser(parser: argparse.ArgumentParser) -> None:
    """Setup arguments for the keyring command."""
    parser.add_argument(
        "-o",
        "--osd",
        type=int,
        required=True,
        help="Number of OSD keys to create",
    )
    parser.add_argument(
        "-d",
        "--mds",
        type=int,
        default=0,
        help="Number of MDS keys to create",
    )
    parser.add_argument(
        "-a",
        "--mgr",
        type=int,
        default=1,
        help="Number of manager keys to create",
    )
    parser.add_argument(
        "-g",
        "--rgw",
        type=int,
        default=0,
        help="Number of RADOSGW keys to create",
    )
    parser.add_argument(
        "-r",
        "--remake",
        action="store_true",
        help="Overwrite existing keys if they exist",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "-f",
        "--file",
        default="keyrings",
        help="File to write the generated keyring to",
    )


def run(args: argparse.Namespace) -> None:
    """Entry point called by the CLI dispatcher."""
    asyncio.run(_run_keyring_async(args))


async def _run_keyring_async(args: argparse.Namespace) -> None:
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(levelname)s > %(message)s")

    daemons: Dict[str, int] = {
        "osd": args.osd,
        "mgr": args.mgr,
        "mds": args.mds,
        "rgw": args.rgw,
    }

    try:
        logging.info("Generating keyrings (%s)", daemons)
        await gen_master_keyring(args.file, daemons, rekey=args.remake)
        logging.info("Keyring generation completed successfully")
    except ValueError as e:
        logging.error("Value error: %s", e)
        sys.exit(1)
    except Exception as e:
        logging.error("Error generating keys: %s", e)
        sys.exit(1)
