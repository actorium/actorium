import hashlib
import os
import platform
from functools import cache
from pathlib import Path
from uuid import UUID, uuid4

__all__ = [
    "process_hash",
    "interpreter_hash",
]


@cache
def process_hash() -> str:
    machine_id = _machine_id()
    pid = os.getpid()
    start_time = _process_start_time()

    fingerprint = f"{machine_id}|{pid}|{start_time}"

    return hashlib.sha256(fingerprint.encode()).hexdigest()


@cache
def interpreter_hash() -> UUID:
    return uuid4()


def _machine_id() -> str:
    system = platform.system()
    if system == "Linux":
        return Path("/etc/machine-id").read_text().strip()

    return ""


def _process_start_time() -> str:
    system = platform.system()
    if system == "Linux":
        return str(_process_start_time_linux())
    return ""


def _process_start_time_linux() -> int:
    """
    Returns process start time in clock ticks since boot.
    """
    stat = Path(f"/proc/{os.getpid()}/stat").read_text()

    # Field 22 is starttime, but the second field can contain spaces inside ()
    # so we split after the last ')'
    after_comm = stat.rsplit(")", 1)[1].strip()
    fields = after_comm.split()

    start_time_ticks = int(fields[19])  # field 22 overall
    return start_time_ticks
