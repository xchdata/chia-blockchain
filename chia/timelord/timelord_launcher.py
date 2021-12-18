import asyncio
import logging
import pathlib
import re
import signal
import socket
import time
from collections import OrderedDict
from statistics import mean
from typing import Dict, List

import pkg_resources

from chia.types.peer_info import PeerInfo
from chia.util.chia_logging import initialize_logging
from chia.util.config import load_config
from chia.util.default_root import DEFAULT_ROOT_PATH
from chia.util.ints import uint16
from chia.util.setproctitle import setproctitle


active_processes: List = []
sent: Dict = OrderedDict()
aborted: Dict = OrderedDict()

stopped = False
lock = asyncio.Lock()
print_stats_task = None

log = logging.getLogger(__name__)


async def print_stats():
    await asyncio.sleep(60)
    while not stopped:
        async with lock:
            try:
                current_hour = int(time.time() / 3600)

                for h in list(sent.keys()):
                    if (current_hour - h) > 24:
                        del sent[h]

                for h in list(aborted.keys()):
                    if (current_hour - h) > 24:
                        del aborted[h]

                total_sent = sum(sent[h] for h in sent)
                total_aborted = sum(aborted[h] for h in aborted)
                average_sent = mean(sent[h] for h in sent if h != current_hour) if len(sent) > 1 else 0
                current_sent = sent.get(current_hour, 0)
                previous_sent = sent.get(current_hour - 1, 0)
                log.info(
                    f"VDF client statistics: tag=vdf_stats"
                    f" sent={total_sent}"
                    f" average_24h={average_sent:.0f}"
                    f" current_hour={current_sent}"
                    f" previous_hour={previous_sent}"
                    f" aborted={total_aborted}"
                )
            except Exception as e:
                log.warning(f"Exception while printing stats, aborting: {(e)}")
                break
        await asyncio.sleep(60)
    log.info("Terminating stats printer")


async def kill_processes():
    global stopped
    global active_processes
    async with lock:
        stopped = True
        for process in active_processes:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        if print_stats_task:
            print_stats_task.cancel()


def find_vdf_client() -> pathlib.Path:
    p = pathlib.Path(pkg_resources.get_distribution("chiavdf").location) / "vdf_client"
    if p.is_file():
        return p
    raise FileNotFoundError("can't find vdf_client binary")


async def spawn_process(host: str, port: int, counter: int):
    global stopped
    global active_processes
    path_to_vdf_client = find_vdf_client()
    first_10_seconds = True
    start_time = time.time()
    while not stopped:
        try:
            dirname = path_to_vdf_client.parent
            basename = path_to_vdf_client.name
            check_addr = PeerInfo(host, uint16(port))
            if check_addr.is_valid():
                resolved = host
            else:
                resolved = socket.gethostbyname(host)
            launch_time = time.time()
            log.info(
                f"Launching VDF client: tag=vdf_launch counter={counter} server_host={resolved} server_port={port}"
            )
            proc = await asyncio.create_subprocess_shell(
                f"{basename} {resolved} {port} {counter}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env={"PATH": dirname},
            )
        except Exception as e:
            log.warning(f"Exception while spawning process {counter}: {(e)}")
            continue
        async with lock:
            active_processes.append(proc)
        stdout, stderr = await proc.communicate()
        end_time = time.time()
        end_hour = int(end_time / 3600)
        seconds = end_time - launch_time
        did_send = False
        if stderr:
            if first_10_seconds:
                if time.time() - start_time > 10:
                    first_10_seconds = False
            else:
                log.error(f"VDF client {counter}: {stderr.decode().rstrip()}")
        if stdout:
            output = stdout.decode().rstrip()
            log.info(f"VDF client {counter}: {output}")
            if "VDF Client: Sent proof" in output:
                md = re.search(r"VDF loop finished. Total iters: ([0-9]+)", output)
                iterations = int(md.group(1)) if md else 0
                ips = iterations / seconds
                log.info(
                    f"VDF client sent proof: tag=vdf_sent"
                    f" counter={counter}"
                    f" duration={seconds:.2f}"
                    f" iterations={iterations}"
                    f" kips={ips / 1000:.0f}"
                )
                did_send = True
            else:
                log.info(f"VDF client aborted: tag=vdf_aborted counter={counter} duration={seconds:.2f}")
        log.info(f"Process number {counter} ended.")
        async with lock:
            if proc in active_processes:
                active_processes.remove(proc)
            if did_send:
                sent[end_hour] = sent.get(end_hour, 0) + 1
            else:
                aborted[end_hour] = aborted.get(end_hour, 0) + 1
            if start_time % 60 == 0:
                print_stats()
        await asyncio.sleep(0.1)


async def spawn_all_processes(config: Dict, net_config: Dict):
    await asyncio.sleep(5)
    hostname = net_config["self_hostname"] if "host" not in config else config["host"]
    port = config["port"]
    process_count = config["process_count"]
    awaitables = [spawn_process(hostname, port, i) for i in range(process_count)]
    await asyncio.gather(*awaitables)


def main():
    global print_stats_task

    root_path = DEFAULT_ROOT_PATH
    setproctitle("chia_timelord_launcher")
    net_config = load_config(root_path, "config.yaml")
    config = net_config["timelord_launcher"]
    initialize_logging("TLauncher", config["logging"], root_path)

    def signal_received():
        asyncio.create_task(kill_processes())

    loop = asyncio.get_event_loop()

    try:
        loop.add_signal_handler(signal.SIGINT, signal_received)
        loop.add_signal_handler(signal.SIGTERM, signal_received)
    except NotImplementedError:
        log.info("signal handlers unsupported")

    try:
        print_stats_task = loop.create_task(print_stats())
        loop.run_until_complete(spawn_all_processes(config, net_config))
    finally:
        log.info("Launcher fully closed.")
        loop.close()


if __name__ == "__main__":
    main()
