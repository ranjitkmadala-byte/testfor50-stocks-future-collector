"""Run futures aggression and first-hour AVWAP workers in one Railway service."""

import signal
import subprocess
import sys
import time


COMMANDS = [
    [sys.executable, "avwap_futures_collector.py"],
    [sys.executable, "futures_aggression_top50_0920.py"],
]


def main():
    processes = [subprocess.Popen(command) for command in COMMANDS]

    def stop_all(*_):
        for process in processes:
            if process.poll() is None:
                process.terminate()

    signal.signal(signal.SIGTERM, stop_all)
    signal.signal(signal.SIGINT, stop_all)

    try:
        while True:
            for process, command in zip(processes, COMMANDS):
                code = process.poll()
                if code is not None:
                    stop_all()
                    raise RuntimeError(f"Collector exited with code {code}: {' '.join(command)}")
            time.sleep(2)
    finally:
        stop_all()
        for process in processes:
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()


if __name__ == "__main__":
    main()
