import subprocess, sys, signal, time

procs = [
    subprocess.Popen([sys.executable, "avwap_futures_collector_v30.py"]),
    subprocess.Popen([sys.executable, "futures_aggression_collector_v30.py"]),
]

def stop(*_):
    for p in procs:
        if p.poll() is None:
            p.terminate()

signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)

try:
    while True:
        for p in procs:
            rc = p.poll()
            if rc is not None:
                stop()
                raise SystemExit(rc)
        time.sleep(2)
except KeyboardInterrupt:
    stop()
