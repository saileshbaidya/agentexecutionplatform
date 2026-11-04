"""Linux Docker isolation. No shell, host mounts, credentials, or network.

Only the trusted gateway uses Docker. Never fall back to host execution.
"""

import json
import subprocess
import threading
import time

from .execution import canonical
from .tool_policy import LIMITS

RUNNER = r'''
import json, subprocess, sys
request = json.load(sys.stdin)
if request["tool"] == "text.stats":
    text = request["arguments"]["text"]
    print(json.dumps({"characters": len(text), "words": len(text.split()), "lines": len(text.splitlines())}))
else:
    # Parent output is bounded independently by the gateway reader.
    p = subprocess.run([sys.executable, "-I", "-c", request["arguments"]["code"]],
                       input=request["arguments"]["stdin"].encode(), timeout=8)
    sys.exit(p.returncode)
'''


def container_name(action_id):
    from uuid import UUID
    return "aep-tool-" + UUID(str(action_id)).hex


class DockerSandbox:
    def cleanup(self, action_id):
        # Action IDs are UUIDs from the database, never arbitrary container names.
        result = subprocess.run(["docker", "rm", "-f", container_name(action_id)], capture_output=True, timeout=10, check=False)
        return result.returncode == 0 or b"No such container:" in result.stderr

    def command(self, row):
        return ["docker", "run", "--pull=never", "--name", container_name(row["id"]),
                "--label", "aep.tool-action=" + str(row["id"]), "--network=none", "--read-only",
                "--user=65534:65534", "--cap-drop=ALL", "--security-opt=no-new-privileges=true",
                "--cpus=" + LIMITS["cpus"], "--memory=" + LIMITS["memory"], "--memory-swap=" + LIMITS["memory"],
                "--pids-limit=" + str(LIMITS["pids"]), "--ulimit=nofile=64:64", "--ulimit=core=0:0",
                "--tmpfs=/tmp:rw,noexec,nosuid,nodev,size=16777216,mode=1777",
                "--log-driver=none", "--ipc=none", "--init", "--workdir=/tmp", "--interactive",
                "--entrypoint=python", row["action"]["image"], "-I", "-c", RUNNER]

    def execute(self, row, active):
        output = bytearray()
        overflow = threading.Event()
        process = None
        try:
            process = subprocess.Popen(self.command(row), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, shell=False)

            def read():
                while True:
                    chunk = process.stdout.read(1024)
                    if not chunk:
                        return
                    remaining = LIMITS["output_bytes"] - len(output)
                    output.extend(chunk[:max(0, remaining)])
                    if len(chunk) > remaining:
                        overflow.set()
                        return

            reader = threading.Thread(target=read, daemon=True)
            reader.start()

            def write():
                try:
                    process.stdin.write(canonical({"tool": row["action"]["tool"], "arguments": row["action"]["arguments"]}))
                    process.stdin.close()
                except (OSError, ValueError):
                    pass

            # A stalled daemon must not block the watchdog on a full stdin pipe.
            writer = threading.Thread(target=write, daemon=True)
            writer.start()
            deadline = time.monotonic() + LIMITS["seconds"]
            error = None
            while process.poll() is None:
                if overflow.is_set():
                    error = "tool_output_limit"
                    break
                if time.monotonic() >= deadline:
                    error = "tool_timeout"
                    break
                if not active():
                    error = "tool_cancelled"
                    break
                time.sleep(0.1)
            if error:
                self.cleanup(row["id"])
                process.kill()
            process.wait(timeout=5)
            writer.join(timeout=1)
            reader.join(timeout=3)
            if overflow.is_set():
                error = "tool_output_limit"
            if error:
                return None, error
            if process.returncode != 0:
                return None, "tool_execution_failed"  # Do not expose daemon errors/host details.
            text = output.decode("utf-8", errors="replace")
            if row["action"]["tool"] == "text.stats":
                return json.loads(text), None
            return {"stdout": text, "exit_code": 0}, None
        except (OSError, subprocess.SubprocessError, ValueError):
            return None, "sandbox_unavailable"
        finally:
            try:
                self.cleanup(row["id"])
            except (OSError, subprocess.SubprocessError):
                pass  # Durable RUNNING actions are recovered on the next gateway pass.
            if process:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
                for pipe in (process.stdin, process.stdout):
                    if pipe:
                        try:
                            pipe.close()
                        except (OSError, ValueError):
                            pass
