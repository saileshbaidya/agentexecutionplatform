"""Opt-in real Linux-container security checks; never executes code on the host."""

import json
import os
from pathlib import Path
import sys
import unittest
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from aep.tool_sandbox import DockerSandbox
from aep.tool_policy import IMAGE


@unittest.skipUnless(os.environ.get("AEP_TEST_TOOL_IMAGE"), "Set AEP_TEST_TOOL_IMAGE to an immutable built sandbox image ID")
class SandboxTests(unittest.TestCase):
    def run_code(self, code, active=lambda: True):
        image = os.environ["AEP_TEST_TOOL_IMAGE"]
        self.assertRegex(image, IMAGE)
        return DockerSandbox().execute({"id": uuid4(), "action": {"image": image, "tool": "python.run",
                                      "arguments": {"code": code, "stdin": ""}}}, active)

    def test_network_filesystem_identity_secrets_and_resource_limits(self):
        result, error = self.run_code('''
import json, os, pathlib, socket
blocked = False
try:
    pathlib.Path('/escape-test').write_text('escape')
except OSError:
    blocked = True
pathlib.Path('/tmp/allowed').write_text('ephemeral')
print(json.dumps({
 'uid': os.getuid(), 'root_write_blocked': blocked,
 'socket_visible': pathlib.Path('/var/run/docker.sock').exists(),
 'artifacts_visible': pathlib.Path('/artifacts').exists(),
 'secrets': any(k in os.environ for k in ('OPENAI_API_KEY','DATABASE_URL','AEP_API_KEY','AEP_TOOL_APPROVAL_KEY')),
 'interfaces': [name for _,name in socket.if_nameindex()],
 'memory': pathlib.Path('/sys/fs/cgroup/memory.max').read_text().strip(),
 'pids': pathlib.Path('/sys/fs/cgroup/pids.max').read_text().strip(),
 'cpu': pathlib.Path('/sys/fs/cgroup/cpu.max').read_text().strip(),
 'status': pathlib.Path('/proc/self/status').read_text()
}))
''')
        self.assertIsNone(error)
        probe = json.loads(result["stdout"])
        self.assertEqual(probe["uid"], 65534)
        self.assertTrue(probe["root_write_blocked"])
        self.assertFalse(probe["socket_visible"] or probe["artifacts_visible"] or probe["secrets"])
        self.assertEqual(probe["interfaces"], ["lo"])
        self.assertEqual(probe["memory"], "134217728")
        self.assertEqual(probe["pids"], "32")
        quota, period = map(int, probe["cpu"].split())
        self.assertEqual(quota / period, 0.5)
        self.assertIn("NoNewPrivs:\t1", probe["status"])
        self.assertIn("CapEff:\t0000000000000000", probe["status"])

    def test_memory_abuse_is_bounded(self):
        result, error = self.run_code("a = bytearray(256 * 1024 * 1024)")
        self.assertIsNone(result)
        self.assertIsNotNone(error)

    def test_output_abuse_is_bounded(self):
        result, error = self.run_code("print('x' * 1000000)")
        self.assertIsNone(result)
        self.assertEqual(error, "tool_output_limit")

    def test_execution_timeout_is_bounded(self):
        result, error = self.run_code("while True: pass")
        self.assertIsNone(result)
        self.assertIn(error, {"tool_timeout", "tool_execution_failed"})

    def test_cancellation_kills_container(self):
        result, error = self.run_code("import time; time.sleep(30)", active=lambda: False)
        self.assertIsNone(result)
        self.assertEqual(error, "tool_cancelled")


if __name__ == "__main__":
    unittest.main()
