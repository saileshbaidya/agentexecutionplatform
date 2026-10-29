import hashlib
from pathlib import Path
import sys
import tempfile
import unittest
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from aep.artifacts import LocalArtifacts


class ArtifactTests(unittest.TestCase):
    def test_atomic_content_addressing_and_integrity(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalArtifacts(root)
            run_id = uuid4()
            first = store.put(run_id, b"evidence")
            self.assertEqual(first, store.put(run_id, b"evidence"))
            self.assertEqual(first["sha256"], hashlib.sha256(b"evidence").hexdigest())
            self.assertEqual(store.read(first), b"evidence")
            (Path(root) / first["storage_key"]).write_bytes(b"corruption")
            with self.assertRaisesRegex(ValueError, "checksum"):
                store.read(first)
            self.assertEqual(len(list((Path(root) / str(run_id)).iterdir())), 1)

    def test_path_traversal_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalArtifacts(root)
            with self.assertRaises(ValueError):
                store.read({"storage_key": "../../secret", "sha256": "a" * 64})
            with self.assertRaises(ValueError):
                store.put("../escape", b"data")
