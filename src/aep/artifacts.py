"""Content-addressed local storage. Only committed DB metadata exposes files."""

import hashlib
import os
from pathlib import Path
import re
import tempfile
from uuid import UUID


class LocalArtifacts:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def put(self, run_id, content: bytes):
        run_id = str(UUID(str(run_id)))
        digest = hashlib.sha256(content).hexdigest()
        directory = self.root / run_id
        directory.mkdir(exist_ok=True)
        if self.root not in directory.resolve().parents:
            raise ValueError("Artifact directory escapes storage root")
        destination = directory / digest
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=directory, delete=False) as stream:
                temporary = Path(stream.name)
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, destination)
            if os.name != "nt":
                # Persist the file entry and the newly created run directory.
                for parent in (directory, self.root):
                    fd = os.open(parent, os.O_RDONLY)
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        return {"sha256": digest, "storage_key": f"{run_id}/{digest}", "size_bytes": len(content)}

    def read(self, metadata):
        key = metadata["storage_key"]
        if not re.fullmatch(r"[a-f0-9-]{36}/[a-f0-9]{64}", key):
            raise ValueError("Invalid artifact storage key")
        path = (self.root / key).resolve()
        if self.root not in path.parents:
            raise ValueError("Artifact escapes storage root")
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != metadata["sha256"]:
            raise ValueError("Artifact checksum mismatch")
        return content
