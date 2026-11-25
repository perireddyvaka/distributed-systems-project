#!/usr/bin/env python3
"""Script to compile protobuf files into Python gRPC bindings."""

import os
import sys
import subprocess
from pathlib import Path


def compile_protos():
    project_root = Path(__file__).resolve().parent.parent
    proto_dir = project_root / "proto"
    output_dir = proto_dir / "generated"

    output_dir.mkdir(parents=True, exist_ok=True)
    init_file = output_dir / "__init__.py"
    if not init_file.exists():
        init_file.write_text(
            'import sys, os\n'
            '_dir = os.path.dirname(os.path.abspath(__file__))\n'
            'if _dir not in sys.path:\n'
            '    sys.path.insert(0, _dir)\n'
        )

    proto_files = list(proto_dir.glob("*.proto"))
    if not proto_files:
        print(f"No proto files found in {proto_dir}")
        return 1

    proto_names = [f.name for f in proto_files]
    print(f"Compiling proto files: {proto_names}")

    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{proto_dir}",
        f"--python_out={output_dir}",
        f"--grpc_python_out={output_dir}",
        *[str(p) for p in proto_files],
    ]

    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"Failed to compile protos: exit code {result.returncode}")
        return result.returncode

    print(f"Successfully compiled {len(proto_files)} proto files into {output_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(compile_protos())
