#!/usr/bin/env python3
"""Script to compile Protocol Buffer definitions into Python gRPC code."""

import os
import sys
import subprocess
from pathlib import Path


def compile_proto() -> None:
    project_root = Path(__file__).parent.parent.resolve()
    proto_dir = project_root / "proto"
    
    proto_files = [
        "shard.proto",
        "replication.proto",
        "coordinator.proto",
    ]
    
    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{proto_dir}",
        f"--python_out={proto_dir}",
        f"--grpc_python_out={proto_dir}",
    ] + [str(proto_dir / pf) for pf in proto_files]
    
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error compiling proto files:\n{result.stderr}", file=sys.stderr)
        sys.exit(result.returncode)
        
    print("Successfully compiled all proto files into proto/")
    
    # Fix relative imports in generated _pb2_grpc.py files if necessary
    for pb2_grpc_file in proto_dir.glob("*_pb2_grpc.py"):
        content = pb2_grpc_file.read_text()
        for pf in ["shard", "replication", "coordinator"]:
            old_import = f"import {pf}_pb2 as {pf}__pb2"
            new_import = f"from . import {pf}_pb2 as {pf}__pb2"
            if old_import in content:
                content = content.replace(old_import, new_import)
        pb2_grpc_file.write_text(content)
    print("Fixed intra-package imports in generated gRPC stubs.")


if __name__ == "__main__":
    compile_proto()
