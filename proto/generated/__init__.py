"""Generated Protocol Buffer bindings.

To allow seamless imports from anywhere in the codebase:
import proto.generated.coordinator_pb2 as coord_pb2
"""
import sys
import os

# Ensure the generated directory is in sys.path so intra-proto imports resolve reliably
_generated_dir = os.path.dirname(os.path.abspath(__file__))
if _generated_dir not in sys.path:
    sys.path.insert(0, _generated_dir)
