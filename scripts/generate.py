"""Run pinned generators installed by the documented setup commands."""
import pathlib
import subprocess
import sys

subprocess.run([sys.executable, "-m", "grpc_tools.protoc", "-I", "proto",
                "--python_out=sdk/python/rungrid", "--grpc_python_out=sdk/python/rungrid",
                "--go_out=.", "--go_opt=module=github.com/ZubairQazi/rungrid",
                "--go-grpc_out=.", "--go-grpc_opt=module=github.com/ZubairQazi/rungrid",
                "proto/rungrid.proto"], check=True)
path = pathlib.Path("sdk/python/rungrid/rungrid_pb2_grpc.py")
path.write_text(path.read_text().replace("import rungrid_pb2 as", "from . import rungrid_pb2 as"))
