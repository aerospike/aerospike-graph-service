# Copyright 2022-2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import subprocess
import platform
import shutil
import time
import pytest
from pathlib import Path

def skip_if_no(cmd):
    if not shutil.which(cmd):
        pytest.skip(f"The command {cmd} is not available on PATH")


def get_script_source(subdir: str = None) -> Path:
    base = Path(__file__).parent.parent.parent
    if subdir:
        return base / subdir / "make-certs.sh"
    return base / "make-certs.sh"


def copy_script_to(temp_dir: Path, script_path: Path) -> Path:
    dest = temp_dir / script_path.name
    shutil.copy2(script_path, dest)
    return dest


def run_make_certs(script_dest: Path, cwd: Path) -> subprocess.CompletedProcess:
    if platform.system() == "Windows":
        wsl_path = str(script_dest).replace("\\", "/").replace("C:", "/mnt/c")
        try:
            return subprocess.run(
                ["wsl", "bash", wsl_path],
                cwd=str(cwd),
                capture_output=True,
                text=True
            )
        except FileNotFoundError:
            return subprocess.run(
                ["bash", "-c", f"cd '{cwd}' && bash '{script_dest}'"],
                capture_output=True,
                text=True
            )
    else:
        return subprocess.run(
            ["bash", str(script_dest)],
            cwd=cwd,
            capture_output=True,
            text=True
        )


def cleanup_dir(temp_dir: Path) -> None:
    try:
        shutil.rmtree(temp_dir)
    except PermissionError:
        time.sleep(0.1)
        try:
            shutil.rmtree(temp_dir)
        except PermissionError:
            pass