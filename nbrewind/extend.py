import os
import shutil
import subprocess
from pathlib import Path
from jupyter_client.kernelspec import KernelSpecManager
import json

def get_nbrewind_kernel():
    # Fix hardcoded paths nbrewind.py  
    return {
            "argv": [
                "python",
                "/home/admin/Talha/nbrewind/nbrewind.py",
                "-f",
                "{connection_file}"
            ],
            "display_name": "NBrewind",
            "language": "python",
            "codemirror_mode": {
                "name": "ipython",
                "version": 3
            }
        }

def install_nbrewind_kernel():
    ksm = KernelSpecManager()
    kernels_dir = ksm.user_kernel_dir

    kernel_name = f"nbrewind"
    kernel_path = os.path.join(kernels_dir, kernel_name)

    # Create or update the kernel directory
    os.makedirs(kernel_path, exist_ok=True)

    # Write or overwrite kernel.json
    kernel_spec = get_nbrewind_kernel()
    kernel_json_path = os.path.join(kernel_path, "kernel.json")

    with open(kernel_json_path, "w") as f:
        json.dump(kernel_spec, f, indent=4)

def setup_extend_kernel_env(eid):
    base_dir = Path(f"./{eid}").resolve()

    # Create (or clean and recreate) the eid directory
    if not base_dir.exists():
        # print(f"Cleaning existing directory: {base_dir}")
        # shutil.rmtree(base_dir)
    
        base_dir.mkdir(parents=True)
        print(f"Created directory: {base_dir}")

        # Change to eid directory
        # audit_dir = base_dir / f"{eid}-audit-kernel"
        # audit_dir.mkdir(parents=True)
        # os.chdir(base_dir)
        # print(f"Changed directory to: {base_dir}")

        # sciunit export
        print(f"Running sciunit export for {eid}...")
        subprocess.run(["/bin/bash", "-c", f"cd {base_dir} && sciunit export {eid} virtualenv"], check=True)
        # subprocess.run(["sciunit", "export", eid, "virtualenv"], check=True, cwd=base_dir)
    # subprocess.run(["cdsciunit", "export", eid, "virtualenv"], check=True)

    audit_dir = base_dir / f"audit-kernel-{eid}"

    # Set SSL_CERT_DIR
    os.environ["SSL_CERT_DIR"] = "/etc/ssl/certs/"
    print("Set SSL_CERT_DIR to /etc/ssl/certs/")

    # install nbrewind kernel
    install_nbrewind_kernel()
    
    # Source virtualenv activate script
    venv_path = Path(f"{base_dir}/env_audit-kernel-{eid}/bin/activate")
    if not venv_path.exists():
        raise FileNotFoundError(f"Cannot find: {venv_path}")

    # print(f"\n\nActivate the environment using: ")
    # print(f"    source {venv_path}")
    
    return venv_path, audit_dir

def patch_sciunit(eid):
    import site
    import pathlib

    def insert_before_class(file_path, insert_lines):
        lines = file_path.read_text().splitlines()
        for i, line in enumerate(lines):
            if line.strip().startswith("class "):
                lines = lines[:i] + insert_lines + lines[i:]
                break
        file_path.write_text('\n'.join(lines) + '\n')

    
    for d in os.listdir(f"e{eid}/env_audit-kernel-e{eid}/lib/"):
        if d.startswith("python3"):
            sciunit_root = pathlib.Path(f"e{eid}/env_audit-kernel-e{eid}/lib/{d}/site-packages/sciunit2") 
 
            # Patch 1: signal handler in sciunit2/command/exec_/__init__.py
            exec_init = sciunit_root / "command" / "exec_" / "__init__.py"
            if exec_init.exists():
                insert_before_class(exec_init, [
                    "import signal",
                    "signal.signal(signal.SIGINT, lambda *_: None)"
                ])

            # Patch 2: unpack fix in sciunit2/core.py
            core_file = sciunit_root / "core.py"
            if core_file.exists():
                print("patching", core_file)
                new_code = core_file.read_text().replace("cd, ls = f", "cd, ls, *extra = f")
                core_file.write_text(new_code)
                # replace_line(core_file, "cd, ls = f", "cd, ls, *extra = f")

            # Patch 3: remove distutils.clear() in sciunit2/command/given.py
            given_file = sciunit_root / "command" / "given.py"
            if given_file.exists():
                new_code = given_file.read_text().replace("distutils.dir_util._path_created.clear()", "")
                given_file.write_text(new_code)
                # replace_line(given_file, "distutils.dir_util._path_created.clear()", "")

            print("✅ sciunit2 patched successfully.")
            break
    