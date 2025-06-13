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