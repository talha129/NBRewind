import os
import json
import shutil
from jupyter_client.kernelspec import KernelSpecManager

def get_repeat_kernel(eid):
    # -TODOO: Fix hardcoded paths for repeat-handler 
    # Fix hardcoded paths nbrewind.py  
    return {
            "argv": [
                "/home/admin/Talha/nbrewind/Flinc/repeat-handler.py",
                "sciunit", "given", "{connection_file}", "repeat", f"e{eid}",
                "/home/admin/Talha/nbrewind/nbrewind.py","-f",
                "%"
            ],
            "env": {
                "AUDIT": "false"
            },
            "display_name": "NbRewind Repeat Kernel",
            "language": "python"
        }

def install_repeat_kernel(eid):
    ksm = KernelSpecManager()
    kernels_dir = ksm.user_kernel_dir  # usually ~/.local/share/jupyter/kernels

    kernel_name = f"nbrewind-repeat"
    kernel_path = os.path.join(kernels_dir, kernel_name)

    # Create or update the kernel directory
    os.makedirs(kernel_path, exist_ok=True)

    # Write or overwrite kernel.json
    kernel_spec = get_repeat_kernel(eid)
    kernel_json_path = os.path.join(kernel_path, "kernel.json")

    with open(kernel_json_path, "w") as f:
        json.dump(kernel_spec, f, indent=4)

def remove_kernel():
    ksm = KernelSpecManager()
    kernels = ksm.find_kernel_specs()

    kernel_name = "nbrewind-repeat"
    if kernel_name not in kernels:
        print(f"Kernel '{kernel_name}' not found.")
        return

    kernel_path = kernels[kernel_name]

    try:
        shutil.rmtree(kernel_path)
        print(f"Removed kernel: {kernel_name}")
        print(f"Path: {kernel_path}")
    except Exception as e:
        print(f"Failed to remove kernel '{kernel_name}': {e}")