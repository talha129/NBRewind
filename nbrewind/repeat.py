import os
import json
import shutil
from jupyter_client.kernelspec import KernelSpecManager

def get_repeat_kernel(eid, nbrewind_path, repeat_handler_path):
    # -TODOO: Fix hardcoded paths for repeat-handler 
    # Fix hardcoded paths nbrewind.py  
    return {
            "argv": [
                "python", f"{repeat_handler_path}",
                "sciunit", "given", "{connection_file}", "repeat", f"e{eid}",
                f"{nbrewind_path}","-f",
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

    audit_kernel_name = "audit-kernel"
    audit_kernel_path = os.path.join(kernels_dir, audit_kernel_name)
    repeat_handler_path = os.path.join(audit_kernel_path, "repeat_handler.py")
    
    kernel_name = f"nbrewind-repeat"
    kernel_path = os.path.join(kernels_dir, kernel_name)

    # Create or update the kernel directory
    os.makedirs(kernel_path, exist_ok=True)

    nbrewind_path = ""
    # Read the existing kernel.json
    with open(os.path.join(audit_kernel_path, "kernel.json"), 'r') as f:
        kernel_json = json.load(f)   
        nbrewind_path = kernel_json["argv"][5]

    # Write or overwrite kernel.json
    kernel_spec = get_repeat_kernel(eid, nbrewind_path, repeat_handler_path)
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