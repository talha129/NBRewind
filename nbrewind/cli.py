import argparse
import subprocess
import os
from pathlib import Path
import json
import nbformat
from jupyter_client.kernelspec import KernelSpecManager
from .notebook_version import NotebookVersion
from .repeat import install_repeat_kernel, remove_kernel
from .extend import setup_extend_kernel_env, patch_sciunit
from sciunit2.records import ExecutionManager

def get_last_execution(sciunit_project):
    em = ExecutionManager(sciunit_project)
    return em.get_last_id()

def update_notebook_kernel(notebook_path, kernel_name):
    # Load available kernels
    ksm = KernelSpecManager()
    kernels = ksm.find_kernel_specs()

    if kernel_name not in kernels:
        raise ValueError(f"Kernel '{kernel_name}' not found. Available kernels: {list(kernels.keys())}")

    # Load kernel spec info
    spec = ksm.get_kernel_spec(kernel_name)
    
    # Load notebook
    with open(notebook_path, 'r', encoding='utf-8') as f:
        nb = nbformat.read(f, as_version=4)

    # Update metadata
    nb.metadata.kernelspec = {
        "name": kernel_name,
        "display_name": spec.display_name,
        "language": spec.language
    }

    # Save updated notebook
    with open(notebook_path, 'w', encoding='utf-8') as f:
        nbformat.write(nb, f)

    # print(f"✅ Updated '{notebook_path}' to use kernel: {kernel_name} ({spec.display_name})")

def update_notebook_mode(notebook_path, mode):
    # Load notebook
    with open(notebook_path, 'r', encoding='utf-8') as f:
        nb = nbformat.read(f, as_version=4)

    nb.metadata['AUDIT'] = mode

    # Save updated notebook
    with open(notebook_path, 'w', encoding='utf-8') as f:
        nbformat.write(nb, f)

def audit_notebook(notebook_path):
    print(f"\nAuditing notebook at: {notebook_path}\n")
    # Add audit logic here

    sciunit_project = os.path.expanduser('~') + '/sciunit/audit-kernel'
    last_eid = get_last_execution(sciunit_project)
    notebook_path = str(Path(notebook_path).expanduser().resolve())
    
    # update given notebook default kernel spec with audit kernel
    update_notebook_kernel(notebook_path, "audit-kernel")

    # update given notebook metadata audit mode
    update_notebook_mode(notebook_path, "true")

    # run the given notebook using audit kernel
    # wait for the kernel to finish and then exit

    try:
        p = subprocess.run(['jupyter', 'notebook', f'{notebook_path}', '--no-browser', '--ip=0.0.0.0', '--port=8889'], check=True)
        # print("hello")
    except KeyboardInterrupt:
        eid = get_last_execution(sciunit_project)
        if last_eid == eid and last_eid != 1:
            print("\nError auditing the notebook.\n")
        else:
            nv = NotebookVersion(sciunit_project)
            # persist eid, notebook path, content of notebook
            with open(notebook_path, 'r') as f:
                content = f.read()
                new_notebook_path = ''.join(notebook_path.split(".")[:-1]) + f"_v{eid}" + ".ipynb"
                nv.persist(eid, new_notebook_path, content)
            nv.close()

            # change the default kernel to python3
            update_notebook_kernel(notebook_path, "python3")
            print("\n\n\nNotebook audited successfully.\n")

            # update given notebook metadata audit mode false
            update_notebook_mode(notebook_path, "false")

def repeat_notebook(notebook_path):
    # print(f"Repeating version {version} of notebook at: {notebook_path}")
    
    # get execution id from sciunitdb associated with notebook 
    sciunit_project = os.path.expanduser('~') + '/sciunit/audit-kernel'
    nv = NotebookVersion(sciunit_project)
    eid = nv.get_id(notebook_path)

    if not eid:
        print(f"No execution found for {notebook_path}") 
    
    # install repeat kernel
    install_repeat_kernel(eid)

    # create the notebook to be repeated
    content = nv.get_content(eid)
    created_notebook = notebook_path.split("/")[-1]
    with open(created_notebook, 'w') as f:
        f.write(content)
    
    # update the given notebook to run nbrewind-repeat
    update_notebook_kernel(created_notebook, "nbrewind-repeat")
    
    # update given notebook metadata audit mode false
    update_notebook_mode(created_notebook, "false")
    
    try:
        _ = subprocess.run(['jupyter', 'notebook', f'{created_notebook}', '--no-browser', '--ip=0.0.0.0', '--port=8889'], check=True)
    except KeyboardInterrupt:
        
        # remove repeat kernel after repeat is finished
        remove_kernel()

def extend(notebook_path):
    
    # get execution id from sciunitdb associated with notebook 
    sciunit_project = os.path.expanduser('~') + '/sciunit/audit-kernel'
    nv = NotebookVersion(sciunit_project)
    eid = nv.get_id(notebook_path)

    if not eid:
        print(f"No Execution found for {notebook_path}")
    
    #setu up development envrionment
    venv_path, audit_dir = setup_extend_kernel_env(f"e{eid}")
    patch_sciunit(eid)
    
    # # create a new shell in which activate the development env and run jupyter notebook there
    try:
        jp = f"jupyter notebook {audit_dir} --no-browser --ip=0.0.0.0 --port=8889"
        subprocess.run(["/bin/bash", "-c", f"source {venv_path} && {jp}"])
    except KeyboardInterrupt:
        "\n\n Shutting down development envrionment"


def list_notebooks():
    sciunit_project = os.path.expanduser('~') + '/sciunit/audit-kernel'
    versions = NotebookVersion(sciunit_project).get_all_versions()

    for v in versions:
        print(v[0])

def init():

    print("Initializing new nbrewind project")
    try:
        subprocess.run(["/bin/bash", "-c", f"sciunit create -f audit-kernel"])
    except KeyboardInterrupt:
        pass

def main():
    parser = argparse.ArgumentParser(prog='nbrewind', description='Notebook Rewind CLI Tool')

    subparsers = parser.add_subparsers(dest='command', required=True)

    # --audit
    audit_parser = subparsers.add_parser('audit', help='Audit a notebook')
    audit_parser.add_argument('--notebook', required=True, help='Path to the notebook')

    # --repeat
    repeat_parser = subparsers.add_parser('repeat', help='Repeat a notebook execution at a given version')
    repeat_parser.add_argument('--notebook', help='Path to the notebook')
    # repeat_parser.add_argument('--version', required=True, help='Version to repeat')

    # --extend
    develop_parser = subparsers.add_parser('extend', help='Develop mode')
    develop_parser.add_argument('--notebook', help='Path to the notebook')
    # develop_parser.add_argument('--version', required=True, help='Version to develop from')

    # --list
    _ = subparsers.add_parser('list', help='list all notebook versions')
    # repeat_parser.add_argument('--notebook', help='Path to the notebook')

    # --init
    _ = subparsers.add_parser('init', help='Repeat a notebook execution at a given version')


    args = parser.parse_args()

    if args.command == 'audit':
        audit_notebook(args.notebook)
    elif args.command == 'repeat':
        repeat_notebook(args.notebook)
    elif args.command == 'extend':
        extend(args.notebook)
    elif args.command == 'list':
        list_notebooks()
    elif args.command == 'init':
        init()
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
