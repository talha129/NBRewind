import sys
import os
from ipykernel.kernelapp import IPKernelApp
from ipykernel.ipkernel import IPythonKernel
from ipyflow import code as ipyflow_code
import re
import dill
import time

from ipyflow.kernel import IPyflowKernel
from IPython import get_ipython

Threshold = 10.0 #sec

from IPython import get_ipython
from ipyflow.singletons import flow
from typing import cast

from ipyflow import code
from ipyflow import deps

import subprocess

initalized = False


class NamedObject:

    def __init__(self, obj_ref, code_dep):
        self.obj = obj_ref
        self.code = code_dep

class TrackedNamespace(dict):
    """Dict subclass to track variable accesses during cell execution"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.accessed = set()
        self.changed = set()

    def __getitem__(self, key):
        if key == 'print':
            return print
        self.accessed.add(key)
        return super().__getitem__(key)

    def __setitem__(self, key, value):
        self.accessed.add(key)
        super().__setitem__(key, value)

    # def __delitem__(self, key):
    #     # self.deleted.add(key)
    #     super().__delitem__(key)

def patch_namespace(ip):
    """Main patching function to install hooks"""
    
    def pre_run_cell(info):
        """Hook executed before each cell runs"""
        ip.tracked_ns = TrackedNamespace(ip.user_ns)
        # print(f"[Pre-run] user_ns keys: {list(ip.user_ns.keys())}")
        ip.user_ns = ip.tracked_ns  # Replace with tracked namespace

    def post_run_cell(info):
        """Hook executed after each cell completes"""
        if hasattr(ip, 'tracked_ns'):
            # Get accessed variables and process them
            accessed_vars = ip.tracked_ns.accessed
            print(f"[Kishu] Accessed variables: {accessed_vars}")

            changed = ip.tracked_ns.changed
            print(f"[Kishu] Changed variables: {changed}")
            
            # Restore original namespace (convert back to dict)
            ip.user_ns = dict(ip.tracked_ns)
            del ip.tracked_ns

    # Register the hooks
    ip.events.register('pre_run_cell', pre_run_cell)
    ip.events.register('post_run_cell', post_run_cell)


class CustomKernel(IPyflowKernel):
    implementation = 'NB-Rewind'
    implementation_version = '1.0'
    language = 'python'
    language_version = sys.version.split()[0]
    language_info = {
        'name': 'python',
        'version': sys.version.split()[0],
        'mimetype': 'text/x-python',
        'file_extension': '.py',
    }
    banner = "Custom Python Kernel"
    
   
    def __init__(self, **kwargs) -> None:
        
        # Activate the patching when imported
        # self.initalized = False
        super().__init__(**kwargs)
        # patch_namespace(self.shell)
        
        # myshell = IPyflowInteractiveShell.

    def dump_namespace(self, ):
        to_dump = {}
        for k,v in self.shell.user_ns.items():
            if k in ["exit", "quit", "get_ipython"]:
                continue
            try:
                dill.dumps(v)
                to_dump[k] = v
            except (TypeError, dill.PicklingError):
                # Skip objects that cannot be pickled
                pass                

        with open(f'{os.getcwd()}/session/checkpoint_{self.shell.execution_count - 1}.pkl', 'wb') as dill_file: 
            # dill.dump_module(dill_file, self.shell)
            dill.dump(to_dump, dill_file)

    def pre_run_cell(self, ip):
        """Hook executed before each cell runs"""
        ip.tracked_ns = TrackedNamespace(ip.user_ns)
        ip.user_ns = ip.tracked_ns  # Replace with tracked namespace

    def post_run_cell(self, ip):
        """Hook executed after each cell completes"""
        if hasattr(ip, 'tracked_ns'):
            # Get accessed variables and process them
            accessed_vars = ip.tracked_ns.accessed
            # print(f"[Kishu] Accessed variables: {accessed_vars}")

            # changed = ip.tracked_ns.changed
            # print(f"[Kishu] Changed variables: {changed}")
            
            # Restore original namespace (convert back to dict)
            ip.user_ns = dict(ip.tracked_ns)
            del ip.tracked_ns

    def getSym(self, id, name):

        if id in flow().aliases:
            for var in flow().aliases[id]:

                if var.readable_name == name:
                    return var
        
    
    def can_dill_serialize(self, obj):
        try:
            # Attempt to serialize the object
            dill.dumps(obj)
            return True
        except Exception as e:
            print(f"Cannot serialize object: {e}")
            return False

    def get_deps(self, sym):

        lst = deps(sym)
        to_ret = []
        for dep in lst:
            to_ret.append(dep.full_path[1])
        return to_ret
    
    def topological_sort(self, deps_graph):
        """Perform a topological sort on the dependency graph."""
        # Create a copy of the graph
        graph = {k: set(v) for k, v in deps_graph.items()}
        # Find all nodes with no dependencies
        no_deps = [k for k, v in graph.items() if not v]
        result = []
        while no_deps:
            node = no_deps.pop(0)
            result.append(node)
            # Find all nodes that depend on this one
            dependent_nodes = [k for k, v in graph.items() if node in v]
            for dependent in dependent_nodes:
                graph[dependent].remove(node)
                if not graph[dependent]:  # If no more dependencies
                    no_deps.append(dependent)
        if any(graph.values()):  # If there are remaining dependencies
            raise ValueError("Circular dependency detected")
        return result

    def dump(self, ip):
        exclusion_list = ["print", "display", "fake_edge_sym", "ipyflow", "flow", "aliases", "_"]
        to_persist = {}
        # print(ip.tracked_ns.accessed)
        for mem in flow().aliases:
            entrySet = flow().aliases[mem]
            varSet = []
            should_persist = False
            for entry in entrySet:
                
                if entry.readable_name.startswith("<literal_sym_"):
                    continue
                if "__ipyflow_mutation" in entry.readable_name:
                    continue
                
                # print(entry, entry.full_path)
                # print(entry, entry.is_module)
                
                if not entry.is_module and not entry.is_anonymous:
                    var = entry.full_path[1]
                # if not entry.is_anonymous:
                #     var = entry.full_path[1]  
                if var in exclusion_list:
                    continue
               
                varSet.append(var)
                if var in ip.tracked_ns.accessed: 
                    should_persist = True

            
            for var in varSet:
                if should_persist:
                    obj = None
                    # if var.is_module:
                    #     obj = var.obj
                    # else:
                    if self.can_dill_serialize(self.shell.user_ns[var]):
                        obj = self.shell.user_ns[var]                    
                    to_persist[var] = {'obj': obj, 'code': code( self.getSym(id(self.shell.user_ns[var]), var) ), 'deps': self.get_deps( self.getSym(id(self.shell.user_ns[var]), var) )}
                    #  NamedObject(self.shell.user_ns[var], code( self.getSym(id(self.shell.user_ns[var]), var) ))

        # for var in to_persist.keys():
        #     if var == 'obj1':
        #         # print(to_persist)
        #         print(code( self.getSym(id(self.shell.user_ns[var]), var) ))
                    
                
        print("Persisted Variables: ", to_persist)
        with open(f'{os.getcwd()}/session/checkpoint_{self.shell.execution_count - 1}.pkl', 'wb') as dill_file: 
            # dill.dump_module(dill_file, self.shell)
            # print(to_persist)
            dill.dump(to_persist, dill_file)

    def run_commit_command(self, path_to_binary, exec_id, path_to_file):
        """
        Generates and runs the command: ./vv commit {exec_id} {path_to_file}
        
        :param exec_id: The execution ID to include in the command
        :param path_to_file: The path to the .pkl file
        """
        if not os.path.isfile(path_to_file):
            raise FileNotFoundError(f"The file {path_to_file} does not exist.")
        
        # Generate the command
        command = f"{path_to_binary} commit {exec_id} {path_to_file}"
        
        # Run the command
        try:
            result = subprocess.run(command, shell=True, check=True, text=True, capture_output=True)
            print("Command executed successfully:")
            os.remove(path_to_file)
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            print("Error while executing the command:")
            print(e.stderr)
    
    def run_checkout_command(self, path_to_binary, exec_id, path_to_file):
        """
        Generates and runs the command: ./vv checkout {exec_id} {path_to_file}
        
        :param path_to_binary: The path to the vv executable
        :param exec_id: The execution ID to include in the command
        :param path_to_file: The path where the file should be checked out to
        """
        # Check if the directory for the output file exists
        output_dir = os.path.dirname(path_to_file)
        if output_dir and not os.path.isdir(output_dir):
            raise FileNotFoundError(f"The directory {output_dir} does not exist for {path_to_file}.")
        
        # Generate the command
        command = f"{path_to_binary} checkout {exec_id} {path_to_file}"
        
        # Run the command
        try:
            result = subprocess.run(command, shell=True, check=True, text=True, capture_output=True)
            print("Checkout command executed successfully:")
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            print("Error while executing the checkout command:")
            print(e.stderr)

    async def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False, *
                        ,cell_meta=None, cell_id=None,):

        ip = self.shell
        self.pre_run_cell(ip)

        # # start_time = time.time()

        # checkpoint_pattern = r"^# commit"
        restore_pattern = r"^# rollback \[(\d+)\].*"

        restore_match = re.match(restore_pattern, code.splitlines()[0])
        # # checkpoint_match = re.match(checkpoint_pattern, code.splitlines()[0])
        
        if restore_match:
            exec_id = re.findall(r'\b\d+\b', code.splitlines()[0])[0]
            
            loaded_vars = {}
            for f in range(int(exec_id ) + 1):
                self.run_checkout_command(f'{os.getcwd()}/session/vv', f, f'{os.getcwd()}/session/checkpoint_{f}.pkl')
                fname = f'{os.getcwd()}/session/checkpoint_{f}.pkl'
                if os.path.isfile(fname):
                    with open(fname, 'rb') as dill_file:
                        loaded_vars.update(dill.load(dill_file))
                    os.remove(fname)
                        
            # Build dependency graph
            deps_graph = {var: data['deps'] for var, data in loaded_vars.items()}

            # Get execution order using topological sort
            execution_order = self.topological_sort(deps_graph)
            print(loaded_vars)
            
            
            # update global namespace with loaded variables
            for var in loaded_vars:
                # loaded_vars[var] = loaded_vars[var]['obj']
                self.shell.user_ns.update({var: loaded_vars[var]['obj']})

            namespace = self.shell.user_ns

            # self.shell.user_ns['A'] = None

            # print(namespace['A'])
            # Execute code for each variable in order, only if missing
            for var_name in execution_order:
                # Check if the variable exists and is not None in the namespace
                # print(namespace['A'])
                if var_name in namespace and namespace[var_name] is not None:
                    continue  # Skip if the object already exists and is not None
                
                # print(loaded_vars[var_name])
                code = str(loaded_vars[var_name]['code'])
                # print(type(str(code)))
                try:
                    # Execute the code in the namespace
                    # print("hello")
                    exec(code, namespace)
                    # If the variable isn't directly assigned, try to find it
                    # if var_name not in namespace:
                    #     for key, value in namespace.items():
                    #         if hasattr(value, '__dict__') and var_name in value.__dict__:
                    #             namespace[var_name] = value.__dict__[var_name]
                    #             break
                except Exception as e:
                    print(f"Error restoring {var_name}: {str(e)}")
                    continue
                # print(f"Restored checkpoint {f}")                        
                        
                # else:
                #     continue
                #     print("No checkpoint found for the given ID")
        
            
            # ip = get_ipython()
            # res =  await super().do_execute(code, silent, store_history, user_expressions, allow_stdin)
            
            # accessed_vars = ip.tracked_ns.accessed
        #     to_dump = {}
        #     for v in accessed_vars:
        #         to_dump[v] = self.shell.user_ns[v]

        #     with open(f'{os.getcwd()}/session/checkpoint_{self.shell.execution_count - 1}.pkl', 'wb') as dill_file: 
        #         # dill.dump_module(dill_file, self.shell)
        #         # print(to_dump)
        #         dill.dump(to_dump, dill_file)
            
        #     self.post_run_cell(ip)
        #     return res

        # # if checkpoint_match:
        # #     result =  await super().do_execute(code , silent, store_history, user_expressions, allow_stdin)
        # #     self.dump_namespace()
        #     # return result

        # # if not self.initalized:
        # #     self.initalized = True
        # #     patch_namespace(self.shell)
        
        
        res = await super().do_execute(code, silent, store_history, user_expressions, allow_stdin)
        # end_time = time.time() 
        
        # # if end_time - start_time > Threshold and res["status"] == "ok":
        # #     self.dump_namespace()
        # accessed_vars = ip.tracked_ns.accessed
        # # print(accessed_vars)
        # to_dump = {}
        # for v in accessed_vars:
        #     if v in self.shell.user_ns:
        #         to_dump[v] = self.shell.user_ns[v]
        # # with open(f'{os.getcwd()}/session/checkpoint_{self.shell.execution_count - 1}.pkl', 'wb') as dill_file: 
        # #     dill.dump(to_dump, dill_file)


        # res = await super().do_execute(code, silent, store_history, user_expressions, allow_stdin)
         
        
        # exclusion_list = ["print", "display", "fake_edge_sym", "ipyflow", "flow", "aliases", "_"]
        # to_persist = {}
        # # print(ip.tracked_ns.accessed)
        # for mem in flow().aliases:
        #     entrySet = flow().aliases[mem]
        #     varSet = []
        #     should_persist = False
        #     for entry in entrySet:
                
        #         if entry.readable_name.startswith("<literal_sym_"):
        #             continue
        #         if "__ipyflow_mutation" in entry.readable_name:
        #             continue
                
        #         var = entry.full_path[1]
        #         if var in exclusion_list:
        #             continue
               
        #         varSet.append(var)
        #         if var in ip.tracked_ns.accessed: 
        #             should_persist = True

            
        #     for var in varSet:
        #         if should_persist:
        #             to_persist[var] = self.shell.user_ns[var]
                
        # print(to_persist)
        # with open(f'{os.getcwd()}/session/checkpoint_{self.shell.execution_count - 1}.pkl', 'wb') as dill_file: 
        #     # dill.dump_module(dill_file, self.shell)
        #     dill.dump(to_persist, dill_file)
        
        self.dump(ip)
        self.run_commit_command(f'{os.getcwd()}/session/vv', self.shell.execution_count - 1, f'{os.getcwd()}/session/checkpoint_{self.shell.execution_count - 1}.pkl')
        self.post_run_cell(ip) 
        
        return res


if __name__ == '__main__':
    
    IPKernelApp.launch_instance(kernel_class=CustomKernel)
