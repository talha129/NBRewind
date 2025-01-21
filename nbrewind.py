import sys
import os
from ipykernel.kernelapp import IPKernelApp
from ipykernel.ipkernel import IPythonKernel
import re
import dill

# from ipyflow.kernel import IPyflowKernel

class CustomKernel(IPythonKernel):
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
        super().__init__(**kwargs)
        # myshell = IPyflowInteractiveShell.


    def get_dumping_code(self):
        with open ('/Users/talhaazaz/Documents/DePaul/Research/winter/rollback/dump.py', 'r') as f:
            return str(f.read())
    
    def get_loading_code(self):
        with open ('/Users/talhaazaz/Documents/DePaul/Research/winter/rollback/load.py', 'r') as f:
            return str(f.read())

    def dump_namespace(self):
        to_dump = {}
        for k,v in self.shell.user_ns.items():
            try:
                dill.dumps(v)
                to_dump[k] = v
            except (TypeError, dill.PicklingError):
                # Skip objects that cannot be pickled
                pass                

        with open(f'/Users/talhaazaz/Documents/DePaul/Research/winter/session/checkpoint_{self.shell.execution_count - 1}.pkl', 'wb') as dill_file: 
            print(to_dump['z'], )
            dill.dump(to_dump, dill_file)

    async def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False, *
                        ,cell_meta=None, cell_id=None,):

        # Checkpoint the process using CRIU
        # os.makedirs(checkpoint_dir, exist_ok=True)

        # pid = os.getpid()
        # checkpoint_dir = "criu_check/"
        # print (pid)
        # checkpoint_cmd = f"criu dump -t {pid} -D {checkpoint_dir} -j -v4 --leave-running --track-mem --shell-job"
        
        # try:
        #     subprocess.run(checkpoint_cmd, shell=True, check=True)
        # except subprocess.CalledProcessError as e:
        #     print(f"CRIU checkpoint failed: {e}", file=sys.stderr)

        restore_pattern = r"^# rollback \[(\d+)\].*"
        checkpoint_pattern = r"^# commit"
        restore_match = re.match(restore_pattern, code.splitlines()[0])
        checkpoint_match = re.match(checkpoint_pattern, code.splitlines()[0])
        
        if restore_match:
            exec_id = re.findall(r'\b\d+\b', code.splitlines()[0])[0]
            print(exec_id)
            fname = f'/Users/talhaazaz/Documents/DePaul/Research/winter/session/checkpoint_{exec_id}.pkl'
            if os.path.isfile(fname):
                print("restore")
                with open(fname, 'rb') as dill_file:
                    loaded_vars = dill.load(dill_file)
                    print(loaded_vars['z'])
                    self.shell.user_ns.update(loaded_vars)
                return await super().do_execute(code, silent, store_history, user_expressions, allow_stdin)
            
        if checkpoint_match:
            result =  await super().do_execute(code , silent, store_history, user_expressions, allow_stdin)
            self.dump_namespace()
            return result
            
        return await super().do_execute(code, silent, store_history, user_expressions, allow_stdin)


if __name__ == '__main__':
        
    IPKernelApp.launch_instance(kernel_class=CustomKernel)
