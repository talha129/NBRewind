# from setuptools import setup, find_packages

# setup(
#     name='nbrewind',
#     version='0.1',
#     packages=find_packages(),
#     install_requires=[],
#     entry_points={
#         'console_scripts': [
#             'nbrewind=nbrewind.cli:main',
#         ],
#     },
#     author='Your Name',
#     description='A CLI tool to audit and repeat Jupyter notebooks',
#     classifiers=[
#         'Programming Language :: Python :: 3',
#         'Environment :: Console',
#         'Operating System :: OS Independent',
#     ],
#     # python_requires='<=3.1',
# )

from setuptools import setup, find_packages
from setuptools.command.install import install
import subprocess
import os

class InstallWithKernels(install):
    def run(self):
        # First run the normal install
        install.run(self)
        
        # Then install the kernels
        kernel_script = os.path.join(os.path.dirname(__file__), 'install_kernels.py')
        subprocess.run(['python', kernel_script], check=True)

setup(
    name='nbrewind',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'ipykernel',
        'ipyflow',
        'dill',
        'nbformat',
        'jupyter_client',
        'sciunit2'
    ],
    entry_points={
        'console_scripts': [
            'nbrewind=nbrewind.cli:main',
        ],
    },
    cmdclass={
        'install': InstallWithKernels,
    },
    author='Your Name',
    description='A CLI tool to audit and repeat Jupyter notebooks',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Environment :: Console',
        'Operating System :: OS Independent',
    ],
)