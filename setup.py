from setuptools import setup, find_packages

setup(
    name='nbrewind',
    version='0.1',
    packages=find_packages(),
    install_requires=[],
    entry_points={
        'console_scripts': [
            'nbrewind=nbrewind.cli:main',
        ],
    },
    author='Your Name',
    description='A CLI tool to audit and repeat Jupyter notebooks',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Environment :: Console',
        'Operating System :: OS Independent',
    ],
    # python_requires='<=3.1',
)
