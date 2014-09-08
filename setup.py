import os

from setuptools import setup, find_packages
from distutils.extension import Extension
from Cython.Distutils import build_ext
from glob import glob

long_description = """.. -*-rst-*-

MDF - Data Flow Programming Toolkit
=======================================

"""

version = '2.2.1'
cython_profile = False
cdebug = False

with open("requirements.txt", "rb") as f:
    requirements = f.read().split(os.linesep)

if __name__ == "__main__":

    extra_compile_args = []
    extra_link_args = []
    if cdebug:
        extra_compile_args = ["/Zi"]
        extra_link_args = ["/DEBUG"]

    ext_modules = [
        Extension("mdf.context", ["mdf/context.py"]),
        Extension("mdf.nodes", ["mdf/nodes.py"]),
        Extension("mdf.nodetypes", ["mdf/nodetypes.py"]),
        Extension("mdf.ctx_pickle", ["mdf/ctx_pickle.py"]),
        Extension("mdf.cqueue", ["mdf/cqueue.py"]),
    ]

    for e in ext_modules:
        e.pyrex_directives = {"profile": cython_profile}
        e.extra_compile_args.extend(extra_compile_args)
        e.extra_link_args.extend(extra_link_args)

    setup(
        name='mdf',
        version=version,
        description='MDF - Data Flow Programming Toolkit',
        long_description=long_description,
        zip_safe=False,

        # The icons directory is not a python package so find_packages will not find it.
        packages=find_packages() + ["mdf.viewer.icons"],
        package_data={'mdf.viewer.icons': ['*.ico']},
        test_suite='nose.collector',
        setup_requires=[],
        scripts=glob("bin/*.py"),
        install_requires=requirements,
        extras_require={
            'win32': ['pywin32'],
            'linux2': []
        },
        cmdclass={"build_ext": build_ext},
        ext_modules=ext_modules,
    )
