"""
Build script for Cython-optimized seed data generators.

Usage:
    python build_cython.py build_ext --inplace

Produces _seed_generators.*.pyd (Windows) or .so (Linux/macOS) in this directory.
"""

from setuptools import setup
from Cython.Build import cythonize

setup(
    ext_modules=cythonize(
        "_seed_generators.pyx",
        compiler_directives={
            "language_level": "3",
            "boundscheck": False,
            "wraparound": False,
        },
    ),
)
