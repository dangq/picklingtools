

from distutils.core import setup, Extension
setup(name="pyobjconvert", version="1.0",
      ext_modules=[
    Extension("pyobjconvertmodule",
              ["pyobjconvertmodule.cc", "pyobjconverter.cc"],
              include_dirs=['../C++', '../C++/opencontainers_1_7_6/include']),
    ])

