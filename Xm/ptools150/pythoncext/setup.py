
from distutils.core import setup, Extension
setup(name="pyobjconvert", version="1.0",
      ext_modules=[
    Extension("pyobjconvertmodule",
              ["pyobjconvertmodule.cc", "pyobjconverter.cc"],
              include_dirs=['../inc']),
    ])

