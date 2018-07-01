from distutils.core import setup
import py2exe

import numpy
import os
import sys

def numpy_dll_paths_fix():
    paths = set();
    np_path = numpy.__path__[0];
    for dirpath, _, filenames in os.walk(np_path):
        for item in filenames:
            if item.endswith('.dll'):
                paths.add(dirpath)
    sys.path.append(*list(paths))

numpy_dll_paths_fix();


exe1 = {
    "script": "wx_main.py",
    "icon_resources": [(0, "a.ico")],
    }
exe2 = {
    "script": "psw_main.py",
    "icon_resources": [(0, "Gem.ico")],
    }
options = {
    'py2exe':{
        'compressed': 1,
        'optimize': 2,
        'bundle_files': 1,
        'dll_excludes':['MSVCP90.dll'],
        'includes':['lxml.etree','lxml._elementpath','gzip'],
        }
    }

setup(windows=[exe1,exe2],options=options);
#setup(console=['helloworld.py'])
#setup(windows=['wx_main.py','psw_main.py'],options={'py2exe':{'compressed': 1,'optimize': 2, 'bundle_files': 1,'dll_excludes':['MSVCP90.dll'],'includes':['lxml.etree','lxml._elementpath','gzip'],}})
#setup(windows=['wx_main.py'],options={'py2exe':{'dll_excludes':['MSVCP90.dll'],'includes':['lxml.etree','lxml._elementpath','gzip'],}})
