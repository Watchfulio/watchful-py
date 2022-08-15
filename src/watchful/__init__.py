"""
Initializes "watchful" as a module.
"""
################################################################################


import os
THIS_FILE_PATH = os.path.dirname(os.path.abspath(__file__))
__version__ = open(os.path.join(THIS_FILE_PATH, "VERSION"), "r").read()


"""
Need to be careful of unintended overriding, and also a deeper consideration
into what should be made directly available in the "watchful" namespace.
"""
from .client import *
from .attributes import *
from .enrich import main
from .enricher import *
