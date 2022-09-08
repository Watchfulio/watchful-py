"""
Initializes "watchful" as a module.
"""
################################################################################


import os
import sys

# We need to be careful of unintended overriding, and also a deeper
# consideration into what should be made directly available in the "watchful"
# namespace.
from .client import *
from .attributes import *
from .enrich import main
from .enricher import *


THIS_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
with open(
    os.path.join(THIS_FILE_DIR, "VERSION"),
    "r",
    encoding=sys.getdefaultencoding()) as f:
    __version__ = f.readline()
