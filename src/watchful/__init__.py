"""
Initializes ``watchful`` as a module.
"""
################################################################################


import os
import sys

# We need to be careful of unintended overriding, and also a deeper
# consideration into what should be made directly available in the ``watchful``
# namespace.
from .client import *
from .attributes import *
from .enrich import main
from .enricher import *
