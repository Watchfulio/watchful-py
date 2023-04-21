"""Watchful python library"""

# We need to be careful of unintended overriding, and also a deeper
# consideration into what should be made directly available in the ``watchful``
# namespace.
from .client import *  # noqa: F403
from .attributes import *  # noqa: F403
from .enrich import main  # noqa: F401
from .enricher import *  # noqa: F403
