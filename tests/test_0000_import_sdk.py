"""
This script tests Watchful SDK imports.
"""
################################################################################

if __name__ == "__main__":
    import sys

    import watchful  # pylint:disable=W0611
    from watchful.client import *  # pylint:disable=W0401,W0614
    from watchful import *  # pylint:disable=W0401,W0614

    sys.exit(0)
