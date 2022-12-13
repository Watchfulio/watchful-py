"""
This script tests Watchful SDK imports.
"""
################################################################################

if __name__ == "__main__":

    import sys

    import watchful as w
    from watchful.client import *  # pylint:disable=W0401,W0614
    from watchful import *  # pylint:disable=W0401,W0614

    print(f"Using Watchful SDK {w.__version__}.")

    sys.exit(0)
