import os

THIS_FILE_PATH = os.path.dirname(os.path.abspath(__file__))

__version__ = open(os.path.join(THIS_FILE_PATH, "VERSION"), "r").read()
