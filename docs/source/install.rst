Installation
============

Creating Python Environment
---------------------------

The following are a few ways to create Python environments:

* `pyenv <https://github.com/pyenv/pyenv>`_
* `conda <https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html>`_
* `venv <https://docs.python.org/3/library/venv.html>`_
* `virtualenv <https://virtualenv.pypa.io/en/latest/>`_

For this document, we'll use ``pyenv`` on a Mac.

.. code-block:: bash

    # Install pyenv
    brew install pyenv

    # Create Python environment
    pyenv install 3.8.12

    # Add your environment to `PATH`
    export PATH=~/.pyenv/versions/3.8.12/bin:$PATH

    # Check that `python3` and `pip3` refer to your environment
    which python3 && which pip3

    # This should give:
    # ~/.pyenv/versions/3.8.12/bin/python3
    # ~/.pyenv/versions/3.8.12/bin/pip3

    # Check that the installed Python version is the one specified
    python3 -V

    # This should give:
    # Python 3.8.12

From this point, you can continue using your environment to install your Python packages and run your Python code.

Install from PyPi
-----------------

Install the latest published Watchful SDK. Add `--upgrade` if you currently have
a previous Watchful SDK version installed.

.. code-block:: python

    # To get the latest published version
    pip3 install watchful [--upgrade]

    # Alternatively, install a specific Watchful SDK version if you need to.
    pip3 install watchful[==your.desired.version]

    # Show the installed Watchful SDK.
    pip3 show watchful

After you've done the preceding steps correctly, you will be able to print the
version of the installed Watchful SDK.

.. code-block:: python

    python3 -c 'import watchful; print(watchful.__version__);'
