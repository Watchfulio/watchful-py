# Watchful Python SDK for Interacting with Watchful Environment

## Overview

The current Watchful SDK features include the following:

- Watchful API
- Data Enrichment

## Steps

### Create Python Enviroment

Create a Python virtual environment and activate it. The following are a few
ways to do it:

- [`pyenv`](https://github.com/pyenv/pyenv)
- [`conda`](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)
- [`venv`](https://docs.python.org/3/library/venv.html)
- [`virtualenv`](https://virtualenv.pypa.io/en/latest/)

For more steps on creating Python environments, refer to [Creating Python
Environment
](https://github.com/Watchfulio/watchful-py/blob/main/README_PY_ENV.md) in our
Watchful SDK Github repository.

### Install Watchful SDK

Install the latest published Watchful SDK. Add `--upgrade` if you currently have
a previous Watchful SDK version installed.

```python
pip3 install watchful [--upgrade]
```

Alternatively, install a specific Watchful SDK version if you need to.

```python
pip3 install watchful[==your.desired.version]
```

Show the installed Watchful SDK.

```python
pip3 show watchful
```

### Run Tests

After you've done the preceding steps correctly, you will be able to print the
version of the installed Watchful SDK.

```python
python3 -c 'import watchful; print(watchful.__version__);'
```

### Examples

After you have installed the Watchful SDK, you can use it to empower your
workflow in your Watchful application. Refer to our [Watchful examples in GitHub
](https://github.com/Watchfulio/watchful-py/tree/main/examples) for
documentation and Jupyter Notebooks to get started.
> Please contact a [Watchful representative](mailto:sales@watchful.io) for the
Watchful application.

### Documentation

Click the following to view the Watchful SDK documentation:

- [Latest Watchful SDK release](https://watchful.readthedocs.io/en/stable/)
- [Latest on Watchful GitHub](https://watchful.readthedocs.io/en/latest/)
- [All Watchful SDK releases](https://readthedocs.org/projects/watchful/)
