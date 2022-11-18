# Watchful Python Package for Interacting with Watchful Environment

## Overview
The current Watchful features include the following:
- Watchful API
<!---
- Data Enrichment
--->

## Steps

### Create Python Enviroment
Create a Python virtual environment and activate it. The following are a few ways to do it:
- [`pyenv`](https://github.com/pyenv/pyenv)
- [`conda`](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)
- [`venv`](https://docs.python.org/3/library/venv.html)
- [`virtualenv`](https://virtualenv.pypa.io/en/latest/)

For more steps on creating Python environments, refer to [Watchful SDK repository](https://github.com/Watchfulio/watchful-py/blob/main/README_PY_ENV.md) or `README_PY_ENV.md` in the package directory.

### Install Watchful Package
Install the built package. Add `--force-reinstall` to guarantee a full (re)installation.
```command
pip3 install watchful[==your.desired.version] [--force-reinstall]
```
Show the installed Watchful package.
```command
pip3 show watchful
```

### Run Tests
After you've done the preceding steps correctly, you will be able to print the version of the installed Watchful package.
```command
python3 -c 'import watchful; print(watchful.__version__);'
```

### Examples
After you have installed the Watchful package, you can use it to empower your workflow in your Watchful application. Refer to our [GitHub examples](https://github.com/Watchfulio/watchful-py/tree/main/examples) to get started.
> Please contact a [Watchful representative](mailto:sales@watchful.io) for the Watchful application.

### Documentation
Click the following to view the Watchful package documentation:
- [Latest Watchful package release](https://watchful.readthedocs.io/en/stable/)
- [Latest on Watchful GitHub](https://watchful.readthedocs.io/en/latest/)
- [All Watchful package releases](https://readthedocs.org/projects/watchful/)
