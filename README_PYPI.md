# Watchful Python Package for Interacting with Watchful Environment
<br>

## Overview
The current Watchful features include the following:
- Watchful API
- Data Enrichment 
<br><br>

## Steps

### Create Python Enviroment
Create a Python virtual environment and activate it. The following are a few ways to do it:
- [`pyenv`](https://github.com/pyenv/pyenv)
- [`conda`](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)
- [`venv`](https://docs.python.org/3/library/venv.html)
- [`virtualenv`](https://virtualenv.pypa.io/en/latest/)

For more steps on creating Python environments, refer to [Watchful SDK repository](https://github.com/Watchfulio/watchful-py/blob/main/README_PY_ENV.md) or `README_PY_ENV.md` in the package directory.
<br><br>

### Install Watchful Package
Install the built package. Add `--force-reinstall` to guarantee a (re)installation if you had installed Watchful previously.
```
pip3 install watchful[==your.desired.version] [--force-reinstall]
```
Show the installed Watchful package.
```
pip3 list | grep 'Package\|watchful'
```
<br>

### Run Tests
After you've done the preceding steps correctly, you will be able to see the following:
```
python3 -c 'import watchful; print(watchful.__version__);'
```
The version for Watchful Package is printed on screen.
<br>
