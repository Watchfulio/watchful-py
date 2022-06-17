# Watchful Python Package for Interacting with Watchful Environment
<br>

## Overview
This project serves to publish the Watchful Python Package, so that manual distribution can be discontinued and users can install it into their Python environment in their machines over the internet from PyPI. This also helps in the automation of Watchful Python features when used with the product. 

The current features include the following and their corresponding user guides:
- Watchful API
  <br>https://github.com/Watchfulio/watchful/blob/main/py_api/api_intro.ipynb
- Data Enrichment 
  <br>https://github.com/Watchfulio/watchful/blob/main/py_api/readme.md
<br><br>

## Steps

### Create Python Enviroment
Refer to https://github.com/Watchfulio/watchful/tree/main/backend/active_learning#running-standalone-active-learning-tools
for detailed options. 

Assuming you chose _Conda_, follow these steps to set up the environment:
```
conda create --name=watchful_py python=3.8.12
```
```
conda activate watchful_py
```
<br>

### Upgrade Tools
Upgrade pip, build and test tools.
```
python3 -m pip install --upgrade pip==22.1.2
```
```
python3 -m pip install --upgrade build==0.8.0
```
```
pip3 install pylint==2.13.9 pylama==8.3.8 black==22.3.0 pytest==7.1.1 nbval==0.9.6
```
<br>

### Code Practices
If contributing to this code, you're encouraged to apply the following code practices.
```
cd to/the/repo_directory/that/being/watchful-py
```
```
pylama src/watchful/api.py -o pylama.ini
```
```
black src/watchful/api.py --config=pyproject.toml
```
<br>

### Build & Install Watchful Package Locally
Build the package.
```
python3 -m build .
```
Install the built package. Add `--force-reinstall` to guarantee a (re)installation if you had installed Watchful previously.
```
pip3 install dist/watchful-<latest.release.version>-py3-none-any.whl [--force-reinstall]
```
`<latest.release.version>` can be found [here](./src/watchful/VERSION).
<br><br>
Show the installed watchful package.
```
pip3 list | grep 'Package\|watchful'
```
<br>

### Use Watchful Package in Development Mode (Optional)
Interactively use or test package while in development mode.
```
pip3 install -e . [--force-reinstall]
```
<br>

### Install Watchful Package from PyPI (Optional)
Install your desired release version. The releases can be found at https://pypi.org/project/watchful/.
```
pip3 install watchful[==your.desired.version] [--force-reinstall]
```
<br>

### Run Tests
After you've done the preceding steps correctly, you will be able to see the following:
```
python3 -c 'import watchful; print(watchful.__version__);'
```
The version for Watchful Package is printed on screen.
```
pytest -W ignore::DeprecationWarning tests/test_*.py -v
```
Screen outputs the tests as passing.
<br>
