# Watchful Python Package for Interacting with Watchful Environment
<br>

## Overview
This project serves to publish the Watchful Python Package, so that manual distribution can be discontinued and users can install it into their Python environment in their machines over the internet from PyPI. This also helps in the automation of Watchful Python features when used with the product. 

The current features include the following and their corresponding user guides:
- Watchful API and [notebook](https://github.com/Watchfulio/watchful-py/blob/main/examples/api_intro.ipynb)
<!---
- [Data Enrichment](https://github.com/Watchfulio/watchful-py/blob/main/examples/README.md) and [notebook](https://github.com/Watchfulio/watchful-py/blob/main/examples/enrichment_intro.ipynb)
--->

## Steps

### Create Python Enviroment
Refer [here](https://github.com/Watchfulio/watchful-py/blob/main/README_PY_ENV.md) for options.

### Upgrade Tools
- Upgrade or install `pip`, build and test tools
```command
pip3 install pip==22.2.2
```
```command
pip3 install build==0.8.0
```
```command
pip3 install pylint==2.13.9 pylama==8.3.8 black==22.3.0 pytest==7.1.1 nbval==0.9.6
```

### Code Practices
If contributing to this code, you're encouraged to apply the following code practices.
- Apply linting and formatting
```command
cd to/the/repo_directory/that/being/watchful-py
```
```command
pylama src/watchful/ -o pylama.ini
```
```command
black src/watchful/ --config=pyproject.toml --diff
```

### Build & Install Watchful Package Locally
- Build the package
```command
python3 -m build .
```
- Install the built package (add `--force-reinstall` to guarantee a full (re)installation)
```command
pip3 install dist/watchful-<latest.release.version>-py3-none-any.whl [--force-reinstall]
```
`<latest.release.version>` can be found [here](./src/watchful/VERSION).
- Show the installed watchful package
```command
pip3 show watchful
```

### Use Watchful Package in Development Mode (Optional)
- Interactively use or test package while in development mode
```command
pip3 install -e . [--force-reinstall]
```

### Install Watchful Package from PyPI (Optional)
- Install your desired release version (they can be found at [PyPI](https://pypi.org/project/watchful/))
```command
pip3 install watchful[==your.desired.version]
```

### Run Tests
After you've done the preceding steps correctly, you will be able to see the following.
- Print Watchful version
```command
python3 -c 'import watchful; print(watchful.__version__);'
```
- Run tests
```command
pytest -W ignore::DeprecationWarning tests/test_*.py -v
```
