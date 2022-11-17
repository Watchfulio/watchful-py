# Watchful Python Package for Interacting with Watchful Environment

## Overview
This project serves to publish the Watchful Python Package, so that manual distribution can be discontinued and users can install it into their Python environment in their machines over the internet from PyPI. This also helps in the automation of Watchful Python features when used with the product. 

The current features include the following and their corresponding user guides:
- Watchful API and [notebook](https://github.com/Watchfulio/watchful-py/blob/main/examples/api_intro.ipynb)
<!---
- [Data Enrichment](https://github.com/Watchfulio/watchful-py/blob/main/examples/README.md) and [notebook](https://github.com/Watchfulio/watchful-py/blob/main/examples/enrichment_intro.ipynb)
--->

## Steps (Use)

### Create Python Enviroment
- Refer [here](https://github.com/Watchfulio/watchful-py/blob/main/README_PY_ENV.md) for options.

### Install Watchful Package from PyPI
- Install your desired release version (they can be found at [PyPI](https://pypi.org/project/watchful/))
```command
pip3 install watchful[==your.desired.version]
```
- Go through Watchful API introduction [notebook](https://github.com/Watchfulio/watchful-py/blob/main/examples/api_intro.ipynb).

## Steps (Development)

### Create Python Enviroment
- Refer [here](https://github.com/Watchfulio/watchful-py/blob/main/README_PY_ENV.md) for options.

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

## Steps (Release)
After you've done the preceding steps correctly, you will be able to publish Watchful Package to PyPI. Before cutting a release, ensure the release changes are communicated with engineering@watchful.io. There may be corresponding changes required in the Watchful application; do not worry about this as it will be taken care of by Watchful.

### Prepare Release
1. Ensure your `main` branch is updated and create a new branch:
```command
git checkout main && git pull && git checkout -b bump-version
```
2. Bump the version in the [version file](https://github.com/Watchfulio/watchful-py/blob/main/src/watchful/VERSION):
- e.g. from 1.0.0 to 1.0.1
3. Commit and push the branch to Github:
```command
git add src/watchful/VERSION && git commit -m 'bump version' && git push -u origin bump-version
```
4. Open a pull request in Github and merge it to `main` after it is approved.

### Create Release
1. Visit [Watchful Python Package Releases](https://github.com/Watchfulio/watchful-py/releases) in Github
2. Click `Draft a new release`
3. Click `Choose a tag`
   - Type in v*a.b.c* where _a.b.c_ is the version in the [version file](https://github.com/Watchfulio/watchful-py/blob/main/src/watchful/VERSION) (e.g. v1.0.1 if the version in the version file is 1.0.1)
   - Type in the release title, which is the same value as above, i.e. v*a.b.c*
4. Ensure `Target` is set to `main` (should be default)
5. Ensure `Previous tag` is set to the most current release tag (should be default)
6. Click `Generate release notes`
7. Click `Publish release`

The release will be triggered and CI will automatically build and publish the Watchful Python Package to [PyPI](https://pypi.org/project/watchful/).
