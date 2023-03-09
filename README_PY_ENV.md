# Creating Python Environment

The following are a few ways to create Python environments:
- [`pyenv`](https://github.com/pyenv/pyenv)
- [`conda`](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)
- [`venv`](https://docs.python.org/3/library/venv.html)
- [`virtualenv`](https://virtualenv.pypa.io/en/latest/)

The steps for `pyenv` and `conda` are provided below.

## pyenv

- Install `pyenv`
```command
brew install pyenv
```
- Create Python environment
```command
pyenv install 3.8.12
```
> We create Python 3.8.12 above.
- Add your environment to `PATH`
```command
export PATH=~/.pyenv/versions/3.8.12/bin:$PATH
```
- Check that `python3` and `pip3` refer to your environment
```command
which python3 && which pip3
```
> This should give:
> ```command
> ~/.pyenv/versions/3.8.12/bin/python3
> ~/.pyenv/versions/3.8.12/bin/pip3
> ```
- Check that the installed Python version is the one specified
```command
python3 -V
```
> This should give:
> ```command
> Python 3.8.12
> ```
- From this point, you can continue using your environment to install your Python packages and run your Python code.

## conda
- Install [`anaconda`](https://conda.io/projects/conda/en/latest/user-guide/install/index.html)
- Create Python environment
```command
conda create --name=watchful_py python=3.8.12
```
> We create Python 3.8.12 above.
- Activate Python environment
```command
conda activate watchful_py
```
- From this point, you can continue using your environment to install your Python packages and run your Python code.
