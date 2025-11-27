====================================
Australian Synchrotron Release Steps
====================================

The build process is not currently automated as the codebase is public but the pypi repo is private.
We are not currently publishing a public package.

One day we will hopefully automate this.

Steps
-----

First make a commit with the version and tag it

:: 
  
  $ git commit -m "REL: v0.11.0"
  $ git push
  $ git tag v0.11.0
  $ git push --tags

This repo doesn't currently use poetry or uv, so use a venv to install the build dependencies

:: 
  
  $ python -m venv .venv
  $ . .venv/bin/activate
  $ pip install wheel twine setuptools
  $ python setup.py sdist bdist_wheel

Log into our local pypi repo

:: 
  
  $ devpi login <usename>
  $ devpi use <asci prod index>

Try it out without actually uploading anything

:: 
  
  $ devpi upload --dry-run --from-dir dist/

If that looks good, actually upload it.

:: 
  
  $ devpi upload --from-dir dist/
