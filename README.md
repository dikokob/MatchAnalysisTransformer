# Refactoring match analysis transformer

Contains migrated code, in addition to tests and a makefile to run full setup through to tests and linting.



## Quick start

For the migration task, a test driven methodology was taken. Basically, write a test for the old/original code. When it passes.
Duplicate those exact tests and change the functions & methods called to the new code. When the new codes tests pass, you can be sure the code produces the same work.

Note: This does depend on the quality of the tests, so tests that don't fully test all edge cases result in missed evaluation and comparision between old and new code.

Continue below to run the tests and confirm the task completion.

#### setup your development environment

- Create a virtualenv with python 3.7 or newer. I have used either [virtualenv & pip](https://virtualenv.pypa.io/en/latest/installation.html) or [pipenv](https://docs.python-guide.org/dev/virtualenvs/)

#### Running tests

To confirm that all code runs as expected run the following:

```
# this will install the requirements.txt & run the tests
# all green is all good

make build

```

#### Inspect the tests

Check what is being tested in the `tests/` directory.

## References

- [Why virtual environments in python](https://realpython.com/python-virtual-environments-a-primer/)
- [Dealing with OS paths between MAC & Windows & LINUX](https://medium.com/@ageitgey/python-3-quick-tip-the-easy-way-to-deal-with-file-paths-on-windows-mac-and-linux-11a072b58d5f)
