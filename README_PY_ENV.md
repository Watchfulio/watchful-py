# Getting started

Watchful-py uses `hatch` to manage the python project. To install `hatch` use `brew install hatch`. From there, everything is relatively straight-forward.

- To build wheels and sdist, run `hatch build`
- To run tests, run `hatch run test`. This will run tests on all supported python versions.
- To run all non-test checks, including formatting, linting, etc, run `hatch run check`.
