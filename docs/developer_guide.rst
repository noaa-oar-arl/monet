MONET Developer Guide
====================

This guide is for contributors and maintainers of MONET. It covers project structure, design philosophy, contribution workflow, and best practices.

Project Structure
-----------------

- ``monet/`` — Core package code
  - ``accessors/`` — xarray and pandas accessors
  - ``plots/`` — Plotting utilities
  - ``util/`` — Utility functions
- ``tests/`` — Unit and integration tests
- ``docs/`` — Documentation (Sphinx)
- ``sample_figures/`` — Example output

Design Philosophy
-----------------

- **Extensibility:** Use accessors to add methods to xarray and pandas objects without modifying their core classes.
- **Modularity:** Keep plotting, regridding, and utility code in separate modules.
- **Interoperability:** Support common data formats and conventions (e.g., CF, COARDS).
- **Performance:** Use parallelization and efficient libraries (e.g., xesmf, pyresample) where possible.

How to Contribute
-----------------

1. **Fork and Clone:**
   - Fork the repo and clone your fork.
2. **Create a Branch:**
   - Use a descriptive branch name (e.g., ``feature/add-new-accessor``).
3. **Implement Changes:**
   - Add or modify code in the appropriate module.
   - Add docstrings and usage examples.
4. **Testing:**
   - Add or update tests in ``tests/``.
   - Run tests locally with ``pytest``.
5. **Linting and Formatting:**
   - Use ``pre-commit`` hooks for linting, formatting, and style checks.
   - Run ``pre-commit run --all-files`` before committing.
6. **Documentation:**
   - Update or add documentation in ``docs/``.
   - Add usage examples and API references.
7. **Pull Request:**
   - Open a PR to the ``stable`` branch.
   - Fill out the PR template and describe your changes.

Code Review and Merging
-----------------------
- All PRs require review by a maintainer.
- Ensure all tests pass and documentation builds without errors.
- Squash and merge when approved.

Best Practices
--------------
- Write clear, concise docstrings and comments.
- Prefer pure functions and avoid side effects.
- Use type hints where possible.
- Keep functions small and focused.
- Add tests for new features and bug fixes.

Design Decisions
----------------
- Accessors are used to extend xarray and pandas objects for seamless integration.
- Regridding uses xesmf (ESMF) and pyresample for flexibility and performance.
- Plotting is built on Cartopy and Matplotlib for high-quality geospatial visualizations.

Getting Help
------------
- Open an issue on GitHub for bugs, questions, or feature requests.
- Join the discussion on the project's GitHub Discussions page.

Release Process
---------------
- Update the version in ``pyproject.toml``.
- Tag the release and push to GitHub.
- Build and upload to PyPI.

License
-------
MONET is licensed under the MIT License. See ``LICENSE`` for details.
