# MONET Developer Guide

This guide is for contributors and maintainers of MONET. It covers project structure, design philosophy, contribution workflow, and best practices.

## Project Structure

- `monet/` — Core package code
  - `accessors/` — xarray and pandas accessors
  - `plots/` — Plotting utilities
  - `util/` — Utility functions
- `tests/` — Unit and integration tests
- `docs/` — Documentation (MkDocs)
- `sample_figures/` — Example output

## Design Philosophy

- **Extensibility:** Use accessors to add methods to xarray and pandas objects without modifying their core classes.
- **Modularity:** Keep plotting, regridding, and utility code in separate modules.
- **Interoperability:** Support common data formats and conventions (e.g., CF, COARDS).
- **Performance:** Adhere to the **Aero Protocol**: ensure pipelines support both Eager (NumPy) and Lazy (Dask) evaluation, prioritize vectorization, and never force computation within processing functions.

## How to Contribute

1. **Fork and Clone:** Fork the repo and clone your fork.
2. **Create a Branch:** Use a descriptive branch name (e.g., `feature/add-new-accessor`).
3. **Implement Changes:**
   - Add or modify code in the appropriate module.
   - Add docstrings and usage examples.
4. **Testing:**
   - Add or update tests in `tests/`.
   - Run tests locally with `pytest`.
5. **Linting and Formatting:**
   - Use `ruff` for linting and formatting.
   - Use `pre-commit` hooks. Run `pre-commit run --all-files` before committing.
6. **Documentation:**
   - Update or add documentation in `docs/` using Markdown.
   - API references are automatically generated via `mkdocstrings`.
7. **Pull Request:** Open a PR to the `develop` branch.

## Best Practices

- Write clear, concise docstrings (NumPy style).
- Use type hints where possible (PEP 484).
- Ensure all core routines are backend-agnostic (NumPy/Dask) and preserve Dask laziness.

## Getting Help

- Open an issue on GitHub for bugs, questions, or feature requests.
