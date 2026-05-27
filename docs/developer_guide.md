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
- **Performance:** Adhere to **backend-agnostic vectorized computation principles**: ensure pipelines support both Eager (NumPy) and Lazy (Dask) evaluation, prioritize vectorization, and never force computation within processing functions.

## The Aero Protocol (Backend-Agnostic Principles) 🍃⚡

MONET adheres to the **Aero Protocol** for architecting scientific pipelines that balance flexibility, maintainability, and provenance.

### 1. Architecture & Compute (The "Optional Dask" Rule)

- **Backend Agnostic**: Write functions that accept generic `xr.DataArray` inputs. Do not assume the data is Dask-backed or NumPy-backed.
- **No Hidden Computes**: NEVER call `.compute()`, `.load()`, or `.values` inside a processing function. This breaks laziness for Dask users.
- **No Forced Chunking**: Do not hardcode `.chunk()` inside functions. Chunking is the user's responsibility (at the I/O stage) or an optional argument.
- **Vectorization**: Use `xarray.apply_ufunc` with `dask='parallelized'` capability to support both backends simultaneously.

### 2. Code Style & Documentation

- **NumPy Docstrings**: EVERY function must have a docstring following the NumPy format (Parameters, Returns, Examples).
- **Type Hinting**: Use `xarray.DataArray` or `xarray.Dataset` types, never specific backend types like `dask.array`.
- **Scientific Hygiene**: Update `ds.attrs['history']` when transforming data. Never drop coordinates.

### 3. Versioning (The SemVer Rule)

- **Semantic Versioning**: MONET follows [Semantic Versioning (SemVer)](https://semver.org/).
    - **MAJOR** version for incompatible API changes.
    - **MINOR** version for functionality added in a backward compatible manner.
    - **PATCH** version for backward compatible bug fixes.
- **Syncing**: Ensure the version is consistent across `pyproject.toml`, `monet/__init__.py`, and `CITATION.cff`.

### 4. Quality & Validation (The "Pre-Commit" Rule)

- **Zero-Trust Coding**: Do not trust your own code until it is tested.
- **Enforcement**: Use `pre-commit run --all-files` before every commit.
- **Testing**: Provide pytest unit tests that verify the logic twice: once with Eager (NumPy) data and once with Lazy (Dask) data.

---

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
