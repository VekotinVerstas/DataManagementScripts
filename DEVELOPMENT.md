# Development Guide

## Repository Structure

This repository contains multiple independent script collections, each in its own directory. Each directory is designed to be used independently with its own virtual environment.

### Directory-specific Dependencies

Each script directory (e.g., `FmiAPI/`, `AQBurk2IoTHub/`, etc.) manages its own dependencies using:

- **`pyproject.toml`** - Modern Python project configuration with metadata and dependency specifications
- **`requirements.txt`** - Lock file with pinned versions (auto-generated, never edit manually)

### Why This Structure?

- **Isolation**: Scripts run in different environments (dev, production, different servers)
- **Minimal Dependencies**: Each environment only installs what it needs
- **Reproducibility**: Lock files ensure consistent installations across environments
- **Independence**: Each directory can be deployed separately

## Setting Up a New Script Directory

### Option 1: Using `uv` (Recommended)

`uv` is a fast, modern Python package manager written in Rust.

1. **Create `pyproject.toml`** in your directory:

```toml
[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[project]
name = "your-script-name"
version = "0.1.0"
description = "Description of your script"
requires-python = ">=3.13"
dependencies = [
    "httpx",
    "pandas",
    # Add your dependencies here
]

[project.optional-dependencies]
dev = [
    "pytest",
    "ruff",
]
```

2. **Generate lock file**:

```bash
cd YourDirectory/
uv pip compile pyproject.toml -o requirements.txt
```

3. **Create virtual environment and install**:

```bash
uv venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
uv pip sync requirements.txt
```

4. **If using `fvhdms` shared module**:

```bash
uv pip install -e ../fvhdms
```

### Option 2: Using `pip-tools`

```bash
cd YourDirectory/
pip install pip-tools
pip-compile pyproject.toml -o requirements.txt
pip-sync requirements.txt
```

### Option 3: Traditional `pip` (Not Recommended)

If you only have `requirements.txt` without version pinning:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Updating Dependencies

### Adding a New Dependency

1. Edit `pyproject.toml` and add the package to `dependencies`:

```toml
dependencies = [
    "existing-package",
    "new-package",  # Add here
]
```

2. Regenerate lock file:

```bash
uv pip compile pyproject.toml -o requirements.txt
```

3. Update your environment:

```bash
uv pip sync requirements.txt
```

### Upgrading Dependencies

To upgrade all dependencies to their latest compatible versions:

```bash
uv pip compile --upgrade pyproject.toml -o requirements.txt
uv pip sync requirements.txt
```

## Common Patterns

### Shared `fvhdms` Module

Many scripts use the shared `fvhdms` module located in the repository root. Install it in editable mode:

```bash
pip install -e ../fvhdms
# or with uv:
uv pip install -e ../fvhdms
```

### Environment Variables

Some scripts require environment variables or `.env` files:

```bash
# Copy example config
cp config.ini.example config.ini

# Edit with your settings
nano config.ini
```

### Running Scripts in Production

In production environments, clone the repo and set up only the needed directory:

```bash
git clone <repo-url>
cd DataManagementScripts/FmiAPI/
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e ../fvhdms  # if needed
python fmiapi.py --help
```

## Tools Used

### `uv` - Fast Python Package Manager

- Install: `pip install uv` or `curl -LsSf https://astral.sh/uv/install.sh | sh`
- Docs: https://github.com/astral-sh/uv

### `ruff` - Fast Python Linter & Formatter

Configured in root `pyproject.toml`:

```bash
# Lint all files
ruff check .

# Format code
ruff format .

# Auto-fix issues
ruff check --fix .
```

### `pre-commit` - Git Hooks

See `.pre-commit-config.yaml` for automated checks before commits.

## Migrating Existing Directories

To migrate a directory from simple `requirements.txt` to `pyproject.toml`:

1. **Analyze current dependencies**:

```bash
cd YourDirectory/
pip list --format=freeze > current-deps.txt
```

2. **Create `pyproject.toml`** with direct dependencies only (not transitive)

3. **Generate new lock file**:

```bash
uv pip compile pyproject.toml -o requirements.txt
```

4. **Test in fresh environment**:

```bash
uv venv test-env
source test-env/bin/activate
uv pip sync requirements.txt
python your_script.py --help
```

5. **Clean up**:

```bash
deactivate
rm -rf test-env current-deps.txt
```

## Example: FmiAPI Directory

See `FmiAPI/` as a reference implementation:

- ✅ Modern `pyproject.toml` with metadata
- ✅ Auto-generated `requirements.txt` lock file
- ✅ Updated `README.md` with installation instructions
- ✅ Optional dev dependencies for testing

## Questions?

- Check existing directories like `FmiAPI/` for examples
- Review this guide for best practices
- Tools documentation: uv, pip-tools, setuptools
