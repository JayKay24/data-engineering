# Pants Command Reference

## When to Read This

Read this reference when you need:
- Quick lookup of Pants command syntax and options
- Examples of common command patterns
- Understanding the difference between Pants goals
- Reference for passing arguments to underlying tools (pytest, ruff, etc.)

## Core Goals

### test - Run Tests

Execute Python tests using pytest under the hood.

**Basic Usage:**
```bash
# Run all tests in repository
pants test ::

# Run all tests in a component
pants test epistemix_platform::

# Run specific test target
pants test epistemix_platform:src-tests

# Run tests in a subdirectory
pants test epistemix_platform/tests::
```

**Key Options:**
```bash
# Test only code affected by changes
pants test --changed-since=main
pants test --changed-since=HEAD~1

# Force all tests to run (ignore cache)
pants test --force epistemix_platform:src-tests

# Show detailed output
pants test --debug epistemix_platform:src-tests
```

**Passing Arguments to pytest:**

Use `--` to separate Pants arguments from pytest arguments:

```bash
# Verbose output
pants test epistemix_platform:src-tests -- -vv

# Run specific test by name pattern
pants test epistemix_platform:src-tests -- -k test_user_login

# Stop on first failure
pants test epistemix_platform:src-tests -- -x

# Show print statements (no capture)
pants test epistemix_platform:src-tests -- -s

# Run with coverage
pants test epistemix_platform:src-tests -- --cov=epistemix_platform --cov-report=html

# Combine multiple pytest options
pants test epistemix_platform:src-tests -- -vv -x -k test_user

# Run with pytest-xdist parallel execution
pants test epistemix_platform:src-tests -- -n auto
```

### fmt - Format Code

Format code using Ruff formatter (Black-compatible).

**Basic Usage:**
```bash
# Format all code
pants fmt ::

# Format specific component
pants fmt epistemix_platform::

# Format only changed files
pants fmt --changed-since=HEAD

# Format specific directories
pants fmt epistemix_platform/src::
pants fmt simulation_runner::
```

**Key Options:**
```bash
# Show what would be formatted without making changes
pants fmt --check ::

# Format only Python files (when project has multiple languages)
pants fmt --only=ruff ::
```

### lint - Lint Code

Lint code using Ruff (replaces pylint, flake8, isort, etc.).

**Basic Usage:**
```bash
# Lint all code
pants lint ::

# Lint specific component
pants lint epistemix_platform::

# Lint only changed files
pants lint --changed-since=HEAD

# Lint specific directories
pants lint epistemix_platform/src::
```

**Key Options:**
```bash
# Only report issues, don't fail the build
pants lint --only=ruff ::

# Lint with specific rule set (configured in pyproject.toml)
pants lint epistemix_platform::
```

**Combined Format and Lint:**
```bash
# Run both fmt and lint together (efficient)
pants fmt lint ::
pants fmt lint --changed-since=HEAD
pants fmt lint epistemix_platform::
```

### package - Build Artifacts

Build PEX binaries and other artifacts.
