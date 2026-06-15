---
name: "Pants Build System"
description: "Expert guidance on using Pants build system for Python projects, focusing on optimal caching, test execution, and target-based workflows."
version: "2.0.0"
---

# Pants Build System

You are a Pants build system expert helping developers maximize build performance and leverage Pants' advanced caching capabilities through proper target-based workflows.

## What is Pants?

Pants is a modern build system providing:
- **Fine-grained caching** - File-level dependency tracking for maximum cache hits
- **Parallel execution** - Concurrent builds and tests across all CPU cores
- **Dependency inference** - Automatic dependency detection without manual BUILD maintenance
- **Hermetic builds** - Reproducible results across machines

## The #1 Critical Rule

### ALWAYS Use Target Addresses, NEVER File Paths

This is the most important concept in Pants. Understanding this prevents 80% of common mistakes.

**Target addresses and file paths create SEPARATE caches**:

```bash
# ✅ CORRECT: Uses target cache, maximizes cache hits
pants test epistemix_platform:src-tests

# ❌ WRONG: Creates separate cache, loses memoization benefits
pants test epistemix_platform/tests/test_something.py
```

**Why This Matters:**

When you run `pants test epistemix_platform:src-tests`:
1. First run executes all tests and caches results per file
2. Subsequent runs only re-run tests affected by changed files
3. Unchanged tests return cached results instantly

**File-path invocations break this** because they create different cache keys, losing all accumulated cache benefits.

**When file paths are acceptable:** Only for one-off debugging sessions. Always return to target-based execution for normal development.

> **Deep dive**: [caching-deep-dive.md](reference/caching-deep-dive.md)

## Essential Commands

### Running Tests
```bash
pants test ::                           # All tests (top-level cache)
pants test epistemix_platform::         # Component tests
pants test epistemix_platform:src-tests # Specific target (PREFERRED)

# Pass arguments to pytest with --
pants test epistemix_platform:src-tests -- -vv  # Verbose
pants test epistemix_platform:src-tests -- -k test_user  # Pattern
pants test epistemix_platform:src-tests -- -x  # Stop on first failure
```

### Code Quality
```bash
pants fmt ::              # Format all code
pants lint ::             # Lint all code
pants fmt lint ::         # Both together
pants fmt --changed-since=HEAD  # Only changed files
```

### Building
```bash
pants package epistemix_platform:epistemix-cli  # Build PEX binary
```

### Dependency Management
```bash
pants generate-lockfiles  # Update all lockfiles
pants export --resolve=epistemix_platform_env  # Export to virtualenv
```

> **Complete command reference**: [command-reference.md](reference/command-reference.md)

## Target Specifications

### The :: Wildcard
```bash
pants test ::               # All targets in repository
pants test epistemix_platform::  # All targets in directory + subdirs
```

### Specific Targets
```bash
pants test epistemix_platform:src-tests  # Named target in BUILD file
```

### Listing Targets
```bash
pants list epistemix_platform::  # List all targets
```
