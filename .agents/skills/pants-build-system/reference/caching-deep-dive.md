# Caching Deep Dive

## When to Read This

Read this reference when you need to understand:
- How Pants caching works at a deep level
- Why target addresses provide better caching than file paths
- Cache key generation and invalidation
- Local vs remote caching strategies
- Troubleshooting cache-related issues
- Optimizing cache hit rates for faster development

## The Critical Principle

**ALWAYS use target addresses, NEVER use file paths for test execution.**

This single principle has the biggest impact on cache effectiveness.

## How Caching Works

### Cache Fundamentals

Pants maintains a **process-level cache** where each test file execution is cached separately:

1. **Input Hashing**: Pants computes a hash of:
   - Test file content
   - All imported files (transitive dependencies)
   - Test configuration
   - Python interpreter version
   - pytest version and configuration

2. **Cache Lookup**: Before running a test, Pants checks if this exact hash exists in cache

3. **Cache Hit**: If found, returns cached result (pass/fail + output)

4. **Cache Miss**: If not found, runs test and caches result

5. **Invalidation**: When any input changes, hash changes, cache misses

### File-Level Granularity

Even when you run `pants test epistemix_platform:src-tests`, Pants caches **per test file**:

```
epistemix_platform:src-tests (target)
├── tests/test_models.py        [cached independently]
├── tests/test_repositories.py  [cached independently]
├── tests/test_api.py           [cached independently]
└── tests/test_utils.py         [cached independently]
```

**If you modify `test_models.py`, only that file re-runs. The others return cached results.**

## Target-Based vs File-Based Caching

### The Problem with File Paths

When you use file paths, Pants creates **different cache keys** than target addresses:

```bash
# Creates cache key: "target:epistemix_platform:src-tests -> file:test_models.py"
pants test epistemix_platform:src-tests

# Creates cache key: "file:epistemix_platform/tests/test_models.py"
pants test epistemix_platform/tests/test_models.py
```

These are **separate cache entries**. Running tests via file path doesn't benefit from target-based cache.

### Example Scenario

**Day 1:**
```bash
pants test epistemix_platform:src-tests
# Runs all 20 test files, caches each
```

**Day 2:**
```bash
# Edit one file
vim epistemix_platform/src/epistemix_platform/models/user.py

pants test epistemix_platform:src-tests
# Only re-runs 3 affected test files
# Returns cached results for 17 unaffected files
# Total time: ~5 seconds instead of ~30 seconds
```

**Day 3 (using file path - WRONG):**
```bash
pants test epistemix_platform/tests/test_models.py
# Creates NEW cache entry
# Doesn't use cached result from Day 1's target-based run
# Must execute test even if nothing changed
```

**Day 4 (back to target - RIGHT):**
```bash
pants test epistemix_platform:src-tests
# Uses cache from Day 1 and Day 2
# Returns cached results for all unchanged files
```

### Why This Happens

Pants uses the **invocation pattern** as part of the cache key to ensure correct behavior:

- **Target-based invocation**: "Run all tests in this target"
- **File-based invocation**: "Run this specific file"

These are semantically different operations, so they get different cache keys.

## Cache Keys in Detail

### What Contributes to a Cache Key

For a test file, the cache key includes:

**1. Source Code**
- Test file content
- All imported production code
- All imported test utilities
- Transitive imports (imports of imports)

**2. Dependencies**
- Python interpreter version
- pytest version
- All pytest plugins (pytest-mock, pytest-cov, etc.)
- Any fixtures from conftest.py

**3. Configuration**
- pytest.ini or pyproject.toml [tool.pytest.ini_options]
- Pants test configuration
- Environment variables that affect test behavior

**4. Invocation Context**
- Target address (if using target)
- File path (if using file)
- pytest arguments (passed after --)

**Example:**
```bash
# Different cache keys due to pytest arguments
pants test epistemix_platform:src-tests -- -v
pants test epistemix_platform:src-tests -- -vv
pants test epistemix_platform:src-tests -- -k test_user
```

### Cache Key Stability

Cache keys are **stable across runs** if inputs don't change:

```bash
# Run 1
pants test epistemix_platform:src-tests
# Cache miss, executes tests, stores results with key K1

# Run 2 (no changes)
pants test epistemix_platform:src-tests
# Key K1 matches, cache hit, returns stored results
```

But **unstable if invocation changes**:

```bash
# Run 1
pants test epistemix_platform:src-tests
# Cache key K1

# Run 2 (different target syntax)
pants test epistemix_platform/tests/test_models.py
# Cache key K2 (different from K1!)
```

## Local vs Remote Caching

### Local Cache

**Location:** `~/.cache/pants/`

**Characteristics:**
- Fast (local disk I/O)
- Private to your machine
