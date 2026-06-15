# Advanced Topics

## When to Read This

Read this reference when you need to understand:
- Test batching configuration and trade-offs
- Multiple Python dependency resolves (resolves)
- CI/CD pipeline optimization
- Using `--changed-since` effectively
- Advanced dependency management
- Performance tuning and troubleshooting

## Test Batching

### Default Behavior: One Process Per File

By default, Pants runs **each test file in a separate process**.

**Example:**
```bash
pants test epistemix_platform:src-tests
# If src-tests has 20 test files, Pants spawns 20 processes
```

**Pros:**
- **Fine-grained caching**: Each file cached independently
- **Better parallelism**: Distributes work across all CPU cores
- **Isolation**: Test failures don't affect other files

**Cons:**
- **Fixture overhead**: Package/session-scoped fixtures execute per file
- **Setup costs**: Each process pays interpreter startup cost

### When Fixtures Are Expensive

If you have expensive session/package-scoped fixtures:

```python
# conftest.py
@pytest.fixture(scope="session")
def database():
    """Set up test database - expensive operation."""
    db = create_test_database()
    run_migrations(db)
    seed_test_data(db)
    yield db
    teardown_database(db)
```

**Default behavior:** This fixture runs **once per test file**, not once per test suite.

### Enabling Test Batching

Mark tests as batch-compatible:

```python
python_tests(
    name="src-tests",
    sources=["tests/**/*.py"],
    batch_compatibility_tag="expensive-fixtures",
)
```

Configure batch size in `pants.toml`:

```toml
[test]
batch_size = 10
```

**Behavior:**
- Tests are grouped into batches of 10 files
- Each batch runs in single pytest process
- Session-scoped fixtures run once per batch

### Batching Trade-offs

**Benefits:**
- Session fixtures run less frequently
- Reduced process overhead
- Lower memory usage

**Costs:**
- **Coarser caching**: If any file in batch changes, entire batch re-runs
- **Less parallelism**: Fewer processes to distribute across CPUs
- **Potential coupling**: Tests in same batch share process state

### Optimal Batch Size

**Small batches (5-10 files):**
- Better cache granularity
- More parallelism
- More fixture executions

**Large batches (20-50 files):**
- Fewer fixture executions
- Coarser cache granularity
- Less parallelism

**Recommendation:** Start with 10 and adjust based on:
- Fixture setup cost
- Number of test files
- Available CPU cores

### Example Configuration

```python
# Fast unit tests - don't batch (default)
python_tests(
    name="unit-tests",
    sources=["tests/unit/**/*.py"],
    # No batch_compatibility_tag - runs one file per process
)

# Integration tests with expensive database fixture - batch them
python_tests(
    name="integration-tests",
    sources=["tests/integration/**/*.py"],
    batch_compatibility_tag="database-tests",
)
```

```toml
# pants.toml
[test]
batch_size = 15  # 15 files per batch for tagged tests
```

## Multiple Python Resolves

### What is a Resolve?

A **resolve** is a set of Python dependencies with a dedicated lockfile.

Think of it as an **isolated Python environment** for a specific purpose.

### Why Multiple Resolves?

**Separation of Concerns:**
- Application code dependencies
- Infrastructure/deployment tools
- Developer tools

**Example from this project:**

```toml
[python.resolves]
epistemix_platform_env = "epistemix_platform/epistemix_platform_env.lock"
infrastructure_env = "epistemix_platform/infrastructure/infrastructure_env.lock"
tcr_env = "tcr/tcr_env.lock"
```

**Use cases:**

1. **epistemix_platform_env**: Flask, SQLAlchemy, Pydantic, pytest
   - Main application dependencies
   - Used by most targets

2. **infrastructure_env**: Sceptre, Boto3, CloudFormation templates
   - Infrastructure deployment tools
   - Only used by infrastructure tests and scripts

3. **tcr_env**: Specific dependencies for TCR tool
   - Isolated from main application
   - No risk of version conflicts

### Benefits of Separation

**1. Avoid Dependency Conflicts**
```
epistemix_platform_env: Boto3 1.28.0 (stable, application needs)
infrastructure_env: Boto3 1.34.0 (latest, infrastructure tools need)
```

No conflict! Each resolve has its own version.

**2. Smaller Lockfiles**
- `epistemix_platform_env.lock`: ~200 dependencies
- `infrastructure_env.lock`: ~50 dependencies
- `tcr_env.lock`: ~10 dependencies

Smaller lockfiles = faster dependency resolution.

**3. Clearer Dependencies**

```python
# Application test - uses epistemix_platform_env
python_tests(
    name="src-tests",
    sources=["tests/**/*.py"],
    resolve="epistemix_platform_env",
)

# Infrastructure test - uses infrastructure_env
python_tests(
    name="infrastructure-tests",
    sources=["infrastructure/tests/**/*.py"],
    resolve="infrastructure_env",
)
```

**Clear separation:** Application tests can't accidentally import infrastructure tools.

### Working with Resolves

#### Generating Lockfiles

```bash
# Generate all lockfiles
pants generate-lockfiles

# Generate specific resolve
pants generate-lockfiles --resolve=shared-lock
```
