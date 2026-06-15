# Target Specifications in Pants

## When to Read This

Read this reference when you need to understand:
- How to address targets in BUILD files
- The difference between `:` and `::` syntax
- How to structure BUILD files for optimal caching
- Target naming conventions and best practices
- How to create and organize multiple test targets

## What is a Target?

A **target** is an addressable unit of metadata describing code in your project. Targets are defined in `BUILD` files and have:

- **Type**: What kind of target (e.g., `python_tests`, `pex_binary`, `python_sources`)
- **Name**: A unique identifier within the BUILD file
- **Sources**: File patterns the target owns
- **Dependencies**: Other targets this target depends on
- **Configuration**: Target-specific settings

## Target Addressing

### Format

Targets use the format: `path/to/directory:target-name`

**Examples:**
```bash
epistemix_platform:src-tests              # Target named "src-tests" in epistemix_platform/BUILD
epistemix_platform/tests:test-utils       # Target named "test-utils" in epistemix_platform/tests/BUILD
simulation_runner:simulation-runner-cli   # Target named "simulation-runner-cli" in simulation_runner/BUILD
```

### Components of a Target Address

```
epistemix_platform/tests:unit-tests
│                        │
│                        └─ Target name (from BUILD file)
│
└─ Directory path (relative to project root)
```

## The :: Wildcard

The `::` syntax means "this directory and **all subdirectories**".

### Recursive Target Selection

```bash
# All targets in entire repository
pants test ::

# All targets in epistemix_platform and subdirectories
pants test epistemix_platform::

# All targets in tests subdirectory and below
pants test epistemix_platform/tests::

# Multiple directories
pants test epistemix_platform:: simulation_runner::
```

### When to Use ::

Use `::` when you want:
- **Broad execution**: Run all tests in a component
- **Convenience**: Avoid listing individual targets
- **Future-proofing**: Automatically include new targets as they're added

### How :: Interacts with Caching

When you run `pants test epistemix_platform::`:
1. Pants finds all test targets under `epistemix_platform/`
2. Executes each target
3. Caches results per-target
4. On subsequent runs, only re-runs affected targets

**This is why using target addresses (even with ::) is better than file paths.**

## Single : for Specific Targets

Use single `:` to address a specific named target.

### Syntax

```bash
# Specific target in a BUILD file
pants test epistemix_platform:src-tests

# Multiple specific targets
pants test epistemix_platform:src-tests epistemix_platform:infrastructure-tests

# Targets across different directories
pants test epistemix_platform:src-tests simulation_runner:src-tests tcr:src-tests
```

### When to Use Single :

Use single `:` when you:
- **Need precision**: Run only one specific target
- **Have multiple targets in same directory**: Select one among many
- **Want explicit control**: Make clear which target you're running

## BUILD File Structure

### Location

BUILD files are placed in directories containing targets:

```
epistemix_platform/
├── BUILD                     # Targets for this directory
├── src/
│   └── epistemix_platform/
│       └── BUILD             # Targets for source code
└── tests/
    └── BUILD                 # Targets for tests
```

### Basic BUILD File Example

```python
# epistemix_platform/BUILD

python_tests(
    name="src-tests",
    sources=[
        "tests/**/test_*.py",
        "tests/test_*.py",
    ],
    dependencies=[
        "./src/epistemix_platform:lib",
        "./tests:test-utils",
        ":test-reqs#pytest",
        ":test-reqs#pytest-mock",
    ],
)

pex_binary(
    name="epistemix-cli",
    entry_point="epistemix_platform.cli.main:main",
    dependencies=[
        "./src/epistemix_platform:lib",
    ],
)

python_requirements(
    name="test-reqs",
    source="test-requirements.txt",
)
```
