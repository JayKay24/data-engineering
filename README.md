# Data Engineering Monorepo

This repository is a monorepo containing data-engineering projects and pipelines.

The monorepo is managed using **Pantsbuild (Pants)**, targeting **Python 3.10.x** and utilizing **Ruff** for high-performance linting and formatting.

---

## 🛠️ Tech Stack & Tooling

*   **Build System:** [Pantsbuild](https://www.pantsbuild.org/) (Fast, hermetic, caching, and polyglot-ready)
*   **Target Language:** Python 3.10.9 (configured via `.python-version` and Pyenv)
*   **Format & Lint:** [Ruff](https://github.com/astral-sh/ruff) (unified linter and formatter)
*   **Dependency Management:** Single shared lockfile (`3rdparty/user_reqs.lock`)

---

## 📁 Repository Structure

```text
data-engineering/
├── .gitignore                 # Python, Pants, and OS ignore rules
├── .python-version            # Sets project-local Python to 3.10.9
├── pants                      # Pants launcher binary (scie-pants)
├── pants.toml                 # Main configuration for Pants and tool backends
├── .venv                      # Local symlink pointing to the Pants-generated virtualenv
├── 3rdparty/
│   ├── BUILD                  # Configures global dependency targets
│   ├── requirements.txt       # Lists project requirements (pandas, PySpark, etc.)
│   └── user_reqs.lock         # Pants generated dependency lockfile
└── projects/                  # Directory containing all sub-projects
```

---

## 🚀 Getting Started

### 1. Prerequisites
Ensure you have Pyenv installed and Python `3.10.9` available on your system:
```bash
pyenv install 3.10.9
```

### 2. Set Up the Virtual Environment
Pants manages its own virtual environments, but you can export one symlinked to `.venv` for editor integration (autocompletion, diagnostics, etc.):
```bash
# Export the Pants-managed environment
./pants export --resolve=shared-lock

# Activate it in your terminal
source .venv/bin/activate
```

---

## ⚡ CLI Cheatsheet

Run these commands from the root directory:

| Goal | Command | Description |
| :--- | :--- | :--- |
| **Lint** | `./pants lint ::` | Runs Ruff linter on all directories |
| **Format check** | `./pants fmt --check ::` | Checks formatting without rewriting files |
| **Format** | `./pants fmt ::` | Runs Ruff formatter to auto-fix styling issues |
| **Update Lockfile**| `./pants generate-lockfiles` | Re-compiles dependencies in `3rdparty/requirements.txt` |
| **Inspect Graph** | `./pants peek ::` | Lists metadata and inferred dependencies for all targets |

> 💡 *Note: The `::` symbol is a wildcard indicating "all directories recursively".*

---

## 📈 Adding a New Project

When you are ready to start coding a new project:

1.  **Create the project directory:**
    ```bash
    mkdir -p projects/my_project
    ```
2.  **Add a `BUILD` file:**
    Create `projects/my_project/BUILD` and define the target sources:
    ```python
    python_sources(
        name="lib",
    )
    ```
3.  **Manage Dependencies:**
    *   If you need a new external library (e.g., `pandas`), add it to `3rdparty/requirements.txt`.
    *   Regenerate the lockfile: `./pants generate-lockfiles`
    *   Pants will **automatically infer** imports in your Python files—no need to manually declare dependencies in `BUILD` files!
