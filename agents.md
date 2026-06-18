# Agent Context & Operating Instructions

Welcome, Agent! This file serves as the source of truth for the codebase architecture, environment setup, and rules of engagement for this repository. 

> [!IMPORTANT]
> **Rule of Preservation:** When making significant updates to the codebase (e.g., adding sub-projects, updating dependencies, changing configurations, or introducing new pipelines), you **MUST** update both the main `README.md` and this `agents.md` file to keep the documentation and agent context synchronized.

---

## 1. Project Context & Architecture

This repository is a **Python Data Engineering Monorepo** managed by the **Pants Build System**.

### Core Stack
* **Build System:** Pants (v2.22.0)
* **Python Version:** strictly `3.10.9` (managed via `pyenv`)
* **Linter/Formatter:** Ruff (integrated via Pants backend)
* **Data Processing:** PySpark (`3.5.8`)

### Project Structure
* `.github/workflows/ai-review.yml`: Automated Gemini AI PR reviewer workflow on GitHub Actions.
* `.pre-commit-config.yaml`: Pre-commit hooks configuration to run `./pants fmt` and `./pants lint` locally.
* `3rdparty/`: Contains requirements (`requirements.txt`) and Pants lockfiles (`user_reqs.lock`).
* `projects/essentials/`: First sub-project (derived generic implementation from *Hello Modern Data Pipelines*, Chapter 2).
  * `word_count.py`: Local text processing WordCount Spark script.
  * `employee_partition_by_hire_date.py`: Local partitioning Spark script.
  * `input_data/`: Small CSV/txt sample inputs.
  * `output_data/`: Automatically generated Spark output targets (ignored by git).
* `projects/ingestion/`: Ingestion project (derived implementation from *Hello Modern Data Pipelines*, Chapter 4).
  * `config/input_config.yml`: Spark Ingestion configuration YAML file.
  * `docker/docker-compose.yml`: Zookeeper, Kafka, and Schema Registry Compose setup.
  * `input_data/user_events.json`: Sample event stream dataset.
  * `json_producer.py`: Kafka JSON message producer script.
  * `kafka_json_to_file_job.py`: PySpark job ingestion script with Spark SQL Kafka integration.
  * `BUILD`: Pants build definition for the ingestion project.
* `scripts/`: Python utility scripts.
  * `ai_pr_reviewer.py`: The AI code reviewer script powered by the Gemini API.
  * `BUILD`: Pants build definition for the scripts directory.
* `.venv`: A root-level symlink pointing to the current active Pants-generated virtual environment.

---

## 2. Local Environment Setup & IDE Integration

This project is bootstrapped to work seamlessly with VS Code / Antigravity IDE without writing any configurations outside the project directory:

* **`.vscode/settings.json`**: Configured to resolve the interpreter and paths dynamically:
  ```json
  {
    "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
    "python.analysis.extraPaths": [
      "${workspaceFolder}/projects"
    ]
  }
  ```
* **`.vscode/launch.json`**: Debugging configurations mapped to current files, leveraging the local `.env` file.
* **`.env` (Git Ignored)**: Sets the correct `JAVA_HOME` to **Java 17** (overriding the system default Java 24 to maintain PySpark 3.x compatibility).

---

## 3. Mandatory Agent Rules

1. **NO GIT COMMITS:** Do not run any `git commit` commands. The user commits all files manually.
2. **STRICTLY LOCAL SCOPE:** Do not write or modify any VS Code/IDE configurations outside the `data-engineering` project directory.
3. **PANTS COMPLIANCE:** Always follow the target-based workflow for Pants commands.
4. **SPARK COMPATIBILITY:** Run PySpark tasks using Java 17 via the `JAVA_HOME` configuration found in `.env`.

---

## 4. Pull Request & Bookkeeping Workflow

When implementing new features or bug fixes, follow this workflow to coordinate PR creation:

1. **Local Implementation & Verification:** Implement changes locally, verify they run/test successfully, and document them in `README.md` and `agents.md`.
2. **User Review & Local Commit:** Present the changes to the user. The user will review the code locally and run `git commit` manually.
3. **PR Creation:** Once the user commits the changes, they will instruct you to create the Pull Request (or you can offer to do so).
4. **PR Formatting:** Use the `github` MCP server to create the PR. The PR must have a detailed description containing:
   * **Summary:** A clear explanation of *why* the changes were made.
   * **Key Changes:** A bulleted list of modified modules/files and what was updated.
   * **Verification:** Documentation of successful test/run commands executed during verification.

