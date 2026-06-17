import os
import sys
from typing import List
from github import Github
from github.PullRequest import PullRequest
import google.generativeai as genai
import pathspec

# Max character limit for diff payload to stay within token limits
MAX_DIFF_CHARACTERS = 150000


def get_ignore_spec() -> pathspec.PathSpec:
    """Loads .gitignore patterns and appends custom file exclusion wildcards."""
    ignore_patterns = []
    if os.path.exists(".gitignore"):
        try:
            with open(".gitignore", "r", encoding="utf-8") as f:
                lines = [
                    line.strip()
                    for line in f
                    if line.strip() and not line.strip().startswith("#")
                ]
                ignore_patterns.extend(lines)
        except Exception as e:
            print(f"Warning: Failed to parse .gitignore: {e}", file=sys.stderr)

    # Custom wildcards for lockfiles and binary assets to skip
    ignore_patterns.extend(["*.lock", "*.png", "*.jpg", "*.jpeg", "*.zip", "*.pdf"])

    return pathspec.PathSpec.from_lines("gitwildmatch", ignore_patterns)


def build_diff_content(pr: PullRequest, ignore_spec: pathspec.PathSpec) -> str:
    """Retrieves and filters PR file diffs, handling size limits and early exits."""
    diff_content: List[str] = []
    current_size = 0

    for file in pr.get_files():
        # Optimization: Stop fetching additional diffs once we are past the limit
        if current_size >= MAX_DIFF_CHARACTERS:
            diff_content.append("\n\n... [TRUNCATED: MAX CHARACTER LIMIT REACHED] ...")
            print(
                "Max character limit reached during diff generation. Stopping file retrieval."
            )
            break

        # Skip files matching ignore patterns
        if ignore_spec.match_file(file.filename):
            print(f"Skipping {file.filename} (ignored)")
            continue

        file_header = f"=== File: {file.filename} ===\n"
        patch_str = ""
        if file.patch:
            if len(file.patch) > 30000:
                patch_str = f"{file_header}[File patch omitted: Exceeds single-file size limit]\n"
                print(
                    f"Skipping patch for {file.filename} (exceeds 30,000 character limit)"
                )
            else:
                patch_str = f"{file_header}{file.patch}\n"
        else:
            patch_str = (
                f"{file_header}[File modified, but no patch details available]\n"
            )

        diff_content.append(patch_str)
        current_size += len(patch_str)

    return "\n".join(diff_content)


def generate_review(gemini_api_key: str, model_name: str, diff: str) -> str:
    """Sends the PR diff to the Gemini API and returns the markdown review."""
    genai.configure(api_key=gemini_api_key)

    system_instruction = (
        "You are an expert Data Engineer and Python Code Reviewer.\n"
        "Your task is to conduct a professional, constructive code review.\n\n"
        "Specific areas to analyze:\n"
        "1. PySpark & Data Engineering Best Practices (unpartitioned writes, redundant caching, .collect() issues).\n"
        "2. Python Code Quality (Ruff/PEP8 standards, naming, docstrings).\n"
        "3. Bugs & Edge Cases (logical bugs, unhandled exceptions).\n\n"
        "Format your review in Markdown with the following sections:\n"
        "- 🤖 AI PR Review Summary\n"
        "- 💡 Key Feedback & Recommendations (with before/after code blocks)\n"
        "- ✅ Verdict (Approve, Comment, Request Changes)"
    )

    model = genai.GenerativeModel(model_name, system_instruction=system_instruction)

    prompt = f"Please review the following PR Diff:\n\n{diff}"
    response = model.generate_content(prompt)
    return response.text


def post_review(pr: PullRequest, review_body: str) -> None:
    """Submits the review as an official Pull Request Review on GitHub. Handles fork permissions gracefully."""
    try:
        pr.create_review(body=review_body, event="COMMENT")
    except Exception as e:
        print(f"Warning: Failed to post PR review comment: {e}", file=sys.stderr)
        print(
            "This is expected for Pull Requests from external forks where GITHUB_TOKEN has read-only access."
        )
        print("Exiting gracefully with code 0.")
        sys.exit(0)


def main():
    # Load Environment Variables
    gemini_api_key = os.getenv("GEMINI_API_KEY")
    github_token = os.getenv("GITHUB_TOKEN")
    pr_number_str = os.getenv("PR_NUMBER")
    repo_name = os.getenv("REPO_NAME")
    gemini_model = os.getenv("GEMINI_MODEL")
    if not gemini_model or not gemini_model.strip():
        gemini_model = "gemini-1.5-flash"

    # Handle missing API Key gracefully (e.g. for PRs from external forks)
    if not gemini_api_key:
        print(
            "Warning: GEMINI_API_KEY is missing. Skipping AI Review (expected for external forks)."
        )
        sys.exit(0)

    if not all([github_token, pr_number_str, repo_name]):
        print(
            "Error: Missing required environment variables (GITHUB_TOKEN, PR_NUMBER, or REPO_NAME).",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        pr_number = int(pr_number_str)
    except ValueError:
        print(
            f"Error: PR_NUMBER '{pr_number_str}' is not a valid integer.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Initialize client and fetch PR
    print(f"Connecting to repo {repo_name} and fetching PR #{pr_number}...")
    g = Github(github_token)
    repo = g.get_repo(repo_name)
    pr = repo.get_pull(pr_number)

    # Compile ignore patterns and build diff
    ignore_spec = get_ignore_spec()
    diff = build_diff_content(pr, ignore_spec)

    if not diff.strip():
        print("No code changes to review.")
        sys.exit(0)

    # Generate review
    print(f"Generating review with Gemini (model: {gemini_model})...")
    try:
        review_body = generate_review(gemini_api_key, gemini_model, diff)
    except Exception as e:
        print(
            f"Warning: Failed to generate review via Gemini API: {e}", file=sys.stderr
        )
        print("Exiting gracefully with code 0 to avoid failing the CI build.")
        sys.exit(0)

    # Post review
    print("Posting review back to GitHub...")
    try:
        post_review(pr, review_body)
    except Exception as e:
        print(f"Warning: Failed to post review comment to GitHub: {e}", file=sys.stderr)
        print("Exiting gracefully with code 0.")
        sys.exit(0)
    print("Successfully posted PR review!")


if __name__ == "__main__":
    main()
