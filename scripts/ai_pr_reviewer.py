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
                ignore_patterns.extend(f.readlines())
        except Exception as e:
            print(f"Warning: Failed to parse .gitignore: {e}", file=sys.stderr)
            
    # Custom wildcards for lockfiles and binary assets to skip
    ignore_patterns.extend([
        "*.lock",
        "*.png",
        "*.jpg",
        "*.jpeg",
        "*.zip",
        "*.pdf"
    ])
    
    return pathspec.PathSpec.from_lines("gitwildmatch", ignore_patterns)


def build_diff_content(pr: PullRequest, ignore_spec: pathspec.PathSpec) -> str:
    """Retrieves and filters PR file diffs, handling size limits."""
    diff_content: List[str] = []
    for file in pr.get_files():
        # Skip files matching ignore patterns
        if ignore_spec.match_file(file.filename):
            print(f"Skipping {file.filename} (ignored)")
            continue
        
        file_header = f"=== File: {file.filename} ===\n"
        if file.patch:
            diff_content.append(f"{file_header}{file.patch}\n")
        else:
            diff_content.append(f"{file_header}[File modified, but no patch details available]\n")
            
    full_diff = "\n".join(diff_content)
    
    # Handle defensive size cutoff
    if len(full_diff) > MAX_DIFF_CHARACTERS:
        print(f"Warning: PR diff size ({len(full_diff)} chars) exceeds threshold. Truncating.")
        full_diff = (
            full_diff[:MAX_DIFF_CHARACTERS]
            + "\n\n... [TRUNCATED DUE TO EXTREME SIZE] ..."
        )
        
    return full_diff


def generate_review(gemini_api_key: str, diff: str) -> str:
    """Sends the PR diff to the Gemini API and returns the markdown review."""
    genai.configure(api_key=gemini_api_key)
    model = genai.GenerativeModel("gemini-3.5-flash")
    
    prompt = f"""
You are an expert Data Engineer and Python Code Reviewer.
Your task is to conduct a professional, constructive code review for the following Pull Request diff.

Specific areas to analyze:
1. **PySpark & Data Engineering Best Practices**: Check for issues like unpartitioned writes, redundant/missing caching, expensive operations like `.collect()` on large datasets, and proper schema enforcement.
2. **Python Code Quality**: Verify Pythonic style, readability, naming conventions, docstrings, and comments (matching Ruff/PEP8 standards).
3. **Bugs & Edge Cases**: Look for logical bugs, incorrect relative paths, unhandled exceptions, or potential null pointer errors.

Format your review in Markdown with the following sections:
- **🤖 AI PR Review Summary**: A brief, high-level summary of what the PR changes.
- **💡 Key Feedback & Recommendations**: Bullet points detailing specific improvements. Include code snippets for "Before" and "After" where applicable.
- **✅ Verdict**: One of the following:
  - **Approve**: The code looks clean, optimized, and ready to merge.
  - **Comment**: Needs minor cleanup, formatting, or documentation.
  - **Request Changes**: Critical bugs or severe PySpark performance risks that should be resolved before merging.

PR Diff:
{diff}
"""
    response = model.generate_content(prompt)
    return response.text


def post_review(pr: PullRequest, review_body: str) -> None:
    """Submits the review as an official Pull Request Review on GitHub."""
    pr.create_review(body=review_body, event="COMMENT")


def main():
    # Load Environment Variables
    gemini_api_key = os.getenv("GEMINI_API_KEY")
    github_token = os.getenv("GITHUB_TOKEN")
    pr_number_str = os.getenv("PR_NUMBER")
    repo_name = os.getenv("REPO_NAME")

    if not all([gemini_api_key, github_token, pr_number_str, repo_name]):
        print("Error: Missing required environment variables.", file=sys.stderr)
        sys.exit(1)

    pr_number = int(pr_number_str)

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
    print("Generating review with Gemini...")
    review_body = generate_review(gemini_api_key, diff)

    # Post review
    print("Posting review back to GitHub...")
    post_review(pr, review_body)
    print("Successfully posted PR review!")


if __name__ == "__main__":
    main()
