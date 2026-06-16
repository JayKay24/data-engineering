import os
import sys
from github import Github
import google.generativeai as genai
import pathspec

def main():
    # 1. Load Environment Variables
    gemini_api_key = os.getenv("GEMINI_API_KEY")
    github_token = os.getenv("GITHUB_TOKEN")
    pr_number_str = os.getenv("PR_NUMBER")
    repo_name = os.getenv("REPO_NAME")

    if not all([gemini_api_key, github_token, pr_number_str, repo_name]):
        print("Error: Missing required environment variables.", file=sys.stderr)
        sys.exit(1)

    pr_number = int(pr_number_str)

    # 2. Initialize GitHub Client and Fetch PR Diffs
    print(f"Fetching PR #{pr_number} from GitHub repository: {repo_name}...")
    g = Github(github_token)
    repo = g.get_repo(repo_name)
    pr = repo.get_pull(pr_number)
    
    # Load .gitignore patterns and append custom patterns (lockfiles, binaries)
    ignore_patterns = []
    if os.path.exists(".gitignore"):
        try:
            with open(".gitignore", "r") as f:
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
    
    ignore_spec = pathspec.PathSpec.from_lines("gitwildmatch", ignore_patterns)
    print("Successfully compiled file ignore patterns.")
    
    # Get modified files and their diffs
    files = pr.get_files()
    diff_content = []
    
    for file in files:
        # Skip files matching ignore patterns
        if ignore_spec.match_file(file.filename):
            print(f"Skipping {file.filename} (ignored)")
            continue
        
        file_header = f"=== File: {file.filename} ===\n"
        # Use file patch/diff if available
        if file.patch:
            diff_content.append(f"{file_header}{file.patch}\n")
        else:
            diff_content.append(f"{file_header}[File modified, but no patch details available]\n")

    if not diff_content:
        print("No code changes to review.")
        sys.exit(0)

    full_diff = "\n".join(diff_content)

    # 3. Configure Gemini and Generate Review
    print("Sending diff to Gemini API for analysis...")
    genai.configure(api_key=gemini_api_key)
    
    # Using gemini-3.5-flash: fast, low latency, and highly capable
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
{full_diff}
"""

    try:
        response = model.generate_content(prompt)
        review_markdown = response.text
    except Exception as e:
        print(f"Error calling Gemini API: {e}", file=sys.stderr)
        sys.exit(1)

    # 4. Post the Review Comment back to the Pull Request
    print("Posting review comment to Pull Request...")
    try:
        # Create a top-level review comment on the PR conversation
        pr.create_issue_comment(review_markdown)
        print("Review successfully posted!")
    except Exception as e:
        print(f"Error posting comment to GitHub: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
