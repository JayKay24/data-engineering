"""Validate GitHub Wiki markdown files for broken links, anchors, and fences.

Checks that required navigation files exist, verifies that all fenced code and
Mermaid blocks are balanced, and validates internal page links and GFM heading
anchors across the wiki directory.
"""

import argparse
import os
import re
import sys

REQUIRED_FILES = ("Home.md", "_Sidebar.md", "_Footer.md")


def gfm_slug(heading: str) -> str:
    """Convert a Markdown heading into its GitHub Flavored Markdown anchor slug.

    Args:
        heading: Raw Markdown heading text without leading ``#`` markers.

    Returns:
        Lowercase GFM anchor slug matching GitHub's heading ID rules.
    """
    slug = heading.strip().lower()
    slug = re.sub(r"[^\w\s-]", "", slug)
    return slug.replace(" ", "-")


def collect_headings(text: str) -> set[str]:
    """Extract all GFM heading slugs from a Markdown document.

    Ignores headings inside fenced code or Mermaid blocks.

    Args:
        text: Full Markdown document text.

    Returns:
        Set of GFM heading anchor slugs defined in the document.
    """
    anchors: set[str] = set()
    in_code_block = False
    for line in text.splitlines():
        # Toggle state on fenced blocks so '#' comments in shell/Python snippets aren't treated as headings.
        if line.lstrip().startswith("```"):
            in_code_block = not in_code_block
            continue
        if in_code_block:
            continue
        match = re.match(r"^#{1,6}\s+(.+)$", line)
        if match:
            anchors.add(gfm_slug(match.group(1)))
    return anchors


def validate_wiki_dir(wiki_dir: str) -> list[str]:
    """Validate all Markdown files in a GitHub Wiki source directory.

    Args:
        wiki_dir: Path to the directory containing Wiki ``.md`` files.

    Returns:
        List of human-readable validation error messages (empty if valid).
    """
    if not os.path.isdir(wiki_dir):
        return [f"Wiki directory does not exist: {wiki_dir}"]

    md_files = sorted(f for f in os.listdir(wiki_dir) if f.endswith(".md"))
    pages = {os.path.splitext(f)[0] for f in md_files}
    errors: list[str] = []

    for req in REQUIRED_FILES:
        if req not in md_files:
            errors.append(f"Missing required wiki file: {req}")

    anchors_by_page: dict[str, set[str]] = {}
    contents_by_file: dict[str, str] = {}

    for fname in md_files:
        page = os.path.splitext(fname)[0]
        with open(os.path.join(wiki_dir, fname), "r", encoding="utf-8") as f:
            text = f.read()
        contents_by_file[fname] = text
        anchors_by_page[page] = collect_headings(text)

    for fname, text in contents_by_file.items():
        page = os.path.splitext(fname)[0]
        fence_count = len(re.findall(r"^```", text, flags=re.MULTILINE))
        if fence_count % 2 != 0:
            errors.append(f"{fname}: Unbalanced code/Mermaid fences ({fence_count})")

        # Strip fenced code blocks before scanning markdown links
        text_no_code = re.sub(r"^```[\s\S]*?^```", "", text, flags=re.MULTILINE)
        for match in re.finditer(r"\[([^\]]+)\]\(([^)]+)\)", text_no_code):
            target = match.group(2).strip()
            if target.startswith(("http://", "https://", "mailto:")):
                continue
            if target.startswith("#"):
                anchor = target[1:]
                if anchor not in anchors_by_page[page]:
                    errors.append(f"{fname}: Missing local anchor '#{anchor}'")
                continue

            if "#" in target:
                target_page, anchor = target.split("#", 1)
            else:
                target_page, anchor = target, None

            if target_page not in pages:
                errors.append(f"{fname}: Broken internal wiki link '({target})'")
            elif anchor and anchor not in anchors_by_page[target_page]:
                errors.append(
                    f"{fname}: Missing anchor '#{anchor}' in target page '{target_page}'"
                )

    return errors


def main() -> None:
    """Run the CLI entry point for validating the GitHub Wiki source directory."""
    default_wiki = (
        "docs/wiki"
        if os.path.isdir("docs/wiki")
        else os.path.join(os.path.dirname(os.path.abspath(__file__)), "wiki")
    )
    parser = argparse.ArgumentParser(
        description="Validate internal links, GFM anchors, and code fences in docs/wiki."
    )
    parser.add_argument(
        "--wiki-dir",
        default=default_wiki,
        help="Path to the wiki markdown directory (default: docs/wiki).",
    )
    args = parser.parse_args()

    errors = validate_wiki_dir(args.wiki_dir)
    if errors:
        print("Wiki validation failed:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        sys.exit(1)

    md_count = len([f for f in os.listdir(args.wiki_dir) if f.endswith(".md")])
    print(
        f"Wiki validation passed: {md_count} markdown files in '{args.wiki_dir}' "
        "verified (links, GFM anchors, and code fences)."
    )


if __name__ == "__main__":
    main()
