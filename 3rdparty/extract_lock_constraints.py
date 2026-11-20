"""Extract exact pinned package versions from a Pants lockfile.

Supports querying a single package version, emitting ``name==version``
constraints for selected packages, or exporting all locked constraints for
reproducible Docker builds.
"""

import argparse
import json
import sys


def load_locked_versions(lockfile_path: str) -> dict[str, str]:
    """Parse a Pants JSON lockfile into a lowercase package-to-version mapping.

    Args:
        lockfile_path: Path to the Pants JSON lockfile (for example
            ``3rdparty/user_reqs.lock``).

    Returns:
        Dictionary mapping lowercase package names to exact locked versions.

    Raises:
        ValueError: If no ``locked_resolves`` entries exist in the lockfile.
    """
    with open(lockfile_path, "r", encoding="utf-8") as f:
        # Pants lockfiles prepend '//' metadata header comments that must be stripped before JSON parsing.
        json_text = "".join(line for line in f if not line.lstrip().startswith("//"))
    data = json.loads(json_text)
    resolves = data.get("locked_resolves", [])
    if not resolves:
        raise ValueError(f"No locked_resolves found in {lockfile_path}")
    # Normalize package names to lowercase because PyPI names are case-insensitive (PEP 503).
    return {
        req["project_name"].lower(): req["version"]
        for req in resolves[0].get("locked_requirements", [])
    }


def main() -> None:
    """Run the CLI to extract locked package versions or pip constraints."""
    parser = argparse.ArgumentParser(
        description="Extract pinned versions or pip constraints from a Pants lockfile."
    )
    parser.add_argument(
        "--lockfile",
        default="3rdparty/user_reqs.lock",
        help="Path to the Pants lockfile.",
    )
    parser.add_argument(
        "--get-version",
        help="Print the exact locked version for a single package.",
    )
    parser.add_argument(
        "--packages",
        nargs="+",
        help="Print name==version lines for specific packages.",
    )
    parser.add_argument(
        "--all-constraints",
        action="store_true",
        help="Print name==version lines for every package in the lockfile.",
    )
    args = parser.parse_args()

    versions = load_locked_versions(args.lockfile)

    if args.get_version:
        key = args.get_version.lower()
        if key not in versions:
            print(
                f"Package '{args.get_version}' not found in lockfile.", file=sys.stderr
            )
            sys.exit(1)
        print(versions[key])
        return

    if args.all_constraints:
        for name in sorted(versions):
            print(f"{name}=={versions[name]}")
        return

    if args.packages:
        for pkg in args.packages:
            key = pkg.lower()
            if key not in versions:
                print(f"Package '{pkg}' not found in lockfile.", file=sys.stderr)
                sys.exit(1)
            print(f"{pkg}=={versions[key]}")
        return

    parser.print_help()
    sys.exit(1)


if __name__ == "__main__":
    main()
