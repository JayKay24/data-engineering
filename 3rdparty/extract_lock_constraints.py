"""Extracts exact pinned package versions from a Pants user_reqs.lock file."""

import argparse
import json
import sys


def load_locked_versions(lockfile_path: str) -> dict[str, str]:
    """Parses a Pants JSON lockfile (skipping header comments) into a {name: version} map."""
    with open(lockfile_path, "r", encoding="utf-8") as f:
        json_text = "".join(line for line in f if not line.lstrip().startswith("//"))
    data = json.loads(json_text)
    resolves = data.get("locked_resolves", [])
    if not resolves:
        raise ValueError(f"No locked_resolves found in {lockfile_path}")
    return {
        req["project_name"].lower(): req["version"]
        for req in resolves[0].get("locked_requirements", [])
    }


def main() -> None:
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
