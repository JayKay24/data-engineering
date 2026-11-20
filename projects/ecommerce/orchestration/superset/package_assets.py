"""Validates and packages Superset Dashboards-as-Code YAML assets into an import ZIP."""

import argparse
import os
import sys
import zipfile

import yaml


def _load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Expected YAML mapping in {path}")
    return data


def _collect_chart_uuids(node: object) -> set[str]:
    """Recursively extracts chart UUID references from a Superset dashboard position tree."""
    uuids: set[str] = set()
    if isinstance(node, dict):
        if node.get("type") == "CHART":
            chart_uuid = node.get("meta", {}).get("uuid")
            if chart_uuid:
                uuids.add(chart_uuid)
        for val in node.values():
            uuids.update(_collect_chart_uuids(val))
    elif isinstance(node, list):
        for item in node:
            uuids.update(_collect_chart_uuids(item))
    return uuids


def validate_assets(assets_dir: str) -> list[str]:
    """Validates YAML syntax, JSON fields, and UUID referential integrity across assets."""
    errors: list[str] = []
    if not os.path.isdir(assets_dir):
        return [f"Assets directory does not exist: {assets_dir}"]

    metadata_path = os.path.join(assets_dir, "metadata.yaml")
    if not os.path.exists(metadata_path):
        errors.append("Missing required metadata.yaml in assets directory")
    else:
        _load_yaml(metadata_path)

    db_uuids: set[str] = set()
    dataset_uuids: set[str] = set()
    chart_uuids: set[str] = set()

    for root, _, files in os.walk(assets_dir):
        rel_root = os.path.relpath(root, assets_dir)
        for fname in sorted(files):
            if not fname.endswith((".yaml", ".yml")):
                continue
            fpath = os.path.join(root, fname)
            data = _load_yaml(fpath)
            if rel_root.startswith("databases"):
                if "uuid" in data:
                    db_uuids.add(data["uuid"])
                extra = data.get("extra")
                if extra is not None and not isinstance(extra, dict):
                    errors.append(f"{fpath}: 'extra' must be a YAML mapping")
            elif rel_root.startswith("datasets"):
                if "uuid" in data:
                    dataset_uuids.add(data["uuid"])
                if data.get("database_uuid") not in db_uuids:
                    errors.append(
                        f"{fpath}: unknown database_uuid '{data.get('database_uuid')}'"
                    )

    for root, _, files in os.walk(assets_dir):
        rel_root = os.path.relpath(root, assets_dir)
        for fname in sorted(files):
            if not fname.endswith((".yaml", ".yml")):
                continue
            fpath = os.path.join(root, fname)
            data = _load_yaml(fpath)
            if rel_root.startswith("charts"):
                if "uuid" in data:
                    chart_uuids.add(data["uuid"])
                if data.get("dataset_uuid") not in dataset_uuids:
                    errors.append(
                        f"{fpath}: unknown dataset_uuid '{data.get('dataset_uuid')}'"
                    )
                params = data.get("params")
                if params is not None and not isinstance(params, dict):
                    errors.append(f"{fpath}: 'params' must be a YAML mapping")

    for root, _, files in os.walk(assets_dir):
        rel_root = os.path.relpath(root, assets_dir)
        for fname in sorted(files):
            if not fname.endswith((".yaml", ".yml")):
                continue
            fpath = os.path.join(root, fname)
            data = _load_yaml(fpath)
            if rel_root.startswith("dashboards"):
                referenced = _collect_chart_uuids(data.get("position", {}))
                missing = referenced - chart_uuids
                if missing:
                    errors.append(
                        f"{fpath}: references unknown chart UUIDs: {sorted(missing)}"
                    )

    return errors


def build_zip_bundle(assets_dir: str, output_zip: str) -> int:
    """Packages validated Superset YAML assets into a Superset v1 import ZIP bundle."""
    os.makedirs(os.path.dirname(os.path.abspath(output_zip)), exist_ok=True)
    count = 0
    with zipfile.ZipFile(output_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(assets_dir):
            for fname in sorted(files):
                if not fname.endswith((".yaml", ".yml")):
                    continue
                full_path = os.path.join(root, fname)
                arcname = os.path.join(
                    "ecommerce_dashboard", os.path.relpath(full_path, assets_dir)
                )
                zf.write(full_path, arcname)
                count += 1
    return count


def main() -> None:
    """CLI entry point for validating and optionally packaging Superset assets."""
    default_assets = (
        "projects/ecommerce/orchestration/superset/assets"
        if os.path.isdir("projects/ecommerce/orchestration/superset/assets")
        else os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets")
    )
    parser = argparse.ArgumentParser(
        description="Validate and package Superset Dashboards-as-Code YAML assets."
    )
    parser.add_argument("--assets-dir", default=default_assets)
    parser.add_argument(
        "--output",
        default=None,
        help="Optional path to write the Superset import ZIP bundle.",
    )
    args = parser.parse_args()

    errors = validate_assets(args.assets_dir)
    if errors:
        print("Superset asset validation failed:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        sys.exit(1)

    print(f"Superset asset validation passed for '{args.assets_dir}'.")
    if args.output:
        file_count = build_zip_bundle(args.assets_dir, args.output)
        print(f"Packaged {file_count} YAML assets into '{args.output}'.")


if __name__ == "__main__":
    main()
