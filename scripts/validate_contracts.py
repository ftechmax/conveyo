#!/usr/bin/env python3
"""Validate the shared schemas and every manifested fixture; no network references."""
import argparse
import copy
import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contracts", type=Path, required=True)
    args = parser.parse_args()
    root = args.contracts.resolve()
    fixtures_dir = root / "fixtures"
    schemas = {}
    resources = []
    for path in sorted((root / "schemas").glob("*.schema.json")):
        schema = json.loads(path.read_text())
        Draft202012Validator.check_schema(schema)
        schemas[path.name.removesuffix(".schema.json")] = schema
        resources.append((schema["$id"], Resource.from_contents(schema)))
    registry = Registry().with_resources(resources)
    checker = FormatChecker()
    # Fail if optional format dependencies are missing rather than silently skipping checks.
    for value, format_name in [("not-a-uuid", "uuid"), ("2026-02-30T00:00:00Z", "date-time"), ("relative", "uri")]:
        if checker.conforms(value, format_name):
            raise ValueError(f"Format checking unavailable: {format_name}")
    validators = {
        name: Draft202012Validator(schema, registry=registry, format_checker=checker)
        for name, schema in schemas.items()
    }
    manifest = json.loads((fixtures_dir / "manifest.json").read_text())
    validators["manifest"].validate(manifest)
    manifested_files = set()
    for entry in manifest:
        name = entry["file"]
        path = (fixtures_dir / name).resolve()
        if not path.is_relative_to(fixtures_dir) or name in manifested_files:
            raise ValueError(f"Unsafe or duplicate fixture: {name}")
        manifested_files.add(name)
        fixture = json.loads(path.read_text())
        errors = [
            error
            for schema in entry["schemas"]
            for error in validators[schema].iter_errors(fixture)
        ]
        valid = not errors
        if valid != entry["valid"]:
            raise ValueError(f"{name}: expected valid={entry['valid']}: " + "; ".join(str(e) for e in errors))
    suite = json.loads((fixtures_dir / "envelope-cases.json").read_text())
    for case in suite["cases"]:
        if "root" in case:
            envelope = case["root"]
        else:
            envelope = copy.deepcopy(suite["template"])
            envelope.update(case.get("set", {}))
            for field in case.get("remove", []):
                del envelope[field]
            if "messageFile" in case:
                if case["messageFile"] not in manifested_files:
                    raise ValueError(f"Unmanifested payload: {case['messageFile']}")
                envelope["message"] = json.loads((fixtures_dir / case["messageFile"]).read_text())
        valid = validators["envelope"].is_valid(envelope)
        if valid != case["valid"]:
            raise ValueError(f"Envelope case {case['name']}: expected valid={case['valid']}, got {valid}")
    references = json.loads((fixtures_dir / "messagedata/references.json").read_text())
    for case in references:
        valid = validators["reference"].is_valid(case["value"])
        if valid != case["valid"]:
            raise ValueError(f"Reference case {case['name']}: expected valid={case['valid']}, got {valid}")
    files = {
        str(path.relative_to(fixtures_dir))
        for path in fixtures_dir.rglob("*.json")
        if path != fixtures_dir / "manifest.json"
    }
    if files != manifested_files:
        raise ValueError(f"Fixture manifest mismatch: {files ^ manifested_files}")
    print(f"Validated {len(schemas)} schemas, {len(manifested_files)} fixture files, "
          f"{len(suite['cases'])} envelope cases and {len(references)} reference cases.")


if __name__ == "__main__":
    main()
