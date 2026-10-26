"""Expand template.yaml's SAM transform into plain CloudFormation JSON, offline.

The SAM `Policies` shorthand (policy templates such as SQSSendMessagePolicy) only
becomes real IAM roles and policies after the transform, so a policy checker has to see
the expanded template. This runs the same translator AWS runs, without credentials
(this template names no AWS managed policies, the only thing it would look up).

Usage: uvx --with aws-sam-translator --with pyyaml \\
    python scripts/expand_sam_template.py template.yaml OUT.json
"""

import json
import sys
from pathlib import Path

from samtranslator.parser.parser import Parser
from samtranslator.public.translator import ManagedPolicyLoader
from samtranslator.translator.translator import Translator
from samtranslator.yaml_helper import yaml_parse


class NoManagedPolicies:
    """Stand-in for the IAM client: a lookup means the template started using managed
    policies, which this offline expansion can't resolve."""

    def get_paginator(self, name: str) -> object:
        raise SystemExit(
            f"template uses an AWS managed policy ({name}); can't expand offline"
        )


def main(source: str, destination: str) -> None:
    template = yaml_parse(Path(source).read_text())
    # CodeUri is a local directory that `sam package` would upload; the translator wants
    # an S3 URI, and the code isn't part of any policy.
    template["Globals"]["Function"]["CodeUri"] = "s3://placeholder-bucket/code.zip"
    parameters = {
        name: spec["Default"]
        for name, spec in template.get("Parameters", {}).items()
        if "Default" in spec
    }
    translator = Translator(ManagedPolicyLoader(NoManagedPolicies()), Parser())  # type: ignore[arg-type]
    expanded = translator.translate(template, parameters)
    Path(destination).write_text(json.dumps(expanded, indent=2) + "\n")
    print(f"wrote {destination}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit(__doc__)
    main(sys.argv[1], sys.argv[2])
