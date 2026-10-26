#!/usr/bin/env bash
# Offline IAM policy check of template.yaml. Expands the SAM transform (so the SAM
# `Policies` shorthand becomes real IAM roles), then runs checkov's IAM checks on it.
# Needs no AWS credentials. Usage: scripts/check_iam.sh
#
# IAM Access Analyzer's validate-policy is the authoritative check but needs AWS
# credentials; see the README for the command to run it by hand.
#
# Findings are failures. To accept one, add its ID to WAIVED below with the reason in a
# comment (the roles SAM generates have no place in template.yaml for inline skips).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# The SAM translator fills in pseudo parameters and needs some region to do it; the
# value doesn't affect the IAM checks. CI runners have none configured.
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

# CI-only tools, pinned so a new checkov release can't fail the build on its own. The
# exact versions pin the two tools; --exclude-newer pins everything they pull in.
CHECKOV="checkov==3.3.30"
TRANSLATOR="aws-sam-translator==1.115.0"
EXCLUDE_NEWER="2026-10-21T00:00:00Z"

# The checkov checks that apply to the expanded template's AWS::IAM::Role resources
# (confirm with `checkov --list --framework cloudformation`): the trust policy
# (60, 61), full "*:*" administrator statements and "*" actions (62, 63), and the
# privilege checks (107 credentials exposure, 108 data exfiltration, 109 permissions
# management, 110 privilege escalation, 111 write access without constraints).
# Not covered: wildcard resources, which no checkov check flags on its own, and the
# S3 bucket policy (MediaBucketPolicy), which none of these checks reads.
IAM_CHECKS="CKV_AWS_60,CKV_AWS_61,CKV_AWS_62,CKV_AWS_63"
IAM_CHECKS+=",CKV_AWS_107,CKV_AWS_108,CKV_AWS_109,CKV_AWS_110,CKV_AWS_111"

# Accepted findings, as comma-separated check IDs, each with its reason above it.
# None at present.
WAIVED=""

OUT="build/iam-check"
rm -rf "$OUT"
mkdir -p "$OUT"
EXPANDED="$OUT/template.expanded.json"

uvx --python 3.14 --exclude-newer "$EXCLUDE_NEWER" --with "$TRANSLATOR" --with pyyaml \
  python scripts/expand_sam_template.py template.yaml "$EXPANDED"

# BC_SKIP_MAPPING stops checkov contacting its vendor's API for guideline links.
# The report goes to the console and to $OUT/results_json.json.
checkov_status=0
BC_SKIP_MAPPING=TRUE uvx --python 3.14 --exclude-newer "$EXCLUDE_NEWER" --from "$CHECKOV" checkov \
  --file "$EXPANDED" \
  --framework cloudformation \
  --check "$IAM_CHECKS" \
  ${WAIVED:+--skip-check "$WAIVED"} \
  --output cli --output json --output-file-path "console,$OUT/results_json.json" \
  --compact --skip-download || checkov_status=$?

# A pass that checked nothing is not a pass: the expansion must have produced the three
# function roles, and checkov must have passed at least one check on each of them.
python3 - "$EXPANDED" "$OUT/results_json.json" <<'PY'
import json
import sys

expanded = json.load(open(sys.argv[1]))
results = json.load(open(sys.argv[2]))
roles = sorted(
    name
    for name, res in expanded["Resources"].items()
    if res["Type"] == "AWS::IAM::Role"
)
if len(roles) < 3:
    sys.exit("expected at least 3 AWS::IAM::Role resources after expansion, got %s" % roles)
passed = results["results"]["passed_checks"]
for role in roles:
    count = sum(1 for c in passed if c["resource"] == "AWS::IAM::Role." + role)
    print("%s: %d IAM checks passed" % (role, count))
    if count == 0:
        sys.exit("no IAM check passed for %s; the check did not see it" % role)
PY

exit "$checkov_status"
