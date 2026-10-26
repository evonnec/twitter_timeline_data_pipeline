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

# CI-only tools, pinned so a new checkov release can't fail the build on its own.
CHECKOV="checkov==3.3.30"
TRANSLATOR="aws-sam-translator==1.115.0"

# Checkov's IAM checks for CloudFormation: wildcard actions and resources, admin
# access, privilege escalation, data exfiltration, credentials exposure, permissions
# management and write access without constraints, trust policies and public or
# wildcard principals.
IAM_CHECKS="CKV_AWS_40,CKV_AWS_49,CKV_AWS_60,CKV_AWS_61,CKV_AWS_62,CKV_AWS_63"
IAM_CHECKS+=",CKV_AWS_107,CKV_AWS_108,CKV_AWS_109,CKV_AWS_110,CKV_AWS_111"
IAM_CHECKS+=",CKV_AWS_274,CKV_AWS_283,CKV_AWS_286,CKV_AWS_287,CKV_AWS_288,CKV_AWS_289"
IAM_CHECKS+=",CKV_AWS_290,CKV_AWS_355,CKV_AWS_356"

# Accepted findings, as comma-separated check IDs, each with its reason above it.
# None at present.
WAIVED=""

OUT="build/iam-check"
mkdir -p "$OUT"

uvx --python 3.14 --with "$TRANSLATOR" --with pyyaml \
  python scripts/expand_sam_template.py template.yaml "$OUT/template.expanded.json"

# BC_SKIP_MAPPING stops checkov contacting its vendor's API for guideline links.
BC_SKIP_MAPPING=TRUE uvx --python 3.14 --from "$CHECKOV" checkov \
  --file "$OUT/template.expanded.json" \
  --framework cloudformation \
  --check "$IAM_CHECKS" \
  ${WAIVED:+--skip-check "$WAIVED"} \
  --compact --skip-download
