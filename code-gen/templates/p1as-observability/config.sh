#!/bin/bash

export CDE_DEPLOY=true
export HAS_PROFILE_DIR=false
export CHUB_DEPLOY=true

KUSTOMIZATION_PATH="$(dirname "$0")/region/kustomization.yaml"

# Disable CHUB_DEPLOY if deploying to customer-hub and p1as-grafana is included in the Helm charts
if [[ "${ENV}" == "customer-hub" ]] && [[ -f "${KUSTOMIZATION_PATH}" ]] && grep -q "name: p1as-grafana" "${KUSTOMIZATION_PATH}"; then
  echo "[config.sh] Detected p1as-grafana in kustomization.yaml for ENV=${ENV} – disabling CHUB_DEPLOY"
  export CHUB_DEPLOY=false
fi