#!/bin/bash

export CDE_DEPLOY=true
export HAS_PROFILE_DIR=false
export CHUB_DEPLOY=true

# Override if p1as-grafana is being deployed to customer-hub
KUSTOMIZATION_PATH="$(dirname "$0")/region/kustomization.yaml"

if [[ "${ENV}" == "customer-hub" ]] && [[ -f "${KUSTOMIZATION_PATH}" ]]; then
  if grep -q "[[:space:]]*name:[[:space:]]*p1as-grafana" "${KUSTOMIZATION_PATH}"; then
    echo "[config.sh] Detected p1as-grafana in kustomization.yaml for customer-hub – disabling CHUB_DEPLOY"
    export CHUB_DEPLOY=false
  fi
fi

echo "[config.sh] Final CHUB_DEPLOY=${CHUB_DEPLOY}"