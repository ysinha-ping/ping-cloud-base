#!/bin/bash

export CDE_DEPLOY=true
export HAS_PROFILE_DIR=false

# Set CHUB_DEPLOY dynamically based on APP_NAME
case "${APP_NAME}" in
  p1as-newrelic|p1as-cloudwatch)
    export CHUB_DEPLOY=true
    ;;
  p1as-grafana|p1as-prometheus)
    export CHUB_DEPLOY=false
    ;;
  *)
    echo "[config.sh] Unknown or unset APP_NAME='${APP_NAME}'. Defaulting CHUB_DEPLOY=false"
    export CHUB_DEPLOY=false
    ;;
esac