#!/bin/bash

CI_SCRIPTS_DIR="${SHARED_CI_SCRIPTS_DIR:-/ci-scripts}"
. "${CI_SCRIPTS_DIR}"/common.sh "${1}"

if skipTest "${0}"; then
  log "Skipping test ${0}"
  exit 0
fi

NAMESPACE="${PING_CLOUD_NAMESPACE:-elastic-stack-logging}"
OPENSEARCH_SERVICE="opensearch-cluster-headless"
OPENSEARCH_DASHBOARDS_SERVICE="opensearch-dashboards"
OPENSEARCH_SECRET="opensearch-admin-credentials"
OPENSEARCH_PORT=9200
OSD_PORT=5601
OS_LOCAL_PORT=19200
OSD_LOCAL_PORT=15601
OS_URL="https://127.0.0.1:${OS_LOCAL_PORT}"
OSD_URL="http://127.0.0.1:${OSD_LOCAL_PORT}"

oneTimeSetUp() {
  USERNAME=$(kubectl get secret "${OPENSEARCH_SECRET}" -n "${NAMESPACE}" \
    -o jsonpath='{.data.username}' 2>/dev/null | base64 -d)
  PASSWORD=$(kubectl get secret "${OPENSEARCH_SECRET}" -n "${NAMESPACE}" \
    -o jsonpath='{.data.password}' 2>/dev/null | base64 -d)

  kubectl port-forward "service/${OPENSEARCH_SERVICE}" \
    "${OS_LOCAL_PORT}:${OPENSEARCH_PORT}" -n "${NAMESPACE}" \
    >/tmp/opensearch-04.log 2>&1 &
  OS_PID=$!

  kubectl port-forward "service/${OPENSEARCH_DASHBOARDS_SERVICE}" \
    "${OSD_LOCAL_PORT}:${OSD_PORT}" -n "${NAMESPACE}" \
    >/tmp/opensearch-dashboards-04.log 2>&1 &
  OSD_PID=$!

  for attempt in {1..12}; do
    if curl -k -fsS -u "${USERNAME}:${PASSWORD}" \
      "${OS_URL}/_cluster/health" >/dev/null 2>&1; then
      log "OpenSearch API ready (attempt ${attempt}/12)"
      return
    fi
    log "Waiting for OpenSearch API (attempt ${attempt}/12)"
    sleep 5
  done

  log "OpenSearch port-forward log:"
  cat /tmp/opensearch-04.log 2>/dev/null || true
}

oneTimeTearDown() {
  kill "${OS_PID}" "${OSD_PID}" 2>/dev/null || true
  wait "${OS_PID}" "${OSD_PID}" 2>/dev/null || true
}

os_get() {
  curl -k -sS -u "${USERNAME}:${PASSWORD}" "${OS_URL}${1}" 2>/dev/null
}

osd_get() {
  curl -sS -u "${USERNAME}:${PASSWORD}" -H 'osd-xsrf: true' \
    "${OSD_URL}${1}" 2>/dev/null
}

# Verify resources created by the deployed os-bootstrap are available.
testOpenSearchISMPoliciesPresent() {
  log "=== ISM policies ==="
  response=$(os_get '/_plugins/_ism/policies')
  count=$(echo "${response}" | jq '.policies | length' 2>/dev/null)
  ids=$(echo "${response}" | jq -r '.policies[]._id' 2>/dev/null | paste -sd ' ' -)
  log "Policies found: ${count:-0}"
  log "Policy IDs: ${ids:-none}"

  assertNotEquals "Expected at least one installed ISM policy" "0" "${count:-0}"
  invalid=$(echo "${response}" | jq '[.policies[] | select((._id // "") == "" or ((.policy.ism_template // []) | length) == 0)] | length' 2>/dev/null)
  assertEquals "Expected every ISM policy to have an ID and template" "0" "${invalid:-0}"
}

testOpenSearchIndexTemplatesPresent() {
  log "=== Index templates ==="
  response=$(os_get '/_index_template')
  count=$(echo "${response}" | jq '.index_templates | length' 2>/dev/null)
  names=$(echo "${response}" | jq -r '.index_templates[].name' 2>/dev/null | paste -sd ' ' -)
  log "Templates found: ${count:-0}"
  log "Template names: ${names:-none}"

  assertNotEquals "Expected at least one installed index template" "0" "${count:-0}"
  invalid=$(echo "${response}" | jq '[.index_templates[] | select((.name // "") == "" or ((.index_template.index_patterns // []) | length) == 0)] | length' 2>/dev/null)
  assertEquals "Expected every index template to have a name and pattern" "0" "${invalid:-0}"
}

testOpenSearchAlertMonitorsPresent() {
  log "=== Alert monitors ==="
  response=$(curl -k -sS -u "${USERNAME}:${PASSWORD}" \
    -H 'Content-Type: application/json' \
    -X POST "${OS_URL}/_plugins/_alerting/monitors/_search" \
    -d '{"size":200,"query":{"match_all":{}}}' 2>/dev/null)
  count=$(echo "${response}" | jq '.hits.total.value // .hits.total // 0' 2>/dev/null)
  names=$(echo "${response}" | jq -r '.hits.hits[] | (._source.monitor.name // ._source.name // empty)' 2>/dev/null | paste -sd ' ' -)
  log "Monitors found: ${count:-0}"
  log "Monitor names: ${names:-none}"

  assertNotEquals "Expected at least one installed alert monitor" "0" "${count:-0}"
  invalid=$(echo "${response}" | jq '[.hits.hits[] | (._source.monitor // ._source) as $monitor | select(($monitor.name // "") == "" or ($monitor.enabled != true) or (($monitor.inputs // []) | length) == 0)] | length' 2>/dev/null)
  assertEquals "Expected every alert monitor to be enabled and have inputs" "0" "${invalid:-0}"
}

testOpenSearchDashboardsAndBootstrapPresent() {
  log "=== Dashboards and bootstrap status ==="
  dashboards=$(osd_get '/api/saved_objects/_find?type=dashboard&per_page=100&page=1')
  patterns=$(osd_get '/api/saved_objects/_find?type=index-pattern&per_page=100&page=1')
  dashboard_count=$(echo "${dashboards}" | jq '.saved_objects | length' 2>/dev/null)
  pattern_count=$(echo "${patterns}" | jq '.saved_objects | length' 2>/dev/null)
  log "Dashboards found: ${dashboard_count:-0}"
  log "Index patterns found: ${pattern_count:-0}"

  assertNotEquals "Expected at least one Dashboard" "0" "${dashboard_count:-0}"
  assertNotEquals "Expected at least one Dashboard index pattern" "0" "${pattern_count:-0}"

  pattern_ids=$(echo "${patterns}" | jq -c '[.saved_objects[].id]' 2>/dev/null)
  missing_refs=$(echo "${dashboards}" | jq --argjson ids "${pattern_ids:-[]}" '
    [.saved_objects[] | .references[]? | select(.type == "index-pattern") | .id | select(($ids | index(.)) == null)] | length' 2>/dev/null)
  assertEquals "Expected Dashboard index-pattern references to resolve" "0" "${missing_refs:-0}"

  bootstrap=$(os_get '/bootstrap-status/_doc/1')
  bootstrap_status=$(echo "${bootstrap}" | jq -r '._source.status // ""' 2>/dev/null)
  bootstrap_message=$(echo "${bootstrap}" | jq -r '._source.message // ""' 2>/dev/null)
  log "Bootstrap status: ${bootstrap_status:-missing}"
  log "Bootstrap message: ${bootstrap_message:-missing}"
  assertEquals "Expected OpenSearch bootstrap status initialized" "initialized" "${bootstrap_status}"
  assertEquals "Expected OpenSearch bootstrap completion message" \
    "OpenSearch has been bootstrapped." "${bootstrap_message}"
}

shift $#
. "${SHUNIT_PATH}"
