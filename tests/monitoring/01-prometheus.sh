#!/bin/bash

CI_SCRIPTS_DIR="${SHARED_CI_SCRIPTS_DIR:-/ci-scripts}"
. "${CI_SCRIPTS_DIR}"/common.sh "${1}"

if skipTest "${0}"; then
  log "Skipping test ${0}"
  exit 0
fi

NAMESPACE="${PING_CLOUD_NAMESPACE:-prometheus}"

# Verify the Prometheus server API is reachable via its externally exposed URL.
testPrometheusAPIAccessible() {
  log "Checking Prometheus API via /api/v1/status/runtimeinfo"
  curl -k -s "${PROMETHEUS}/api/v1/status/runtimeinfo" >> /dev/null
  assertEquals "Prometheus API is unreachable. URL: ${PROMETHEUS}/api/v1/status/runtimeinfo" 0 $?
}

# Verify k8s_cluster_name and k8s_cluster_region external labels are configured.
# Required for Grafana dashboards to filter by cluster.
testPrometheusExternalLabelsPresent() {
  log "Verifying k8s_cluster_name and k8s_cluster_region external labels are configured"

  response=$(curl -k -s "${PROMETHEUS}/api/v1/status/runtimeinfo" 2>/dev/null)

  assertContains "k8s_cluster_name should be present in Prometheus external labels" \
    "${response}" "k8s_cluster_name"
  assertContains "k8s_cluster_region should be present in Prometheus external labels" \
    "${response}" "k8s_cluster_region"

  log "External labels are present in Prometheus runtime info"
}

# Verify kube_node_info is collected by agent from kube-state-metrics and remote-written.
# Proves: agent scraping works + remote write pipeline is functional.
testPrometheusKubeStateMetricsCollected() {
  log "Verifying kube_node_info is present (collected by agent from kube-state-metrics via remote write)"

  for i in {1..10}; do
    response=$(curl -k -s "${PROMETHEUS}/api/v1/query?query=kube_node_info" 2>/dev/null)
    if echo "${response}" | grep -q '"resultType":"vector"' && \
       echo "${response}" | grep -q '"result":\[{'; then
      log "kube_node_info is present in Prometheus server"
      break
    fi
    log "Attempt ${i}/10 - waiting for kube_node_info..."
    sleep 10
  done

  assertContains "kube_node_info should be present in server" "${response}" "kube_node_info"
}

# Verify machine_cpu_cores from kubernetes-cadvisor scrape job is remote-written.
# Used by kubernetes-dashboard for CPU utilisation calculations.
testPrometheusCAdvisorMetricsCollected() {
  log "Verifying machine_cpu_cores is present (collected by agent from kubernetes-cadvisor job)"

  for i in {1..10}; do
    response=$(curl -k -s "${PROMETHEUS}/api/v1/query?query=machine_cpu_cores" 2>/dev/null)
    if echo "${response}" | grep -q '"resultType":"vector"' && \
       echo "${response}" | grep -q '"result":\[{'; then
      log "machine_cpu_cores is present in Prometheus server"
      break
    fi
    log "Attempt ${i}/10 - waiting for machine_cpu_cores..."
    sleep 10
  done

  assertContains "machine_cpu_cores should be present in server" "${response}" "machine_cpu_cores"
}

# Verify Prometheus Job Exporter pod is running.
# Collects user counts from PingDirectory and exposes as prometheus metrics.
testPrometheusJobExporterRunning() {
  log "Checking Prometheus Job Exporter pod is running in namespace: ${NAMESPACE}"

  POD=$(kubectl -n "${NAMESPACE}" get pods -l app=prometheus-job-exporter \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  test -n "${POD}" && \
    kubectl -n "${NAMESPACE}" get pod "${POD}" -o jsonpath='{.status.phase}' | grep -q "Running"
  assertEquals "Prometheus job exporter pod not running in namespace ${NAMESPACE}" 0 $?
}

# Verify users_count_1..4 metrics are scraped by job exporter and remote-written.
# Proves kubernetes-pods scrape job discovery is working.
testPrometheusJobExporterMetricsScraped() {
  log "Verifying users_count metrics from Job Exporter are present in Prometheus server"

  for i in {1..10}; do
    response=$(curl -k -s "${PROMETHEUS}/api/v1/query?query=users_count_1" 2>/dev/null)
    if echo "${response}" | grep -q '"resultType":"vector"'; then
      log "users_count_1 metric is present in Prometheus server"
      break
    fi
    log "Attempt ${i}/10 - waiting for users_count_1..."
    sleep 10
  done

  assertContains "users_count_1 should be scraped and present in Prometheus server" \
    "${response}" "users_count"
}

# Verify opensearch_cluster_status metric is scraped from OpenSearch service.
# Proves agent authentication and scraping of OpenSearch is working.
testPrometheusOpenSearchMetricsScraped() {
  log "Verifying opensearch_cluster_status metric is present in Prometheus server"

  for i in {1..10}; do
    response=$(curl -k -s "${PROMETHEUS}/api/v1/query?query=opensearch_cluster_status" 2>/dev/null)
    if echo "${response}" | grep -q '"resultType":"vector"'; then
      log "opensearch_cluster_status metric is present in Prometheus server"
      break
    fi
    log "Attempt ${i}/10 - waiting for opensearch_cluster_status..."
    sleep 10
  done

  assertContains "opensearch_cluster_status should be scraped and present in Prometheus server" \
    "${response}" "opensearch_cluster_status"
}

# When arguments are passed to a script you must
# consume all of them before shunit is invoked
# or your script won't run.  For integration
# tests, you need this line.
shift $#

# load shunit
. ${SHUNIT_PATH}

