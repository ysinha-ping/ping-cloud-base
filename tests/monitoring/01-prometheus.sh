#!/bin/bash

CI_SCRIPTS_DIR="${SHARED_CI_SCRIPTS_DIR:-/ci-scripts}"
. "${CI_SCRIPTS_DIR}"/common.sh "${1}"

if skipTest "${0}"; then
  log "Skipping test ${0}"
  exit 0
fi

# Verify the Prometheus server API is reachable via its externally exposed URL.
testPrometheusAPIAccessible() {
  log "Checking Prometheus API via /api/v1/status/runtimeinfo"
  
  status=$(curl -k -s -o /dev/null -w "%{http_code}" \
    "${PROMETHEUS}/api/v1/status/runtimeinfo" 2>/dev/null)
  
  assertEquals "Prometheus API should return 200 OK" "200" "${status}"
}

# Verify each agent scrape job has active targets collecting valid data.
# Queries up{job="<name>"} per job — Prometheus sets up=1 for every successful scrape.
# Covers all jobs defined in p1as-prometheus-agent values.yaml.
testPrometheusAgentJobsCollectingData() {
  log "Verifying each agent scrape job has active targets via up metric"

  expected_jobs="prometheus kube-state-metrics kubernetes-apiservers kubernetes-nodes kubernetes-pods kubernetes-cadvisor kubernetes-service-endpoints opensearch-service"

  for job in ${expected_jobs}; do
    response=""
    for i in {1..10}; do
      encoded_job=$(echo "up{job=\"${job}\"}" | sed 's/{/%7B/g;s/}/%7D/g;s/"/%22/g')
      response=$(curl -k -s "${PROMETHEUS}/api/v1/query?query=${encoded_job}" 2>/dev/null)
      result_count=$(echo "${response}" | jq '.data.result | length' 2>/dev/null)
      if [[ ${result_count} -gt 0 ]]; then
        value=$(echo "${response}" | jq -r '.data.result[0].value[1]' 2>/dev/null)
        if [[ "${value}" == "1" ]]; then
          log "Job '${job}' has active targets (up=1 confirmed via jq)"
          break
        fi
      fi
      log "Attempt ${i}/10 - waiting for up=1 for job: ${job}..."
      sleep 10
    done
    assertNotNull "Job '${job}' should have up=1 (target active and scraping)" "${value}"
    assertEquals "Job '${job}' up metric should be 1" "1" "${value}"
  done
}

testPrometheusJobExporterRunning() {
  POD=$(kubectl -n prometheus get pods -l app=prometheus-job-exporter -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  test -n "$POD" && kubectl -n prometheus get pod "$POD" -o jsonpath='{.status.phase}' | grep -q "Running"
  assertEquals "Prometheus job exporter pod not running" 0 $?
}

# Verify machine_cpu_cores{job="kubernetes-cadvisor"} is present.
# Proves kubernetes-cadvisor job is scraping node cadvisor endpoints.
# Used by kubernetes-dashboard for CPU utilisation calculations.
testPrometheusCAdvisorMetricsCollected() {
  log "Verifying machine_cpu_cores is present (collected by agent from kubernetes-cadvisor job)"

  for i in {1..10}; do
    response=$(curl -k -s "${PROMETHEUS}/api/v1/query?query=machine_cpu_cores%7Bjob%3D%22kubernetes-cadvisor%22%7D" 2>/dev/null)
    if echo "${response}" | grep -q '"resultType":"vector"' && \
       echo "${response}" | grep -q '"result":\[{'; then
      log "machine_cpu_cores{job=kubernetes-cadvisor} is present in Prometheus server"
      break
    fi
    log "Attempt ${i}/10 - waiting for machine_cpu_cores..."
    sleep 10
  done

  assertContains "machine_cpu_cores from kubernetes-cadvisor job should be present" "${response}" "machine_cpu_cores"
}

# Verify users_count metrics from the prometheus-job-exporter are present.
# The job exporter runs ldapsearch commands against PingDirectory pods (pingdirectory-0)
# to count users and exposes them as metrics. Scraped by agent's kubernetes-pods job.
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

