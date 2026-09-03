import unittest
import subprocess
import time
import base64
import requests
import urllib3
from opensearchpy import OpenSearch
from k8s_utils import K8sUtils

NAMESPACE = "elastic-stack-logging"
LOGSTASH_LABEL = "app=logstash-elastic"
BOOTSTRAP_INIT_CONTAINER = "opensearch-bootstrap"


class TestOpenSearchBootstrapResources(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.k8s = K8sUtils()
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        cls.port_forward_process = subprocess.Popen(
            ["kubectl", "port-forward", "service/opensearch-cluster-headless", "9200:9200",
             "-n", NAMESPACE], stdout=subprocess.PIPE
        )
        time.sleep(5)
        opensearch_creds_secret = cls.k8s.get_namespaced_secret(
            "opensearch-admin-credentials", NAMESPACE
        )
        if opensearch_creds_secret is None:
            raise Exception("Secret opensearch-admin-credentials not found. Is the logging stack deployed?")
        username = base64.b64decode(opensearch_creds_secret.data['username']).decode('utf-8')
        password = base64.b64decode(opensearch_creds_secret.data['password']).decode('utf-8')
        response = requests.get(f"https://localhost:{9200}", verify=False, auth=(username, password))
        if response.status_code == 200:
            print("Port-forward established successfully.")
        else:
            raise Exception("Port-forward failed. Exiting test.")
        cls.opensearch_client = OpenSearch(
            hosts=[{'host': 'localhost', 'port': 9200}],
            http_auth=(username, password),
            use_ssl=True,
            verify_certs=False,
            ssl_show_warn=False,
            timeout=240
        )
        print("OpenSearch client created")

    @classmethod
    def tearDownClass(cls):
        cls.port_forward_process.terminate()

    def test_stub_indices_present_and_readable(self):
        """
        The bootstrap creates *-stub indices so index templates and ISM policies
        have concrete indices to attach to. Verify they exist and are queryable
        on the deployed cluster (PEB is the only place this is checked for real).
        """
        cat = self.opensearch_client.cat.indices(index="*-stub", format="json")
        stub_names = [idx["index"] for idx in cat]
        print(f"Found {len(stub_names)} stub indices: {stub_names}")
        self.assertGreater(
            len(stub_names), 0,
            "No *-stub indices found. OpenSearch bootstrap did not create stub indices."
        )
        for stub in stub_names:
            try:
                count_response = self.opensearch_client.count(index=stub)
                print(f"Stub index '{stub}': {count_response['count']} docs, readable")
            except Exception as e:
                self.fail(f"Stub index '{stub}' exists but is not readable (count failed): {e}")

    def test_bootstrap_init_containers_ran(self):
        """
        Every real Logstash pod carries an opensearch-bootstrap init container
        that seeds ISM policies/templates/monitors into OpenSearch. Verify the
        deployed pods actually ran it (catches the initContainer being dropped
        or broken by the Helm migration). The S3 Logstash variant uses a
        different label and has no bootstrap init container, so it is excluded
        by the label selector.
        """
        pods = self.k8s.core_client.list_namespaced_pod(
            namespace=NAMESPACE, label_selector=LOGSTASH_LABEL
        )
        pod_list = list(pods.items)
        # A pod whose bootstrap init container has not reached a terminal or
        # running state yet would surface as Pending, so give the API a moment.
        time.sleep(2)
        print(f"Found {len(pod_list)} Logstash pods with label {LOGSTASH_LABEL}")
        self.assertGreater(
            len(pod_list), 0,
            f"No pods found with label {LOGSTASH_LABEL} in namespace {NAMESPACE}."
        )
        for pod in pod_list:
            init_names = [c.name for c in (pod.spec.init_containers or [])]
            self.assertIn(
                BOOTSTRAP_INIT_CONTAINER, init_names,
                f"Pod {pod.metadata.name} is missing the {BOOTSTRAP_INIT_CONTAINER} init container. "
                f"Present init containers: {init_names}"
            )
            status = next(
                s for s in (pod.status.init_container_statuses or [])
                if s.name == BOOTSTRAP_INIT_CONTAINER
            )
            if status.state.terminated is not None:
                self.assertEqual(
                    status.state.terminated.exit_code, 0,
                    f"Pod {pod.metadata.name}: {BOOTSTRAP_INIT_CONTAINER} init container "
                    f"exited with code {status.state.terminated.exit_code} "
                    f"(reason: {status.state.terminated.reason})"
                )
                print(f"Pod {pod.metadata.name}: {BOOTSTRAP_INIT_CONTAINER} completed (exit 0)")
            elif status.state.running is not None:
                print(f"Pod {pod.metadata.name}: {BOOTSTRAP_INIT_CONTAINER} still running")
            else:
                self.fail(
                    f"Pod {pod.metadata.name}: {BOOTSTRAP_INIT_CONTAINER} init container is in "
                    f"an unexpected state: {status.state}"
                )

    def test_rollover_readiness(self):
        """
        Index templates declare plugins.index_state_management.rollover_alias.
        If the alias does not resolve to an index with is_write_index=true,
        ISM rollover silently never happens and retention stops working.
        (Aliases are declared in index templates, not ISM policies.)
        """
        templates = self.opensearch_client.indices.get_index_template(name="*")
        rollover_aliases = set()
        for tpl in templates["index_templates"]:
            settings = (
                tpl.get("index_template", {})
                .get("template", {})
                .get("settings", {})
                .get("index", {})
            )
            alias = (
                settings.get("plugins", {})
                .get("index_state_management", {})
                .get("rollover_alias")
            )
            if alias:
                rollover_aliases.add(alias)
        if not rollover_aliases:
            print("No rollover aliases declared in index templates - nothing to check.")
            return
        for alias in sorted(rollover_aliases):
            alias_response = self.opensearch_client.indices.get_alias(name=alias)
            write_indices = [
                idx for idx, meta in alias_response.items()
                if meta.get("aliases", {}).get(alias, {}).get("is_write_index") is True
            ]
            self.assertEqual(
                len(write_indices), 1,
                f"Rollover alias '{alias}' must resolve to exactly one write index. "
                f"Resolved indices: {list(alias_response.keys())}, "
                f"write indices: {write_indices}"
            )
            print(f"Rollover alias '{alias}': write index '{write_indices[0]}' (OK)")

    def test_alert_monitors_present_and_enabled(self):
        """
        The bootstrap seeds alerting monitors into OpenSearch. Verify at least
        one monitor exists and every monitor is enabled with a query input,
        so alerting is armed on the deployed cluster.
        """
        search_body = {"size": 200, "query": {"match_all": {}}}
        response = self.opensearch_client.transport.perform_request(
            "POST", "/_plugins/_alerting/monitors/_search", body=search_body
        )
        monitors = response["hits"]["hits"]
        print(f"Found {len(monitors)} alert monitors")
        self.assertGreater(
            len(monitors), 0,
            "No alert monitors found. Bootstrap did not create any alerting monitors."
        )
        for hit in monitors:
            monitor = hit["_source"]
            name = monitor.get("name", hit["_id"])
            inputs = monitor.get("inputs", [])
            self.assertEqual(
                monitor.get("enabled"), True,
                f"Monitor '{name}' ({hit['_id']}) is not enabled."
            )
            self.assertGreater(
                len(inputs), 0,
                f"Monitor '{name}' ({hit['_id']}) has no inputs configured."
            )
            print(f"Monitor '{name}': enabled, {len(inputs)} input(s) (OK)")

    def test_template_applies_to_new_index(self):
        """
        test_rollover_readiness only checks indices that already exist, so
        it can't catch a template whose index_patterns glob is wrong --
        existing indices already matched *something*. This creates one
        throwaway index matching a template's pattern and verifies
        OpenSearch actually stamped that template's rollover_alias onto it,
        proving the template will fire on new indices, not just that it's
        declared. The created index is always cleaned up.
        """
        template_name = "pdg-access"
        test_index = f"{template_name}-template-apply-test"
        try:
            template = self.opensearch_client.indices.get_index_template(name=template_name)
            tpl_entry = template["index_templates"][0]["index_template"]
            expected_alias = (
                tpl_entry.get("template", {}).get("settings", {}).get("index", {})
                .get("plugins", {}).get("index_state_management", {}).get("rollover_alias")
            )
            self.assertIsNotNone(
                expected_alias, f"Template '{template_name}' declares no rollover_alias to verify."
            )

            self.opensearch_client.indices.create(index=test_index)
            new_index_settings = (
                self.opensearch_client.indices.get(index=test_index)[test_index]
                .get("settings", {}).get("index", {})
            )
            actual_alias = (
                new_index_settings.get("plugins", {})
                .get("index_state_management", {}).get("rollover_alias")
            )
            self.assertEqual(
                actual_alias, expected_alias,
                f"New index '{test_index}' did not inherit rollover_alias '{expected_alias}' "
                f"from template '{template_name}' (got '{actual_alias}'). "
                f"The template's index_patterns may not actually match this index name."
            )
            print(f"New index '{test_index}' correctly inherited rollover_alias '{actual_alias}' "
                  f"from template '{template_name}'")
        finally:
            if self.opensearch_client.indices.exists(index=test_index):
                self.opensearch_client.indices.delete(index=test_index)

    def test_dr_repair_after_resource_deletion(self):
        """
        Simulates a DR scenario: a resource the bootstrap owns disappears
        (e.g. lost in a restore). The real repair mechanism is NOT a
        standalone Job -- it's the opensearch-bootstrap init container
        inside each logstash-elastic pod (the same one
        test_bootstrap_init_containers_ran verifies): on every pod
        (re)start it runs os_bootstrap.py's validate/repair pass, which
        re-PUTs every template/policy/monitor JSON it ships, idempotently.
        This deletes one low-traffic index template (pf-transaction),
        forces one logstash-elastic pod to restart, and asserts the
        template comes back byte-for-byte. Runs on every invocation
        (team-approved blast radius: one template briefly missing, one of
        the logstash replicas briefly restarting).

        Per the "Restore Missing OpenSearch Default Resources" runbook: a
        RED cluster blocks all bootstrap and repair. Deleting a template
        while RED would leave it unrepairable until the cluster recovers,
        so this refuses to run in that state.
        """
        health = self.opensearch_client.cat.health(format="json")[0]
        print(f"Pre-check: cluster status is '{health.get('status')}'")
        if health.get("status") == "red":
            self.skipTest(
                f"Cluster status is RED ({health}); per the OpenSearch DR runbook, "
                f"RED blocks all bootstrap and repair. Refusing to delete a template "
                f"that could not be repaired until the cluster recovers."
            )

        template_name = "pf-transaction"
        try:
            snapshot = self.opensearch_client.indices.get_index_template(name=template_name)
        except Exception as e:
            self.skipTest(f"Sacrificial template '{template_name}' not found, cannot run DR test: {e}")
        snapshot_body = snapshot["index_templates"][0]["index_template"]
        print(f"Snapshotted template '{template_name}' before deletion (rollover_alias="
              f"{snapshot_body.get('template', {}).get('settings', {}).get('index', {}).get('plugins', {}).get('index_state_management', {}).get('rollover_alias')})")

        try:
            print(f"Deleting template '{template_name}' to simulate a DR resource-loss event")
            self.opensearch_client.indices.delete_index_template(name=template_name)
            self.assertFalse(
                self.opensearch_client.indices.exists_index_template(name=template_name),
                f"Template '{template_name}' still present after delete."
            )
            print(f"Confirmed template '{template_name}' is gone")

            print("Restarting one logstash-elastic pod to trigger the opensearch-bootstrap "
                  "init container's targeted-repair pass")
            exit_code = self._restart_logstash_pod_and_wait_for_bootstrap()
            print(f"opensearch-bootstrap init container finished with exit code {exit_code}")
            self.assertEqual(
                exit_code, 0,
                f"opensearch-bootstrap init container exited {exit_code} on the repair restart."
            )

            print(f"Checking whether template '{template_name}' was recreated by the repair pass")
            repaired = self.opensearch_client.indices.get_index_template(name=template_name)
            repaired_body = repaired["index_templates"][0]["index_template"]
            self.assertEqual(
                repaired_body, snapshot_body,
                f"Repaired template '{template_name}' does not match its pre-deletion snapshot."
            )
            print(f"Template '{template_name}' correctly restored by bootstrap repair run "
                  f"and matches its pre-deletion snapshot byte-for-byte")
        finally:
            if not self.opensearch_client.indices.exists_index_template(name=template_name):
                print(f"Cleanup: template '{template_name}' is still missing after the test body, "
                      f"restarting a logstash-elastic pod once more to repair it")
                self._restart_logstash_pod_and_wait_for_bootstrap()

    def _restart_logstash_pod_and_wait_for_bootstrap(self, timeout_seconds=180):
        """
        Deletes one logstash-elastic pod (it's a StatefulSet, so it's
        recreated with the same name) and waits for its opensearch-bootstrap
        init container to reach a terminal state. Returns the exit code.

        Tracks the pre-deletion pod's UID: Kubernetes doesn't clear
        init_container_statuses just because a pod is Terminating, so
        reading status by name alone can return the OLD pod's already-
        terminated (exit 0) status before the NEW pod's init container has
        even started -- the deletion must produce a pod with a different
        UID before its init-container status means anything.
        """
        pods = self.k8s.core_client.list_namespaced_pod(
            namespace=NAMESPACE, label_selector=LOGSTASH_LABEL
        )
        pod_name = pods.items[0].metadata.name
        old_uid = pods.items[0].metadata.uid
        print(f"Deleting pod '{pod_name}' (uid={old_uid}); StatefulSet will recreate it with the same name")
        self.k8s.core_client.delete_namespaced_pod(name=pod_name, namespace=NAMESPACE)

        deadline = time.time() + timeout_seconds
        seen_new_pod = False
        while time.time() < deadline:
            try:
                pod = self.k8s.core_client.read_namespaced_pod(pod_name, NAMESPACE)
            except Exception:
                print(f"Waiting for pod '{pod_name}' to be recreated...")
                time.sleep(3)
                continue
            if pod.metadata.uid == old_uid:
                print(f"Pod '{pod_name}' is still terminating (same uid={old_uid}), waiting...")
                time.sleep(3)
                continue
            if not seen_new_pod:
                print(f"New pod '{pod_name}' (uid={pod.metadata.uid}) detected, "
                      f"waiting for its opensearch-bootstrap init container to finish")
                seen_new_pod = True
            statuses = pod.status.init_container_statuses or []
            bootstrap_status = next(
                (s for s in statuses if s.name == BOOTSTRAP_INIT_CONTAINER), None
            )
            if bootstrap_status is not None and bootstrap_status.state.terminated is not None:
                return bootstrap_status.state.terminated.exit_code
            time.sleep(3)
        raise TimeoutError(
            f"opensearch-bootstrap init container on '{pod_name}' did not complete "
            f"within {timeout_seconds}s"
        )

    def test_security_roles_mapped(self):
        """
        test_opensearch_ui_login checks the login flow for the
        os-configteam backend role but only proves the FRONT DOOR works.
        os-configteam is not itself an OpenSearch role -- it's a backend
        role that must be mapped INTO the kibana_user OpenSearch role for
        Dashboards access to actually work (confirmed live: GET
        .../rolesmapping shows kibana_user.backend_roles containing
        'os-configteam'). This checks that mapping directly: if it's
        missing or empty, RBAC is broken even if the login page happens
        to load for an unrelated reason.
        """
        required_role = "kibana_user"
        required_backend_role = "os-configteam"
        response = self.opensearch_client.transport.perform_request(
            "GET", "/_plugins/_security/api/rolesmapping"
        )
        print(f"Found {len(response)} role mapping(s)")
        self.assertIn(
            required_role, response,
            f"OpenSearch role '{required_role}' has no role mapping configured."
        )
        backend_roles = response[required_role].get("backend_roles", [])
        self.assertIn(
            required_backend_role, backend_roles,
            f"Backend role '{required_backend_role}' is not mapped to OpenSearch role "
            f"'{required_role}' (mapped backend_roles: {backend_roles}). Dashboards "
            f"access for '{required_backend_role}' users would be broken."
        )
        print(f"Backend role '{required_backend_role}' correctly mapped to OpenSearch role '{required_role}'")


if __name__ == '__main__':
    unittest.main()
