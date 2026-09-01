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


if __name__ == '__main__':
    unittest.main()
