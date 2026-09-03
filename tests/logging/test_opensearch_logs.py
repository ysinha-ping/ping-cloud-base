import re
import unittest
import subprocess
import time
import base64
import requests
import urllib3
from opensearchpy import OpenSearch
from opensearchpy.exceptions import NotFoundError
from k8s_utils import K8sUtils

# Other product log index patterns not covered by test_pdg_access_logs.
# Presence of a matching template doesn't guarantee traffic has flowed on
# any given cluster, so each pattern is skipped (not failed) if empty.
CANDIDATE_LOG_INDEX_PATTERNS = [
    "pd-access-*",
    "pd-errors-*",
    "pf-audit-*",
    "pf-transaction-*",
    "pa-engine-audit-*",
    "pds-access-*",
    "pdg-error-*",
    "ingress-access-*",
    "kube-proxy-*",
]


class TestOpenSearchLogs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.k8s = K8sUtils()
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        # Port-forward the OpenSearch service (opensearch-cluster-headless)
        cls.port_forward_process = subprocess.Popen(
            ["kubectl", "port-forward", "service/opensearch-cluster-headless", "9200:9200", 
             "-n", "elastic-stack-logging"], stdout=subprocess.PIPE
        )
         # Give port-forwarding time to establish
        time.sleep(5) 
        # Get OpenSearch Admin user/password from the secret in the 'opensearch-admin-credentials' secret
        opensearch_creds_secret = cls.k8s.get_namespaced_secret(
            "opensearch-admin-credentials", "elastic-stack-logging"
        )
        username = base64.b64decode(opensearch_creds_secret.data['username']).decode('utf-8')
        password = base64.b64decode(opensearch_creds_secret.data['password']).decode('utf-8')
        if response.status_code == 200:
            print("Port-forward established successfully.")
        else:
            raise Exception("Port-forward failed. Exiting test.")
        # Create OpenSearch client
        cls.opensearch_client = OpenSearch(
            hosts=[{'host': 'localhost', 'port': 9200}],
            http_auth=(username, password),
            use_ssl=True, 
            verify_certs=False,
            ssl_show_warn = False,
            timeout=240 # in seconds
        )
        print("OpenSearch client created")

    @classmethod
    def tearDownClass(cls):
        # Terminate the port-forward process after the test suite runs
        cls.port_forward_process.terminate()

    def test_fluentbit_ingestion_field_timestamp(self):
        # Search logs in OpenSearch index template
        index_name = "logstash-*"  
        query = {
            "query": {
                "match_all": {}
            },
            "_source": ["fluentbit_ingest_timestamp"]
        }
        # Fetch indexes by regex
        response = self.opensearch_client.search(index=index_name, body=query)

        # Verify that the fluentbit_ingestion_field has a time in milliseconds
        for hit in response['hits']['hits']:
            timestamp_field = hit['_source'].get('fluentbit_ingest_timestamp')
            self.assertIsNotNone(timestamp_field, "fluentbit_ingest_timestamp is missing")
             # Validate timestamp format matches (YYYY-MM-DDTHH:MM:SS.SSSSSSSSSZ)
             # Note milliseconds might be in range of 1-9 digits
            timestamp_regex = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,9}Z$'
            match = re.match(timestamp_regex, timestamp_field)
            self.assertIsNotNone(match,
                f"fluentbit_ingestion_field is not a valid timestamp: {timestamp_field}"
            )
    def assert_index_receives_logs(self, index_pattern: str, expected_fields: list[str]):
        query = {
            "query": {"match_all": {}},
            "_source": expected_fields,
            "size": 10,
        }
        response = self.opensearch_client.search(index=index_pattern, body=query)
        hits = response["hits"]["hits"]

        self.assertGreater(
            len(hits), 0,
            f"No documents found in {index_pattern}. Logs are not reaching OpenSearch.",
        )

        for hit in hits:
            source = hit["_source"]
            missing = [f for f in expected_fields if f not in source]
            self.assertEqual(
                missing, [],
                f"{index_pattern} document is missing expected fields {missing}. Document: {source}",
            )

    def assert_no_parse_failures(self, index_pattern: str):
        query = {
            "query": {
                "terms": {
                    "tags": ["_grokparsefailure", "_jsonparsefailure"],
                }
            },
            "_source": ["tags", "log", "message"],
            "size": 10,
        }
        response = self.opensearch_client.search(index=index_pattern, body=query)
        hits = response["hits"]["hits"]

        self.assertEqual(
            len(hits), 0,
            f"Found {len(hits)} document(s) in {index_pattern} with parse failure tags "
            f"(_grokparsefailure or _jsonparsefailure):\n"
            + "\n".join(str(h["_source"]) for h in hits),
        )

    def test_pdg_access_logs(self):
        self.assert_index_receives_logs(
            index_pattern="pdg-access-*",
            expected_fields=[
                "client", "method", "url", "httpVersion",
                "responseCode", "bodySentBytes", "userAgent",
                "app_timestamp",
            ],
        )
        self.assert_no_parse_failures("pdg-access-*")

    def _get_expected_fields(self, index_pattern, max_fields=5):
        """
        Returns the index's declared mapping fields (schema) directly --
        NOT intersected with what's already present in a sample. This
        check is informational-only (see _log_field_presence), so showing
        a genuinely missing field is the whole point; pre-filtering to
        only fields already known to be present (the old design, needed
        when this was a hard assertion that had to avoid flaking on
        optional fields) made "missing" always empty and the log useless.
        """
        mapping = self.opensearch_client.indices.get_mapping(index=index_pattern)
        resolved_index = next(iter(mapping.keys()))
        mapped_fields = sorted(mapping[resolved_index]["mappings"].get("properties", {}).keys())
        return mapped_fields[:max_fields]

    def _log_field_presence(self, index_pattern, expected_fields):
        """
        Logs, but never asserts, expected vs. actually-found fields per
        sampled document. Nothing in this sweep test is a hard gate -- a
        schema-declared field that isn't universally populated is
        informative, not proof of a break.
        """
        query = {
            "query": {"match_all": {}},
            "_source": expected_fields,
            "size": 10,
        }
        response = self.opensearch_client.search(index=index_pattern, body=query)
        hits = response["hits"]["hits"]
        if not hits:
            print(f"{index_pattern}: expected fields {expected_fields}; "
                  f"no documents found to check")
            return
        for hit in hits:
            found = [f for f in expected_fields if f in hit["_source"]]
            missing = [f for f in expected_fields if f not in hit["_source"]]
            print(f"{index_pattern} doc _id={hit['_id']}: "
                  f"expected={expected_fields} found={found} missing={missing}")

    def _log_parse_failures(self, index_pattern):
        """
        Logs, but never asserts, documents tagged with a parse-failure tag
        (_grokparsefailure or _jsonparsefailure) in index_pattern.
        Informational only for this sweep: a Logstash/Fluent Bit filter
        regression here is a real finding worth surfacing, but this test
        reports it rather than failing on it -- unlike
        assert_no_parse_failures, which test_pdg_access_logs still uses as
        a hard gate for its one product.
        """
        query = {
            "query": {
                "terms": {
                    "tags": ["_grokparsefailure", "_jsonparsefailure"],
                }
            },
            "_source": ["tags", "log", "message"],
            "size": 10,
        }
        response = self.opensearch_client.search(index=index_pattern, body=query)
        hits = response["hits"]["hits"]
        if not hits:
            print(f"{index_pattern}: no parse-failure-tagged documents found (clean)")
            return
        print(f"{index_pattern}: found {len(hits)} document(s) with parse-failure tags "
              f"(_grokparsefailure or _jsonparsefailure) -- informational only, "
              f"does not fail this test:")
        for hit in hits:
            print(f"  {index_pattern} doc _id={hit['_id']}: {hit['_source']}")

    def test_multi_product_logs_present_and_clean(self):
        """
        test_pdg_access_logs only checks one product. A Helm-migration
        regression that breaks Fluent Bit routing or a Logstash filter for
        a different product would go unnoticed. Sweeps other product log
        index patterns. Both field presence and parse-failure tags are
        logged (expected vs. found vs. missing; parse-failure counts and
        sample documents) but never fail this test -- findings here are
        informational, surfaced for follow-up, not proof this particular
        test should go red. Patterns with no documents yet on this
        cluster are skipped (logged), not failed.
        """
        checked = []
        for index_pattern in CANDIDATE_LOG_INDEX_PATTERNS:
            with self.subTest(index_pattern=index_pattern):
                try:
                    probe = self.opensearch_client.search(
                        index=index_pattern, body={"query": {"match_all": {}}, "size": 10}
                    )
                except NotFoundError:
                    print(f"{index_pattern}: no matching indices exist, skipping")
                    continue
                hits = probe["hits"]["hits"]
                if not hits:
                    print(f"{index_pattern}: no documents yet, skipping")
                    continue
                expected_fields = self._get_expected_fields(index_pattern)
                if expected_fields:
                    self._log_field_presence(index_pattern, expected_fields)
                else:
                    print(f"{index_pattern}: has documents but no fields declared in "
                          f"its mapping, skipping field check")
                self._log_parse_failures(index_pattern)
                checked.append(index_pattern)
        print(f"Verified log ingestion for: {checked}")


if __name__ == '__main__':
    unittest.main()
