import unittest
import boto3
import os
import logging
import re
from kubernetes import client
from k8s_utils import K8sUtils

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TestThanos(unittest.TestCase):
    def setUp(self):
        self.k8s_utils = K8sUtils()
        self.core_client = self.k8s_utils.core_client
        self.namespace = "prometheus"
        self.s3_client = boto3.client('s3')
        self.cluster_name = os.environ("CLUSTER_NAME")
        self.bucket_name = f"{self.cluster_name}-thanos-bucket"

    def test_thanos_pods_running_and_ready(self):
        labels = [
            "app.kubernetes.io/component=storegateway",  
            "app.kubernetes.io/component=compactor",
            "app.kubernetes.io/component=receive"
        ]
        time_limits = {
            "app.kubernetes.io/component=compactor": 2,
            "app.kubernetes.io/component=storegateway": 17
        }

        for label in labels:
            with self.subTest(label=label):
                logging.info(f"Checking if pods with label {label} are in the Ready state.")

                self.assertTrue(
                    self.k8s_utils.wait_for_pod_ready(label, self.namespace),
                    f"Pods with label '{label}' are not ready."
                )
                logging.info(f"Pods with label '{label}' are ready.")

                pod_names = self.k8s_utils.get_deployment_pod_names(label, self.namespace)
                for pod_name in pod_names:
                    pod = self.core_client.read_namespaced_pod(name=pod_name, namespace=self.namespace)
                    for container_status in pod.status.container_statuses:
                        if container_status.restart_count > 0:
                            logging.error(f"Pod '{pod_name}' has restarted {container_status.restart_count} times.")
                            self.fail(f"Pod '{pod_name}' has restarts. Test failed.")
                        else:
                            logging.info(f"Pod '{pod_name}' has no restarts.")

                if label in time_limits:
                    for pod_name in pod_names:
                        logging.info(f"Fetching logs for pod '{pod_name}' in namespace '{self.namespace}'")
                        logs = self.k8s_utils.get_latest_pod_logs(pod_name, None, self.namespace, 100)
                        time_limit = time_limits[label]
                        self.assertTrue(
                            self.check_logs_for_sync_pattern(logs, time_limit),
                            f"Pattern not found in logs for pod '{pod_name}' within last {time_limit} minutes."
                        )
                        logging.info(f"Successfully synchronized block metadata pattern found in logs for pod '{pod_name}' within last {time_limit} minutes.")

    def check_logs_for_sync_pattern(self, logs, time_limit):
        # Check if the logs contain the 'successfully synchronized block metadata' pattern
        pattern = re.compile(r"successfully synchronized block metadata")
        for log in logs:
            if pattern.search(log):
                return True
        return False


if __name__ == "__main__":
    unittest.main()
