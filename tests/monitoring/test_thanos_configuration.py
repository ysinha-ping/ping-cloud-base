import unittest
import boto3
import os
import logging
import re
from k8s_utils import K8sUtils

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TestThanos(unittest.TestCase):
    def setUp(self):
        self.k8s_utils = K8sUtils()
        self.namespace = "prometheus"
        self.s3_client = boto3.client('s3')
        self.cluster_name = os.environ["CLUSTER_NAME"]
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
            with self.subTest(pod_component=label):
                logging.info(f"Checking if pods with label {label} are in the Ready state.")
                
                pods_ready = self.k8s_utils.wait_for_pod_ready(label, self.namespace)
                self.assertTrue(pods_ready, f"Pods with label '{label}' are not ready.")
                logging.info(f"Pods with label '{label}' are ready.")

                pod_names = self.k8s_utils.get_deployment_pod_names(label, self.namespace)

                for pod_name in pod_names:
                    with self.subTest(pod_name=pod_name):
                        pod = self.k8s_utils.core_client.read_namespaced_pod(name=pod_name, namespace=self.namespace)
                        container_statuses = pod.status.container_statuses

                        self.assertIsNotNone(container_statuses, f"Pod '{pod_name}' has no container statuses or is not fully initialized.")
                        
                        for container_status in container_statuses:
                            self.assertEqual(container_status.restart_count, 0, 
                                             f"Pod '{pod_name}' has restarted {container_status.restart_count} times.")
                            logging.info(f"Pod '{pod_name}' has no restarts.")
                
                if label in time_limits:
                    for pod_name in pod_names:
                        with self.subTest(log_check=pod_name):
                            logging.info(f"Fetching logs for pod '{pod_name}' in namespace '{self.namespace}'")
                            logs = self.k8s_utils.get_latest_pod_logs(pod_name, None, self.namespace, 100)
                            self.assertIsNotNone(logs, f"Logs for pod '{pod_name}' are empty")

                            time_limit = time_limits[label]
                            log_contains_sync_pattern = self.check_logs_for_sync_pattern(logs, time_limit)
                            self.assertTrue(
                                log_contains_sync_pattern, 
                                f"Pattern not found in logs for pod '{pod_name}' within last {time_limit} minutes."
                            )
                            logging.info(f"Successfully synchronized block metadata pattern found in logs for pod '{pod_name}' within last {time_limit} minutes.")

    def check_logs_for_sync_pattern(self, logs, time_limit):
        pattern = re.compile(r"successfully synchronized block metadata")
        return any(pattern.search(log) for log in logs)

if __name__ == "__main__":
    unittest.main()
