import unittest
import boto3
import os
import logging
import re
from kubernetes import client, config
from k8s_utils import K8sUtils  

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TestThanos(unittest.TestCase):
    def setUp(self):
        config.load_kube_config()
        self.core_client = client.CoreV1Api()
        self.namespace = "prometheus"
        contexts, active_context = config.list_kube_config_contexts()
        logging.info(f"Current context: {active_context['name']}")
        logging.info(f"Using cluster: {active_context['context']['cluster']}")
        logging.info(f"Using namespace: {self.namespace}")
        self.s3_client = boto3.client('s3')
        self.cluster_name = os.getenv("CLUSTER_NAME")
        if not self.cluster_name:
            logging.error("CLUSTER_NAME environment variable is not set. Exiting test.")
            self.fail("CLUSTER_NAME environment variable is required but not set.")
        self.bucket_name = f"{self.cluster_name}-thanos-bucket"
        self.k8s_utils = K8sUtils()  

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
            logging.info(f"Checking if pods with label {label} are running in namespace '{self.namespace}'")

            if self.k8s_utils.wait_for_pod_running(label, self.namespace):
                logging.info(f"Pods with label '{label}' are running.")
            else:
                logging.error(f"No pods with label '{label}' are in the 'Running' state.")
                self.fail(f"Pods with label '{label}' are not running.")

            if not self.k8s_utils.wait_for_pod_ready(label, self.namespace):
                logging.error(f"Pods with label '{label}' in namespace '{self.namespace}' are not ready.")
                self.fail(f"Pods with label '{label}' are not ready.")
            else:
                logging.info(f"Pods with label '{label}' are ready.")
                
            pod_names = self.list_pods_by_label(label)
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
                    if self.check_logs_for_sync_pattern(logs, time_limit):
                        logging.info(f"Pattern found in logs for pod '{pod_name}' within last {time_limit} minutes.")
                    else:
                        logging.error(f"Pattern not found in logs for pod '{pod_name}' within last {time_limit} minutes.")
                        self.fail(f"Pattern not found in logs for pod '{pod_name}' within last {time_limit} minutes.")

    def list_pods_by_label(self, label_selector):
        pods = self.core_client.list_namespaced_pod(
            namespace=self.namespace, label_selector=label_selector
        )
        return [pod.metadata.name for pod in pods.items]

    def check_logs_for_sync_pattern(self, logs, time_limit):
        pattern = re.compile(r"successfully synchronized block metadata")
        for log in logs:
            if pattern.search(log):
                return True
        return False

if __name__ == "__main__":
    unittest.main()
