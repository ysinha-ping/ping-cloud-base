import unittest
import boto3
import os
import logging
from kubernetes import client, config
from k8s_utils import K8sUtils  # Import the K8sUtils class

logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

class TestThanos(unittest.TestCase):
    def setUp(self):
        # Load the kubeconfig and set up the Kubernetes client
        config.load_kube_config()
        self.core_client = client.CoreV1Api()

        # Explicitly set the namespace to 'prometheus'
        self.namespace = "prometheus"

        # Print current context and namespace for debugging
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

        # Create an instance of K8sUtils to use its methods
        self.k8s_utils = K8sUtils()

    def test_thanos_pods_running_and_ready(self):
        # Use the correct label selectors based on the pod labels
        labels = [
            "app.kubernetes.io/component=storegateway",
            "app.kubernetes.io/component=compactor",
            "app.kubernetes.io/component=receive"
        ]
        for label in labels:
            logging.info(f"Checking if pods with label {label} are running in namespace '{self.namespace}'")

            # Use the instance methods from K8sUtils
            if not self.k8s_utils.wait_for_pod_running(label, self.namespace):
                logging.error(f"No pods with label '{label}' are in the 'Running' state.")
                self.fail(f"Pods with label '{label}' are not running.")
            else:
                logging.info(f"Pods with label '{label}' are running.")

            if not self.k8s_utils.wait_for_pod_ready(label, self.namespace):
                logging.error(f"Pods with label '{label}' in namespace '{self.namespace}' are not ready.")
                self.fail(f"Pods with label '{label}' are not ready.")
            else:
                logging.info(f"Pods with label '{label}' are ready.")

if __name__ == "__main__":
    unittest.main()
