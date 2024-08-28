import unittest
import boto3
import os
import logging
from k8s_utils import K8sUtils

logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

class TestThanos(unittest.TestCase):
    def setUp(self):
        self.k8s_utils = K8sUtils()
        self.s3_client = boto3.client('s3')
        
        self.cluster_name = os.getenv("CLUSTER_NAME")
        if not self.cluster_name:
            logging.error("CLUSTER_NAME environment variable is not set. Exiting test.")
            self.fail("CLUSTER_NAME environment variable is required but not set.")

        self.bucket_name = f"{self.cluster_name}-thanos-bucket"

    def test_check_logs_in_s3_bucket(self):
        try:
            logs_objects = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
            self.assertIsNotNone(logs_objects.get('Contents'), f"No logs found in S3 bucket {self.bucket_name}")
        except self.s3_client.exceptions.NoSuchBucket:
            self.fail(f"S3 bucket {self.bucket_name} not found")

    def test_thanos_pods_running_and_ready(self):
        labels = ["storegateway", "compactor", "receive"]
        for label in labels:
            logging.info(f"Checking pods with label: {label}")
            
            self.assertTrue(self.k8s_utils.wait_for_pod_running(label, "prometheus"), f"Pods with label {label} are not running")
            self.assertTrue(self.k8s_utils.wait_for_pod_ready(label, "prometheus"), f"Pods with label {label} are not ready")
            
            pods = self.k8s_utils.get_pods(label, "prometheus")
            for pod in pods:
                restart_count = pod.status.container_statuses[0].restart_count
                self.assertEqual(restart_count, 0, f"Pod {pod.metadata.name} with label {label} has restarted (restart count: {restart_count})")

if __name__ == "__main__":
    unittest.main()
