import unittest
import boto3
import os
import logging
from kubernetes import client, config
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

class TestThanos(unittest.TestCase):
    def setUp(self):
        self.iam_client = boto3.client('iam')
        self.ssm_client = boto3.client('ssm')
        self.s3_client = boto3.client('s3')
        
        self.cluster_name = os.getenv("CLUSTER_NAME")
        if not self.cluster_name:
            logging.error("CLUSTER_NAME environment variable is not set. Exiting test.")
            self.fail("CLUSTER_NAME environment variable is required but not set.")

        self.bucket_name = f"{self.cluster_name}-thanos-bucket"
        self.environments = os.getenv("SUPPORTED_ENV", "dev,test,stage,prod").split(',')

        config.load_kube_config()
        self.k8s_client = client.CoreV1Api()

    def test_irsa_role_exists(self):
        role_name = f"{self.cluster_name}-irsa-thanos"
        try:
            self.iam_client.get_role(RoleName=role_name)
        except self.iam_client.exceptions.NoSuchEntityException:
            self.fail(f"IRSA role {role_name} not found")

    def test_iam_policy_attachment(self):
        policy_name = f"{self.cluster_name}-thanos-irsa-policy"
        role_name = f"{self.cluster_name}-irsa-thanos"
        attached_policies = self.iam_client.list_attached_role_policies(RoleName=role_name)['AttachedPolicies']
        self.assertIn(policy_name, [policy['PolicyName'] for policy in attached_policies], f"Policy {policy_name} not attached to role {role_name}")

    def test_ssm_parameters(self):
        for env in self.environments:
            param_name = f"/{self.cluster_name}/pcpt/config/k8s-config/accounts/{env}/irsa-role/thanos/arn"
            try:
                self.ssm_client.get_parameter(Name=param_name)
            except self.ssm_client.exceptions.ParameterNotFound:
                self.fail(f"SSM parameter {param_name} not found")

    def test_s3_bucket_exists(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except self.s3_client.exceptions.NoSuchBucket:
            self.fail(f"S3 bucket {self.bucket_name} not found")

    def test_thanos_pods_running(self):
        labels = ["storegateway", "compactor", "receive"]
        for label in labels:
            logging.info(f"Checking pods with label: {label}")
            pods = self.k8s_client.list_namespaced_pod(namespace="prometheus", label_selector=f"app.kubernetes.io/component={label}").items
            self.assertGreater(len(pods), 0, f"No pods found for {label}")
            for pod in pods:
                self.assertEqual(pod.status.phase, "Running", f"Pod {pod.metadata.name} not running")
                self.assertEqual(pod.status.container_statuses[0].restart_count, 0, f"Pod {pod.metadata.name} has restarts")

if __name__ == "__main__":
    unittest.main()
