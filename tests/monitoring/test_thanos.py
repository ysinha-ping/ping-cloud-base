import unittest
import boto3
import os
import json
import logging
from kubernetes import client, config
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(filename='test_results.log', level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s')

class TestThanos(unittest.TestCase):
    def setUp(self):
        self.iam_client = boto3.client('iam')
        self.ssm_client = boto3.client('ssm')
        self.s3_client = boto3.client('s3')
        self.environments = os.getenv("SUPPORTED_ENV", "dev,test,stage,customer-hub,prod").split(',')
        self.cluster_name = os.getenv("CLUSTER_NAME", "my-cluster")
        self.bucket_name = f"{self.cluster_name}-thanos-bucket"
        
        # Initialize Kubernetes client
        config.load_kube_config()
        self.k8s_client = client.CoreV1Api()

    # IAM and SSM Tests
    def test_irsa_roles_created(self):
        for env in self.environments:
            role_name = f"{self.cluster_name}-irsa-thanos-{env}"
            try:
                response = self.iam_client.get_role(RoleName=role_name)
                self.assertIsNotNone(response, f"IRSA role {role_name} not found")
            except self.iam_client.exceptions.NoSuchEntityException:
                self.fail(f"IRSA role {role_name} does not exist")

    def test_iam_policy_attachment(self):
        policy_name = f"{self.cluster_name}-thanos-irsa-policy"
        for env in self.environments:
            role_name = f"{self.cluster_name}-irsa-thanos-{env}"
            response = self.iam_client.list_attached_role_policies(RoleName=role_name)
            attached_policies = [policy['PolicyName'] for policy in response['AttachedPolicies']]
            self.assertIn(policy_name, attached_policies, f"Policy {policy_name} not attached to role {role_name}")

    def test_ssm_parameters_created(self):
        for env in self.environments:
            parameter_name = f"/{self.cluster_name}/pcpt/config/k8s-config/accounts/{env}/irsa-role/thanos/arn"
            try:
                response = self.ssm_client.get_parameter(Name=parameter_name)
                self.assertIsNotNone(response, f"SSM parameter {parameter_name} not found")
            except self.ssm_client.exceptions.ParameterNotFound:
                self.fail(f"SSM parameter {parameter_name} does not exist")

        parameter_name = f"/{self.cluster_name}/pcpt/config/k8s-config/accounts/customer-hub/service/storage/thanos/uri"
        try:
            response = self.ssm_client.get_parameter(Name=parameter_name)
            self.assertIsNotNone(response, f"SSM parameter {parameter_name} not found")
        except self.ssm_client.exceptions.ParameterNotFound:
            self.fail(f"SSM parameter {parameter_name} does not exist")

    # S3 Bucket Tests
    def test_s3_bucket_exists(self):
        try:
            response = self.s3_client.head_bucket(Bucket=self.bucket_name)
            self.assertIsNotNone(response, f"S3 bucket {self.bucket_name} does not exist")
        except self.s3_client.exceptions.NoSuchBucket:
            self.fail(f"S3 bucket {self.bucket_name} does not exist")

    def test_s3_bucket_lifecycle_configuration(self):
        try:
            response = self.s3_client.get_bucket_lifecycle_configuration(Bucket=self.bucket_name)
            self.assertTrue(any(rule['ID'] == 'thanos_bucket_lifecycle_rule' for rule in response['Rules']),
                            f"S3 bucket lifecycle configuration for {self.bucket_name} does not contain thanos_bucket_lifecycle_rule")
        except self.s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
                self.fail(f"S3 bucket lifecycle configuration for {self.bucket_name} does not exist")
            else:
                raise

   # Kubernetes Pod Tests
    def test_thanos_pods_running(self):
        pod_labels = [
            "app.kubernetes.io/component=storegateway",
            "app.kubernetes.io/component=compactor",
            "app.kubernetes.io/component=receive"
        ]
        for label in pod_labels:
            logging.info(f"Checking pods with label: {label}")
            pods = self.k8s_client.list_namespaced_pod(namespace="prometheus", label_selector=label).items
            logging.info(f"Found {len(pods)} pods with label {label}")
            self.assertGreater(len(pods), 0, f"No pods found for label {label}")
            for pod in pods:
                logging.info(f"Pod {pod.metadata.name} status: {pod.status.phase}, restarts: {pod.status.container_statuses[0].restart_count}")
                self.assertEqual(pod.status.phase, "Running", f"Pod {pod.metadata.name} is not running")
                self.assertEqual(pod.status.container_statuses[0].restart_count, 0, f"Pod {pod.metadata.name} has restarts")

    def test_thanos_pod_logs(self):
        pod_labels = [
            "app.kubernetes.io/component=storegateway",
            "app.kubernetes.io/component=compactor",
            "app.kubernetes.io/component=receive"
        ]
        for label in pod_labels:
            logging.info(f"Checking logs for pods with label: {label}")
            pods = self.k8s_client.list_namespaced_pod(namespace="prometheus", label_selector=label).items
            for pod in pods:
                logs = self.k8s_client.read_namespaced_pod_log(name=pod.metadata.name, namespace="prometheus")
                self.assertIsNotNone(logs, f"No logs found for pod {pod.metadata.name}")
                logging.info(f"Logs for pod {pod.metadata.name}:\n{logs[:500]}")  # Printing the first 500 characters of the logs

if __name__ == "__main__":
    unittest.main()
