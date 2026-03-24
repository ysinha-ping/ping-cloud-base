import unittest
import time
from kubernetes import client, config

class TestApplicationStatus(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config.load_kube_config()
        cls.v1 = client.CoreV1Api()
        
        # Prefixes of all the essential applications to be tested
        required_prefixes = [
            'fluent-bit',
            'opensearch-cluster-hot',
            'logstash-elastic',
            'opensearch-cluster-dashboards',
            'os-controller-manager'
        ]

        # Wait up to 2 minutes for all pods to stabilize
        max_attempts = 24
        for attempt in range(max_attempts):
            cls.all_pods = cls.v1.list_namespaced_pod(namespace='elastic-stack-logging', watch=False).items
            
            all_apps_ready = True
            
            for prefix in required_prefixes:
                matching_pods = [p for p in cls.all_pods if p.metadata.name.startswith(prefix)]
                
                if not matching_pods or not all(p.status.phase == 'Running' for p in matching_pods):
                    all_apps_ready = False
                    break
            
            if all_apps_ready:
                break
                
            if attempt < max_attempts - 1:
                time.sleep(5)
        else:
            cls.all_pods = cls.v1.list_namespaced_pod(namespace='elastic-stack-logging', watch=False).items

    def test_opensearch_pods_running(self):
        pods = self.all_pods
        opensearch_hot_running = all( pod.status.phase == 'Running' for pod in pods if pod.metadata.name.startswith('opensearch-cluster-hot'))
        self.assertTrue(opensearch_hot_running, "opensearch-cluster-hot pod is not running")

    def test_logstash_pods_running(self):
        pods = self.all_pods
        logstash_running = all(
            pod.status.phase == 'Running'
            for pod in pods
            if pod.metadata.name.startswith('logstash-elastic')
        )
        self.assertTrue(logstash_running, "logstash pod is not running")

    def test_os_bootstrap_pod_running_or_completed(self):
        pods = self.all_pods
        for pod in pods:
            if pod.metadata.name.startswith('logstash-elastic') and not pod.metadata.name.startswith('logstash-elastic-s3'):
                init_statuses = pod.status.init_container_statuses or []
                is_running_or_completed = any(
                    init.name == 'opensearch-bootstrap' and (
                        (init.state.running is not None) or
                        (init.state.terminated is not None and init.state.terminated.exit_code == 0)
                    )
                    for init in init_statuses
                )
                self.assertTrue(
                    is_running_or_completed,
                    f"'opensearch-bootstrap' initContainer is neither running nor completed in pod {pod.metadata.name}"
                )

    def test_opensearch_cluster_dashboards_pods_running(self):
        pods = self.all_pods
        opensearch_cluster_dashboards_running = all(pod.status.phase == 'Running' for pod in pods if pod.metadata.name.startswith('opensearch-cluster-dashboards'))
        self.assertTrue(opensearch_cluster_dashboards_running, "opensearch-cluster-dashboards pods are not running")

    def test_os_controller_manager_pods_running(self):
        pods = self.all_pods
        os_controller_manager_running = all(pod.status.phase == 'Running' for pod in pods if pod.metadata.name.startswith('os-controller-manager'))
        self.assertTrue(os_controller_manager_running, "os-controller-manager pods are not running")

    def test_fluent_bit_pods_running(self):
        pods = self.all_pods
        fluent_bit_pods = [pod for pod in pods if pod.metadata.name.startswith('fluent-bit')]
        
        # Verify all fluent-bit pods are running
        for pod in fluent_bit_pods:
            self.assertEqual(
                pod.status.phase, 'Running',
                f"fluent-bit pod {pod.metadata.name} is in {pod.status.phase} state, not Running"
            )

if __name__ == '__main__':
    unittest.main()
