The operator Deployment, RBAC, ServiceAccount, ConfigMap, metrics Service,
and CRDs were moved to the `p1as-opensearch` Helm chart in `p1as-observability`
(PDO-5984). The OpenSearchCluster CR, services, storageclass, security
configs, tenants, and the `cluster-info` ConfigMap moved with it.

CRDs are now in the chart's `crds/` directory and applied by ArgoCD via
`includeCRDs: true` in the kustomization helmCharts entry.
