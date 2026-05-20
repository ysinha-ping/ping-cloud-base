This directory holds only the OpenSearch Operator CRDs (`crd/`).

The operator Deployment, RBAC, ServiceAccount, ConfigMap, and metrics Service
were moved to the `p1as-opensearch` Helm chart in `p1as-observability`
(PDO-5984). The OpenSearchCluster CR, services, storageclass, security
configs, tenants, and the `cluster-info` ConfigMap moved with it.

CRDs intentionally stay in PCB and are applied via kustomize. Removing them
from PCB requires a dedicated CRD-migration MR.
