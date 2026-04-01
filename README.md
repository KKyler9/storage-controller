# storage-admin

`storage-admin` is a Kubernetes controller that reconciles Rook `CephCluster.spec.storage.nodes`
from external `NodeConfiguration` resources.

## Behavior summary

- Watches cluster-scoped `NodeConfiguration` objects from `cloud-admin.paas.usw/v1`.
- Uses `NodeConfiguration.metadata.name` as the target Kubernetes `Node` name.
- Manages only CephClusters labeled `storage-admin.paas.usw/managed-by=true`.
- Supports explicit workflows via node annotations:
  - `storage-admin.paas.usw/osd-purge=true`
  - `storage-admin.paas.usw/osd-recover-reinstalled=true`
- Does **not** destructively purge/remove storage automatically when a node is missing.

## Local development

Because `NodeConfiguration` is imported from an external API module, use either:

- a `go.work` file to include a local checkout of cloud-admin, or
- a `replace` directive in `go.mod`.

Then run:

```bash
go mod tidy
go build ./...
```
