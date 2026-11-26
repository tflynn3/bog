# Bog operator

Install `config/bog.yaml` and `config/rbac.yaml`, then run the operator using the `bog-operator` service account. Build this module with `go build .`. Example resource:

```yaml
apiVersion: bog.example.com/v1alpha1
kind: Bog
metadata:
  name: logs
spec:
  image: example/bog:version
  replicas: 1
  storage: 20Gi
  storageClassName: local-durable
```

The storage class must provide Linux storage meeting the durability, ownership, locking, atomic rename, sync and powersafe overwrite requirements in the main README. The PVC is retained when the StatefulSet is deleted. Ensure the image runs as the owner of the data directory; it must not be group/world writable. The startup probe allows up to one hour for replay. The headless service is for the single process; connect directly to `logs-0.logs` for cursor affinity.

The server has no replication, membership, leader transfer or version negotiation. The operator rejects scaling and blocks image changes; it does not claim high availability. To change versions, first stop writes and back up the WAL and commit marker together, verify client and WAL compatibility externally, and perform a controlled manual migration. Rollback also requires explicit WAL compatibility. Never run two processes against the same volume. Storage settings are fixed at creation and require an administrative migration to change.
