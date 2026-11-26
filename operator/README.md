# Operator status

No Kubernetes operator is shipped yet. The current Bog server is single-node and
cannot provide quorum commit, fencing, leader routing, or safe failover. A
controller that creates multiple replicas would silently create independent
stores. Implementing these guarantees requires server and WAL changes outside
this module's scope. See `docs/operations/bogctl.md` for standalone operations.
