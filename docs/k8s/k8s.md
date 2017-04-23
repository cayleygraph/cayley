# Running in Kubernetes

Most examples requires Kubernetes 1.5+ and PersistentVolumes are configured.

After running scripts namespace `cayley` will be created and service with the same name will be available in cluster. Service is of type `ClusterIP` by default. If you want to expose it, consider changing type to `LoadBalancer`.

## Single instance (Bolt)

This is a simplest possible configuration: single Cayley instance with persistent storage, using Bolt as a backend.

```bash
kubectl create -f ./docs/k8s/cayley-single.yml
```

## Distributed (MongoDB cluster)

This example is based on [thesandlord/mongo-k8s-sidecar](https://github.com/thesandlord/mongo-k8s-sidecar) and runs Cayley on top of 3-node Mongo cluster.

```bash
kubectl create -f ./docs/k8s/cayley-mongo.yml
```

## Distributed

*TODO: PostgreSQL, CockroachDB*