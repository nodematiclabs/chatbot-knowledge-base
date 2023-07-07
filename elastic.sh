cat <<EOF | kubectl apply -f -
apiVersion: elasticsearch.k8s.elastic.co/v1
kind: Elasticsearch
metadata:
  name: quickstart
spec:
  version: 8.8.1
  nodeSets:
  - name: default
    count: 1
    config:
      node.store.allow_mmap: false
    podTemplate:
      spec:
        containers:
        - name: elasticsearch
          resources:
            requests:
              memory: 4Gi
              cpu: 2
            limits:
              memory: 4Gi
  http:
    tls:
      selfSignedCertificate:
        disabled: true
    service:
      metadata:
        annotations:
          networking.gke.io/load-balancer-type: "Internal"
      spec:
        type: LoadBalancer
        externalTrafficPolicy: Cluster
EOF