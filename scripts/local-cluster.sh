#!/usr/bin/env bash
# Local only. Never changes an external kubectl context or pushes an image.
set -euo pipefail
cd "$(dirname "$0")/.."
cluster_name="${CLUSTER_NAME:-async-processing-scale}"
namespace="${NAMESPACE:-async-processing}"
tools="$PWD/.runtime/tools"
mkdir -p "$tools"
kind_version=v0.33.0
keda_version=2.21.0
node_image=kindest/node:v1.34.11@sha256:44e222ee2132dab25ff87301682f89eb82c7880ea3a1bf543bfe9708fd08d67d
if [ ! -x "$tools/kind" ]; then
  curl -fL --retry 2 "https://kind.sigs.k8s.io/dl/$kind_version/kind-linux-amd64" -o "$tools/kind"
  chmod +x "$tools/kind"
fi
if [ ! -x "$tools/kubectl" ]; then
  curl -fL --retry 2 https://dl.k8s.io/release/v1.34.11/bin/linux/amd64/kubectl -o "$tools/kubectl"
  chmod +x "$tools/kubectl"
fi
k() { "$tools/kubectl" --context "kind-$cluster_name" "$@"; }
if [ "${1:-up}" = down ]; then
  "$tools/kind" delete cluster --name "$cluster_name"
  exit
fi
if ! "$tools/kind" get clusters | grep -Fxq "$cluster_name"; then
  "$tools/kind" create cluster --name "$cluster_name" --image "$node_image" \
    --config deploy/kind/cluster.yaml --wait 180s
fi
docker build --target runtime -t async-processing:local .
docker build --target test -t async-processing-test:local .
for image in async-processing:local async-processing-test:local \
  postgres:16.9-alpine rabbitmq:4.3.6-management jaegertracing/jaeger:2.22.0; do
  docker image inspect "$image" >/dev/null 2>&1 || docker pull "$image"
  "$tools/kind" load docker-image --name "$cluster_name" "$image"
done
manifest="$tools/keda-$keda_version.yaml"
if [ ! -f "$manifest" ]; then
  curl -fL --retry 2 "https://github.com/kedacore/keda/releases/download/v$keda_version/keda-$keda_version.yaml" -o "$manifest"
fi
printf '%s  %s\n' b43c89ffeef81722d7e2dd2c079d74789767a0f89cae1336cff784994814f6d7 "$manifest" | sha256sum -c -
k apply --server-side -f "$manifest"
for deployment in keda-operator keda-metrics-apiserver keda-admission; do
  k -n keda rollout status deployment/"$deployment" --timeout=240s
done
k create namespace "$namespace" --dry-run=client -o yaml | k apply -f -
# Public development values match the repository's versioned broker definitions.
# Provision and rotate real credentials separately, including broker definitions.
k -n "$namespace" create secret generic runtime \
  --from-literal=username=async_processing \
  --from-literal=password=async_processing \
  --from-literal=DATABASE_URL=postgresql+psycopg://async_processing:async_processing@postgres:5432/async_processing \
  --from-literal=RABBITMQ_URL=amqp://async_processing:async_processing@rabbitmq:5672/ \
  --from-literal="RABBITMQ_MANAGEMENT_HOST=http://async_processing:async_processing@rabbitmq.$namespace.svc.cluster.local:15672/" \
  --dry-run=client -o yaml | k -n "$namespace" apply -f -
k -n "$namespace" create secret generic rabbitmq-definitions \
  --from-file=docker/rabbitmq/definitions.json \
  --dry-run=client -o yaml | k -n "$namespace" apply -f -
k -n "$namespace" create configmap rabbitmq-config \
  --from-file=docker/rabbitmq/rabbitmq.conf \
  --dry-run=client -o yaml | k -n "$namespace" apply -f -
k -n "$namespace" apply -f deploy/kubernetes/infra.yaml -f deploy/kubernetes/config.yaml
k -n "$namespace" rollout status statefulset/postgres --timeout=240s
k -n "$namespace" rollout status statefulset/rabbitmq --timeout=240s
k -n "$namespace" apply -f deploy/kubernetes/migrate.yaml
k -n "$namespace" wait --for=condition=complete job/migrate --timeout=120s
k -n "$namespace" apply -f deploy/kubernetes/application.yaml \
  -f deploy/kubernetes/scaler.yaml -f deploy/kubernetes/prometheus.yaml
for deployment in api consumer jaeger prometheus; do
  k -n "$namespace" rollout status deployment/"$deployment" --timeout=240s
done
k -n "$namespace" wait --for=condition=Ready scaledobject/consumer --timeout=120s
k -n "$namespace" get pods,hpa,scaledobject
