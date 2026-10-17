#!/usr/bin/env bash
# Local Temporal dev server with Fairness enabled. On Temporal Cloud, turn on the
# Namespace's Fairness toggle and create the search attributes below instead.
set -euo pipefail

# One partition per Task Queue keeps dispatch order easy to see on a laptop. Temporal
# Cloud partitions Task Queues, which makes fairness ratios less exact (still best effort).
exec temporal --disable-config-file server start-dev \
  --ip 127.0.0.1 \
  --namespace default \
  --dynamic-config-value matching.enableFairness=true \
  --dynamic-config-value matching.numTaskqueueReadPartitions=1 \
  --dynamic-config-value matching.numTaskqueueWritePartitions=1 \
  --search-attribute DocSession=Keyword \
  --search-attribute DocTenant=Keyword \
  --search-attribute DocStartedAt=Datetime \
  "$@"
