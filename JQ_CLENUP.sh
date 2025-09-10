#!/bin/bash
# Disable ACTIVE AWS Batch Job Queues, wait, then delete DISABLED ones

REGION="us-east-1"   # <-- change your region

echo "🔍 Checking for ACTIVE job queues in $REGION ..."
ACTIVE_QUEUES=$(aws batch describe-job-queues \
  --region $REGION \
  --query 'jobQueues[?state==`ENABLED`].jobQueueName' \
  --output text)

if [ -n "$ACTIVE_QUEUES" ]; then
  for JQ in $ACTIVE_QUEUES; do
    echo "⚠️ Disabling job queue: $JQ"
    aws batch update-job-queue \
      --region $REGION \
      --job-queue $JQ \
      --state DISABLED
  done
  echo "⏳ Waiting 20s for queues to disable..."
  sleep 20
else
  echo "✅ No ACTIVE job queues found."
fi

echo "🔍 Fetching DISABLED job queues in $REGION ..."
DISABLED_QUEUES=$(aws batch describe-job-queues \
  --region $REGION \
  --query 'jobQueues[?state==`DISABLED`].jobQueueName' \
  --output text)

if [ -n "$DISABLED_QUEUES" ]; then
  for JQ in $DISABLED_QUEUES; do
    echo "🗑️ Deleting disabled job queue: $JQ"
    aws batch delete-job-queue \
      --region $REGION \
      --job-queue $JQ
  done
else
  echo "✅ No DISABLED job queues to delete."
fi

echo "🎉 Finished disabling and deleting job queues in region $REGION"

