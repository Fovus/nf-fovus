#!/bin/bash
REGION="us-east-2"   # Change region if needed
SLEEP_TIME=30        # Time (seconds) to wait after disabling

# Step 1: Get all compute environments
COMPUTE_ENVS=$(aws batch describe-compute-environments \
  --region $REGION \
  --query 'computeEnvironments[].computeEnvironmentName' \
  --output text)

if [ -z "$COMPUTE_ENVS" ]; then
  echo "✅ No compute environments found in region $REGION"
  exit 0
fi

echo "🔍 Found compute environments in $REGION: $COMPUTE_ENVS"

# Step 2: Disable all environments
for CE in $COMPUTE_ENVS; do
  echo "➡️ Disabling $CE ..."
  aws batch update-compute-environment \
    --region $REGION \
    --compute-environment $CE \
    --state DISABLED >/dev/null 2>&1

  if [ $? -eq 0 ]; then
    echo "   ✅ $CE disable requested"
  else
    echo "   ⚠️ Failed to disable $CE (maybe already disabled)"
  fi
done

# Step 3: Wait before deletion
echo "⏳ Waiting $SLEEP_TIME seconds for state to update..."
sleep $SLEEP_TIME

# Step 4: Delete all environments
for CE in $COMPUTE_ENVS; do
  echo "🗑️ Deleting $CE ..."
  aws batch delete-compute-environment \
    --region $REGION \
    --compute-environment $CE >/dev/null 2>&1

  if [ $? -eq 0 ]; then
    echo "   ✅ $CE deleted successfully"
  else
    echo "   ⚠️ Failed to delete $CE (check if attached to job queues)"
  fi
done

echo "🎉 Cleanup complete in region $REGION"

