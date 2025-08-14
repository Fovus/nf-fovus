#!/bin/bash
set -euo pipefail

PATTERN="batch-ec2-instance-profile-*-e1rcpsb1z97t"

# Step 1: List all instance profiles
echo "🔎 Fetching all instance profiles..."
all_profiles=$(aws iam list-instance-profiles \
  --query "InstanceProfiles[].InstanceProfileName" \
  --output text)

if [[ -z "$all_profiles" ]]; then
  echo "✅ No instance profiles found in account."
  exit 0
fi

echo "📋 All instance profiles in account:"
for p in $all_profiles; do
  echo "   - $p"
done
echo ""

# Step 2: Filter profiles matching the exact pattern
echo "🔎 Filtering profiles matching exact pattern: $PATTERN"
filtered_profiles=()
for profile in $all_profiles; do
  if [[ "$profile" == $PATTERN ]]; then
    filtered_profiles+=("$profile")
  fi
done

# Step 3: Print filtered list
if [[ ${#filtered_profiles[@]} -eq 0 ]]; then
  echo "✅ No profiles matched the pattern."
  exit 0
fi

echo "📋 Profiles to be deleted:"
for fp in "${filtered_profiles[@]}"; do
  echo "   - $fp"
done
echo ""
sleep 60
# Step 4: Delete filtered profiles
for profile in "${filtered_profiles[@]}"; do
  echo "🗑️  Deleting instance profile: $profile"

  # Remove attached roles if any
  roles=$(aws iam get-instance-profile --instance-profile-name "$profile" \
    --query "InstanceProfile.Roles[].RoleName" --output text || true)

  if [[ -n "$roles" ]]; then
    for role in $roles; do
      echo "   → Removing role $role from $profile"
      aws iam remove-role-from-instance-profile \
        --instance-profile-name "$profile" \
        --role-name "$role"
    done
  fi

  # Delete the instance profile
  aws iam delete-instance-profile --instance-profile-name "$profile"
  echo "   ✔ Deleted $profile"
done

echo "🎉 Cleanup complete."

