#!/bin/bash
# No 'set -e' here so it continues clearing resources even if one step fails

REGION="us-east-1"

echo "🔍 Finding active Resy runner instances..."
INSTANCE_IDS=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=resy-runner" "Name=instance-state-name,Values=running,stopped,pending" \
  --region $REGION \
  --query "Reservations[*].Instances[*].InstanceId" --output text)

if [ -z "$INSTANCE_IDS" ]; then
    echo "No active instances found."
else
    echo "Terminating instances: $INSTANCE_IDS"
    aws ec2 terminate-instances --instance-ids $INSTANCE_IDS --region $REGION > /dev/null
    echo "Waiting for termination to complete..."
    aws ec2 wait instance-terminated --instance-ids $INSTANCE_IDS --region $REGION
    echo "Instances terminated."
fi

echo "🗑️ Clearing tokens from SSM Parameter Store..."
aws ssm delete-parameter --name "/resy/api_key" --region $REGION 2>/dev/null || true
aws ssm delete-parameter --name "/resy/auth_token" --region $REGION 2>/dev/null || true
aws ssm delete-parameter --name "/resy/payment_id" --region $REGION 2>/dev/null || true

echo "Teardown complete."