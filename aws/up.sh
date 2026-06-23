#!/bin/bash
set -e

# ==========================================
# CONFIGURATION - CHANGE THESE IF NEEDED
# ==========================================
REGION="us-east-1"
KEY_NAME="resy-key"  # <-- Change to your actual AWS Key Pair name
SG_NAME="resy-runner-sg"
ROLE_NAME="ResyRunnerRole"
PROFILE_NAME="ResyRunnerProfile"

ENV_FILE="$(dirname "$0")/../.env"

if [ -f "$ENV_FILE" ]; then
    echo "📝 Found .env file. Loading secrets..."
    # Export variables from .env, ignoring commented lines
    export $(grep -v '^#' "$ENV_FILE" | xargs)
else
    echo "⚠️  No .env file found. Falling back to manual entry..."
    read -p "Resy API Key: " RESY_API_KEY
    read -p "Resy Auth Token (JWT): " RESY_AUTH_TOKEN
    read -p "Resy Payment Method ID: " RESY_PAYMENT_METHOD_ID
fi
echo "🚀 Starting setup in $REGION..."

# Quick debug print to see what went wrong (without exposing the whole secret)
echo "--- Validation Check ---"
echo "API Key Length: ${#RESY_API_KEY}"
echo "Auth Token Length: ${#RESY_AUTH_TOKEN}"
echo "Payment Method ID Length: ${#RESY_PAYMENT_METHOD_ID}"

# 1. Network discovery
VPC_ID=$(aws ec2 describe-vpcs --filters "Name=is-default,Values=true" --region $REGION --query "Vpcs[0].VpcId" --output text)
SUBNET_ID=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC_ID" --region $REGION --query "Subnets[0].SubnetId" --output text)
MY_IP=$(curl -s https://checkip.amazonaws.com)/32

# 2. Security Group setup
echo "📦 Setting up Security Group..."
if aws ec2 describe-security-groups --group-names $SG_NAME --region $REGION >/dev/null 2>&1; then
    SG_ID=$(aws ec2 describe-security-groups --group-names $SG_NAME --region $REGION --query "SecurityGroups[0].GroupId" --output text)
    echo "Found existing Security Group: $SG_ID"
else
    SG_ID=$(aws ec2 create-security-group --group-name $SG_NAME --description "Resy runner SG" --vpc-id $VPC_ID --region $REGION --query "GroupId" --output text)
    aws ec2 authorize-security-group-ingress --group-id $SG_ID --protocol tcp --port 22 --cidr $MY_IP --region $REGION
    echo "Created new Security Group: $SG_ID"
fi

# 3. Push secrets to SSM Parameter Store
echo "🔑 Storing secrets securely in SSM..."
aws ssm put-parameter --name "/resy/api_key" --value "$RESY_API_KEY" --type "SecureString" --overwrite --region $REGION > /dev/null
aws ssm put-parameter --name "/resy/auth_token" --value "$RESY_AUTH_TOKEN" --type "SecureString" --overwrite --region $REGION > /dev/null
aws ssm put-parameter --name "/resy/payment_method_id" --value "$RESY_PAYMENT_METHOD_ID" --type "SecureString" --overwrite --region $REGION > /dev/null

# 4. Use AWS Systems Manager to fetch the latest AL2023 x86_64 AMI dynamically
echo "🔍 Resolving latest Amazon Linux 2023 AMI via SSM..."
AMI_ID=$(aws ssm get-parameters --names "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64" --region $REGION --query "Parameters[0].Value" --output text)

echo "Latest AL2023 AMI resolved: $AMI_ID"

# 5. Launch Instance
echo "🖥️ Launching t3.micro instance and installing packages..."
INSTANCE_ID=$(aws ec2 run-instances \
  --image-id $AMI_ID \
  --instance-type t3.micro \
  --key-name $KEY_NAME \
  --security-group-ids $SG_ID \
  --subnet-id $SUBNET_ID \
  --iam-instance-profile Name=$PROFILE_NAME \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=resy-runner}]' \
  --region $REGION \
  --user-data '#!/bin/bash
sudo dnf update -y
sudo dnf install -y python3.11 python3.11-pip git
python3.11 -m pip install --user 'httpx[http2]' python-dotenv
' \
  --query "Instances[0].InstanceId" --output text)

# 6. Wait for public IP to be assigned
echo "⏳ Waiting for instance to boot and get a public IP..."
aws ec2 wait instance-running --instance-ids $INSTANCE_ID --region $REGION

PUBLIC_IP=$(aws ec2 describe-instances --instance-ids $INSTANCE_ID --region $REGION --query "Reservations[0].Instances[0].PublicIpAddress" --output text)

echo "--------------------------------------------------------"
echo "✅ Setup Complete!"
echo "Instance ID: $INSTANCE_ID"
echo "Public IP:   $PUBLIC_IP"
echo "--------------------------------------------------------"
echo "Run this command to SSH in:"
echo "ssh -i ./resy-key.pem ec2-user@$PUBLIC_IP"
echo "--------------------------------------------------------"