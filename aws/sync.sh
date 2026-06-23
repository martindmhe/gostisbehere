#!/bin/bash

# Ensure we exit if any step fails
set -e

# Prompt for the IP address
read -p "Enter the EC2 Public IP: " INPUT_IP

# Clean the input in case you accidentally pasted "ec2-user@IP" or "ssh ec2-user@IP"
CLEAN_IP=$(echo "$INPUT_IP" | sed -e 's/ssh //g' -e 's/ec2-user@//g' | xargs)

# Double check that we actually got an input
if [ -z "$CLEAN_IP" ]; then
    echo "No IP address provided."
    exit 1
fi

echo "Syncing /adatia to ec2-user@$CLEAN_IP..."
scp -i ./resy-key.pem -r ../../adatia ec2-user@"$CLEAN_IP":/home/ec2-user/
echo "Sync complete!."
