#!/usr/bin/env bash
set -euo pipefail

# Update system packages
sudo apt-get update

# Install prerequisites
sudo apt-get install -y gnupg curl

# Import MongoDB public GPG key
curl -fsSL https://pgp.mongodb.com/server-6.0.asc | sudo gpg --dearmor -o /usr/share/keyrings/mongodb-server-6.0.gpg

# Create the apt sources list for MongoDB
echo "deb [ arch=amd64 signed-by=/usr/share/keyrings/mongodb-server-6.0.gpg ] \
https://repo.mongodb.org/apt/ubuntu $(lsb_release -sc)/mongodb-org/6.0 multiverse" \
  | sudo tee /etc/apt/sources.list.d/mongodb-org-6.0.list

# Update packages again and install MongoDB
sudo apt-get update
sudo apt-get install -y mongodb-org

# Enable and start the service
sudo systemctl enable mongod
sudo systemctl start mongod

echo "MongoDB installation complete."
