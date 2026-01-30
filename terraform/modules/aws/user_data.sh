#!/bin/bash
set -e

# Log output for debugging
exec > >(tee /var/log/user-data.log) 2>&1

echo "=== Starting EC2 bootstrap ==="
echo "Timestamp: $(date)"

# Update system
dnf update -y

# Install Python 3.12 and dependencies
dnf install -y python3.12 python3.12-pip git

# Create symlinks for convenience
alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1

# Clone the repository as ec2-user
cd /home/ec2-user
sudo -u ec2-user git clone ${github_repo} aiven-clickstream-demo

# Install Python dependencies as ec2-user
sudo -u ec2-user bash -c '
  cd ~/aiven-clickstream-demo
  python3.12 -m pip install --user -e .
  echo "export PATH=\$PATH:\$HOME/.local/bin" >> ~/.bashrc
'

echo "=== Bootstrap complete ==="
echo "SSH in and run:"
echo "  cd aiven-clickstream-demo"
echo "  # Copy your .env file"
echo "  clickstream status"
echo "  clickstream benchmark --limit 100000 -y"
