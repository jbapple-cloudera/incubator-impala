#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This is a script for setting up Ubuntu 14.04 ec2 instances with
# storage mounted in /mnt. It must be run as root.

set -x

if [[ "root" != "$(whoami)" ]]
then
  echo "This script must be run as root"
  exit 1
fi

# Set up /home/ubuntu (needed by impala-setup) to link to an area with lots of space
mkdir -p /mnt/expanse
chown ubuntu:ubuntu /mnt/expanse
cd /home
sudo -u ubuntu cp -r /home/ubuntu/. /mnt/expanse/
rm -rf /home/ubuntu
ln -s /mnt/expanse ubuntu
chown ubuntu:ubuntu /mnt/expanse

# install prerequisites not taken care of by impla-setup
sudo apt-get update
while ! sudo apt-get --yes --show-progress install openjdk-7-jdk git ninja-build ntp
do
  sudo apt-get update
done

# Make sure clocks are updated for Kudu
sudo service ntp restart
sudo service ntp stop
sudo ntpdate us.pool.ntp.org
sudo service ntp start

# impala-setup
cd /home/ubuntu
IMPALA_SETUP_REPO_URL="https://github.com/awleblang/impala-setup"
TMPDIR=$(mktemp -d)
chmod a+rwx "${TMPDIR}"
cd "${TMPDIR}"
sudo -u ubuntu git clone "${IMPALA_SETUP_REPO_URL}" impala-setup
cd impala-setup
sudo -u ubuntu chmod a+x ./install.sh
# impala-setup's install.sh expects to be run in a sudo
su - ubuntu --command "sudo ${TMPDIR}/impala-setup/install.sh"

# configuration from bin/bootstrap_development.sh that only needs to be done once:
echo "127.0.0.1 $(hostname -s) $(hostname)" | sudo tee -a /etc/hosts
echo 'NoHostAuthenticationForLocalhost yes' \
  | sudo -u ubuntu tee -a /home/ubuntu/.ssh/config
