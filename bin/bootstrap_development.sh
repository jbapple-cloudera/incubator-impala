#!/bin/bash

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

# This script bootstraps a development environment from almost nothing; it is known to
# work on Ubuntu 14.04 and 16.04. It clobbers some local environment and system
# configurations, so it is best to run this in a fresh install.
#
# The intended user is a person who wants to start contributing code to Impala. This
# script serves as an executable reference point for how to get started.
#
# At this time, it completes in about 4 hours. It generates and loads the test data and
# metadata without using a snapshot (which takes about 2 hours) and it then runs the full
# testsuite (frontend, backend, end-to-end, JDBC, and custom cluster) in "core"
# exploration mode.

set -eux -o pipefail

if ! lsb_release -d | grep "Ubuntu"
then
  echo "This script only supports Ubuntu" >&2
  exit 1
fi

VERSION=$(lsb_release -rs)

if ! [[ $VERSION = 14.04 || $VERSION = 16.04 ]]
then
  echo "This script only supports Ubuntu 14.04 and 16.04" >&2
  exit 1
fi

sudo apt-get update

sudo apt-get --yes install ccache g++ gcc git liblzo2-dev libsasl2-dev libssl-dev make \
     maven ninja-build ntp ntpdate python-dev python-setuptools postgresql

# TODO: config ccache to give it plenty of space
# TODO: check that there is enough space on disk to do a build and data load

JDK_VERSION=7
if [[ $VERSION = 16.04 ]]
then
  JDK_VERSION=8
fi
sudo apt-get --yes install openjdk-${JDK_VERSION}-jdk
SET_JAVA_HOME="export JAVA_HOME=/usr/lib/jvm/java-${JDK_VERSION}-openjdk-amd64"
echo "$SET_JAVA_HOME" >> ~/.bashrc
eval "$SET_JAVA_HOME"

sudo service ntp restart
sudo service ntp stop
sudo ntpdate us.pool.ntp.org
# If on EC2, use Amazon's ntp servers
if sudo dmidecode -s bios-version | grep amazon
then
  sudo sed -i 's/ubuntu\.pool/amazon\.pool/' /etc/ntp.conf
  grep amazon /etc/ntp.conf
  grep ubuntu /etc/ntp.conf
fi
sudo service ntp start

# If there is no Impala git repo, get one now
if ! [[ -d ~/Impala ]]
then
  time -p git clone https://git-wip-us.apache.org/repos/asf/incubator-impala.git ~/Impala
fi

# IMPALA-3932, IMPALA-3926
SET_LD_LIBRARY_PATH="export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/"
if ! [[ -z ${LD_LIBRARY_PATH+x} ]]
then
  SET_LD_LIBRARY_PATH="$SET_LD_LIBRARY_PATH:"'$LD_LIBRARY_PATH'
fi
echo "$SET_LD_LIBRARY_PATH" >> ~/.bashrc
eval "$SET_LD_LIBRARY_PATH"

# Set up postgress for HMS
sudo -u postgres psql -c "CREATE ROLE hiveuser LOGIN PASSWORD 'password';" postgres
sudo -u postgres psql -c "ALTER ROLE hiveuser WITH CREATEDB;" postgres
# TODO: What are the security implications of this?
for PG_AUTH_FILE in /etc/postgresql/*/main/pg_hba.conf
do
  sudo sed -ri 's/local +all +all +peer/local all all trust/g' $PG_AUTH_FILE
done
sudo service postgresql restart
sudo /etc/init.d/postgresql reload
sudo service postgresql restart

# Setup ssh to ssh to localhost
if [[ -f ~/.ssh/id_rsa ]]
then
  ssh-keygen -t rsa -N '' -q -f ~/.ssh/id_rsa
fi
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
ssh-keyscan -H github.com >> ~/.ssh/known_hosts
echo "NoHostAuthenticationForLocalhost yes" >> ~/.ssh/config

# Workarounds for HDFS networking issues
echo "127.0.0.1 $(hostname -s) $(hostname)" | sudo tee -a /etc/hosts
sudo sed -i 's/127.0.1.1/127.0.0.1/g' /etc/hosts

sudo mkdir -p /var/lib/hadoop-hdfs
sudo chown $(whoami) /var/lib/hadoop-hdfs/

echo "* - nofile 1048576" | sudo tee -a /etc/security/limits.conf

cd ~/Impala
export IMPALA_HOME="$(pwd)"

# LZO is not needed to compile or run Impala, but it is needed for the data load
cd ~
git clone https://github.com/cloudera/impala-lzo.git
ln -s impala-lzo Impala-lzo
git clone https://github.com/cloudera/hadoop-lzo.git
cd hadoop-lzo/
time -p ant package
cd "$IMPALA_HOME"

export MAX_PYTEST_FAILURES=0
if [[ VERSION = "Ubuntu 14.04" ]]
then
  unset LD_LIBRARY_PATH
fi
source bin/impala-config.sh
export NUM_CONCURRENT_TESTS=$(nproc)
time -p ./buildall.sh -noclean -format -testdata
