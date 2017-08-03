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
# work on Ubuntu 14.04 and 16.04. It clobbers some local environment, so it is best to
# run this in a fresh install.
#
# The intended user is a person who wants to start contributing code to Impala. This
# script serves as an executable reference point for how to get started.
#
# At this time, it completes in about 4 hours. It generates and loads the test data and
# metadata without using a snapshot (which takes about 2 hours) and it then runs the full
# testsuite (frontend, backend, end-to-end, JDBC, and custom cluster) in "core"
# exploration mode.

set -eux -o pipefail

sudo apt-get update

sudo apt-get --yes install ccache ninja-build g++ gcc git libsasl2-dev libssl-dev make \
     maven python-dev python-setuptools postgresql liblzo2-dev ntp ntpdate

if lsb_release -d | grep "Ubuntu 16.04"
then
  sudo apt-get --yes install openjdk-8-jdk
  export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
  echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.bashrc
elif lsb_release -d | grep "Ubuntu 14.04"
then
  sudo apt-get --yes install openjdk-7-jdk
  export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64
  echo 'export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64' >> ~/.bashrc
else
  echo "CANNOT INSTALL JAVA" >&2
  exit 1
fi

sudo service ntp restart
sudo service ntp stop
sudo ntpdate us.pool.ntp.org
# If on EC2, use Amazon's ntp servers
if [ -f /sys/hypervisor/uuid ] && [ `head -c 3 /sys/hypervisor/uuid` == ec2 ]
then
  sudo sed -i 's/ubuntu\.pool/amazon\.pool/' /etc/ntp.conf
  grep amazon /etc/ntp.conf
  grep ubuntu /etc/ntp.conf
fi
sudo service ntp start

# TODO: config ccache

# TODO: check that there is enough space on disk to do a data load

# If there is no Impala git repo, get one now
if ! test -d ~/Impala
then
  time -p git clone //git-wip-us.apache.org/repos/asf/incubator-impala.git
fi
cd ~/Impala

# IMPALA-3932, IMPALA-3926
if [ -z ${LD_LIBRARY_PATH+x} ]
then
  export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/
  echo 'export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/' >> ~/.bashrc
else
  export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/:$LD_LIBRARY_PATH
  echo 'export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/:$LD_LIBRARY_PATH' >> ~/.bashrc
fi

# Set up postgress for HMS
sudo -u postgres psql -c "CREATE ROLE hiveuser LOGIN PASSWORD 'password';" postgres
sudo -u postgres psql -c "ALTER ROLE hiveuser WITH CREATEDB;" postgres
# TODO: What are the security implications of this?
if lsb_release -d | grep "Ubuntu 16.04"
then
  sudo sed -i 's/local   all             all                                     peer/local   all             all                                     trust/g' /etc/postgresql/9.5/main/pg_hba.conf
elif lsb_release -d | grep "Ubuntu 14.04"
then
  sudo sed -i 's/local   all             all                                     peer/local   all             all                                     trust/g' /etc/postgresql/9.3/main/pg_hba.conf
else
  echo "CANNOT FIX POSTGRES"
  exit 1
fi
sudo service postgresql restart
sudo /etc/init.d/postgresql reload
sudo service postgresql restart

# Setup ssh to ssh to localhost
ssh-keygen -t rsa -N '' -q -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
ssh-keyscan -H github.com >> ~/.ssh/known_hosts
echo "NoHostAuthenticationForLocalhost yes" >> ~/.ssh/config

# Workarounds for HDFS networking issues
echo "127.0.0.1 $(hostname -s) $(hostname)" | sudo tee -a /etc/hosts
sudo sed -i 's/127.0.1.1/127.0.0.1/g' /etc/hosts

sudo mkdir /var/lib/hadoop-hdfs
sudo chown $(whoami) /var/lib/hadoop-hdfs/

echo "*               hard    nofile          1048576" | sudo tee -a /etc/security/limits.conf
echo "*               soft    nofile          1048576" | sudo tee -a /etc/security/limits.conf

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
if lsb_release -d | grep "Ubuntu 14.04"
then
  unset LD_LIBRARY_PATH
fi
source bin/impala-config.sh
export NUM_CONCURRENT_TESTS=$(nproc)
time -p ./buildall.sh -noclean -format -testdata
