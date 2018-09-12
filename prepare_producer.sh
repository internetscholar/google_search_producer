#!/usr/bin/env bash
sudo apt -y update
sudo apt install -y python3-pip

pip3 install --trusted-host pypi.python.org -r requirements.txt