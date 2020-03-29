#!/bin/bash
apt-get install python3-pip
cd /home/ubuntu/
git clone https://github.com/adnaks97/cloud_based_surveillance.git
cd cloud_based_surveillance
pip3 install -r requirements.txt
crontab -l > crontabFile
echo "@reboot sh /home/ubuntu/cloud_based_surveillance/detect.sh" >> crontabFile
crontab crontabFile
