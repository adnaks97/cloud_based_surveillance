Xvfb :1 &
export DISPLAY=:1
cd cloud_based_surveillance
rm 2020*
python3 ec2_poller.py

