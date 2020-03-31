Xvfb :1 &
export DISPLAY=:1
cd cloud_based_surveillance
rm *.h264
python3 ec2_poller.py

