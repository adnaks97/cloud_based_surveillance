# cloud_based_surveillance
CSE 546 Project 1: IaaS Cloud Based Video Surveillance using Raspberry Pi and EC2 instances

### Instructions to run code
There are 3 primary components in this project, namely the RPi, controller EC2, and the worker EC2 (ec2_poller). First, login to AWS Educate and create S3 Bucket, 2 SQS Queues and an EC2 instance. Make note of the Queue and Bucket URLs.

#### Raspberry Pi
* Unzip cloud_based_surveillance.zip.
* cd cloud_based_surveillance.
* ```pip3  install -r requirements.txt```
* Make a folder named config. Inside this create a file cfg.yml. Copy paste AWS tokens here in YAML format (<key>: <value>).
* Run ```python motion.py```.
* Inside aws/AWS.py replace the values for variables queue_url and bucket with the appropriate URL from AWS Educate.

#### Creating EC2 Instances
* Create an IAM Role with S3FullAccess and SQSFullAccess policies.
* Create an EC2 instance with the provided AMI (original) and assign it with the IAM role in step 1. Enable a public IP address.
* Launch the instance and SSH into it. Copy cloud_based_surveillance into the EC2 instance.
* Now run the following commands
* ```sudo apt-get install python3-pip```
* ```cd /home/ubuntu/```
* ```cd cloud_based_surveillance.```
* ```pip3 install -r requirements.txt.```
* ```echo "@reboot sh /home/ubuntu/cloud_based_surveillance/detect.sh" >> crontabFile```
* ```sudo crontab crontabFile```
* In the AWS Educate EC2 dashboard, right click the instance and select image -> create image. Choose an appropriate name.
* Now launch  the required number of EC2 instances with newly created Image and IAM roles mentioned in step 1. Enable public IP in all.
* Select one of the instances and make it your controller (note its Instance Id).
#### Execution
* SSH into the controller. Run ```python controller.py```.
* On the RPi, setup sensor and camera and ```run python motion.py```. Start recording videos of length 5s.
* Monitor controller, S3, SQS and EC2 instance dashboard to view the processing pipeline of videos.
* All videos are found in S3 under **Inputs/**. 
* Results are found in S3 under **outputs/**.



