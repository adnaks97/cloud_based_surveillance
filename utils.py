import os
from aws.AWS import AWSClient, queue_url, bucket
import random

def run_darknet(input_path,output_path):
	os.chdir('../darknet')
	#file_name = input_path.split("/")[-1]
	#output_file_name = file_name.split('.')[0]+".txt"
	if os.path.exists(output_path):
		os.remove(output_path)
	cmd = "./darknet detector demo cfg/coco.data cfg/yolov3-tiny.cfg yolov3-tiny.weights {} >> {}".format(input_path, output_path)
	#print cmd
	state = os.system(cmd)
	print("Darknet extecution status: {}".format(state))

def run_controller():
    aws = AWSClient(auth=False)
    while(True):
	queue_len = aws.get_queue_length(queue_url)   
	if(queue_len!=0):
	    instance_usage_map = aws.get_python_object_s3(bucket,'status')
	    instance_id = get_random_free_ec2_id(instance_usage_map)
	    if(instance_id==None):
		continue
	    aws.siwtch_on_ec2_instance(instance_id)   

def get_random_free_ec2_id(instance_usage_map):
	instance_id = None
	keys = list(instance_usage.keys())
	random.shuffle(keys)
	for temp_instance_id in keys:
	    if(instance_usage_map[temp_instance_id]==0):
		return None
	    elif(instance_usage_map[temp_instance_id]==-1):
		instance_id = temp_instance_id
	return instance_id
