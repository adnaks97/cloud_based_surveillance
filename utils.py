import os
from aws.AWS import AWSClient, queue_url, bucket
import random

def run_darknet(video_path):
	output_path = file_name.split(".")[0]+".txt"
	if os.path.exists(output_path):
		os.remove(output_path)
	cmd = "./darknet detector demo cfg/coco.data cfg/yolov3-tiny.cfg yolov3-tiny.weights {} >> {}"
	os.system(cmd.format(video_path, output_path))


def run_controller():
	def get_random_free_ec2_id():
	    instance_usage_map = instance_usage #make backend call
	    instance_id = None
	    keys = list(instance_usage.keys())
	    random.shuffle(keys)
	    for temp_instance_id in keys:
	        if(instance_usage_map[temp_instance_id]==0):
	            return None
	        elif(instance_usage_map[temp_instance_id]==-1):
	            instance_id = temp_instance_id
	    return instance_id

    aws = AWSClient(auth=True)
    while(True):
        queue_len = aws.get_queue_length(queue_url)   
        if(queue_len!=0):
            instance_id = get_random_free_ec2_id()
            if(instance_id==None):
                continue
            aws.siwtch_on_ec2_instance(instance_id)   