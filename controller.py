import os
from aws.AWS import AWSClient, queue_url, bucket, output_queue_url
import random

def get_random_free_ec2_id(instance_usage_map):
    instance_id = None
    keys = list(instance_usage_map.keys())
    random.shuffle(keys)
    for temp_instance_id in keys:
        if(instance_usage_map[temp_instance_id]==0):
            return None
        elif(instance_usage_map[temp_instance_id]==-1):
            instance_id = temp_instance_id
    return instance_id

def run_controller():
	aws = AWSClient(auth=False)
	aws.reset_instances_status()
	while(True):
		queue_len = aws.get_queue_length(queue_url)
		if(queue_len!=0):
			print("Queue is not empty, so looking for a new instance to start")
			instance_id = get_random_free_ec2_id(aws.get_python_object_s3(bucket,'status'))
			if(instance_id==None):
				print("All are busy or live instance is free")
				continue
			if(aws.get_queue_length(queue_url)!=0):
				print("Switching on new instance:",instance_id)
				aws.push_msg_from_input_queue_to_output_queue()
				aws.update_instance_status(instance_id,1)
				aws.switch_on_ec2_instance(instance_id)


if __name__ == "__main__":
	run_controller()
