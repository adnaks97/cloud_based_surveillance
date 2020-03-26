import os
from aws.AWS import AWSClient, queue_url, bucket
import random

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


    aws = AWSClient(auth=False)
    while(True):
        queue_len = aws.get_queue_length(queue_url)
        if(queue_len!=0):
            instance_id = get_random_free_ec2_id()
            if(instance_id==None):
                continue
            aws.siwtch_on_ec2_instance(instance_id)
