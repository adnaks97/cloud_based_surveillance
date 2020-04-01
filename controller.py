import os
from aws.AWS import AWSClient, queue_url, bucket, output_queue_url
import random
import time

def get_random_free_ec2_id(instance_usage_map):
    keys = list(instance_usage_map.keys())
    random.shuffle(keys)
    for temp_instance_id in keys:
        if(instance_usage_map[temp_instance_id]==-1):
           return temp_instance_id
    return None

def run_controller():
        aws = AWSClient(auth=False)
        aws.reset_instances_status()
        prev = None
        while(True):
                time.sleep(1)
                #queue_len = aws.get_queue_length(queue_url)
                msg = aws.push_msg_from_input_queue_to_output_queue()
                if msg is not None:
                        print ("Servicing : " + msg)
                        #print("Queue is not empty, so looking for a new instance to start")
                        while(True):
                                instance_id = get_random_free_ec2_id(aws.get_python_object_s3(bucket,'status'))
                                #print (instance_id)
                                if instance_id is not None:
                                        #print("All are busy or live instance is free")
                                        print("Switching on new instance:",instance_id)
                                        #aws.push_msg_from_input_queue_to_output_queue()
                                        aws.update_instance_status(instance_id,1)
                                        aws.switch_on_ec2_instance(instance_id)
                                        time.sleep(3)
                                        break


if __name__ == "__main__":
        run_controller()
