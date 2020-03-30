from concurrent.futures import ThreadPoolExecutor
from aws.AWS import AWSClient, bucket, queue_url
from utils import run_darknet
from time import sleep
import psutil
import os

max_threads = 1

def check_all_threads_complete(futures):
    ctr = 0
    for i,future in enumerate(futures):
        if future.done():
            futures.pop(i)
            ctr += 1
    return (ctr ==len(futures))

def check_atleast_one_thread_is_free(futures):
    for i,future in enumerate(futures):
        if future.done():
            futures.pop(i)
    return len(futures)< max_threads

if __name__ == "__main__":
    pool = ThreadPoolExecutor(max_threads)
    futures = []
    aws = AWSClient(auth=False)
    InstID = os.popen("ec2metadata --instance-id").read().strip()
    aws.update_instance_status(InstID,0)
    root = os.getcwd()

    while(True):
        #do we need to limit the number of threads as well

        if aws.get_queue_length(queue_url) == 0:
            if(check_all_threads_complete(futures)):
                #set flag=-1 for instance = InstID in S3
                #instance_status[InstID] = -1
                #aws.put_python_object_s3(bucket, 'status', instance_status)
                aws.update_instance_status(InstID,-1)
                print ("Shutdown")
                #aws.switch_off_ec2_instance(InstID)
                #ec2.instances.filter(InstanceIds=InstID).stop()
        else:
            status_check = check_atleast_one_thread_is_free(futures)
            if(len(futures)<max_threads or status_check):
                if(status_check):
                    aws.update_instance_status(InstID,0)
                os.chdir(root)
                video_name = aws.get_message_sqs_download_video_from_s3(bucket, queue_url)
                if video_name is None:
                    continue
                video_path = root + '/' + video_name
                output_file = root + '/' + video_name.split('.')[0] + '.txt'
                #print(root, video_path, output_file)
                future = pool.submit(run_darknet, root, video_path, output_file, aws)
                futures.append(future)
                #send_video_to_sqs_s3('sample.h264',s3_bucket_name,queue_url)
                print('submitted')
            else:
                aws.update_instance_status(InstID,1)

        #print(flag)
