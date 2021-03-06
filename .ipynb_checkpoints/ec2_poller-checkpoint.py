from concurrent.futures import ThreadPoolExecutor
from aws.AWS import AWSClient, bucket, queue_url
from time import sleep
import psutil
import os

def check_all_threads_complete(futures):
    ctr = 0
    for i,future in enumerate(futures):
        if future.done():
            futures.pop(i)
            ctr += 1
    return (ctr ==len(futures))

def run_darknet(video_path, output_path):
    darknet_dir = "/home/adnaks/darknet/"
    os.chdir(darknet_dir)
    if os.path.exists(output_path):
        os.remove(output_path)
    cmd = "./darknet detector demo cfg/coco.data cfg/yolov3-tiny.cfg yolov3-tiny.weights {} >> {}"
    os.system(cmd.format(video_path, output_path))

    
if __name__ == "__main__":
    pool = ThreadPoolExecutor(3)
    futures = []
    aws = AWSClient(auth=True)
    instance_status = aws.get_python_object_s3(bucket, 'status')
    InstID = 'i-017231cbb330871ed'
    if InstID not in instance_status.keys():
        instance_status[InstID] = 0

    flag = 0
    aws.put_python_object_s3(bucket, 'status', instance_status)

    while(True):
        #do we need to limit the number of threads as well
        proc_usage = (psutil.cpu_percent() +  psutil.cpu_percent() +  psutil.cpu_percent())/3.0
        if(proc_usage >= 80 ):
            #set flag=1 for instance = InstID in S3
            flag = 1
            #add_message_to_queue(current_instance_id,instance_queue_url)
        if aws.get_queue_length(queue_url) == 0:
            if(check_all_threads_complete(futures)):
                #set flag=-1 for instance = InstID in S3
                flag=-1
                instance_status[InstID] = -1
                aws.put_python_object_s3(bucket, 'status', instance_status)
                aws.switch_off_ec2_instance(InstID)
                #ec2.instances.filter(InstanceIds=InstID).stop()
        else:
            video_name = aws.get_message_sqs_download_video_from_s3(bucket, queue_url)
            if video_name is None:
                continue
            video_path = os.getcwd() + '/' + video_name
            output_file = os.getcwd() + '/' + video_name.split('.')[0] + '.txt'
            future = pool.submit(run_darknet, video_path, output_file)
            futures.append(future)
            #send_video_to_sqs_s3('sample.h264',s3_bucket_name,queue_url)
            #for i in range(1,5):
            print('submitted')
        print(flag)
