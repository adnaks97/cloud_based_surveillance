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

def check_atleast_one_thread_is_free(futures):
    for i,future in enumerate(futures):
        if future.done():
            futures.pop(i)
    return len(futures)< max_threads

def run_darknet(root, video_path, output_path):
    darknet_dir = "../darknet/"
    os.chdir(darknet_dir)
    if os.path.exists(output_path):
        os.remove(output_path)
    cmd = "./darknet detector demo cfg/coco.data cfg/yolov3-tiny.cfg yolov3-tiny.weights {} >> {}"
    print ("Processing {}".format(video_path.split('/'))[-1])
    status = os.system(cmd.format(video_path, output_path))
    os.system('\n')
    os.chdir(root)
    print('Processed video {} with status {}'.format(video_path.split('/')[-1], status))

if __name__ == "__main__":
    max_threads = 3
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
                future = pool.submit(run_darknet, root, video_path, output_file)
                futures.append(future)
                #send_video_to_sqs_s3('sample.h264',s3_bucket_name,queue_url)
                print('submitted')
            else:
                aws.update_instance_status(InstID,1)

        #print(flag)
