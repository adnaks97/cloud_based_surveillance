import os
from aws.AWS import *

def parse_output_for_detected_objects(filename):
    with open('coco.names') as f:
        objects = f.read().splitlines()
    with open(filename) as f:
        output = f.read().splitlines()
    detected_objects = set()
    for out in output:
        for obj in objects:
            if obj.lower() in out.lower():
                detected_objects.add(obj)
    if(len(detected_objects)==0):
        detected_objects.add("no object detected")
    return list(detected_objects)

def run_darknet(root, video_path, output_path, aws):
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
    if status==0 or status==34816:
        detected_objects= parse_output_for_detected_objects(output_path)
        formatted_output_objects = ",".join(detected_objects)
        video_name = video_path.split('/')[-1]
        aws.put_python_object_s3(bucket, aws.output_folder_path+video_name,formatted_output_objects)