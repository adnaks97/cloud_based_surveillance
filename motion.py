from motion_capture.camera_controller import RaspiVidController
from aws.AWS import AWSClient, queue_url, bucket
from utils import *
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from time import sleep
import RPi.GPIO as GPIO
import datetime
import time
import sys
import os

path = "videos/{}"

def _make_video_file():
	now = datetime.datetime.now()
	s = now.strftime('%Y-%m-%d_%H-%M-%S') + '.h264'
	return s

def _make_video(vidcontrol):

	#os.system(' '.join(vidcontrol.raspividcmd))

    try:
        print("Starting camera")
        #start up raspivid controller
        vidcontrol.start()
        #wait for it to finish
        while(vidcontrol.isAlive()):
            time.sleep(0.5)

    #Ctrl C
    except KeyboardInterrupt:
        print "Cancelled"

    #Error
    except:
        print "Unexpected error:", sys.exc_info()[0]

        raise

    #if it finishes or Ctrl C, shut it down
    finally:
        print "Stopping camera"
        #stop the controller
        vidcontrol.stopController()
        #wait for the tread to finish if it hasn't already
        vidcontrol.join()

    print "Camera is closed"

if __name__ == "__main__":
	GPIO.setwarnings(False)
	GPIO.setmode(GPIO.BOARD)
	GPIO.setup(11, GPIO.IN)         #Read output from PIR motion sensor
	#GPIO.setup(3, GPIO.OUT)         #LED output pin

	aws = AWSClient(auth=True)
	ctr = 0
	j=0
	thread_pool = ThreadPoolExecutor(5)
	process_pool = ProcessPoolExecutor(1)
	darknet_future_holder = None
	rpi_is_free = True
	root = os.getcwd()
	while True:
		i=GPIO.input(11)
		if i==1:
			j += 1
			#print path
			file_name = path.format(_make_video_file())
			vid = RaspiVidController(file_name, 5000, False)
			print "Intruder detected",i
			#os.system("touch " + str(j))
			_make_video(vid)
			print("{}: Created Video".format(file_name))
			print("{}: Sending to Cloud and SQS".format(file_name))
			sqs_s3_upload_future = thread_pool.submit(aws.upload_video_s3_send_message_sqs,file_name,bucket,queue_url)

			if(rpi_is_free):
				rpi_file_name = aws.receive_message_queue(queue_url)
				if rpi_file_name is not None:
					print("{}: Running on RPi".format(rpi_file_name))
					rpi_is_free=False
					try:
						input_file_path = root +"/videos/" + rpi_file_name
						output_file_name = rpi_file_name.split('.')[0] + '.txt'
						output_file_path = root + "/outputs/" + output_file_name
						darknet_future = process_pool.submit(run_darknet_RPi, root, input_file_path, output_file_path)
						#execute darknet parallel
						darknet_future_holder = darknet_future
					except Exception as e:
						print(e)


		if darknet_future_holder is not None:
			#rint("status checker:",darknet_future_holder.done())
			rpi_is_free = darknet_future_holder.done()
            		if(rpi_is_free):
				print ("RPi freed")
                		status, output_path, video_name = darknet_future_holder.result()
                		print ("Result fetched", status)
				output_future = thread_pool.submit(send_output_to_s3,status,output_path,video_name,aws)
				darknet_future_holder = None
			ctr += 1
			#GPIO.output(3, 0)  #Turn OFF LED
			#time.sleep(1)
