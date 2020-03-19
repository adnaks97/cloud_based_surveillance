from motion_capture.camera_controller import RaspiVidController
from aws.AWS import AWSClient, queue_url, bucket
from utils import run_darknet
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from time import sleep
import RPi.GPIO as GPIO
import datetime
import time
import sys
import os

path = "./videos/{}"

def _make_video_file():
	now = datetime.datetime.now()
	s = now.strftime('%Y-%m-%d_%H-%M-%S') + '.h264'
	return s

def _make_video(vidcontrol):

	#os.system(' '.join(vidcontrol.raspividcmd))

    try:
        print("Starting raspivid controller")
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
        print "Stopping raspivid controller"
        #stop the controller
        vidcontrol.stopController()
        #wait for the tread to finish if it hasn't already
        vidcontrol.join()

    print "Done"

if __name__ == "__main__":
	GPIO.setwarnings(False)
	GPIO.setmode(GPIO.BOARD)
	GPIO.setup(11, GPIO.IN)         #Read output from PIR motion sensor
	#GPIO.setup(3, GPIO.OUT)         #LED output pin

	aws = AWSClient(auth=True)
	ctr = 0
	j=0
	thread_pool = ThreadPoolExecutor(3)
	process_pool = ProcessPoolExecutor(1)
	rpi_is_free = True
	darknet_future_holder = None
	while True:
		i=GPIO.input(11)
		if i==1:
			j += 1
			print path
			file_name = path.format(_make_video_file())
			vid = RaspiVidController(file_name, 3000, False,['-h', '480', '-w', '640'])
			print "Intruder detected",i
			_make_video(vid)
			if(rpi_is_free):	
				rpi_is_free=False
				s3_upload_future = thread_pool.submit(aws.upload_file_s3,file_name,bucket,"input/")
				darknet_future = process_pool.submit(run_darknet,file_name)#execute darknet parallel
				darknet_future_holder = darknet_future
			else:
				sqs_s3_upload_future = thread_pool.submit(aws.upload_video_s3_send_message_sqs,file_name,bucket,queue_url)
				print('submitted {}'.format(file_name))
			
		if darknet_future_holder is not None:
			rpi_is_free = darknet_future_holder.done()
			darknet_future_holder = None


			ctr += 1
			#GPIO.output(3, 0)  #Turn OFF LED
			#time.sleep(1)
