import boto3
import yaml
import sys
from botocore.exceptions import ClientError
import pickle

queue_url = 'https://queue.amazonaws.com/684896435815/videos_queue'
bucket = 'cse546project1svv'

class AWSClient:
    '''
    AWS Client wrapper class
    '''
    def __init__(self):

        #Read access keys
        with open("config/cfg.yml", 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
            self.access_key_id=cfg['aws_access_key_id']
            self.secret_access_key=cfg['aws_secret_access_key']
            self.session_token = cfg['aws_session_token']
            self.region_name = cfg['region_name']

        #Creating session object
        self.session = boto3.Session(
        aws_access_key_id = self.access_key_id,
        aws_secret_access_key = self.secret_access_key,
        aws_session_token = self.session_token,
        region_name = self.region_name,
        )

        #Initiating clients
        self.s3_client = self.session.client('s3')
        self.sqs_client  = self.session.client('sqs')

        #path constants
        self.input_folder_path = "input/"
        self.output_folder_path = "output/"


    #----------- S3 HELPER FUNCTIONS ---------------------
    def upload_file_s3(self, file_name, bucket_name, folder_path):
        try:
            response = self.s3_client.upload_file(file_name, bucket_name, folder_path+file_name.split('/')[-1])
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def download_file_s3(self, file_name, bucket_name, folder_path):
        self.s3_client.download_file(bucket_name, folder_path+file_name, file_name)


    #----------- SQS  HELPER FUNCTIONS ---------------------
    def add_message_to_queue(self, message, queue_url):
        self.sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=
            message
        )


    def receive_message_queue(self, queue_url):
        response = self.sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            VisibilityTimeout=5
        )
        if "Messages" in response.keys():
            message = response['Messages'][0]['Body']
            receipt_handle = response['Messages'][0]['ReceiptHandle']

            self.sqs_client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            return message
        else:
            return None

    #----------- USER FACING FUNCTIONS ---------------------
    def upload_video_s3_send_message_sqs(self, video_file_name,bucket,queue_url):
        upload_status = self.upload_file_s3(video_file_name, bucket, self.input_folder_path)
        if(upload_status):
            self.add_message_to_queue(video_file_name.split('/')[-1],queue_url)
            return True
        return False

    def get_message_sqs_download_video_from_s3(self, bucket, queue_url):
        video_file_name = self.receive_message_queue(queue_url)
        print(video_file_name)
        if(video_file_name):
            self.download_file_s3(video_file_name, bucket, self.input_folder_path)
        return video_file_name

    def get_queue_length(self, queue_url):
        response = self.sqs_client.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=[
                                'ApproximateNumberOfMessages'
                                ]
                    )
        return int(response['Attributes']['ApproximateNumberOfMessages'])
