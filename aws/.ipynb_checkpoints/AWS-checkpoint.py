import boto3
import yaml
import sys
import pickle
from botocore.exceptions import ClientError

queue_url = 'https://queue.amazonaws.com/684896435815/videos_queue'
bucket = 'cse546project1svv'

class AWSClient:
    '''
    AWS Client wrapper class
    '''
    def __init__(self,auth):

        #Read access keys
        if(auth):
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
        else:
            self.session = boto3.Session()

        #Initiating clients
        self.s3_client = self.session.client('s3')
        self.sqs_client  = self.session.client('sqs')
        self.ec2_client =  self.session.client('ec2')

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

    def put_python_object_s3(self, bucket_name, key, inp_obj):
        try:
            response = self.s3_client.put_object(Bucket=bucket_name, Key=key, Body=pickle.dumps(inp_obj))
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def get_python_object_s3(self, bucket_name, key):
        obj = self.s3_client.get_object(Bucket=bucket_name,Key=key)
        serializedObject = obj['Body'].read()
        return pickle.loads(serializedObject)

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

    #-------------EC2 FUNCTIONS------------------------
    def switch_on_ec2_instance(self, instance_id):
        # Do a dryrun first to verify permissions
        try:
            self.ec2_client.start_instances(InstanceIds=[instance_id], DryRun=True)
        except ClientError as e:
            if 'DryRunOperation' not in str(e):
                raise

        # Dry run succeeded, run start_instances without dryrun
        try:
            response = self.ec2_client.start_instances(InstanceIds=[instance_id], DryRun=False)
            return response
        except ClientError as e:
            print(e)
            return None

    def switch_off_ec2_instance(self, instance_id):
        # Do a dryrun first to verify permissions
        try:
            self.ec2_client.stop_instances(InstanceIds=[instance_id], DryRun=True)
        except ClientError as e:
            if 'DryRunOperation' not in str(e):
                raise

        # Dry run succeeded, call stop_instances without dryrun
        try:
            response = self.ec2_client.stop_instances(InstanceIds=[instance_id], DryRun=False)
            return response
        except ClientError as e:
            return None
            
    #----------- USER FACING FUNCTIONS ---------------------
    def upload_video_s3_send_message_sqs(self, video_file_name,bucket,queue_url):
        upload_status = self.upload_file_s3(video_file_name, bucket, self.input_folder_path)
        if(upload_status):
            self.add_message_to_queue(video_file_name,queue_url)
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
