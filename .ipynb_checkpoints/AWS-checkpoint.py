
import boto3

def upload_video_s3(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name    
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def add_message_to_queue(message,queue_url):
    sqs_client.send_message(
    QueueUrl=queue_url,
    MessageBody=
        message
    )   


def download_video_s3(bucket, object_name, file_name=None):
    if file_name is None:
        file_name = object_name  
    s3_client.download_file(bucket, object_name, file_name)
    
def receive_message_queue(queue_url):
    response = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        VisibilityTimeout=5
    )
    if "Messages" in response.keys():
        message = response['Messages'][0]['Body']
        receipt_handle = response['Messages'][0]['ReceiptHandle']

        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        return message
    else:
        return None



def send_video_to_sqs_s3(file_name,bucket,queue_url):
    if(upload_video_s3(file_name,bucket)):
        add_message_to_queue(file_name,queue_url)    
        return True
    return False

def get_video_from_sqs_s3(bucket,queue_url):
    video_name = receive_message_queue(queue_url)
    if(video_name):
        download_video_s3(bucket,video_name)
    return video_name
    
def get_queue_length(queue_url):
    response = sqs_client.get_queue_attributes(
    QueueUrl=queue_url,
    AttributeNames=[
        'ApproximateNumberOfMessages'
    ]
    )
    return int(response['Attributes']['ApproximateNumberOfMessages'])



session = boto3.Session(
    aws_access_key_id="ASIAZ65YCHZT36VHK777",
    aws_secret_access_key="N7JRqxQxtFGClih1GnrliY0gm56NNIpcqYM8QAHK",
    aws_session_token = "FwoGZXIvYXdzEGYaDBAfloPFV7OPf0cdFCK/AVvjSAAYszZQt561v+34WKDLxzeAJTD8eHWJvF+JSLGS4X2Ok9aMarmKIlN5KebIuBxL+zNjbxs5DsSE783dRk7/AJzTBXm9Ij2womUc99LFFnuk06PLE0OGsC4OgF729FPLBN632UevPuAKMSOrams9aJeu13bnmH9ScMBu1R8ea+yV4D8+uRdL9ofL7caLyMRcqiCVcb+jh79bSpA8eoVrEuntm6W0aIV1Z+emCq+5PShKTCF5IrLKSyVxCLG3KI2jrPMFMi37jVZHacj4i2LKbm4TLcSb7A+M9M5YQ9I/u208Q4uE7Edmi2Ge/tXlR5d5z9o=",
    region_name = "us-east-1",
)


# singleton resource access objects
s3_client = session.client('s3')
sqs_client  = session.client('sqs')
queue_url = 'https://queue.amazonaws.com/684896435815/videos_queue'
s3_bucket_name = 'cse546project1svv'


send_video_to_sqs_s3('sample.h264',s3_bucket_name,queue_url)
get_video_from_sqs_s3(s3_bucket_name,queue_url)
get_queue_length(queue_url)





