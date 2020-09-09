import string
import threading
import os
import random
from PyQt5.QtCore import QObject, pyqtSignal
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from gui.error_display_functions import error, worker_error_page


def s3_initialize(s3_resource, s3_client, region):
    """Gets the application's bucket if it exists in the region, and creates a new one if it doesn't exist"""

    for bucket in s3_client.list_buckets()["Buckets"]:
        if s3_client.get_bucket_location(Bucket=bucket['Name'])['LocationConstraint'] == 'eu-west-1':
            if 'retail-demand-forecasting-' in bucket["Name"]:
                return bucket["Name"]

    while True:
        try:
            letters_and_digits = string.ascii_lowercase + string.digits
            random_str = ''.join((random.choice(letters_and_digits) for i in range(15)))
            s3_bucket_name = 'retail-demand-forecasting-application-' + random_str
            print(s3_bucket_name)
            response = s3_resource.create_bucket(
                Bucket=s3_bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region
                                           }
            )

            response = s3_client.put_bucket_encryption(
                Bucket=s3_bucket_name,
                ServerSideEncryptionConfiguration={
                    'Rules': [
                        {
                            'ApplyServerSideEncryptionByDefault': {
                                'SSEAlgorithm': 'AES256'
                            }
                        },
                    ]
                }
            )
            return s3_bucket_name
        except s3_resource.meta.client.exceptions.BucketAlreadyExists:
            pass
    # TODO: handle rest of exceptions without exiting the program


def multi_part_upload_with_s3(s3, bucket_name, key_path, file_path, progress_bar):
    # Multipart upload
    try:
        config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                                multipart_chunksize=1024 * 25, use_threads=True)
        s3.meta.client.upload_file(file_path, bucket_name, key_path,
                                   Config=config, Callback=ProgressPercentage(file_path, progress_bar))
        return 'success'
    except ClientError as e:
        worker_error_page('Access Denied!', str(e))
        return 'failure'
    except Exception as e:
        error(str(e))
        return 'failure'


def list_objects_in_dataset_bucket(session, region, bucket_name, dataset_group_name):
    try:
        s3_client = session.client(
            's3', region_name=region
        )
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=dataset_group_name + '/datasets/'
        )
        objects_list = []
        if 'Contents' in response:
            for dataset in response['Contents']:
                objects_list.append(dataset['Key'])
        return objects_list, 'success'
    except Exception as e:
        error(str(e))
        return None, 'failure'


class ProgressPercentage(object):
    def __init__(self, filename, progress_bar):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        progress_bar.setRange(0, self._size)
        print(self._size, "id", id(progress_bar))
        self._progress_bar = progress_bar
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self.progress_signal_emitter = self.ProgressSignalEmitter()

    class ProgressSignalEmitter(QObject):
        trigger = pyqtSignal(int)

        def emit_trigger(self, progress_bar, value):
            self.trigger.connect(progress_bar.setValue)
            self.trigger.emit(value)

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            self.progress_signal_emitter.emit_trigger(self._progress_bar, self._seen_so_far)
