import boto3
from PyQt5.QtWidgets import QListWidgetItem
from botocore.exceptions import EndpointConnectionError, ClientError

from aws_utils.forecast_utils import list_all_forecast_regions
from src.shared_utils import call_worker
from gui.error_display_functions import login_error


def login_clicked(ui):
    """Checks credentials and permissions and logs in to the AWS account"""

    access_key = ui.access_line_edit.text()
    secret_key = ui.secret_line_edit.text()
    if access_key == "" or secret_key == "":
        login_error("Please enter your programmatic access keys")
    else:
        logged_in, error_name, error_text = aws_login(access_key, secret_key)
        if logged_in:
            ui.access_key = access_key
            ui.secret_key = secret_key
            ui.session = boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
            call_worker(lambda: populate_regions(ui), ui, ui.select_region_page, "Logging in..")
        else:
            if error_name == "connection_error":
                login_error("Can't connect to AWS, please check your internet connection.")
            elif error_name == 'invalid_access':
                login_error("Invalid login credentials.")
            else:
                login_error("Unexpected error occurred. " + error_text)


def populate_regions(ui):
    """Fills out the regions list"""

    ui.select_region_pb.setEnabled(False)
    regions, status = list_all_forecast_regions(ui.session)
    if status == 'failure':
        return status
    list_widget = ui.list_region
    list_widget.clear()
    for region_id, region_name in regions.items():
        item = QListWidgetItem(region_name + ' | ' + region_id, list_widget)
    return 'success'


def aws_login(access_key, secret_key):
    """Logins in to AWS using access and secret keys"""

    try:
        client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        response = client.list_buckets()
        return True, 'none', ''
    except EndpointConnectionError:
        return False, 'connection_error', ''
    except ClientError:
        return False, 'invalid_access', ''
    except Exception as e:
        return False, 'unspecified_error', str(e)
