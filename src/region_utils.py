from aws_utils.iam_utils import get_s3_role_arn
from aws_utils.s3_utils import s3_initialize
from src.shared_utils import populate_dataset_groups


def dataset_group_page_selected_worker(ui):
    """Initializes application and populates dataset group list"""

    initialize_application(ui)
    populate_dataset_groups(ui)


def initialize_application(ui):
    """Initializes variables needed for the application"""

    ui.selected_csv = {}
    ui.dataset = {}
    ui.dataset_path = {}
    ui.forecast_client = ui.session.client('forecast', region_name=ui.region)
    ui.athena_client = ui.session.client('athena', region_name=ui.region)
    ui.glue_client = ui.session.client('glue', region_name=ui.region)
    ui.s3_client = ui.session.client('s3', region_name=ui.region)
    ui.s3_resource = ui.session.resource('s3')
    ui.iam_client = ui.session.client('iam')
    ui.dataset_attributes = {}
    ui.ask_for_freq = False

    # TODO move this?
    ui.s3_bucket_name = s3_initialize(ui.s3_resource, ui.s3_client, ui.region)
    ui.s3_bucket_path = f"s3://{ui.s3_bucket_name}"
    ui.s3_role_arn = get_s3_role_arn(ui.iam_client)


