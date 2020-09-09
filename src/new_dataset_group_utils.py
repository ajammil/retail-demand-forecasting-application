from aws_utils.forecast_utils import create_dataset_group
from src.shared_utils import populate_dataset_groups


def new_dataset_group_create_clicked_worker(ui):
    create_dataset_group(ui.forecast_client, ui.new_dataset_group_name_line_edit.text())
    populate_dataset_groups(ui)
