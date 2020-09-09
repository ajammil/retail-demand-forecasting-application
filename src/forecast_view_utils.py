import os

import pandas as pd

from aws_utils.athena_utils import is_location_in_table, run_forecast_query
from gui.error_display_functions import error


def populate_forecast_view(ui):
    # Clear line edits
    ui.forecast_view_location_line_edit.clear()
    ui.forecast_view_item_id_line_edit.clear()

    # Check if location is a dimension in data
    ui.use_location, status = is_location_in_table(ui.athena_client, ui.selected_forecast)
    if status == 'failure':
        return 'status'

    # Enable/Disable gui for location
    ui.forecast_view_location_line_edit.setEnabled(ui.use_location)
    ui.forecast_view_location_label.setEnabled(ui.use_location)

    # Clear forecast view
    ui.clear_fig()


def lookup_item_clicked(ui):
    # Get item id and location if enabled
    item_id = ui.forecast_view_item_id_line_edit.text()
    location = None
    if ui.use_location:
        location = ui.forecast_view_location_line_edit.text()

    # Run Query on AWS Athena with the provided item_id and location
    data_file_name, status = run_forecast_query(ui.athena_client, ui.selected_forecast, ui.s3_bucket_path, item_id, location)
    if status == 'failure':
        return 'status'
    key = 'queries/'
    object_name = key + data_file_name
    downloading_path = os.path.join(ui.tmp_dir_name, data_file_name)
    try:
        ui.s3_client.download_file(ui.s3_bucket_name, object_name, downloading_path)
        df = pd.read_csv(downloading_path)
        ui.draw_df(df)
        return 'success'
    except Exception as e:
        error(str(e))
        return 'failure'