from PyQt5.QtWidgets import QListWidgetItem

from aws_utils.forecast_utils import list_all_forecast_forecasts


def populate_forecasts(ui):
    ui.select_forecast_pb.setEnabled(False)
    s3_path = ui.s3_bucket_path + "/" + ui.dataset_group_name + "/forecast"
    forecasts, status = list_all_forecast_forecasts(ui.forecast_client, ui.dataset_group_arn, s3_path)
    list_widget = ui.list_forecast
    list_widget.clear()
    for forecast in forecasts:
        item = QListWidgetItem(forecast, list_widget)