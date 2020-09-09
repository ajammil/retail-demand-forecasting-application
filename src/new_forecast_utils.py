import time

import caffeine

from aws_utils.forecast_utils import list_all_forecasts, is_location_in_datasets, create_predictor, create_forecast, create_forecast_export
from aws_utils.glue_utils import create_database, create_crawler, run_crawler
from gui.error_display_functions import error
from src.shared_utils import combobox_to_freq


def clear_create_new_forecast_page(ui):
    ui.new_forecast_line_edit.clear()
    ui.remote_forecasts = list_all_forecasts(ui.forecast_client)
    ui.forecast_frequency_value_combobox.setCurrentText('minute(s)')
    ui.forecast_frequency_value_combobox.setCurrentText('1')
    ui.create_new_predictor.setEnabled(False)
    ui.create_new_predictor_advanced.setEnabled(False)
    ui.country_checkbox.setChecked(False)
    ui.auto_ml_checkbox.setChecked(True)
    enable_auto_ml(ui)
    disable_country_specific_training(ui)


def create_predictor_and_forecast(ui):
    try:
        # Using caffeine to prevent pc from sleeping.
        caffeine.on(display=False)

        forecast_export_name = forecast_name = predictor_name = ui.new_forecast_line_edit.text()
        forecast_horizon = ui.predictor_forecast_horizon_spinbox.value()
        forecast_client = ui.forecast_client
        dataset_group_arn = ui.dataset_group_arn
        auto_ml = ui.auto_ml_checkbox.isChecked()

        # Check if a country is selected to use its holidays for forecasting.
        holidays = None
        if ui.country_checkbox.isChecked():
            holidays = selected_country(ui)

        # Check if location is a dimension in the data
        location_in_datasets, status = is_location_in_datasets(forecast_client, dataset_group_arn)
        if status == 'failure':
            caffeine.off()
            return status

        # Check if auto machine learning is selected
        algorithm = None
        if not auto_ml:
            algorithm = ui.algorithms[ui.algorithm_combobox.currentText()]
            print(algorithm)

        # Get frequency from combobox
        forecast_frequency = combobox_to_freq(ui, 'FORECAST')

        # Create a predictor
        predictor_arn, status = create_predictor(forecast_client, predictor_name, algorithm, auto_ml,
                                                 forecast_horizon, forecast_frequency,
                                                 dataset_group_arn, location_in_datasets, holidays)
        if status == 'failure':
            caffeine.off()
            return status

        # Wait until predictor is created
        response = forecast_client.describe_predictor(
            PredictorArn=predictor_arn
        )
        while response['Status'] != 'ACTIVE' and response['Status'] != 'CREATE_FAILED':
            response = forecast_client.describe_predictor(
                PredictorArn=predictor_arn
            )
            time.sleep(10)
        if response['Status'] == 'CREATE_FAILED':
            caffeine.off()
            return 'failure'

        # Create a forecast
        forecast_arn, status = create_forecast(forecast_client, forecast_name, predictor_arn)
        if status == 'failure':
            caffeine.off()
            return status

        # Wait until forecast is created
        response = forecast_client.describe_forecast(
            ForecastArn=forecast_arn
        )
        while response['Status'] != 'ACTIVE' and response['Status'] != 'CREATE_FAILED':
            response = forecast_client.describe_forecast(
               ForecastArn=forecast_arn
            )
            time.sleep(10)
        if response['Status'] == 'CREATE_FAILED':
            caffeine.off()
            return 'failure'
        s3_path = ui.s3_bucket_path + "/" + ui.dataset_group_name + "/forecast/" + forecast_name + "/"
        s3_role_arn = ui.s3_role_arn

        # Create a forecast export
        forecast_export_arn, status = create_forecast_export(ui.forecast_client, forecast_export_name,
                                                             forecast_arn, s3_path, s3_role_arn)
        if status == 'failure':
            caffeine.off()
            return status

        # Wait until forecast export is created
        while response['Status'] != 'ACTIVE' and response['Status'] != 'CREATE_FAILED':
            response = forecast_client.describe_forecast_export_job(
                ForecastExportJobArn=forecast_export_arn
            )
            time.sleep(10)
        if response['Status'] == 'CREATE_FAILED':
            caffeine.off()
            return 'failure'

        # Wait 30 seconds to make sure exported files show on S3 (this should be immediate)
        time.sleep(30)

        # Create a database in AWS Glue
        status = create_database(ui.glue_client)
        if status == 'failure':
            caffeine.off()
            return status

        # Create a crawler in AWS Glue
        status = create_crawler(ui.glue_client, s3_role_arn, s3_path, forecast_name)
        if status == 'failure':
            caffeine.off()
            return status

        # Run the created crawler
        status = run_crawler(forecast_name, ui.glue_client)
        if status == 'failure':
            caffeine.off()
            return status

        caffeine.off()
        return 'success'
    except Exception as e:
        caffeine.off()
        error(str(e))
        return 'failure'


def selected_country(ui):
    selected = ui.countries_combobox.currentText()
    selected_abbr = selected.split(' - ')[0]
    return selected_abbr


def enable_auto_ml(ui):
    ui.auto_ml = True
    ui.algorithm_combobox.setEnabled(False)
    ui.choose_algorithm_label.setEnabled(False)


def disable_auto_ml(ui):
    ui.auto_ml = False
    ui.algorithm_combobox.setEnabled(True)
    ui.choose_algorithm_label.setEnabled(True)


def enable_country_specific_training(ui):
    ui.country = True
    ui.countries_combobox.setEnabled(True)
    ui.choose_country_label.setEnabled(True)


def disable_country_specific_training(ui):
    ui.country = False
    ui.countries_combobox.setEnabled(False)
    ui.choose_country_label.setEnabled(False)