import csv
import os
from datetime import datetime

import caffeine
import numpy as np
import pandas as pd
from PyQt5.QtCore import QObject, pyqtSignal
from PyQt5.QtWidgets import QFileDialog

from aws_utils.forecast_utils import list_all_datasets_names, create_target_dataset, create_related_dataset, \
    create_metadata_dataset, create_dataset_import_job, list_all_forecast_datasets, link_dataset_to_dataset_group
from aws_utils.s3_utils import list_objects_in_dataset_bucket, multi_part_upload_with_s3
from src.data_utils import PandasModel
from gui.error_display_functions import error
from src.shared_utils import combobox_to_freq, change_current_page


def merge_button_clicked(ui):
    save_location = ui.tmp_dir_name
    csv_file_name = ui.merge_file
    related_location_name = ui.related_location_name
    related_date_name = ui.related_date_name
    original_df = ui.dataset['RELATED']
    additional_location_name = ui.merge_location_combobox.currentText()
    additional_date_name = ui.merge_timestamp_combobox.currentText()
    try:
        with open(csv_file_name, newline='') as csv_file:
            reader = csv.reader(csv_file, delimiter=',')
            columns_count = len(next(reader))
            csv_file.seek(0)
            sniffer = csv.Sniffer()
            has_header = sniffer.has_header(csv_file.read(2048))
        if not has_header:
            headers = []
            for number in range(columns_count):
                headers.append("Column " + str(number + 1))
            df = pd.read_csv(csv_file_name, names=headers)
        else:
            df = pd.read_csv(csv_file_name)
        convert_date(df, additional_date_name)
        convert_date(original_df, related_date_name)
        result = pd.merge(original_df, df, left_on=[related_date_name, related_location_name],
                          right_on=[additional_date_name, additional_location_name], how='left')
        result.to_csv(os.path.join(save_location, "merged.csv"), date_format='%Y-%m-%d %H:%M:%S',
                      index=False)

        select_csv_file(ui, 'RELATED', merge_file=os.path.join(save_location, "merged.csv"))

    except Exception as e:
        error(str(e))
        return 'failure'


def reset_progress_bar(ui):
    class ResetProgressBarEmitter(QObject):
        reset_progress_trigger = pyqtSignal()

        def reset_progress(self):
            self.reset_progress_trigger.connect(ui.upload_progress_bar.reset)
            self.reset_progress_trigger.emit()

    reset_progress_emitter = ResetProgressBarEmitter()
    reset_progress_emitter.reset_progress()


def prepare_data(ui, dataset_type):
    ui.remote_datasets = list_all_datasets_names(ui.forecast_client)
    tmp_dir_name = ui.tmp_dir_name
    df = ui.dataset[dataset_type]
    if dataset_type == 'TARGET':
        item_id_name = str(ui.target_item_id_combobox.currentText())
        timestamp_name = str(ui.target_timestamp_combobox.currentText())
        demand_name = str(ui.target_demand_combobox.currentText())
        location_name = str(ui.target_location_combobox.currentText())

        attributes = [
            {
                "AttributeName": "timestamp",
                "AttributeType": "timestamp"
            },
            {
                "AttributeName": "item_id",
                "AttributeType": "string"
            },
            {
                "AttributeName": "demand",
                "AttributeType": "float"
            }
        ]
        if location_name != '':
            attributes.append({
                "AttributeName": "location",
                "AttributeType": "string"
            })
            df = df.reindex(columns=[timestamp_name, item_id_name, demand_name, location_name])
        else:
            df = df.reindex(columns=[timestamp_name, item_id_name, demand_name])
        ui.dataset_attributes[dataset_type] = attributes
        if df[timestamp_name].dtype == 'object':
            try:
                df[timestamp_name] = pd.to_datetime(df[timestamp_name])
            except Exception as e:
                error("Can't convert timestamp to datetime, please check your data. " + str(e))
                return 'failure'
        else:
            error("Can't convert timestamp to datetime, please check your data.")
            return 'failure'
        if not df[timestamp_name].is_monotonic_increasing:
            df = df.sort_values(by=timestamp_name)
        diff_values = np.unique(np.diff(df[timestamp_name].values)).tolist()
        frequency = detect_frequency(diff_values)
        if frequency == 'none':
            output = set_enable_freq_input(ui, True)
        else:
            output = set_enable_freq_input(ui, False, frequency)
        if output == 'failure':
            raise Exception("Failed to set enable frequency inputs.")
        ui.frequency = frequency
        df.to_csv(os.path.join(tmp_dir_name, "tmp_dataset.csv"), date_format='%Y-%m-%d %H:%M:%S', header=False,
                  index=False)
        return 'success'
    elif dataset_type == 'RELATED':
        item_id_name = str(ui.related_item_id_combobox.currentText())
        timestamp_name = str(ui.related_timestamp_combobox.currentText())
        weather_name = str(ui.related_weather_combobox.currentText())
        price_name = str(ui.related_price_combobox.currentText())
        promotion_name = str(ui.related_promotion_combobox.currentText())
        location_name = str(ui.related_location_combobox.currentText())
        columns = [timestamp_name, item_id_name]
        attributes = [
            {
                "AttributeName": "timestamp",
                "AttributeType": "timestamp"
            },
            {
                "AttributeName": "item_id",
                "AttributeType": "string"
            }
        ]
        if location_name != '':
            attributes.append({
                "AttributeName": "location",
                "AttributeType": "string"
            })
            columns.append(location_name)
        if weather_name != '':
            attributes.append({
                "AttributeName": "weather",
                "AttributeType": "float"
            })
            columns.append(weather_name)
        if price_name != '':
            attributes.append({
                "AttributeName": "price",
                "AttributeType": "float"
            })
            columns.append(price_name)
        if promotion_name != '':
            attributes.append({
                "AttributeName": "price",
                "AttributeType": "float"
            })
            columns.append(promotion_name)
        df = df.reindex(columns=columns)
        if promotion_name != '':
            df[promotion_name] = df[promotion_name].replace(
                {True: 1, False: 0, 'True': 1, 'False': 0,
                 'true': 1, 'false': 0, 1.0: 1, 0.0: 0,
                 '1.0': 1, '0.0': 0, '1': 1, '0': 0
                 }
            )
        ui.dataset_attributes[dataset_type] = attributes
        if df[timestamp_name].dtype == 'object':
            try:
                df[timestamp_name] = pd.to_datetime(df[timestamp_name])
            except Exception as e:
                error("Can't convert timestamp to datetime, please check your data. " + str(e))
                return 'failure'
        else:
            error("Can't convert timestamp to datetime, please check your data.")
            return 'failure'
        if not df[timestamp_name].is_monotonic_increasing:
            df = df.sort_values(by=timestamp_name)
        diff_values = np.unique(np.diff(df[timestamp_name].values)).tolist()
        frequency = detect_frequency(diff_values)
        if frequency == 'none':
            output = set_enable_freq_input(ui, True)
        else:
            output = set_enable_freq_input(ui, False, frequency)
        if output == 'failure':
            raise Exception("Failed to enable frequency inputs.")
        ui.frequency = frequency
        ui.dataset[dataset_type] = df
        df.to_csv(os.path.join(tmp_dir_name, "tmp_dataset.csv"), date_format='%Y-%m-%d %H:%M:%S', header=False,
                  index=False)
        return 'success'
    else:
        item_id_name = str(ui.metadata_item_id_combobox.currentText())
        color_name = str(ui.metadata_color_combobox.currentText())
        category_name = str(ui.metadata_category_combobox.currentText())
        brand_name = str(ui.metadata_brand_combobox.currentText())
        columns = [item_id_name]
        attributes = [
            {
                "AttributeName": "item_id",
                "AttributeType": "string"
            }
        ]
        if color_name != '':
            attributes.append({
                "AttributeName": "color",
                "AttributeType": "string"
            })
            columns.append(color_name)
        if category_name != '':
            attributes.append({
                "AttributeName": "category",
                "AttributeType": "string"
            })
            columns.append(category_name)
        if brand_name != '':
            attributes.append({
                "AttributeName": "brand",
                "AttributeType": "string"
            })
            columns.append(brand_name)

        set_enable_freq_input(ui, False)
        df = df.reindex(columns=columns)
        ui.dataset_attributes[dataset_type] = attributes
        df.to_csv(os.path.join(tmp_dir_name, "tmp_dataset.csv"), header=False,
                  index=False)
        return 'success'


def upload_data(ui):
    caffeine.on(display=False)
    tmp_dir_name = ui.tmp_dir_name
    dataset_type = ui.current_dataset_type
    csv_file_name = ui.selected_csv[dataset_type]
    s3 = ui.session.resource('s3')
    s3_objects, status = list_objects_in_dataset_bucket(ui.session, ui.region, ui.s3_bucket_name, ui.dataset_group_name)
    if status == 'failure':
        caffeine.off()
        return status
    file_prefix = "/" + ui.dataset_group_name + "/datasets/" + dataset_type.lower() + '_'
    file_name = os.path.basename(csv_file_name)
    new_file_name = file_name
    ds_number = 0
    while file_prefix + new_file_name in s3_objects:
        ds_number += 1
        new_file_name = os.path.splitext(file_name)[0] + '_' + str(ds_number) + '.csv'

    status = multi_part_upload_with_s3(s3, ui.s3_bucket_name,
                                       ui.dataset_group_name + '/' + "datasets/" + dataset_type.lower() + '_'
                                       + new_file_name,
                                       os.path.join(tmp_dir_name, "tmp_dataset.csv"),
                                       ui.upload_progress_bar)
    if status != 'failure':
        ui.dataset_path[dataset_type] = ui.s3_bucket_path + file_prefix + new_file_name
    caffeine.off()
    return status


def set_enable_freq_input(ui, value, interval=None):
    ui.dataset_frequency_label.setEnabled(value)
    ui.dataset_frequency_value_combobox.setEnabled(value)
    ui.dataset_frequency_unit_combobox.setEnabled(value)
    if value is False and interval is not None:
        if interval == '1min':
            interval_value = '1'
            interval_unit = 'minutes(s)'
        elif interval == '5min':
            interval_value = '5'
            interval_unit = 'minutes(s)'
        elif interval == '10min':
            interval_value = '10'
            interval_unit = 'minutes(s)'
        elif interval == '15min':
            interval_value = '15'
            interval_unit = 'minutes(s)'
        elif interval == '30min':
            interval_value = '30'
            interval_unit = 'minutes(s)'
        elif interval == 'H':
            interval_value = '1'
            interval_unit = 'hour'
        elif interval == 'D':
            interval_value = '1'
            interval_unit = 'day'
        elif interval == 'W':
            interval_value = '1'
            interval_unit = 'week'
        elif interval == 'M':
            interval_value = '1'
            interval_unit = 'month'
        elif interval == 'Y':
            interval_value = '1'
            interval_unit = 'year'
        else:
            return 'failure'
        ui.dataset_frequency_value_combobox.setCurrentText(interval_value)
        ui.dataset_frequency_unit_combobox.setCurrentText(interval_unit)
        return 'success'
    return 'success'


def detect_frequency(frequency_list_ns):
    if 0 in frequency_list_ns:
        frequency_list_ns.remove(0)
    frequency_list_days = []
    for nanoseconds in frequency_list_ns:
        frequency_list_days.append(nanoseconds / 86400000000000)
    monthly = [28, 29, 30, 31]
    yearly = [365, 364]
    if all(x in monthly for x in frequency_list_days):
        return 'M'
    elif all(x in yearly for x in frequency_list_days):
        return 'Y'
    if len(frequency_list_ns) == 1:
        nanoseconds = frequency_list_ns[0]
        minutes = nanoseconds / 60000000000
        hours = minutes / 60
        days = hours / 24
        if minutes == 1:
            return '1min'
        elif minutes == 5:
            return '5min'
        elif minutes == 10:
            return '10min'
        elif minutes == 15:
            return '15min'
        elif minutes == 30:
            return '30min'
        elif hours == 1:
            return 'H'
        elif days == 1:
            return 'D'
        elif days == 7:
            return 'W'
    return 'none'


def convert_date(df, date_name):
    if df[date_name].dtype == 'object':
        df[date_name] = pd.to_datetime(df[date_name])
        print("Converted to", df[date_name].dtype)
    elif df[date_name].dtype == datetime:
        pass
    else:
        raise Exception("Error while converting to date, please check your date format.")


def select_csv_file(ui, dataset_type, merge_file=None):
    if merge_file is None:
        csv_file = QFileDialog.getOpenFileName(filter="csv(*.csv)")[0]
    else:
        csv_file = merge_file
    if csv_file != '':
        ui.selected_csv[dataset_type] = csv_file
        populate_dataset_view(ui, dataset_type)


def populate_dataset_view(ui, dataset_type):
    try:
        csv_file_name = ui.selected_csv[dataset_type]
        with open(csv_file_name, newline='') as csv_file:
            reader = csv.reader(csv_file, delimiter=',')
            columns_count = len(next(reader))
            csv_file.seek(0)
            sniffer = csv.Sniffer()
            has_header = sniffer.has_header(csv_file.read(2048))
        if not has_header:
            headers = []
            for number in range(columns_count):
                headers.append("Column " + str(number + 1))
            df = pd.read_csv(csv_file_name, parse_dates=True, names=headers)
        else:
            df = pd.read_csv(csv_file_name, parse_dates=True)
    except Exception as e:
        error(str(e))
        return
    if df.shape[0] == 0 or df.shape[1] == 0:
        error("Can't import an empty dataset")
        return
    header_list = list(df.columns)
    view_df = df.head(10)
    ui.dataset[dataset_type] = df
    model = PandasModel(view_df)
    if dataset_type == 'TARGET':
        ui.target_dataset_preview_tableview.setModel(None)
        ui.target_item_id_combobox.clear()
        ui.target_timestamp_combobox.clear()
        ui.target_demand_combobox.clear()
        ui.target_location_combobox.clear()
        ui.target_dataset_preview_tableview.setModel(model)
        ui.target_item_id_combobox.addItems(header_list)
        ui.target_timestamp_combobox.addItems(header_list)
        ui.target_demand_combobox.addItems(header_list)
        ui.target_location_combobox.addItem('')
        ui.target_location_combobox.addItems(header_list)
    elif dataset_type == 'RELATED':
        ui.related_dataset_preview_tableview.setModel(None)
        ui.related_item_id_combobox.clear()
        ui.related_timestamp_combobox.clear()
        ui.related_price_combobox.clear()
        ui.related_weather_combobox.clear()
        ui.related_promotion_combobox.clear()
        ui.related_location_combobox.clear()
        ui.related_dataset_preview_tableview.setModel(model)
        ui.related_item_id_combobox.addItems(header_list)
        ui.related_timestamp_combobox.addItems(header_list)
        ui.related_price_combobox.addItem('')
        ui.related_weather_combobox.addItem('')
        ui.related_location_combobox.addItem('')
        ui.related_promotion_combobox.addItem('')
        ui.related_price_combobox.addItems(header_list)
        ui.related_weather_combobox.addItems(header_list)
        ui.related_location_combobox.addItems(header_list)
        ui.related_promotion_combobox.addItems(header_list)
    else:
        ui.metadata_dataset_preview_tableview.setModel(None)
        ui.metadata_item_id_combobox.clear()
        ui.metadata_color_combobox.clear()
        ui.metadata_category_combobox.clear()
        ui.metadata_brand_combobox.clear()
        ui.metadata_dataset_preview_tableview.setModel(model)
        ui.metadata_item_id_combobox.addItems(header_list)
        ui.metadata_color_combobox.addItem('')
        ui.metadata_brand_combobox.addItem('')
        ui.metadata_category_combobox.addItem('')
        ui.metadata_color_combobox.addItems(header_list)
        ui.metadata_brand_combobox.addItems(header_list)
        ui.metadata_category_combobox.addItems(header_list)


def upload_and_create_dataset(ui):
    if ui.current_dataset_type != 'METADATA':
        if ui.frequency == 'none':
            status = ui.frequency = combobox_to_freq(ui, 'DATASET')
            if status == 'failure':
                error("Failed to identify frequency")
                return 'failure'
    change_current_page(ui, ui.upload_page)
    reset_progress_bar(ui)
    status = upload_data(ui)
    if status == 'failure':
        change_current_page(ui, ui.new_dataset_name_page)
        return status
    status = create_dataset(ui)
    if status == 'failure':
        change_current_page(ui, ui.new_dataset_name_page)
        return status
    ui.select_ds_group_pb.click()
    return 'success'


def create_dataset(ui):
    dataset_type = ui.current_dataset_type
    attributes = ui.dataset_attributes[dataset_type]
    s3_path = ui.dataset_path[dataset_type]
    dataset_name = ui.new_dataset_name_line_edit.text()
    if dataset_type == 'TARGET':
        dataset_arn, status = create_target_dataset(ui.forecast_client, dataset_name, ui.frequency, attributes)
    elif dataset_type == 'RELATED':
        dataset_arn, status = create_related_dataset(ui.forecast_client, dataset_name, ui.frequency, attributes)
    else:
        dataset_arn, status = create_metadata_dataset(ui.forecast_client, dataset_name, attributes)
    if status == 'failure':
        return status
    status = create_dataset_import_job(ui.forecast_client, dataset_name, dataset_arn, s3_path, ui.s3_role_arn)
    if status == 'failure':
        return status
    datasets_arn, datasets_type, dataset_status = list_all_forecast_datasets(ui.forecast_client, ui.dataset_group_arn)
    dataset_arn_list = []
    for dataset_arn_2 in datasets_arn.values():
        dataset_arn_list.append(dataset_arn_2)
    dataset_arn_list.append(dataset_arn)
    status = link_dataset_to_dataset_group(ui.forecast_client, dataset_arn_list, ui.dataset_group_arn)
    return status