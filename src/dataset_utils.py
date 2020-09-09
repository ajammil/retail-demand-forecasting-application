from PyQt5 import QtCore

from aws_utils.forecast_utils import list_all_forecast_datasets, get_import_jobs_status_list


def are_data_attributes_different(ui):
    dataset_type = ui.current_dataset_type
    text_list = []
    if dataset_type == 'TARGET':
        text_list.append(ui.target_item_id_combobox.currentText())
        text_list.append(ui.target_timestamp_combobox.currentText())
        text_list.append(ui.target_demand_combobox.currentText())
        if ui.target_location_combobox.currentText() != '':
            text_list.append(ui.target_location_combobox.currentText())
        text_set = set(text_list)
        enable_next = len(text_set) == len(text_list)
        ui.target_next_new_dataset.setEnabled(enable_next)
    elif dataset_type == 'RELATED':
        merge_text_list = []
        text_list.append(ui.related_item_id_combobox.currentText())
        text_list.append(ui.related_timestamp_combobox.currentText())
        merge_text_list.append(ui.related_item_id_combobox.currentText())
        merge_text_list.append(ui.related_timestamp_combobox.currentText())
        one_item_selected = False
        location_selected = False
        if ui.related_weather_combobox.currentText() != '':
            text_list.append(ui.related_weather_combobox.currentText())
            one_item_selected = True
        if ui.related_price_combobox.currentText() != '':
            text_list.append(ui.related_price_combobox.currentText())
            one_item_selected = True
        if ui.related_promotion_combobox.currentText() != '':
            text_list.append(ui.related_promotion_combobox.currentText())
            one_item_selected = True
        if ui.related_location_combobox.currentText() != '':
            text_list.append(ui.related_location_combobox.currentText())
            merge_text_list.append(ui.related_location_combobox.currentText())
            location_selected = True
        ui.related_location_name = ui.related_location_combobox.currentText()
        ui.related_date_name = ui.related_timestamp_combobox.currentText()
        text_set = set(text_list)
        merge_text_set = set(merge_text_list)
        enable_next = len(text_set) == len(text_list)
        enable_merge = len(merge_text_set) == len(merge_text_list)
        ui.related_next_new_dataset.setEnabled(enable_next and one_item_selected)
        ui.related_merge_pb.setEnabled(enable_merge and location_selected)
    else:
        text_list.append(ui.metadata_item_id_combobox.currentText())
        one_item_selected = False
        if ui.metadata_category_combobox.currentText() != '':
            text_list.append(ui.metadata_category_combobox.currentText())
            one_item_selected = True
        if ui.metadata_brand_combobox.currentText() != '':
            text_list.append(ui.metadata_brand_combobox.currentText())
            one_item_selected = True
        if ui.metadata_color_combobox.currentText() != '':
            text_list.append(ui.metadata_color_combobox.currentText())
            one_item_selected = True
        text_set = set(text_list)
        enable_next = len(text_set) == len(text_list)
        ui.metadata_next_new_dataset.setEnabled(enable_next and one_item_selected)


def populate_datasets(ui):
    _translate = QtCore.QCoreApplication.translate
    # TODO change this to be supported in the call_worker function, and all of the other functions inside
    #  forecast_functions
    creating_status = ['CREATE_PENDING' , 'CREATE_IN_PROGRESS' , 'CREATE_FAILED']

    datasets_arn, datasets_type, dataset_status = list_all_forecast_datasets(ui.forecast_client, ui.dataset_group_arn)
    dataset_exist = {'TARGET_TIME_SERIES': False, 'RELATED_TIME_SERIES': False, 'ITEM_METADATA': False}
    for dataset_name, dataset_arn in datasets_arn.items():
        dataset_type = datasets_type[dataset_name]
        import_job_status_list = get_import_jobs_status_list(ui.forecast_client, dataset_arn)
        if dataset_type == 'TARGET_TIME_SERIES':
            ui.target_time_series_label_2.setText(_translate("main_window", dataset_name))
            ui.target_time_series_pb.setText(_translate("main_window", 'Show'))
            ui.target_dataset_exists = True
            if dataset_status[dataset_name] == 'ACTIVE' and not any(x in creating_status for x in import_job_status_list):
                ui.target_status.setText('Active')
                ui.datasets_create_forecasts_pb.setEnabled(True)
                ui.create_forecast_pb.setEnabled(True)
            else:
                ui.target_status.setText('Creating')
                ui.datasets_create_forecasts_pb.setEnabled(False)
                ui.create_forecast_pb.setEnabled(False)


        elif dataset_type == 'RELATED_TIME_SERIES':
            ui.related_time_series_label_2.setText(_translate("main_window", dataset_name))
            ui.related_time_series_pb.setText(_translate("main_window", 'Show'))
            ui.related_dataset_exists = True
            if dataset_status[dataset_name] == 'ACTIVE' and not any(x in creating_status for x in import_job_status_list):
                ui.related_status.setText('Active')
            else:
                ui.related_status.setText('Creating')

        else:
            ui.item_metadata_label_2.setText(_translate("main_window", dataset_name))
            ui.item_metadata_pb.setText(_translate("main_window", 'Show'))
            ui.metadata_dataset_exists = True
            if dataset_status[dataset_name] == 'ACTIVE' and not any(x in creating_status for x in import_job_status_list):
                ui.metadata_status.setText('Active')
            else:
                ui.metadata_status.setText('Creating')

        dataset_exist[datasets_type[dataset_name]] = True

    for dataset_type, exist in dataset_exist.items():
        if not exist:
            if dataset_type == 'TARGET_TIME_SERIES':
                ui.target_time_series_label_2.setText(_translate("main_window", "Not uploaded yet."))
                ui.target_time_series_pb.setText(_translate("main_window", 'Upload'))
                ui.target_dataset_exists = False
                ui.target_status.setText('')
                ui.datasets_create_forecasts_pb.setEnabled(False)
                ui.create_forecast_pb.setEnabled(False)


            elif dataset_type == 'RELATED_TIME_SERIES':
                ui.related_time_series_label_2.setText(_translate("main_window", "Not uploaded yet."))
                ui.related_time_series_pb.setText(_translate("main_window", 'Upload'))
                ui.related_dataset_exists = False
                ui.related_status.setText('')

            else:
                ui.item_metadata_label_2.setText(_translate("main_window", "Not uploaded yet."))
                ui.item_metadata_pb.setText(_translate("main_window", 'Upload'))
                ui.metadata_dataset_exists = False
                ui.metadata_status.setText('')