import csv

from PyQt5.QtWidgets import QFileDialog
import pandas as pd

from gui.error_display_functions import error
from src.data_utils import PandasModel
from src.dataset_utils import are_data_attributes_different, populate_datasets
from src.forecast_utils import populate_forecasts
from src.forecast_view_utils import populate_forecast_view, lookup_item_clicked
from src.login_utils import login_clicked
from src.new_dataset_group_utils import new_dataset_group_create_clicked_worker
from src.new_dataset_utils import merge_button_clicked, prepare_data, select_csv_file, upload_and_create_dataset
from src.new_forecast_utils import clear_create_new_forecast_page, create_predictor_and_forecast, enable_auto_ml, disable_auto_ml, enable_country_specific_training, \
    disable_country_specific_training
from src.region_utils import dataset_group_page_selected_worker
from src.shared_utils import call_worker, change_current_page, return_from_error_page, populate_dataset_groups


def buttons_connector(ui):
    # Login Page Connectors
    ui.login_button.clicked.connect(lambda: login_clicked(ui))
    ui.access_line_edit.returnPressed.connect(ui.login_button.click)
    ui.secret_line_edit.returnPressed.connect(ui.login_button.click)

    # Region Page Connectors
    ui.list_region.itemClicked.connect(lambda: region_selected(ui))
    ui.select_region_pb.clicked.connect(lambda: call_worker(
        lambda: dataset_group_page_selected_worker(ui), ui,
        ui.select_ds_group_page, 'Retrieving your dataset groups..')
                                        )

    # Dataset Groups Page Connectors
    ui.list_ds_group.itemClicked.connect(lambda: dataset_group_selected(ui))
    ui.select_ds_group_pb.clicked.connect(lambda: dataset_page_selected(ui))
    ui.create_ds_group.clicked.connect(lambda: change_current_page(ui, ui.create_ds_page))
    ui.back_ds_group_pb.clicked.connect(lambda: change_current_page(ui, ui.select_region_page))

    # Create New Dataset Group Page Connectors
    ui.new_dataset_group_name_line_edit.textChanged[str].connect(
        lambda: ui.create_new_dataset_group_pb.setEnabled(ui.new_dataset_group_name_line_edit.text() != "")
    )
    ui.create_new_dataset_group_pb.clicked.connect(lambda: call_worker(
        lambda: new_dataset_group_create_clicked_worker(ui),
        ui, ui.select_ds_group_page, "Creating dataset group..")
                                                   )
    ui.back_new_dataset_group_pb.clicked.connect(lambda: call_worker(
        lambda: populate_forecasts(ui), ui, ui.select_forecast_page, 'Loading..')
                                                 )

    # Datasets Page Connectors
    ui.datasets_view_forecasts_pb.clicked.connect(lambda: call_worker(
        lambda: populate_forecasts(ui), ui, ui.select_forecast_page, 'Loading..')
                                                  )
    ui.datasets_create_forecasts_pb.clicked.connect(lambda: call_worker(
        lambda: clear_create_new_forecast_page(ui), ui, ui.create_forecast_page, 'Loading..')
                                                    )
    ui.datasets_back_pb.clicked.connect(lambda: call_worker(
        lambda: populate_dataset_groups(ui), ui, ui.select_ds_group_page, "Loading..")
                                        )
    ui.target_time_series_pb.clicked.connect(lambda: target_time_series_clicked(ui))
    ui.related_time_series_pb.clicked.connect(lambda: related_time_series_clicked(ui))
    ui.item_metadata_pb.clicked.connect(lambda: metadata_clicked(ui))

    # Crete New Target Dataset Page Connectors
    ui.target_select_csv_pb.clicked.connect(lambda: select_csv_file(ui, 'TARGET'))
    ui.target_back_new_dataset_pb.clicked.connect(lambda: dataset_page_selected(ui))
    ui.target_next_new_dataset.clicked.connect(lambda: dataset_page_next_clicked(ui))
    ui.target_item_id_combobox.currentTextChanged.connect(lambda: are_data_attributes_different(ui))
    ui.target_timestamp_combobox.currentTextChanged.connect(lambda: are_data_attributes_different(ui))
    ui.target_demand_combobox.currentTextChanged.connect(lambda: are_data_attributes_different(ui))
    ui.target_location_combobox.currentTextChanged.connect(lambda: are_data_attributes_different(ui))

    # Create New Metadata Dataset Page Connectors
    ui.metadata_select_csv_pb.clicked.connect(lambda: select_csv_file(ui, 'METADATA'))
    ui.metadata_back_new_dataset_pb.clicked.connect(lambda: dataset_page_selected(ui))
    ui.metadata_next_new_dataset.clicked.connect(lambda: dataset_page_next_clicked(ui))
    ui.metadata_item_id_combobox.currentTextChanged.connect(lambda: are_data_attributes_different(ui))
    ui.metadata_color_combobox.currentTextChanged.connect(lambda: are_data_attributes_different(ui))
    ui.metadata_category_combobox.currentTextChanged.connect(lambda: are_data_attributes_different(ui))
    ui.metadata_brand_combobox.currentTextChanged.connect(lambda: are_data_attributes_different(ui))

    # Create New Related Dataset Page Connectors
    ui.related_select_csv_pb.clicked.connect(lambda: select_csv_file(ui, 'RELATED'))
    ui.related_back_new_dataset_pb.clicked.connect(lambda: dataset_page_selected(ui))
    ui.related_next_new_dataset.clicked.connect(lambda: dataset_page_next_clicked(ui))
    ui.related_item_id_combobox.currentTextChanged.connect(lambda: are_data_attributes_different(ui))
    ui.related_timestamp_combobox.currentTextChanged.connect(lambda: are_data_attributes_different(ui))
    ui.related_price_combobox.currentTextChanged.connect(lambda: are_data_attributes_different(ui))
    ui.related_weather_combobox.currentTextChanged.connect(lambda: are_data_attributes_different(ui))
    ui.related_promotion_combobox.currentTextChanged.connect(lambda: are_data_attributes_different(ui))
    ui.related_location_combobox.currentTextChanged.connect(lambda: are_data_attributes_different(ui))
    ui.related_merge_pb.clicked.connect(
        lambda: merge_related_clicked(ui)
    )

    # Merge Page Connectors
    ui.merge_merge_pb.clicked.connect(
        lambda: call_worker(
            lambda: merge_button_clicked(ui), ui, ui.create_related_dataset_page, "Loading..")
    )

    # Select Name and Frequency Page Connectors
    ui.dataset_name_create_pb.clicked.connect(lambda: call_worker(
        lambda: upload_and_create_dataset(ui), ui)
                                              )
    ui.dataset_name_back_pb.clicked.connect(lambda: change_current_page(ui, ui.back_dataset_page))
    ui.new_dataset_name_line_edit.textChanged[str].connect(
        lambda: ui.dataset_name_create_pb.setEnabled(dataset_name_valid(ui))
    )
    ui.dataset_frequency_unit_combobox.currentIndexChanged.connect(lambda: check_frequency_accuracy(ui, 'DATASET'))

    # Create Forecast Page Connectors
    ui.advanced_forecast_pb.clicked.connect(lambda: change_current_page(ui, ui.advanced_settings_page))
    ui.back_new_predictor.clicked.connect(lambda: dataset_page_selected(ui))
    ui.new_forecast_line_edit.textChanged[str].connect(
        lambda: ui.create_new_predictor.setEnabled(forecast_name_valid(ui))
    )
    ui.create_new_predictor.clicked.connect(
        lambda: call_worker(lambda: create_predictor_and_forecast(ui), ui, ui.select_forecast_page, 'Loading..')
    )
    ui.create_new_predictor_advanced.clicked.connect(
        lambda: call_worker(lambda: create_predictor_and_forecast(ui), ui, ui.select_forecast_page, 'Loading..')
    )
    ui.forecast_frequency_unit_combobox.currentIndexChanged.connect(lambda: check_frequency_accuracy(ui, 'FORECAST'))

    # Advanced Create Forecast Page Connectors
    ui.back_new_predictor_advanced.clicked.connect(lambda: change_current_page(ui, ui.create_forecast_page))
    ui.auto_ml_checkbox.stateChanged.connect(lambda: algorithm_checkbox_changed(ui))
    ui.country_checkbox.stateChanged.connect(lambda: country_checkbox_changed(ui))
    ui.new_forecast_line_edit.textChanged[str].connect(
        lambda: ui.create_new_predictor_advanced.setEnabled(forecast_name_valid(ui))
    )
    # Forecasts Page Connectors
    ui.list_forecast.itemClicked.connect(lambda: forecast_selected(ui))
    ui.datasets_forecast_pb.clicked.connect(lambda: dataset_page_selected(ui))
    ui.select_forecast_pb.clicked.connect(lambda: call_worker(
        lambda: populate_forecast_view(ui), ui, ui.forecast_view_page, 'Loading')
    )
    ui.create_forecast_pb.clicked.connect(lambda: call_worker(
        lambda: clear_create_new_forecast_page(ui), ui, ui.create_forecast_page, 'Loading..')
    )

    # Forecast View Page Connectors
    ui.look_up_forecast_view_pb.clicked.connect(lambda: call_worker(
        lambda: lookup_item_clicked(ui), ui, ui.forecast_view_page, 'Looking up..'))
    ui.forecast_view_item_id_line_edit.textChanged[str].connect(
        lambda: ui.look_up_forecast_view_pb.setEnabled(look_up_valid(ui))
    )
    ui.forecast_view_location_line_edit.textChanged[str].connect(
        lambda: ui.look_up_forecast_view_pb.setEnabled(look_up_valid(ui))
    )
    ui.back_forecast_view_pb.clicked.connect(lambda: call_worker(
        lambda: populate_forecasts(ui), ui, ui.select_forecast_page, 'Loading..')
    )

    # Error Page Connectors
    ui.error_ok_pb.clicked.connect(lambda: return_from_error_page(ui))


def algorithm_checkbox_changed(ui):
    checked = ui.auto_ml_checkbox.isChecked()
    if checked:
        enable_auto_ml(ui)
    else:
        disable_auto_ml(ui)


def country_checkbox_changed(ui):
    checked = ui.country_checkbox.isChecked()
    if checked:
        enable_country_specific_training(ui)
    else:
        disable_country_specific_training(ui)


def target_time_series_clicked(ui):
    ui.current_dataset_type = 'TARGET'
    ui.back_dataset_page = ui.create_target_dataset_page
    if ui.target_dataset_exists:
        # TODO here
        pass
    else:
        call_worker(lambda: create_new_target_dataset_page_selected(ui), ui, ui.create_target_dataset_page, 'Loading..')


def metadata_clicked(ui):
    ui.current_dataset_type = 'METADATA'
    ui.back_dataset_page = ui.create_metadata_dataset_page
    if ui.metadata_dataset_exists:
        # TODO here
        pass
    else:
        call_worker(lambda: create_new_metadata_dataset_page_selected(ui), ui, ui.create_metadata_dataset_page,
                    'Loading..')


def related_time_series_clicked(ui):
    ui.current_dataset_type = 'RELATED'
    ui.back_dataset_page = ui.create_related_dataset_page
    if ui.related_dataset_exists:
        # TODO here
        pass
    else:
        call_worker(lambda: create_new_related_dataset_page_selected(ui), ui, ui.create_related_dataset_page,
                    'Loading..')


def dataset_name_valid(ui):
    text = ui.new_dataset_name_line_edit.text()
    if text in ui.remote_datasets:
        ui.dataset_name_already_exists_label.setText('A dataset already exists wit this name.')
        return False
    else:
        ui.dataset_name_already_exists_label.setText('')
        return text != ""


def forecast_name_valid(ui):
    text = ui.new_forecast_line_edit.text()
    if text in ui.remote_forecasts:
        ui.forecast_name_already_exists_label.setText('A dataset already exists wit this name.')
        return False
    else:
        ui.forecast_name_already_exists_label.setText('')
        return text != ""


def look_up_valid(ui):
    location = ui.forecast_view_location_line_edit.text()
    item_id = ui.forecast_view_item_id_line_edit.text()
    if item_id != '' and location != '':
        return True
    elif item_id != '' and not ui.use_location:
        return True
    return False


def dataset_page_next_clicked(ui):
    dataset_type = ui.current_dataset_type
    ui.new_dataset_name_line_edit.clear()
    call_worker(lambda: prepare_data(ui, dataset_type), ui, ui.new_dataset_name_page,
                "Processing your data, please wait")


def create_new_target_dataset_page_selected(ui):
    ui.target_dataset_preview_tableview.setModel(None)
    ui.target_item_id_combobox.clear()
    ui.target_timestamp_combobox.clear()
    ui.target_demand_combobox.clear()
    ui.target_location_combobox.clear()
    ui.target_next_new_dataset.setEnabled(False)


def create_new_metadata_dataset_page_selected(ui):
    ui.metadata_dataset_preview_tableview.setModel(None)
    ui.metadata_item_id_combobox.clear()
    ui.metadata_color_combobox.clear()
    ui.metadata_category_combobox.clear()
    ui.metadata_brand_combobox.clear()
    ui.metadata_next_new_dataset.setEnabled(False)


def merge_related_clicked(ui):
    csv_file_name = QFileDialog.getOpenFileName(filter="csv(*.csv)")[0]
    ui.merge_csv_file_name = csv_file_name
    call_worker(
        lambda: merge_csv_page_selected(ui), ui, ui.merge_csv_page, "Loading..")


def create_new_related_dataset_page_selected(ui):
    ui.related_dataset_preview_tableview.setModel(None)
    ui.related_item_id_combobox.clear()
    ui.related_timestamp_combobox.clear()
    ui.related_price_combobox.clear()
    ui.related_weather_combobox.clear()
    ui.related_promotion_combobox.clear()
    ui.related_location_combobox.clear()
    ui.related_next_new_dataset.setEnabled(False)


def merge_csv_page_selected(ui):
    csv_file_name = ui.merge_csv_file_name
    ui.merge_timestamp_combobox.clear()
    ui.merge_location_combobox.clear()
    ui.merge_tableview.setModel(None)
    if csv_file_name != '':
        ui.merge_file = csv_file_name
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
                df = pd.read_csv(csv_file_name, names=headers, nrows=10)
            else:
                df = pd.read_csv(csv_file_name, nrows=10)
        except Exception as e:
            error(str(e))
            return 'failure'
        header_list = list(df.columns)
        model = PandasModel(df)
        ui.merge_tableview.setModel(model)
        ui.merge_timestamp_combobox.addItems(header_list)
        ui.merge_location_combobox.addItems(header_list)
        return 'success'
    else:
        return 'failure'


def region_selected(ui):
    ui.select_region_pb.setEnabled(True)
    ui.region = ui.list_region.currentItem().text().split(" | ")[1]


def dataset_group_selected(ui):
    ui.select_ds_group_pb.setEnabled(True)
    ui.dataset_group_arn = ui.list_ds_group.currentItem().text().split(" | ")[1]
    ui.dataset_group_name = ui.list_ds_group.currentItem().text().split(" | ")[0]


def dataset_page_selected(ui):
    call_worker(lambda: populate_datasets(ui), ui, ui.select_ds_page, 'Loading..')


def forecast_selected(ui):
    ui.select_forecast_pb.setEnabled(True)
    ui.selected_forecast = ui.list_forecast.currentItem().text()


def check_frequency_accuracy(ui, caller):
    if caller == 'FORECAST':
        value = ui.forecast_frequency_value_combobox
        unit = ui.forecast_frequency_unit_combobox
    else:
        value = ui.dataset_frequency_value_combobox
        unit = ui.dataset_frequency_unit_combobox
    if unit.currentText() == 'minute(s)':
        value.setEnabled(True)
    else:
        value.setCurrentText('1')
        value.setEnabled(False)