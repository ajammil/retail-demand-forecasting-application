from PyQt5 import QtGui
from PyQt5.QtCore import QThreadPool, QObject, pyqtSignal, pyqtSlot
from PyQt5.QtWidgets import QWidget, QListWidgetItem

from aws_utils.forecast_utils import list_all_forecast_dataset_groups
from gui.PlottingWidget import add_plotting_widget
from gui.error_display_functions import error_ui_init
from src.worker import Worker


def call_worker(fn, ui=None, target_page=None, text=''):
    """takes a function and runs it in another thread, and shows loading screen with the specified text.
     upon success it proceeds to the target page. If it failed, it returns to the previous page"""

    worker = Worker(fn, target_page, ui, text)
    ui.thread_pool.start(worker)


def gui_initializer(ui, main_window, tmp_dir_name):
    error_ui_init(ui)
    ui.thread_pool = QThreadPool()
    ui.setupUi(main_window)
    add_plotting_widget(ui)
    ui.stackedWidget.setCurrentWidget(ui.login_page)
    ui.waiting_spinner.start()
    ui.tmp_dir_name = tmp_dir_name
    ui.error_body.setWordWrap(True)
    ui.worker_waiting = False
    ui.auto_ml = True
    ui.country = False
    ui.algorithms = {}
    initialize_country_list(ui)
    initialize_algorithm_list(ui)


def initialize_country_list(ui):
    with open('countries.txt') as countries_file:
        for line in countries_file:
            if line != '':
                ui.countries_combobox.addItem(line.rstrip('\n'))


def initialize_algorithm_list(ui):
    with open('algorithms.txt') as algorithm_file:
        for line in algorithm_file:
            if line != '':
                ui.algorithm_combobox.addItem(line.rstrip('\n'))
                ui.algorithms[line.rstrip('\n')] = next(algorithm_file, None).rstrip('\n')


def combobox_to_freq(ui, which_combobox):
    """Reads selected frequency from the combobox"""

    if which_combobox == 'DATASET':
        interval_unit = ui.dataset_frequency_unit_combobox.currentText()
        interval_value = ui.dataset_frequency_value_combobox.currentText()
    else:
        interval_unit = ui.forecast_frequency_unit_combobox.currentText()
        interval_value = ui.forecast_frequency_value_combobox.currentText()

    if interval_unit == 'minute(s)':
        if interval_value == '1':
            return '1min'
        elif interval_value == '5':
            return '5min'
        elif interval_value == '10':
            return '10min'
        elif interval_value == '15':
            return '15min'
        elif interval_value == '30':
            return '30min'
        else:
            return 'failure'
    elif interval_unit == 'hour':
        if interval_value == '1':
            return 'H'
        else:
            return 'failure'
    elif interval_unit == 'day':
        if interval_value == '1':
            return 'D'
        else:
            return 'failure'
    elif interval_unit == 'week':
        if interval_value == '1':
            return 'W'
        else:
            return 'failure'
    elif interval_unit == 'month':
        if interval_value == '1':
            return 'M'
        else:
            return 'failure'
    elif interval_unit == 'year':
        if interval_value == '1':
            return 'Y'
        else:
            return 'failure'
    else:
        return 'failure'


def dark_mode(ui):
    ui.label_2.setPixmap(QtGui.QPixmap(":/images/logos/AWS_AWS_logo_RGB_REV.png"))


def change_current_page(ui, target_page):
    class ChangePageSignalEmitter(QObject):
        change_page_trigger = pyqtSignal(QWidget)

        def change_page(self, stacked_widget, target):
            self.change_page_trigger.connect(stacked_widget.setCurrentWidget)
            self.change_page_trigger.emit(target)

    change_page_signal_emitter = ChangePageSignalEmitter()
    change_page_signal_emitter.change_page(ui.stackedWidget, target_page)


@pyqtSlot(QWidget)
def return_from_error_page(ui):
    change_current_page(ui, ui.error_source_page)
    ui.worker_waiting = False


def populate_dataset_groups(ui):
    """Populates dataset group list"""

    ui.select_ds_group_pb.setEnabled(False)
    dataset_groups = list_all_forecast_dataset_groups(ui.forecast_client)
    list_widget = ui.list_ds_group
    list_widget.clear()
    for dataset_group_name, dataset_group_arn in dataset_groups.items():
        item = QListWidgetItem(dataset_group_name + ' | ' + dataset_group_arn, list_widget)