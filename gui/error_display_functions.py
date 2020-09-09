import time

from PyQt5.QtCore import QObject, pyqtSignal
from PyQt5.QtWidgets import QMessageBox, QWidget

error_ui_holder = None


def worker_error_page(title, body):
    global error_ui_holder
    title = str(title)
    body = str(body)
    ui = error_ui_holder
    ui.error_source_page = ui.stackedWidget.currentWidget()
    ui.worker_waiting = True

    class ErrorSignalEmitter(QObject):
        change_page_trigger = pyqtSignal(QWidget)
        change_title_trigger = pyqtSignal(str)
        change_body_trigger = pyqtSignal(str)
        adjust_title = pyqtSignal()
        adjust_body = pyqtSignal()
        def display_error(self):
            self.change_page_trigger.connect(ui.stackedWidget.setCurrentWidget)
            self.change_page_trigger.emit(ui.error_page)
            self.change_title_trigger.connect(ui.error_tite.setText)
            self.change_title_trigger.emit(title)
            self.change_body_trigger.connect(ui.error_body.setText)
            self.change_body_trigger.emit(body)
            self.adjust_title.connect(ui.error_tite.adjustSize)
            self.adjust_title.emit()
            self.adjust_body.connect(ui.error_body.adjustSize)
            self.adjust_body.emit()
            print(body)

    change_page_signal_emitter = ErrorSignalEmitter()
    change_page_signal_emitter.display_error()
    # to prevent from returning to previous page in waiting screens.
    while ui.worker_waiting:
        time.sleep(0.1)
    time.sleep(0.1)


def error_ui_init(ui):
    global error_ui_holder
    error_ui_holder = ui


def exec_message_box(message_box):
    class MessageBoxSignalEmitter(QObject):
        message_box_trigger = pyqtSignal()

        def exec(self, message_box):
            self.message_box_trigger.connect(message_box.exec)
            try:
                self.message_box_trigger.emit()
            except Exception as e:
                print(str(e))

    exec_message_box_signal_emitter = MessageBoxSignalEmitter()
    exec_message_box_signal_emitter.exec(message_box)


def login_error(text):
    credentials_missing_popup = QMessageBox()
    credentials_missing_popup.setWindowTitle("Login Failed")
    credentials_missing_popup.setText(text)
    credentials_missing_popup.setIcon(QMessageBox.Warning)
    exec_message_box(credentials_missing_popup)


def error(text):
    worker_error_page("Error!", text)


def access_error(text):
    access_error_popup = QMessageBox()
    access_error_popup.setWindowTitle("Access Denied!")
    access_error_popup.setText(text)
    access_error_popup.setIcon(QMessageBox.Critical)
    exec_message_box(access_error_popup)

