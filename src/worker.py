from PyQt5.QtCore import QRunnable, pyqtSlot, QObject, pyqtSignal
from PyQt5.QtWidgets import QWidget
from gui.error_display_functions import worker_error_page


class Worker(QRunnable):

    def __init__(self, fn, target_page=None, ui=None, text='', *args, **kwargs):
        super(Worker, self).__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.target_page = target_page
        self.text = text
        self.ui = ui
        if self.loading_screen_selected():
            self.current_page = ui.stackedWidget.currentWidget()
            self.change_page_signal_emitter = self.ChangePageSignalEmitter()

    def loading_screen_selected(self):
        return self.target_page is not None and self.ui is not None and self.text != ''

    class ChangePageSignalEmitter(QObject):
        change_page_trigger = pyqtSignal(QWidget)
        change_waiting_text_trigger = pyqtSignal(str)

        def change_page(self, stacked_widget, target):
            self.change_page_trigger.connect(stacked_widget.setCurrentWidget)
            self.change_page_trigger.emit(target)

        def change_waiting_text(self, loading_text, text):
            self.change_waiting_text_trigger.connect(loading_text.setText)
            self.change_waiting_text_trigger.emit(text)

    @pyqtSlot()
    def run(self):
        try:
            if self.loading_screen_selected():
                self.change_page_signal_emitter.change_waiting_text(self.ui.loading_text, self.text)
                self.change_page_signal_emitter.change_page(self.ui.stackedWidget, self.ui.loading_page)

            output = self.fn(*self.args, **self.kwargs)

            if self.loading_screen_selected():
                if output == 'failure':
                    self.change_page_signal_emitter.change_page(self.ui.stackedWidget, self.current_page)
                else:
                    self.change_page_signal_emitter.change_page(self.ui.stackedWidget, self.target_page)

        except Exception as e:
            worker_error_page("Error", str(e))
            if self.loading_screen_selected():
                self.change_page_signal_emitter.change_page(self.ui.stackedWidget, self.current_page)
