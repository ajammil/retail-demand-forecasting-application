import tempfile
from botocore.exceptions import ClientError
import sys
from PyQt5 import QtWidgets, QtCore
from src.gui_connector_utils import buttons_connector
from gui.gui import Ui_main_window
from gui.error_display_functions import error, access_error
import darkdetect
from src.shared_utils import gui_initializer, dark_mode

with tempfile.TemporaryDirectory() as tmp_dir_name:
    try:
        app = QtWidgets.QApplication(sys.argv)
        main_window = QtWidgets.QWidget()
        ui = Ui_main_window()
        gui_initializer(ui, main_window, tmp_dir_name)

	#If darkmode make use the dark logo
        if darkdetect.isDark():
            dark_mode(ui)
        main_window.show()
        buttons_connector(ui)
    except ClientError as e:
        access_error(str(e))
    except Exception as e:
        error(str(e))
    sys.exit(app.exec_())

