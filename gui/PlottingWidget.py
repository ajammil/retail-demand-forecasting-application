import matplotlib.pyplot as plt
import scipy.spatial as spatial
import numpy as np
import pandas as pd
from PyQt5.QtWidgets import *
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import matplotlib.dates as mdates


def fmt(x, y):
    return '{y:0.2f}\nat\n {x}'.format(x=mdates.num2date(x).strftime("%m/%d/%Y\n%H:%M").rstrip('\n00:00'), y=y)


def convert_date(df):
    if df["date"].dtype == 'object':
        try:
            df["date"] = pd.to_datetime(df["date"])
        except ValueError:
            print('error')
            pass


class FollowDotCursor(object):
    """Display the x,y location of the nearest data point.
    https://stackoverflow.com/a/4674445/190597 (Joe Kington)
    https://stackoverflow.com/a/13306887/190597 (unutbu)
    https://stackoverflow.com/a/15454427/190597 (unutbu)
    """

    def __init__(self, ax, x, y, tolerance=5, formatter=fmt, offsets=(-20, 20)):
        try:
            x = np.asarray(x, dtype='float')
        except (TypeError, ValueError):
            x = np.asarray(mdates.date2num(x), dtype='float')
        y = np.asarray(y, dtype='float')
        mask = ~(np.isnan(x) | np.isnan(y))
        x = x[mask]
        y = y[mask]
        self._points = np.column_stack((x, y))
        self.offsets = offsets
        y = y[np.abs(y-y.mean()) <= 3*y.std()]
        self.scale = x.ptp()
        self.scale = y.ptp() / self.scale if self.scale else 1
        self.tree = spatial.cKDTree(self.scaled(self._points))
        self.formatter = formatter
        self.tolerance = tolerance
        self.ax = ax

        self.fig = ax.figure
        self.ax.xaxis.set_label_position('top')
        self.dot = ax.scatter(
            [x.min()], [y.min()], s=130, color='green', alpha=0.7)
        self.annotation = self.setup_annotation()
        plt.connect('motion_notify_event', self)

    def scaled(self, points):
        points = np.asarray(points)
        return points * (self.scale, 1)

    def __call__(self, event):
        ax = self.ax
        print('s')
        if event.inaxes == ax:
            x, y = event.xdata, event.ydata
        elif event.inaxes is None:
            return
        else:
            inv = ax.transData.inverted()
            x, y = inv.transform([(event.x, event.y)]).ravel()
        annotation = self.annotation
        x, y = self.snap(x, y)
        annotation.xy = x, y
        annotation.set_text(self.formatter(x, y))
        self.dot.set_offsets((x, y))
        bbox = ax.viewLim
        event.canvas.draw()

    def setup_annotation(self):
        """Draw and hide the annotation box."""
        annotation = self.ax.annotate(
            '', xy=(0, 0), ha='center',
            xytext=self.offsets, textcoords='offset points', va='bottom',
            bbox=dict(
                boxstyle='round,pad=0.5', fc='whitesmoke', alpha=1))
        return annotation

    def snap(self, x, y):
        """Return the value in self.tree closest to x, y."""
        dist, idx = self.tree.query(self.scaled((x, y)), k=1, p=1)
        try:
            return self._points[idx]
        except IndexError:
            # IndexError: index out of bounds
            return self._points[0]


class MainWindow(QMainWindow):
    def __call__(self, event):
        pass

    def __init__(self):
        super().__init__()

        self.width = 1000
        self.height = 800
        self.setGeometry(0, 0, self.width, self.height)


def get_canvas(self):
    self.fig, self.ax = plt.subplots()
    self.canvas = FigureCanvas(self.fig)
    self.clear_fig()


def clear_fig(self):
    self.ax.clear()
    self.ax.set_xlabel('Date')
    self.ax.set_ylabel('Demand')
    plt.subplots_adjust(left=0.15, right=0.95)
    self.ax.tick_params(axis='x', labelcolor='dimgray', labelsize='small')
    self.ax.tick_params(axis='y', labelcolor='dimgray', labelsize='small')
    self.ax.grid(color='whitesmoke', linestyle='-.', linewidth=1)
    # date_form = mdates.DateFormatter("%b, %d %Y")
    #
    # self.ax.xaxis.set_major_formatter(date_form)
    locator = mdates.AutoDateLocator(interval_multiples=False)
    formatter = mdates.AutoDateFormatter(locator)

    self.ax.xaxis.set_major_locator(locator)
    self.ax.xaxis.set_major_formatter(formatter)

    self.fig.autofmt_xdate()


def draw_df(self, df):
    self.df = df
    self.clear_fig()
    convert_date(self.df)
    self.x = df['date'].to_numpy()
    self.y = df['p50'].to_numpy()
    self.ax.plot(self.x, self.y, marker='o')
    self.ax.set_ylim(ymin=0)
    self.fig.canvas.draw()
    self.cursor = FollowDotCursor(self.ax,self.x, self.y, tolerance=20)


def add_plotting_widget(ui):
    ui.get_canvas = get_canvas.__get__(ui)
    ui.clear_fig = clear_fig.__get__(ui)
    ui.draw_df = draw_df.__get__(ui)
    ui.get_canvas()
    ui.w = QWidget()
    ui.w.layout = QHBoxLayout()
    ui.w.layout.addWidget(ui.canvas)
    ui.w.setLayout(ui.w.layout)
    ui.verticalLayout_66.addWidget(ui.w)


