import numpy as np
from matplotlib import pyplot as plt

from .logic import LogicMixin
from .breakout import BreakoutMixin
from .trendline import TrendlineMixin
from .indicator import IndicatorMixin
from .plot import PlotMixin

class PointFigureChart(LogicMixin, BreakoutMixin, TrendlineMixin, IndicatorMixin, PlotMixin):
    def __init__(self, ts, method='cl', reversal=3, boxsize=1, scaling='log', title=None):

        # chart parameter
        self.method = self._is_valid_method(method)
        self.reversal = self._is_valid_reversal(reversal)
        self.scaling = self._is_valid_scaling(scaling)
        self.boxsize = self._is_valid_boxsize(boxsize)

        # prepare timeseries
        self.time_step = None  # calculated in _prepare_ts: 'm','D', None
        self.ts = self._prepare_ts(ts)

        # chart
        self.title = self._make_title(title)
        self.boxscale = self._get_boxscale()
        self.pnf_timeseries = self._get_pnf_timeseries()
        self.action_index_matrix = None  # assigned in _pnf_timeseries2matrix()
        self.matrix = self._pnf_timeseries2matrix()
        self.column_labels = self._get_column_entry_dates()

        # trendlines
        self.trendlines = None
        self.show_trendlines = False  # 'external', 'internal', 'both', False, 'False'

        # signals
        self.breakouts = None
        self.buys = {}
        self.sells = {}
        self.show_breakouts = False
        self.bullish_breakout_color = 'g'
        self.bearish_breakout_color = 'm'

        # indicator
        self.column_midpoints = None
        self.indicator = {}
        self.vap = {}
        self.indicator_colors = plt.cm.Set2
        self.indicator_fillcolor_opacity = 0.2

        # plotting coordinates/adjusted indicator
        self.plot_boxscale = None
        self.plot_matrix = None
        self.plot_column_index = None
        self.plot_column_label = None
        self.plot_y_ticks = None
        self.plot_y_ticklabels = None
        self.matrix_top_cut_index = None
        self.matrix_bottom_cut_index = None
        self.plot_indicator = {}
        self.cut2indicator = False
        self.cut2indicator_length = None

        # plotting options
        self.size = 'auto'
        self.max_figure_width = 10
        self.max_figure_height = 8
        self.left_axis = False
        self.right_axis = True
        self.column_axis = True

        self.add_empty_columns = 0

        self.show_markers = True
        self.grid = None
        self.x_marker_color = 'grey'
        self.o_marker_color = 'grey'
        self.grid_color = 'grey'

        self.figure_width = None
        self.figure_height = None
        self.matrix_min_width = None

        self.margin_left = None
        self.margin_right = None
        self.margin_top = 0.3
        self.margin_bottom = None
        self.box_height = None

        self.marker_linewidth = None
        self.grid_linewidth = None

        self.x_label_step = None
        self.y_label_step = None

        self.label_fontsize = 8
        self.title_fontsize = 8
        self.legend_fontsize = 8

        self.legend = True
        self.legend_position = None
        self.legend_entries = None

        self.plotsize_options = {'size': ['huge', 'large', 'medium', 'small', 'tiny'],
                                 'grid': [True, True, True, False, False],
                                 'matrix_min_width': [12, 12, 27, 57, 117],
                                 'box_height': [0.2, 0.15, 0.1, 0.05, 0.025],
                                 'marker_linewidth': [1, 1, 1, 0.5, 0.5],
                                 'grid_linewidth': [0.5, 0.5, 0.5, 0.25, 0.125],
                                 'x_label_step': [1, 1, 2, 4, 8],
                                 'y_label_step': [1, 1, 2, 4, 8],
                                 }

        # Figure and axis objects
        self.fig = None
        self.ax1 = None
        self.ax2 = None
        self.ax3 = None

    @staticmethod
    def _is_valid_method(method):
        if method not in ['cl', 'h/l', 'l/h', 'hlc', 'ohlc']:
            raise ValueError("Not a valid method. Valid methods are: cl, h/l, l/h, hlc, ohlc")
        return method

    @staticmethod
    def _is_valid_reversal(reversal):
        if not isinstance(reversal, int):
            ValueError('Value for reversal must be an integer. Reversal is usually between 1 and 5.')
        return reversal

    @staticmethod
    def _is_valid_scaling(scaling):
        if scaling not in ['abs', 'log', 'cla', 'atr']:
            raise ValueError("Not a valid scaling. Valid scales are: abs, log, cla and atr")
        return scaling

    def _is_valid_boxsize(self, boxsize):
        if self.scaling == 'cla':
            valid_boxsize = [0.02, 0.05, 0.1, 0.25, 1 / 3, 0.5, 1, 2]
            if boxsize not in valid_boxsize:
                msg = 'ValueError: For cla scaling valid values for boxsize are 0.02, 0.05, 0.1, 0.25, 1/3, 0.5, 1, 2'
                raise ValueError(msg)
        elif self.scaling == 'log':
            if boxsize < 0.01:
                raise ValueError('ValueError: The smallest possible boxsize for log-scaled axis is 0.01%')
        elif self.scaling == 'abs':
            if boxsize < 0:
                raise ValueError('ValueError: The boxsize must be a value greater than 0.')
        elif self.scaling == 'atr':
            if boxsize != 'total' and int(boxsize) != boxsize:
                raise ValueError(
                    'ValueError: The boxsize must be a integer of periods or \'total\' for atr box scaling.')
            if boxsize != 'total' and boxsize < 0:
                raise ValueError('ValueError: The boxsize must be a value greater than 0.')
        return boxsize

    def _make_title(self, title):
        if title is None:
            if self.scaling == 'log':
                title = f'Point & Figure ({self.scaling}|{self.method}) {self.boxsize}% x {self.reversal}'
            elif self.scaling == 'cla':
                title = f'Point & Figure ({self.scaling}|{self.method}) {self.boxsize}@50 x {self.reversal}'
            elif self.scaling == 'abs' or self.scaling == 'atr':
                title = f'Point & Figure ({self.scaling}|{self.method}) {self.boxsize} x {self.reversal}'
        else:
            if self.scaling == 'log':
                title = f'Point & Figure ({self.scaling}|{self.method}) {self.boxsize}% x {self.reversal} | {title}'
            elif self.scaling == 'cla':
                title = f'Point & Figure ({self.scaling}|{self.method}) {self.boxsize}@50 x {self.reversal} | {title}'
            elif self.scaling == 'abs' or self.scaling == 'atr':
                title = f'Point & Figure ({self.scaling}|{self.method}) {self.boxsize} x {self.reversal} | {title}'
        return title