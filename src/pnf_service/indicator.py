import numpy as np

class IndicatorMixin:
    def _get_midpoints(self):
        """
        Calculates the midpoints for every column of an Point and Figure Chart
        """

        boxes = self.boxscale
        mtx = self.matrix

        points = np.zeros(np.size(mtx, 1))

        for n in range(0, np.size(mtx, 1)):

            column = mtx[:, n]
            column = np.where(column != 0)[0]
            column = boxes[column]

            if self.method == 'log':

                i = np.floor(np.size(column) / 2).astype(int) - 1

                if i < (np.size(column) / 2) - 1:
                    center_value = column[i + 1]
                else:
                    center_value = np.exp((np.log(column[i]) + np.log(column[i + 1])) / 2)

            else:
                i = np.floor(np.size(column) / 2).astype(int) - 1

                if i < (np.size(column) / 2) - 1:
                    center_value = column[i + 1]
                else:
                    center_value = column[i] + (column[i + 1] - column[i]) / 2

            points[n] = center_value

        self.column_midpoints = points

        return points

    def midpoints(self):
        if self.column_midpoints is None:
            self._get_midpoints()

        self.indicator['Midpoints'] = self.column_midpoints

        return self.column_midpoints

    def sma(self, period):
        """
         Calculates the simple moving average for every column of an Point and Figure Chart
         """
        label = f'SMA({period})'

        if self.column_midpoints is None:
            self.column_midpoints = self._get_midpoints()
            values = self.column_midpoints
        else:
            values = self.column_midpoints

        ma = np.zeros(np.size(self.matrix,1))
        ma[:] = np.nan

        if len(ma) >= period:

            for n in range(period - 1, len(values)):
                ma[n] = np.mean(values[n - period + 1:n + 1])

        self.indicator[label] = ma

        return ma

    def ema(self, period):
        """
        Calculates the exponential moving average for every column of an Point and Figure Chart
        """
        label = f'EMA({period})'

        if self.column_midpoints is None:
            self.column_midpoints = self._get_midpoints()
            values = self.column_midpoints
        else:
            values = self.column_midpoints

        ma = np.zeros(np.size(self.matrix,1))
        ma[:] = np.nan

        if len(ma) >= period:

            ma[period - 1] = np.sum(values[0:period]) / period
            k = 2 / (period + 1)

            for n in range(period, len(values)):
                ma[n] = k * (values[n] - ma[n - 1]) + ma[n - 1]

        self.indicator[label] = ma

        return ma

    def bollinger(self, period, factor):
        """
        Calculates the bollinger bands for every column of an Point and Figure Chart
        """

        label = f'Bollinger({period},{factor})'

        mtx = self.matrix

        upper_band = np.zeros(np.size(mtx, 1))
        upper_band[:] = np.nan

        bb_l = np.zeros(np.size(mtx, 1))
        bb_l[:] = np.nan

        std = np.zeros(np.size(mtx, 1))
        std[:] = np.nan

        if f'SMA({period})' in self.indicator:
            ma = self.indicator[f'SMA({period})']
        else:
            ma = self.sma(period)
            self.indicator.pop(f'SMA({period})')

        mp = self.column_midpoints

        if len(upper_band) >= period:

            for n in range(period - 1, len(std)):
                std[n] = np.std(mp[n - period + 1:n + 1])

        upper_band = ma + factor * std
        lower_band = ma - factor * std

        self.indicator[label + '-upper'] = upper_band
        self.indicator[label + '-lower'] = lower_band

        return upper_band, lower_band

    def donchian(self, period, ignore_columns=0):
        """
        Calculates the Donchian channels for every column of an Point and Figure Chart.
        ignore_column is the number of columns that will be ignored at the end
        and it's equivalent to shifting the channels to the right.
        """
        label = f'Donchian({period},{ignore_columns})'

        matrix = np.abs(self.matrix).astype('float')
        boxscale = self.boxscale

        boxscale = boxscale.reshape(len(boxscale), 1)

        boxscale = np.repeat(boxscale, repeats=np.shape(matrix)[1], axis=1)

        matrix = np.multiply(boxscale, matrix)

        matrix[matrix == 0] = np.nan

        high = np.nanmax(matrix, 0)
        low = np.nanmin(matrix, 0)

        donchian_channel_middle = np.zeros(len(high))
        donchian_channel_middle[:] = np.nan

        donchian_channel_upper = np.zeros(len(high))
        donchian_channel_upper[:] = np.nan

        donchian_channel_lower = np.zeros(len(low))
        donchian_channel_lower[:] = np.nan

        if len(donchian_channel_upper) >= period:

            for n in range(period - 1, len(donchian_channel_upper)):
                donchian_channel_upper[n] = np.max(high[n - period + 1:n + 1])
                donchian_channel_lower[n] = np.min(low[n - period + 1:n + 1])
                # donchian_channel_middle[n] = (donchian_channel_upper[n]-donchian_channel_lower[n])/2

        if ignore_columns > 0 and ignore_columns <= len(donchian_channel_upper):
            donchian_channel_upper = np.append(np.repeat(np.nan, ignore_columns),
                                               donchian_channel_upper[:-ignore_columns])
            donchian_channel_lower = np.append(np.repeat(np.nan, ignore_columns),
                                               donchian_channel_lower[:-ignore_columns])
            # donchian_channel_middle = np.append(np.repeat(np.nan, ignore_columns), donchian_channel_middle[:-ignore_columns])

        self.indicator[label + '-upper'] = donchian_channel_upper
        self.indicator[label + '-lower'] = donchian_channel_lower

        return donchian_channel_upper, donchian_channel_lower

    def psar(self, step, leap):
        """
        Calculates the parabolic Stop and Reverse (pSAR) for every column of an Point and Figure Chart
        """

        label = f'pSAR({step},{leap})'
        boxes = self.boxscale
        mtx = self.matrix

        # check length here and leave function
        if np.size(mtx, 1) <= 2:
            psar = np.zeros(np.size(mtx, 1))
            psar[:] = np.nan
            self.indicator[label] = psar

            return psar

        mtx = [np.repeat([boxes], np.size(mtx, 1), axis=0)][0].transpose() * mtx
        mtx = np.abs(mtx)

        high = np.zeros(np.size(mtx, 1))
        low = np.zeros(np.size(mtx, 1))

        for n in range(0, np.size(mtx, 1)):
            t = mtx[:, n]
            high[n] = np.max(t)
            t[t == 0] = np.max(t)
            low[n] = np.min(mtx[:, n])

        psar = np.zeros(np.size(high))
        ep = np.zeros(np.size(high))
        diff = np.zeros(np.size(high))
        prod = np.zeros(np.size(high))
        trendflag = np.zeros(np.size(high))
        accFactor = np.zeros(np.size(high))
        trendlength = np.zeros(np.size(high))
        trendlength[0] = 1

        if high[0] > high[2]:

            psar[0] = high[0]
            ep[0] = low[0]
            trendflag[0] = -1

        else:
            psar[0] = low[0]
            ep[0] = high[0]
            trendflag[0] = 1

        diff[0] = ep[0] - psar[0]
        accFactor[0] = step
        prod[0] = diff[0] * accFactor[0]

        for n in range(1, np.size(high)):

            if trendflag[n - 1] == 1 and prod[n - 1] + psar[n - 1] > low[n]:
                psar[n] = ep[n - 1]
            elif trendflag[n - 1] == -1 and prod[n - 1] + psar[n - 1] < high[n]:
                psar[n] = ep[n - 1]
            else:
                psar[n] = psar[n - 1] + prod[n - 1]

            if psar[n] < high[n]:
                trendflag[n] = 1
            elif psar[n] > low[n]:
                trendflag[n] = -1

            if trendflag[n] == 1 and high[n] > ep[n - 1]:
                ep[n] = high[n]
            elif trendflag[n] == 1 and high[n] <= ep[n - 1]:
                ep[n] = ep[n - 1]
            elif trendflag[n] == -1 and low[n] < ep[n - 1]:
                ep[n] = low[n]
            elif trendflag[n] == -1 and low[n] >= ep[n - 1]:
                ep[n] = ep[n - 1]

            if trendflag[n] == trendflag[n - 1]:
                trendlength[n] = trendlength[n - 1] + 1
                if accFactor[n - 1] == leap:
                    accFactor[n] = leap
                elif trendflag[n] == 1 and ep[n] > ep[n - 1]:
                    accFactor[n] = accFactor[n - 1] + step
                elif trendflag[n] == 1 and ep[n] <= ep[n - 1]:
                    accFactor[n] = accFactor[n - 1]
                elif trendflag[n] == -1 and ep[n] < ep[n - 1]:
                    accFactor[n] = accFactor[n - 1] + step
                elif trendflag[n] == -1 and ep[n] >= ep[n - 1]:
                    accFactor[n] = accFactor[n - 1]
            else:
                accFactor[n] = step
                trendlength[n] = 1

            diff[n] = ep[n] - psar[n]
            prod[n] = accFactor[n] * diff[n]

        psar = psar * trendflag

        self.indicator[label] = psar

        return psar

    def next_simple_signal(self):

        next_buy = np.nan
        next_sell = np.nan

        # last trend need to be identified from pnfts
        idx = np.where(~np.isnan(self.pnf_timeseries['trend']))[0][-1]

        last_trend = int(self.pnf_timeseries['trend'][idx])

        if np.shape(self.matrix)[1] >= 3:

            mtx = self.matrix.copy()
            mtx = mtx[:, -3:]

            x_col_1 = np.where(mtx[:, 0] == 1)[0]
            x_col_2 = np.where(mtx[:, 1] == 1)[0]
            x_col_3 = np.where(mtx[:, 2] == 1)[0]

            o_col_1 = np.where(mtx[:, 0] == -1)[0]
            o_col_2 = np.where(mtx[:, 1] == -1)[0]
            o_col_3 = np.where(mtx[:, 2] == -1)[0]

            if last_trend == 1:

                if np.any(x_col_2):
                    idx = x_col_2[-1]
                else:
                    idx = x_col_1[-1]

                if idx + 1 > x_col_3[-1]:
                    # if idx  > x_col_3[-1]:
                    next_buy = self.boxscale[idx + 1]
                else:
                    next_buy = np.nan

                if np.any(o_col_3):
                    idx = o_col_3[0]
                else:
                    idx = o_col_2[0]

                next_sell = self.boxscale[idx - 1]

            elif last_trend == -1:

                if np.any(o_col_2):
                    idx = o_col_2[0]
                else:
                    idx = o_col_1[0]

                if idx - 1 < o_col_3[0]:
                    # if idx < o_col_3[0]:
                    next_sell = self.boxscale[idx - 1]
                else:
                    next_sell = np.nan

                if np.any(x_col_3):
                    idx = x_col_3[-1]
                else:
                    idx = x_col_2[-1]

                next_buy = self.boxscale[idx + 1]

        # print('Next Buy: ', next_buy)
        # print('Next Sell: ', next_sell)

        return next_buy, next_sell

    def multiple_top_buy(self, label, multiple):

        max_width = 2 * multiple - 1

        array = np.zeros(len(self.pnf_timeseries['box index']))
        array[:] = np.nan

        x = ((self.breakouts['trend'] == 1)
             & (self.breakouts['width'] <= max_width)
             & (self.breakouts['hits'] == multiple))

        col = self.breakouts['column index'][x]
        row = self.breakouts['box index'][x]

        for r, c in zip(row, col):
            col_idx = (self.pnf_timeseries['column index'] == c)
            row_idx = self.pnf_timeseries['box index'][col_idx]
            ts_idx = int(row_idx[row_idx >= r][0])
            x = ((self.pnf_timeseries['box index'] == ts_idx) & (self.pnf_timeseries['column index'] == c))
            array[x] = self.boxscale[r]

            self.buys[label] = array

    def multiple_bottom_sell(self, label, multiple):

        max_width = 2 * multiple - 1

        array = np.zeros(len(self.pnf_timeseries['box index']))
        array[:] = np.nan

        x = ((self.breakouts['trend'] == -1)
             & (self.breakouts['width'] <= max_width)
             & (self.breakouts['hits'] == multiple))

        col = self.breakouts['column index'][x]
        row = self.breakouts['box index'][x]

        for r, c in zip(row, col):
            col_idx = (self.pnf_timeseries['column index'] == c)
            row_idx = self.pnf_timeseries['box index'][col_idx]
            ts_idx = int(row_idx[row_idx <= r][0])
            x = ((self.pnf_timeseries['box index'] == ts_idx) & (self.pnf_timeseries['column index'] == c))
            array[x] = self.boxscale[r]

            self.sells[label] = array

    def double_top_buy(self):

        self.multiple_top_buy(label='DTB', multiple=2)

    def double_bottom_sell(self):

        self.multiple_bottom_sell(label='DBS', multiple=2)

    def triple_top_buy(self):

        self.multiple_top_buy(label='TTB', multiple=3)

    def triple_bottom_sell(self):

        self.multiple_bottom_sell(label='TBS', multiple=3)