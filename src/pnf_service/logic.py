import numpy as np
from warnings import warn

class LogicMixin:
    def _prepare_ts(self, ts):
        """
        Initiates the time series data and adjust to the required format.
        """
        # bring all keys to lowercase characters
        ts = {key.lower(): val for key, val in ts.items()}

        # check if all required keys are available
        if self.method == 'cl':

            if 'close' not in ts:
                raise KeyError("The required key 'close' was not found in ts")

        elif self.method == 'h/l' or self.method == 'l/h':

            if 'low' not in ts:
                raise KeyError("The required key 'low' was not found in ts")

            if 'high' not in ts:
                raise KeyError("The required key 'high' was not found in ts")

        elif self.method == 'hlc':

            if 'close' not in ts:
                raise KeyError("The required key 'close' was not found in ts")

            if 'low' not in ts:
                raise KeyError("The required key 'low' was not found in ts")

            if 'high' not in ts:
                raise KeyError("The required key 'high' was not found in ts")

        elif self.method == 'ohlc':

            if 'close' not in ts:
                raise KeyError("The required key 'close' was not found in ts")

            if 'low' not in ts:
                raise KeyError("The required key 'low' was not found in ts")

            if 'high' not in ts:
                raise KeyError("The required key 'high' was not found in ts")

            if 'open' not in ts:
                raise KeyError("The required key 'open' was not found in ts")

        if self.scaling == 'atr':

            if 'close' not in ts:
                raise KeyError("The required key 'close' was not found in ts")

            if 'low' not in ts:
                raise KeyError("The required key 'low' was not found in ts")

            if 'high' not in ts:
                raise KeyError("The required key 'high' was not found in ts")

            if self.boxsize != 'total' and self.boxsize + 1 > len(ts['close']):
                raise IndexError("ATR boxsize is larger than length of data.")

        # bring all inputs to the final format as dict with numpy.ndarrays.
        for key in ts.keys():
            if isinstance(ts[key], list):
                ts[key] = np.array(ts[key])
            if not type(ts[key]) == np.ndarray:
                if type(ts[key]) == str or float or int:
                    ts[key] = np.array([ts[key]])

        # if ts['date'] exist check for the type, if it's a string convert
        # to datetime64 else create index of integers.
        # If the string can't converted to datetime64 create index of integers.
        if 'date' not in ts:
            ts['date'] = np.arange(0, ts['close'].shape[0])

        if isinstance(ts['date'][0], str):

            try:
                ts['date'] = ts['date'].astype('datetime64')

                datetime_diff = ts['date'][0:-1] - ts['date'][1:]

                if any(np.mod(datetime_diff / np.timedelta64(1, "D"), 1) != 0):
                    self.time_step = 'm'
                elif any(np.mod(datetime_diff / np.timedelta64(1, "D"), 1) == 0):
                    self.time_step = 'D'
                else:
                    self.time_step = None

            except ValueError:
                warn('Date string can`t be converted to datetime64. Date is set to index of integers')
                ts['date'] = np.arange(0, ts['close'].shape[0])

        # if date is datetime64 check if last date in array is the latest and
        # flip the array if not.
        if isinstance(ts['date'][0], np.datetime64):
            if ts['date'][0] > ts['date'][-1]:
                for key in ts.keys():
                    ts[key] = np.flip(ts[key])

            datetime_diff = ts['date'][0:-1] - ts['date'][1:]

            if any(np.mod(datetime_diff / np.timedelta64(1, "D"), 1) != 0):
                self.time_step = 'm'
            elif any(np.mod(datetime_diff / np.timedelta64(1, "D"), 1) == 0):
                self.time_step = 'D'
            else:
                self.time_step = None

        if not isinstance(ts['date'][0], np.datetime64):
            ts['date'] = np.arange(0, ts['close'].shape[0])

        # check if all arrays have the same length
        length = [x.shape[0] for x in ts.values()]
        if not all(x == length[0] for x in length):
            raise IOError('All arrays in the time-series must have the same length')

        return ts

    def _get_boxscale(self, overscan=None):
        """
        creates the box scale for Point and Figure Chart
        """

        if self.method == 'cl':
            minimum = np.min(self.ts['close'])
            maximum = np.max(self.ts['close'])
        else:
            minimum = np.min(self.ts['low'])
            maximum = np.max(self.ts['high'])

        # initiate variable for boxscale
        boxes = np.array([])

        # initiate overscan range for top and bottom of the scale
        overscan_top = 0
        overscan_bot = 0

        # define range for overscan. If no value is given take the reversal
        if overscan is None:
            overscan = 20  # self.reversal

        if type(overscan) == int:
            overscan_bot = overscan
            overscan_top = overscan
        elif type(overscan) == list or type(overscan) == tuple:
            overscan_bot = overscan[0]
            overscan_top = overscan[1]

        # make scale for absolute scaling
        if self.scaling == 'abs' or self.scaling == 'atr':
            if self.scaling == 'atr':
                # Calculate components of the True Range
                p = self.boxsize == 'total' and len(self.ts['close']) - 1 or self.boxsize
                high_low = self.ts['high'][-p:] - self.ts['low'][-p:]
                high_close_prev = np.abs(self.ts['high'][-p:] - self.ts['close'][-p - 1:-1])
                low_close_prev = np.abs(self.ts['low'][-p:] - self.ts['close'][-p - 1:-1])

                # Combine and find the maximum for each day to get the True Range, excluding the first day due to shift
                true_range = np.maximum(np.maximum(high_low, high_close_prev), low_close_prev)

                # Calculate a single average value for the True Range, to be used as the box size
                self.boxsize = np.mean(true_range)

                self.scaling = 'abs'

            decimals = len(str(self.boxsize).split(".")[-1])

            boxes = np.array(np.float64([0]))
            boxsize = np.round(np.float64(self.boxsize), decimals)

            while boxes[0] <= minimum - (overscan_bot + 1) * boxsize:
                boxes[0] = np.round(boxes[0] + boxsize, decimals)

            n = 0
            while boxes[n] <= maximum + (overscan_top - 1) * boxsize:
                boxes = np.append(boxes, np.round(boxes[n] + boxsize, decimals))
                n += 1

        # make scale for logarithmic scaling
        elif self.scaling == 'log':

            boxsize = np.float64(self.boxsize)
            minval = 0.0001  # minimum value for log-scaled axis

            boxes = np.array([np.log(minval)])
            log_boxsize = np.log(1 + boxsize / 100)

            while boxes[0] <= np.log(minimum) - (overscan_bot + 1) * log_boxsize:
                boxes[0] = boxes[0] + log_boxsize

            n = 0
            while boxes[n] <= np.log(maximum) + (overscan_top - 1) * log_boxsize:
                boxes = np.append(boxes, boxes[n] + log_boxsize)
                n += 1

            boxes = np.exp(boxes)

            if boxsize >= 0.1:
                boxes = np.where((boxes >= 0.1) & (boxes < 1), np.round(boxes, 5), boxes)
                boxes = np.where((boxes >= 1) & (boxes < 10), np.round(boxes, 4), boxes)
                boxes = np.where((boxes >= 10) & (boxes < 100), np.round(boxes, 3), boxes)
                boxes = np.where(boxes >= 100, np.round(boxes, 2), boxes)

        # make scale for classic scaling
        elif self.scaling == 'cla':

            f = self.boxsize
            s = np.array([0.2, 0.5, 1]) * f

            b1 = np.arange(6, 14 - s[0], s[0])
            b2 = np.arange(14, 29 - s[1], s[1])
            b3 = np.arange(29, 60 - s[2], s[2])

            b0 = np.hstack((b1, b2, b3)) / 10000

            g = np.array([1])
            boxes = np.append(0, b0 * g)

            while boxes[-overscan_top - 1] < maximum:
                g = g * 10
                boxes = np.append(boxes, np.round(b0 * g, 5))

            start = np.where(boxes <= minimum)[0][-1] - overscan_bot
            if start < 0:
                start = 0
            end = np.where(boxes > maximum)[-1][0] + overscan_top

            boxes = boxes[start:end]

        return boxes

    def _get_first_trend(self):
        """
        Determines the first box and trend
        """

        if self.method == 'cl' or self.method == 'ohlc':
            H = self.ts['close']
            L = self.ts['close']
        else:
            H = self.ts['high']
            L = self.ts['low']

        Boxes = self.boxscale

        iBu = np.where(Boxes >= H[0])[0][0]

        if H[0] != Boxes[iBu]:
            iBu = iBu - 1

        iBd = np.where(Boxes <= L[0])[0][-1]

        k = 1
        uTF = 0  # uptrend flag
        dTF = 0  # downtrend flag

        while uTF == 0 and dTF == 0 and k <= np.size(H) - 1:
            if H[k] >= Boxes[iBu + 1]:
                uTF = 1
            else:
                if L[k] <= Boxes[iBd - 1]:
                    dTF = -1
            k += 1

        # first trend is up
        if uTF > 0:
            TF = uTF
            iB = iBu

        # first trend is down
        elif dTF < 0:
            TF = dTF
            iB = iBd

        # no trend
        else:
            TF = 0
            iB = 0

        iC = 0  # column index
        fB = 1  # number of filled Boxes
        box = Boxes[iB]

        iD = k - 1  # index of date with first entry

        if TF == 0:
            raise ValueError('Choose a smaller box size. There is no trend using the current parameter.')

        return iD, box, iB, iC, TF, fB

    def _basic(self, P, iB, iC, TF, fB):
        """
        basic logic to build point and figure charts
        """

        Boxes = self.boxscale
        reversal = self.reversal

        iBp = iB  # Box index from previous iteration
        fBp = fB  # number of filled Boxes from previous iteration

        if TF == 1:

            # check if there is a further 'X' in the trend
            if P >= Boxes[iB + 1]:

                # increase box index until the price reaches the next box level
                while P >= Boxes[iB + 1]:
                    iB = iB + 1

                # calculate number of filled Boxes
                fB = fB + iB - iBp

            # the Box index can not be zero
            if iB - reversal < 1:
                iB = 1 + reversal

            # check for reversal
            if P <= Boxes[iB - reversal]:

                # set Box index to the bottom box
                iB = np.where(Boxes >= P)[0][0]

                TF = -1  # trend becomes negative
                iC = iC + 1  # go to next column
                fB = iBp - iB  # calculate number of filled Boxes

                # check for one-step-back
                if reversal == 1 and fBp == 1:
                    iC = iC - 1  # set column to previous column
                    fB = fB + 1  # calculate number of filled Boxes

        elif TF == -1:

            # the Box index can not be zero
            if iB - 1 < 1:
                iB = 1 + 1

            # check if there is a further 'O' in the trend
            if P <= Boxes[iB - 1]:

                # decrease box index until the price falls down under the next box level
                while P <= Boxes[iB - 1]:
                    iB = iB - 1

                # calculate number of filled Boxes
                fB = fB + iBp - iB

            # check for reversal
            if P >= Boxes[iB + reversal]:

                # set Box index to the top box
                iB = np.where(Boxes <= P)[0][-1]

                TF = 1  # trend becomes positive
                iC = iC + 1  # go to next column
                fB = iB - iBp  # calculate number of filled Boxes

                # check for one-step-back
                if reversal == 1 and fBp == 1:
                    iC = iC - 1  # set column to previous column
                    fB = fB + 1  # calculate number of filled Boxes

        Box = Boxes[iB]

        return Box, iB, iC, TF, fB

    def _close(self, iD, Box, iB, iC, TF, fB):
        """
        logic for point and figure charts based on closing prices
        """

        C = self.ts['close']

        ts = np.zeros([np.size(C), 5])

        # make the first entry right before the first change
        # otherwise filled boxes can be not correctly determined
        # in next iteration.
        ts[0: iD, :] = [Box, iB, iC, TF, fB]

        C = C[iD:]

        for n, C in enumerate(C):
            [Box, iB, iC, TF, fB] = self._basic(C, iB, iC, TF, fB)
            ts[iD + n, :] = [Box, iB, iC, TF, fB]

        return ts

    def _hilo(self, iD, Box, iB, iC, TF, fB):
        """
        logic for point and figure charts adapting the high/low method
        """

        H = self.ts['high']
        L = self.ts['low']

        Boxes = self.boxscale
        reversal = self.reversal

        ts = np.zeros([np.size(H), 5])

        # make the first entry right before the first change
        # otherwise filled boxes can be not correctly determined
        # in next iteration.
        ts[0: iD, :] = [Box, iB, iC, TF, fB]

        for n in range(iD, np.size(H)):

            iBp = iB  # Box index from previous iteration
            fBp = fB  # number of filled Boxes from previous iteration

            if TF == 1:

                # check if there is a further 'X' in the trend
                if H[n] >= Boxes[iB + 1]:
                    [Box, iB, iC, TF, fB] = self._basic(H[n], iB, iC, TF, fB)

                else:

                    # the Box index can not be zero
                    if iB - reversal < 1:
                        iB = 1 + reversal

                    # check low for reversal
                    if L[n] <= Boxes[iB - reversal]:
                        TF = -1
                        [Box, iB, iC, TF, _] = self._basic(L[n], iB, iC, TF, fB)
                        iC = iC + 1  # go to next column
                        fB = iBp - iB  # calculate number of filled Boxes

                        # check for one-step-back
                        if reversal == 1 and fBp == 1:
                            iC = iC - 1  # set column to previous column
                            fB = fB + 1  # calculate number of filled Boxes

                ts[n, :] = [Box, iB, iC, TF, fB]

            elif TF == -1:

                # the Box index can not be zero
                if iB - 1 < 1:
                    iB = 1 + 1

                # check if there is a further 'O' in the trend
                if L[n] <= Boxes[iB - 1]:
                    [Box, iB, iC, TF, fB] = self._basic(L[n], iB, iC, TF, fB)

                else:

                    # check high for reversal
                    if H[n] >= Boxes[iB + reversal]:
                        TF = 1
                        [Box, iB, iC, TF, _] = self._basic(H[n], iB, iC, TF, fB)
                        iC = iC + 1  # go to next column
                        fB = iB - iBp  # calculate number of filled Boxes

                        # check for one-step-back
                        if reversal == 1 and fBp == 1:
                            iC = iC - 1  # set column to previous column
                            fB = fB + 1  # calculate number of filled Boxes

            ts[n, :] = [Box, iB, iC, TF, fB]

        return ts

    def _lohi(self, iD, Box, iB, iC, TF, fB):
        """
        logic for point and figure charts adapting the low/high method
        """
        H = self.ts['high']
        L = self.ts['low']

        Boxes = self.boxscale
        reversal = self.reversal

        ts = np.zeros([np.size(H), 5])

        # make the first entry right before the first change
        # otherwise filled boxes can be not correctly determined
        # in next iteration.
        ts[0: iD, :] = [Box, iB, iC, TF, fB]

        for n in range(iD, np.size(H)):

            iBp = iB  # Box index from previous iteration
            fBp = fB  # number of filled Boxes from previous iteration

            if TF == 1:

                # the Box index can not be zero
                if iB - reversal < 1:
                    iB = 1 + reversal

                # check for reversal
                if L[n] <= Boxes[iB - reversal]:
                    TF = -1
                    [Box, iB, iC, TF, _] = self._basic(L[n], iB, iC, TF, fB)
                    iC = iC + 1  # go to next column
                    fB = iBp - iB  # calculate number of filled Boxes

                    # check for one-step-back
                    if reversal == 1 and fBp == 1:
                        iC = iC - 1  # set column to previous column
                        fB = fB + 1  # calculate number of filled Boxes
                else:

                    # check if there is a further 'X' in the trend
                    if H[n] >= Boxes[iB + 1]:
                        [Box, iB, iC, TF, fB] = self._basic(H[n], iB, iC, TF, fB)

            elif TF == -1:

                # check for reversal
                if H[n] >= Boxes[iB + reversal]:
                    TF = 1
                    [Box, iB, iC, TF, _] = self._basic(H[n], iB, iC, TF, fB)
                    iC = iC + 1  # go to next column
                    fB = iB - iBp  # calculate number of filled Boxes

                    # check for one-step-back
                    if reversal == 1 and fBp == 1:
                        iC = iC - 1  # set column to previous column
                        fB = fB + 1  # calculate number of filled Boxes

                else:

                    # check if there is a further 'O' in the trend
                    if L[n] <= Boxes[iB - 1]:
                        [Box, iB, iC, TF, fB] = self._basic(L[n], iB, iC, TF, fB)

                    # else:  # do nothing
                    #   pass

            ts[n, :] = [Box, iB, iC, TF, fB]

        return ts

    def _hlc(self, iD, Box, iB, iC, TF, fB):
        """
        logic for point and figure charts adapting the high/low/close method
        """

        H = self.ts['high']
        L = self.ts['low']
        C = self.ts['close']

        Boxes = self.boxscale
        reversal = self.reversal

        ts = np.zeros([np.size(H), 5])

        # make the first entry right before the first change
        # otherwise filled boxes can be not correctly determined
        # in next iteration.
        ts[0: iD, :] = [Box, iB, iC, TF, fB]

        for n in range(iD, np.size(H)):

            iBp = iB  # Box index from previous iteration
            fBp = fB  # number of filled Boxes from previous iteration

            # trend is up
            if TF == 1:

                # check if there is a further 'X' in the trend
                if C[n] >= Boxes[iB + 1]:
                    [Box, iB, iC, TF, fB] = self._basic(H[n], iB, iC, TF, fB)

                else:

                    # the Box index can not be zero
                    if iB - reversal < 1:
                        iB = 1 + reversal

                    # check for reversal
                    if C[n] <= Boxes[iB - reversal]:
                        TF = -1
                        [Box, iB, iC, TF, _] = self._basic(L[n], iB, iC, TF, fB)
                        iC = iC + 1  # go to next column
                        fB = iBp - iB  # calculate number of filled Boxes

                        if reversal == 1 and fBp == 1:  # check for one-step-back
                            iC = iC - 1  # set column to previous column
                            fB = fB + 1  # calculate number of filled Boxes

                ts[n, :] = [Box, iB, iC, TF, fB]

            # trend is down
            elif TF == -1:

                # the Box index can not be zero
                if iB - 1 < 1:
                    iB = 1 + 1

                # check if there is a further 'O' in the trend
                if C[n] <= Boxes[iB - 1]:
                    [Box, iB, iC, TF, fB] = self._basic(L[n], iB, iC, TF, fB)

                else:

                    # check close for reversal
                    if C[n] >= Boxes[iB + reversal]:
                        TF = 1
                        [Box, iB, iC, TF, _] = self._basic(H[n], iB, iC, TF, fB)
                        iC = iC + 1  # go to next column
                        fB = iB - iBp  # calculate number of filled Boxes

                        # check for one-step-back
                        if reversal == 1 and fBp == 1:
                            iC = iC - 1  # set column to previous column
                            fB = fB + 1  # calculate number of filled Boxes

                ts[n, :] = [Box, iB, iC, TF, fB]

        return ts

    def _ohlc(self):
        """
        logic for point and figure charts adapting the open/high/low/close method
        """

        O = self.ts['open']
        H = self.ts['high']
        L = self.ts['low']
        C = self.ts['close']

        P = np.zeros(4 * np.size(C))

        tP = []
        counter = 0
        for n in range(counter, np.size(C)):

            if C[n] > O[n]:
                tP = [O[n], L[n], H[n], C[n]]

            elif C[n] < O[n]:
                tP = [O[n], H[n], L[n], C[n]]

            elif C[n] == O[n] and C[n] == L[n]:
                tP = [O[n], H[n], L[n], C[n]]

            elif C[n] == O[n] and C[n] == H[n]:
                tP = [O[n], L[n], H[n], C[n]]

            elif C[n] == O[n] and (H[n] + L[n]) / 2 > C[n]:
                tP = [O[n], H[n], L[n], C[n]]

            elif C[n] == O[n] and (H[n] + L[n]) / 2 < C[n]:
                tP = [O[n], L[n], H[n], C[n]]

            elif C[n] == O[n] and (H[n] + L[n]) / 2 == C[n]:

                if n > 1:
                    # if trend is uptrend
                    if C[n - 1] < C[n]:
                        tP = [O[n], H[n], L[n], C[n]]

                    # downtrend
                    elif C[n - 1] > C[n]:
                        tP = [O[n], L[n], H[n], C[n]]

                else:
                    tP = [O[n], H[n], L[n], C[n]]

            P[counter:counter + 4] = tP

            counter += 4

        # store initial close values temporary
        close = self.ts['close'].copy()

        # set the new time-series as close
        self.ts['close'] = P

        # determine the fist box entry
        [iD, Box, iB, iC, TF, fB] = self._get_first_trend()

        # restore initial close
        self.ts['close'] = close

        ts = np.zeros([np.size(P), 5])

        ts[0: iD, :] = [Box, iB, iC, TF, fB]

        for n in range(iD, len(P)):
            [Box, iB, iC, TF, fB] = self._basic(P[n], iB, iC, TF, fB)
            ts[n, :] = [Box, iB, iC, TF, fB]

        return ts

    def _get_pnf_timeseries(self):
        """
        builds time-series for point and figure chart
        """

        ts = self.ts

        date = ts['date']
        pfdate = date.copy()

        [iD, Box, iB, iC, TF, fB] = self._get_first_trend()

        if self.method == 'cl':
            ts = self._close(iD, Box, iB, iC, TF, fB)

        elif self.method == 'h/l':
            ts = self._hilo(iD, Box, iB, iC, TF, fB)

        elif self.method == 'l/h':
            ts = self._lohi(iD, Box, iB, iC, TF, fB)

        elif self.method == 'hlc':
            ts = self._hlc(iD, Box, iB, iC, TF, fB)

        elif self.method == 'ohlc':
            ts = self._ohlc()

            # reset the index and calculate missing datetimes
            if isinstance(self.ts['date'][0], np.datetime64):

                # extend initial index by 4 times and convert to seconds
                pfdate = np.repeat(pfdate, 4).astype('datetime64[s]')

                # find minimum in timedelta and assign to timestep
                timestep = np.min(np.diff(date))
                timestep = np.timedelta64(timestep, 's')

                # re-index the data
                counter = 0

                for n in range(0, np.size(date)):
                    pfdate[counter:counter + 4] = np.array([date[n],
                                                            date[n] + timestep * 0.25,
                                                            date[n] + timestep * 0.5,
                                                            date[n] + timestep * 0.75], dtype='datetime64[s]')
                    counter = counter + 4

            # date is not in datetime format, set index to integers
            else:
                pfdate = np.arange(0, np.shape(ts)[0])

        iTc = np.diff(np.append(0, ts[:, 3])).astype(bool)  # index of Trend change
        iBc = np.diff(np.append(0, ts[:, 1])).astype(bool)  # index of Box changes

        ic = np.logical_or(iBc, iTc)  # index of steps with changes

        ts[~ic, :] = np.nan  # set elements without action to NaN

        # index values cant be integer because of the nans in the arrays.
        pftseries = {'date': pfdate,
                     'box value': ts[:, 0],
                     'box index': ts[:, 1],
                     'column index': ts[:, 2],
                     'trend': ts[:, 3],
                     'filled boxes': ts[:, 4]}

        return pftseries

    def _get_column_entry_dates(self):

        date = self.pnf_timeseries['date']
        column_index = self.pnf_timeseries['column index']

        if self.time_step is not None:
            n = 0
            column_date_labels = []

            for d, c in zip(date, column_index):
                if c == n:
                    n = n + 1
                    d = np.datetime_as_string(d, unit=self.time_step)
                    d = d.replace('T', ' ')
                    column_date_labels.append(d)
        else:
            column_date_labels = None

        return column_date_labels

    def _pnf_timeseries2matrix(self):
        """
        builds Point and Figure matrix from Point and Figure time-series.
        """

        ts = self.pnf_timeseries
        boxes = self.boxscale

        iTS = np.arange(len(ts['box index']))
        iB = ts['box index'].copy()
        iC = ts['column index'].copy()
        TF = ts['trend'].copy()

        iNaN = np.isnan(iB)  # find indices of nan entries

        # remain entries without NaNs qne convert to int
        iB = iB[~iNaN].astype(int)
        iC = iC[~iNaN].astype(int)
        TF = TF[~iNaN].astype(int)
        iTS = iTS[~iNaN]

        mtx = np.zeros([np.size(boxes), iC[-1] + 1], dtype=int)
        self.action_index_matrix = np.zeros([np.size(boxes), iC[-1] + 1], dtype=int)

        # mark first box
        if TF[0] == 1:
            mtx[iB[0], 0] = 1
            self.action_index_matrix[iB[0], 0] = iTS[0]
        elif TF[0] == -1:
            mtx[iB[1], 0] = -1
            self.action_index_matrix[iB[0], 0] = iTS[0]

        # mark the other boxes
        for n in range(1, np.size(iB)):

            # positive trend goes on
            if TF[n - 1] == 1 and TF[n] == 1:
                mtx[iB[n - 1]:iB[n] + 1, iC[n]] = TF[n]
                self.action_index_matrix[iB[n - 1]:iB[n] + 1, iC[n]] = iTS[n]

            # positive trend reverses
            elif TF[n - 1] == 1 and TF[n] == -1:
                mtx[iB[n]:iB[n - 1], iC[n]] = TF[n]
                self.action_index_matrix[iB[n]:iB[n - 1], iC[n]] = iTS[n]

            # negative trend goes on
            elif TF[n - 1] == -1 and TF[n] == -1:
                mtx[iB[n]:iB[n - 1] + 1, iC[n]] = TF[n]
                self.action_index_matrix[iB[n]:iB[n - 1] + 1, iC[n]] = iTS[n]

            # negative trend reverses
            elif TF[n - 1] == -1 and TF[n] == 1:
                mtx[iB[n - 1] + 1:iB[n] + 1, iC[n]] = TF[n]
                self.action_index_matrix[iB[n - 1] + 1:iB[n] + 1, iC[n]] = iTS[n]

        return mtx
