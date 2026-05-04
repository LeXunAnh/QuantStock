import numpy as np
from warnings import warn

class TrendlineMixin:
    def get_trendlines(self, length=4, mode='strong'):
        """
        Gets trendlines of an PointfigChart object

        Parameter:
        ==========

        length: int
            minimum length for trendlines default(4).
        mode: str
            'strong' or 'weak' default('strong')
            Strong trendlines break is the line hits a filled box whereas weak lines
            break after a breakout in the other direction occurred above a bearish
            resistance line or below a bullish support line.

        Returns:
        ========

        trendlines: dict
            trendlines['bounded']:
                Array of str: Trendlines are bounded 'internal' or 'external'.
            trendlines['type']: str
                Array of str: Trendlines are 'bullish support' or 'bearish resistance' lines.
            trendlines['length']: int
                Array of int: Length of the trendline.
            trendlines['column index']: int
                Array of int: Index of column where the trendline starts.
            trendlines['box index']: int
                Array of int: Index of row where the trendline starts.
        """

        mtx = self.matrix.copy()

        # correct/initiate minimum length for trendlines:
        if mode == 'weak' and length <= 3:
            length = 4
            warn('Set trendline length to 4. Minimum Length for trendlines of mode=weak is 4.')

        elif mode == 'strong' and length <= 2:
            length = 3
            warn('Set trendline length to 3. Minimum Length for trendlines of mode=strong is 3.')

        # if there is just 1 box filled in first column of mtx add another one
        # to prevent letting trendlines run out of range.
        if np.sum(np.abs(mtx[:, 0])) == 1:

            if np.sum(mtx[:, 0]) > 0:
                idx = np.where(mtx[:, 0] != 0)[0][-1]
                mtx[idx - 1, 0] = 1

            elif np.sum(mtx[:, 0]) > 0:
                idx = np.where(mtx[:, 0] != 0)[0][0]
                mtx[idx + 1, 0] = 1

        # find high and low index for each column; sign indicates trend direction
        T = [np.repeat([np.arange(1, np.size(mtx, 0) + 1, 1)], np.size(mtx, 1), axis=0)][0].transpose() * mtx
        T = np.abs(T)

        ceil = np.zeros(np.size(T, 1)).astype(int)
        floor = np.zeros(np.size(T, 1)).astype(int)

        for n in range(0, np.size(T, 1)):

            high = np.max(T[:, n])
            low = np.min(T[np.where(T[:, n] != 0), n])

            ceil[n] = np.where(T[:, n] == high)[0][0]

            if np.sign(mtx[ceil[n], n]) < 0:
                ceil[n] = ceil[n] * (-1)

            floor[n] = np.where(T[:, n] == low)[0][0]

            if np.sign(mtx[floor[n], n]) < 0:
                floor[n] = floor[n] * (-1)

        # extent mtx in variable T to prevent that trendlines run out of the
        # matrix the offset will be later removed from the data
        offset = np.size(mtx, 1)

        T = np.vstack((np.zeros([np.size(mtx, 1), np.size(mtx, 1)]),
                       mtx,
                       np.zeros([np.size(mtx, 1), np.size(mtx, 1)])
                       )).astype(int)

        T = np.hstack((T, np.zeros([np.size(T, 0), length - 1])))

        # add ones in the last column to stop the latest trendlines
        T = np.hstack((T, np.ones([np.size(T, 0), 1])))

        # new indices after extension
        ceil[ceil > 0] = ceil[ceil > 0] + offset
        ceil[ceil < 0] = ceil[ceil < 0] - offset

        floor[floor > 0] = floor[floor > 0] + offset
        floor[floor < 0] = floor[floor < 0] - offset

        # initiate tl_mtx as matrix containing all possible trendlines
        tl_mtx = np.zeros([np.size(T, 0), np.size(T, 1)])

        if mode == 'weak':

            # initiate matrix for breakpoints for trendlines
            brkpt = np.zeros([np.size(T, 0), np.size(T, 1)])
            # brkpt[:,-1] = 1

            # check if breakouts have been initiated earlier
            if self.breakouts is None:
                bo = self.get_breakouts()

            else:
                bo = self.breakouts

            col = bo['column index'][bo['trend'] == 1]
            row = bo['box index'][bo['trend'] == 1] + offset
            brkpt[row, col] = 1

            col = bo['column index'][bo['trend'] == -1]
            row = bo['box index'][bo['trend'] == -1] + offset
            brkpt[row, col] = -1

            # fill tl_mtx with the length of the trendline at the position of
            # the starting point

            # bearish resistance line starts above every X-column and moves downwards
            # with an 45°-angle until a buy signal is hit or above the line
            for n in range(0, np.size(floor)):

                if ceil[n] > 0:
                    k = ceil[n] + 1
                    col = n

                    while np.sum(brkpt[k:-1, col]) <= 0 and col < np.size(brkpt, 1) - 1:
                        col = col + 1
                        k = k - 1

                    tl_mtx[np.abs(ceil[n]) + 1, n] = n - col

            # bullish support line starts below every O-column and moves upwards with
            # an 45°-angle until a sell signal is hit or below the line
            for n in range(0, np.size(ceil)):

                if floor[n] < 0:
                    k = np.abs(floor[n]) - 1
                    col = n

                    while np.sum(brkpt[0:k, col]) >= 0 and col < np.size(brkpt, 1) - 1:
                        col = col + 1
                        k = k + 1

                    tl_mtx[np.abs(floor[n]) - 1, n] = col - n

            tl_mtx = tl_mtx.astype(int)

            # set all trendlines to zero which are shorter than the minimum length
            tl_mtx[np.abs(tl_mtx) < length] = 0

        # find strong trendlines that will be broken once hit a filled box
        elif mode == 'strong':

            # bearish resistance line starts above every X-column and moves downwards
            # with an 45°-angle until there is any entry different from zero in trendline_mtx
            for n in range(0, np.size(floor)):

                if ceil[n] > 0:
                    k = ceil[n] + 1
                    col = n

                    while T[k, col] == 0:
                        col = col + 1
                        k = k - 1

                    tl_mtx[np.abs(ceil[n]) + 1, n] = n - col

            # bullish support line starts below every O-column and moves upwards with
            # an 45°-angle until there is any entry different from zero in trendline_mtx
            for n in range(0, np.size(ceil)):

                if floor[n] < 0:
                    k = np.abs(floor[n]) - 1
                    col = n

                    while T[k, col] == 0:
                        col = col + 1
                        k = k + 1

                    tl_mtx[np.abs(floor[n]) - 1, n] = col - n

            tl_mtx = tl_mtx.astype(int)
            tl_mtx[np.abs(tl_mtx) < length] = 0

        # counter for the loop to exit if an unexpected case occurred
        loop_run = 0

        # find first trendline
        col = 0
        while np.sum(np.abs(tl_mtx[:, col])) == 0:
            col = col + 1

        # initiate variables for the lookup of external trendlines
        iB = np.argwhere(tl_mtx[:, col] != 0)[0]  # index of last Box
        tF = np.sign(tl_mtx[iB, col])[0]  # TrendFlag
        span = np.abs(tl_mtx[iB, col])[0]  # length of trendline

        tl_vec = np.zeros(np.size(tl_mtx, 1))  # tl_vec: 1d vector of trendlines
        tl_vec[col] = span * tF

        while col + span <= np.size(T, 1) - length - 1 and loop_run <= np.size(T, 1):

            # v_down contains trendlines in the current interval moving downwards
            # v_up contains trendlines in the current interval moving upwards
            v_down = tl_mtx[:, col:col + span].copy()
            v_down[v_down > 0] = 0
            v_down = np.sum(v_down, 0)
            v_up = tl_mtx[:, col:col + span].copy()
            v_up[v_up < 0] = 0
            v_up = np.sum(v_up, 0)

            # remove possible trendlines which are touching occupied boxes within
            # the current interval (necessary for "weak" mode - no impact on strong
            # mode)
            if tF == 1:

                for x in range(0, np.size(v_down)):

                    if v_down[x] != 0:
                        a = np.size(v_down) - np.where(v_down == v_down[x])[0][0]
                        b = np.where(v_down == v_down[x])[0][0]
                        z = np.flipud(np.eye(a))
                        iB = np.argwhere(tl_mtx[:, col + b] != 0)[0][0]
                        check = T[iB - np.size(z, 0) + 1:iB + 1, col + b: col + b + np.size(z, 0)]

                        if np.any(check * z):
                            v_down[x] = 0

            elif tF == -1:

                for x in range(0, np.size(v_up)):

                    if v_up[x] != 0:

                        a = np.size(v_up) - np.where(v_up == v_up[x])[0][0]
                        b = np.where(v_up == v_up[x])[0][0]
                        z = np.eye(a)
                        iB = np.argwhere(tl_mtx[:, col + b] != 0)[0][0]  # index of last Box
                        check = T[iB - 1:iB + np.size(z, 0) - 1, col + b: col + b + np.size(z, 0)]

                        if np.any(check * z):
                            v_up[x] = 0

            if tF == 1:

                # direction of current trendline is up
                # create array containing the position(index+1) of elements of v_down
                # which are not zero. The length of the corresponding line is added to
                # the position. If the number is greater than length of variable, the
                # trendline does leave the interval
                check = (v_down < 0) * np.arange(1, np.size(v_down) + 1, 1) + np.abs(v_down)

                if np.any(v_down) == 1:  # there is a reversal trendline in the interval

                    # check if the reversal trendline leaves the interval
                    if np.any(check > np.size(v_down)) == 1:
                        col = col + np.where(check == np.max(check))[0][0]
                        span = np.sum(np.abs(tl_mtx[:, col]))
                        tF = np.sign(np.sum(tl_mtx[:, col]))
                        tl_vec[col] = span * tF

                    # the reversal trendline does not leave the interval
                    else:
                        tl_mtx[:, col + 1:col + span - 1] = 0

                # there is no reversal trendline in the interval
                elif np.any(check) == 0:

                    # go to next trendline regardless of their direction
                    col = col + np.size(check)
                    span = 1

                    while np.sum(np.sum(np.abs(tl_mtx[:, col:col + span]), 0)) == 0:
                        span = span + 1

                    col = col + span - 1
                    span = np.abs(np.sum(tl_mtx[:, col]))
                    tF = np.sign(np.sum(tl_mtx[:, col]))
                    tl_vec[col] = span * tF

            elif tF == -1:

                # direction of current trendline is down
                # create array containing the position(index+1) of elements of v_down
                # which are not zero. The length of the corresponding line is added to
                # the position. If the number is greater than length of variable, the
                # trendline does leave the interval
                check = (v_up > 0) * np.arange(1, np.size(v_up) + 1, 1) + v_up

                # there is a reversal trendline in the interval
                if np.any(v_up) == 1:

                    # check if the reversal trendline leaves the interval
                    if np.any(check > np.size(v_up)) == 1:
                        col = col + np.where(check == np.max(check))[0][0]
                        span = np.sum(np.abs(tl_mtx[:, col]))
                        tF = np.sign(np.sum(tl_mtx[:, col]))
                        tl_vec[col] = span * tF

                    # the reversal trendline does not leave the interval
                    else:
                        tl_mtx[:, col + 1:col + span - 1] = 0

                # there is no reversal trendline in the interval
                elif np.any(check) == 0:

                    # go to next trendline despite of their direction
                    col = col + np.size(check)
                    span = 1

                    while np.sum(np.sum(np.abs(tl_mtx[:, col:col + span]), 0)) == 0:
                        span = span + 1

                    col = col + span - 1
                    span = np.abs(np.sum(tl_mtx[:, col]))
                    tF = np.sign(np.sum(tl_mtx[:, col]))
                    tl_vec[col] = span * tF

            loop_run += 1

            if loop_run >= np.size(T, 1):
                # raise IndexError('An unexpected case occurred during evaluating the trendlines.')
                break

        # prepare returned variable for trendlines
        row, col = np.where(tl_mtx != 0)

        tlines = {'bounded': np.zeros(np.size(col)).astype(str),
                  'type': np.zeros(np.size(col)).astype(str),
                  'length': np.zeros(np.size(col)).astype(int),
                  'column index': np.zeros(np.size(col)).astype(int),
                  'box index': np.zeros(np.size(col)).astype(int)
                  }

        for n in range(0, np.size(col)):

            # check for bounding
            if tl_vec[col[n]] != 0:
                tlines['bounded'][n] = 'external'
            else:
                tlines['bounded'][n] = 'internal'

            tlines['column index'][n] = col[n]
            tlines['box index'][n] = row[n] - offset

            # the latest trendlines can be shorter than the minimum length.
            # correct the latest trendlines to the actual length.
            if np.abs(tl_mtx[row[n], col[n]]) + col[n] >= np.size(mtx, 1):
                tlines['length'][n] = np.abs(tl_mtx[row[n], col[n]]) - length + 1

            else:
                tlines['length'][n] = np.abs(tl_mtx[row[n], col[n]])

            if tl_mtx[row[n], col[n]] > 0:
                tlines['type'][n] = 'bullish support'

            else:
                tlines['type'][n] = 'bearish resistance'

        # find  and delete index without entries
        x = np.argwhere(tlines['length'] == 0)
        for key in tlines.keys():
            tlines[key] = np.delete(tlines[key], x)

        # sort columns
        idx = np.argsort(tlines['column index'])
        for key, value in tlines.items():
            tlines[key] = tlines[key][idx]

        self.trendlines = tlines

        return tlines