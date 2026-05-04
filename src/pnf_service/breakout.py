import numpy as np

class BreakoutMixin:
    def get_breakouts(self):
        """
        Gets the breakouts of an PointFigureChart object

        Returns:
        ========

        breakouts: dict
            The dict contains following keys:
        breakouts['trend']:
            Array of int: 1 for bullish breakouts and -1 for bearish breakouts
        breakouts['type']:
            Array of str: continuation; fulcrum, resistance or reversal
        breakouts['hits']:
            Array of int: Values represent number of how often the
            line has been hit before the breakout.
        breakouts['width']:
            elements contain int of how long the line is
            between the first hit and the breakout.
        breakouts['outer width']:
            elements contain int of how long the line is from the breakout to
            the last filled box in previous columns on the same level.
            If there is no filled column the signal is counted as conti signal
            and the first column of the PointFigureChart is used to calculate the
            outer width.
        """

        mtx = self.matrix

        a = np.zeros([np.size(mtx, 0), 1])
        b = mtx[:, 1:] - mtx[:, :-1]

        # find potential bullish breakouts
        T = np.concatenate((a, b), axis=1)
        T[(T < 1) | (mtx < 1)] = 0

        # row and col index of potential breakouts
        row_bull, col_bull = np.where(T == 1)

        # find potential bearish breakouts
        T = np.concatenate((a, b), axis=1)
        T[(T < -1) | (mtx > -1)] = 0

        # row and col index of potential breakouts
        row_bear, col_bear = np.where(T == -1)

        # initiate dictionary
        keys = ['ts index','trend', 'type', 'column index', 'box index', 'hits', 'width', 'outer width']
        bo = {}
        for key in keys:
            bo[key] = np.zeros(np.size(row_bull) + np.size(row_bear)).astype(int)
        bo['type'] = bo['type'].astype(str)

        if isinstance(self.ts['date'][0], np.datetime64):
            bo['ts index'] = bo['ts index'].astype(f'''datetime64[{self.time_step}]''')
        elif isinstance(self.ts['date'][0], str):
            bo['ts index'] = bo['ts index'].astype(f'''datetime64[{self.time_step}]''')
        else:
            bo['ts index'] = bo['ts index'].astype(int)

        # assign trends
        bo['trend'][0:np.size(row_bull)] = 1
        bo['trend'][np.size(row_bull):np.size(row_bull) + np.size(row_bear)] = -1

        # bullish breakouts
        if np.any(row_bull):

            for n in range(0, np.size(row_bull)):

                bo['box index'][n] = row_bull[n]
                bo['column index'][n] = col_bull[n]
                bo['ts index'][n] = self.ts['date'][self.action_index_matrix[row_bull[n], col_bull[n]]]

                hRL = mtx[row_bull[n] - 1, 0:col_bull[n] + 1]  # horizontal resistance line
                boL = mtx[row_bull[n], 0:col_bull[n] + 1]  # breakout line

                if np.any(np.where(hRL == -1)):
                    i = np.where(hRL == -1)[0][-1]
                else:
                    i = -1

                if np.any(np.where(hRL == 1)):
                    k = np.where(hRL == 1)[0]
                else:
                    k = 0

                if np.any(np.where(k > i)):
                    k = k[np.where(k > i)]

                # find type of signal
                z = 0
                if np.any(np.where(boL[:-1] != 0)) and np.size(k) >= 2:
                    z = np.where(boL[:-1] != 0)[0][-1]
                    bo['outer width'][n] = k[-1] - z + 1

                elif np.size(k) >= 2:
                    bo['outer width'][n] = k[-1] + 1

                if z >= 1:

                    if mtx[row_bull[n], z - 1] == 0 and mtx[row_bull[n], z] == 1:
                        bo['type'][n] = 'resistance'

                    elif mtx[row_bull[n], z - 1] == 1 and mtx[row_bull[n], z] == 1:
                        bo['type'][n] = 'resistance'

                    elif mtx[row_bull[n], z - 1] == -1 and mtx[row_bull[n], z] == -1:
                        bo['type'][n] = 'fulcrum'

                    elif mtx[row_bull[n], z - 1] == -1 and mtx[row_bull[n], z] == 1:
                        bo['type'][n] = 'reversal'

                    elif mtx[row_bull[n], z - 1] == 0 and mtx[row_bull[n], z] == -1:
                        bo['type'][n] = 'reversal'

                    elif mtx[row_bull[n], z - 1] == 1 and mtx[row_bull[n], z] == -1:
                        bo['type'][n] = 'reversal'

                    elif mtx[row_bull[n], z - 1] == 0 and mtx[row_bull[n], z] == 0:
                        bo['type'][n] = 'conti'

                elif z == 0:

                    if mtx[row_bull[n], z] == 0:
                        bo['type'][n] = 'conti'

                    elif mtx[row_bull[n], z] == 1:
                        bo['type'][n] = 'conti'

                    elif mtx[row_bull[n], z] == -1:
                        bo['type'][n] = 'reversal'

                if np.size(k) >= 2:
                    bo['hits'][n] = np.size(k)
                    bo['width'][n] = k[-1] - k[0] + 1

                # find smaller breakouts within other breakouts
                if np.size(k) > 2:

                    for p in range(1, np.size(k) - 1):
                        bo['trend'] = np.append(bo['trend'], 1)
                        bo['type'] = np.append(bo['type'], bo['type'][n])
                        bo['column index'] = np.append(bo['column index'], bo['column index'][n])
                        bo['box index'] = np.append(bo['box index'], bo['box index'][n])
                        bo['hits'] = np.append(bo['hits'], np.sum(mtx[row_bull[n] - 1, k[p]:k[-1] + 1]))
                        bo['width'] = np.append(bo['width'], [k[-1] - k[p] + 1])
                        bo['outer width'] = np.append(bo['outer width'], bo['outer width'][n])
                        bo['ts index'] = np.append(bo['ts index'], bo['ts index'][n])

        # bearish breakouts
        if np.any(row_bear):

            for n in range(0, np.size(row_bear)):

                bo['box index'][np.size(row_bull) + n] = row_bear[n]
                bo['column index'][np.size(row_bull) + n] = col_bear[n]
                bo['ts index'][np.size(row_bull) + n] = self.ts['date'][self.action_index_matrix[row_bull[n], col_bull[n]]]

                hRL = mtx[row_bear[n] + 1, 0:col_bear[n] + 1]  # horizontal resistance line
                boL = mtx[row_bear[n], 0:col_bear[n] + 1]  # breakout line

                if np.any(np.where(hRL == 1)):
                    i = np.where(hRL == 1)[0][-1]

                else:
                    i = -1

                if np.any(np.where(hRL == -1)):
                    k = np.where(hRL == -1)[0]

                else:
                    k = 0

                if np.any(np.where(k > i)):
                    k = k[np.where(k > i)]

                # find type of signal
                z = 0
                if np.any(np.where(boL[:-1] != 0)) and np.size(k) >= 2:
                    z = np.where(boL[:-1] != 0)[0][-1]
                    bo['outer width'][np.size(row_bull) + n] = k[-1] - z + 1

                elif np.size(k) >= 2:
                    bo['outer width'][np.size(row_bull) + n] = k[-1] + 1

                if z >= 1:

                    if mtx[row_bear[n], z - 1] == 0 and mtx[row_bear[n], z] == -1:
                        bo['type'][np.size(row_bull) + n] = 'resistance'

                    elif mtx[row_bear[n], z - 1] == -1 and mtx[row_bear[n], z] == -1:
                        bo['type'][np.size(row_bull) + n] = 'resistance'

                    elif mtx[row_bear[n], z - 1] == 1 and mtx[row_bear[n], z] == 1:
                        bo['type'][np.size(row_bull) + n] = 'reversal'

                    elif mtx[row_bear[n], z - 1] == 1 and mtx[row_bear[n], z] == -1:
                        bo['type'][np.size(row_bull) + n] = 'reversal'

                    elif mtx[row_bear[n], z - 1] == 0 and mtx[row_bear[n], z] == 1:
                        bo['type'][np.size(row_bull) + n] = 'reversal'

                    elif mtx[row_bear[n], z - 1] == -1 and mtx[row_bear[n], z] == 1:
                        bo['type'][np.size(row_bull) + n] = 'reversal'

                    elif mtx[row_bear[n], z - 1] == 0 and mtx[row_bear[n], z] == 0:
                        bo['type'][np.size(row_bull) + n] = 'conti'

                elif z == 0:

                    if mtx[row_bear[n], z] == 0:
                        bo['type'][np.size(row_bull) + n] = 'conti'
                    elif mtx[row_bear[n], z] == -1:
                        bo['type'][np.size(row_bull) + n] = 'conti'
                    elif mtx[row_bear[n], z] == 1:
                        bo['type'][np.size(row_bull) + n] = 'reversal'

                if np.size(k) >= 2:
                    bo['hits'][np.size(row_bull) + n] = np.size(k)
                    bo['width'][np.size(row_bull) + n] = k[-1] - k[0] + 1

                # find smaller breakouts within other breakouts
                if np.size(k) > 2:

                    for p in range(1, np.size(k) - 1):
                        bo['trend'] = np.append(bo['trend'], -1)
                        bo['type'] = np.append(bo['type'], bo['type'][np.size(row_bull) + n])
                        bo['column index'] = np.append(bo['column index'], bo['column index'][np.size(row_bull) + n])
                        bo['box index'] = np.append(bo['box index'], bo['box index'][np.size(row_bull) + n])
                        bo['hits'] = np.append(bo['hits'], np.abs(np.sum(mtx[row_bear[n] + 1, k[p]:k[-1] + 1])))
                        bo['width'] = np.append(bo['width'], [k[-1] - k[p] + 1])
                        bo['outer width'] = np.append(bo['outer width'], bo['outer width'][np.size(row_bull) + n])
                        bo['ts index'] = np.append(bo['ts index'], bo['ts index'][np.size(row_bull) + n])

        # find index without entries:
        x = np.argwhere(bo['hits'] == 0)
        for key in bo.keys():
            bo[key] = np.delete(bo[key], x)

        # sort order: col , row, hits
        T = np.column_stack((bo['column index'], bo['box index'], bo['hits']))
        idx = np.lexsort((T[:, 2], T[:, 1], T[:, 0]))
        for key, value in bo.items():
            bo[key] = bo[key][idx]

        self.breakouts = bo

        return bo