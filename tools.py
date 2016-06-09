__author__ = 'barry'


def search_listinlist(array1, array2):
    #find intersections
    import numpy as np

    s1 = set(array1.flatten())
    s2 = set(array2.flatten())

    inter = s1.intersection(s2)

    index1 = np.array([])
    index2 = np.array([])
    #find the indexes in array1
    for i in inter:
        index11 = np.where(array1 == i)
        index22 = np.where(array2 == i)
        index1 = np.concatenate([index1[:], index11[0]])
        index2 = np.concatenate([index2[:], index22[0]])

    return np.sort(np.int32(index1)), np.sort(np.int32(index2))


def linregress(x, y):
    import statsmodels.api as sm
    import numpy as np

    xx = sm.add_constant(x)
    model = sm.OLS(y, xx)
    fit = model.fit()
    b, a = fit.params[0], fit.params[1]
    rsquared = fit.rsquared
    std_err = np.sqrt(fit.mse_resid)
    return a, b, rsquared, std_err


def smooth(x, window_len=9, window='hanning'):
    """smooth the data using a window with requested size.

    This method is based on the convolution of a scaled window with the signal.
    The signal is prepared by introducing reflected copies of the signal
    (with the window size) in both ends so that transient parts are minimized
    in the begining and end part of the output signal.

    input:
        x: the input signal
        window_len: the dimension of the smoothing window
        window: the type of window from 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'
            flat window will produce a moving average smoothing.

    output:
        the smoothed signal

    example:

    t=linspace(-2,2,0.1)
    x=sin(t)+randn(len(t))*0.1
    y=smooth(x)

    see also:

    numpy.hanning, numpy.hamming, numpy.bartlett, numpy.blackman, numpy.convolve
    scipy.signal.lfilter

    TODO: the window parameter could be the window itself if an array instead of a string
    @type window_len: object
    """
    import numpy as np

    if type([]) == type(x):
        x = np.array(x)

    if x.ndim != 1:
        raise ValueError, "smooth only accepts 1 dimension arrays."

    if x.size < window_len:
        raise ValueError, "Input vector needs to be bigger than window size."

    if window_len < 3:
        return x

    if not window in ['flat', 'hanning', 'hamming', 'bartlett', 'blackman']:
        raise ValueError, "Window is not one of the following: 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'"

    s = np.r_[2 * x[0] - x[window_len:1:-1], x, 2 * x[-1] - x[-1:-window_len:-1]]
    #print(len(s))
    if window == 'flat':  # moving average
        w = np.ones(window_len, 'd')
    else:
        w = eval('np.' + window + '(window_len)')

    y = np.convolve(w / w.sum(), s, mode='same')
    # return the smoothed signal, chopping off the ends so that it has the previous size.
    return y[window_len - 1:-window_len + 1]


def adc_interp(signal, window='hanning', step_size=None):
    """
        this function returns signal after trying to figure out the underlying
    analog signal that has been quantized (perhaps signifigantly) by an
    analog to digital converter.
        in this algorithm, only 'ambiguous' data are included in the convolution,
    or considered in the interpolation.  To be considered ambiguous,
    data points must be within +/- 1 quantized step_size of the actual
    signal value.
    inputs:
        signal
        window_len=9   : the size of the window over which to average
                            "ambiguous" data.
    returns:
        interpolated_signal
    """
    import numpy as np

    if step_size is None:
        # figure out the quantization step size.
        s = list(set(signal))
        s.sort()
        diffs = np.diff(s)
        step_size = np.median(diffs)

    window_lens = [129, 17, 11, 9, 5]
    step_sizes = np.array([1.1, 1.4, 3.0, 6.5, 6.5]) * step_size
    for window_len, step_size in zip(window_lens, step_sizes):
        print window_len, step_size
        # create a simple smooth normalized window to do weighted averages
        win = np.zeros(2 * window_len, np.float64)
        win[window_len] = 1.0
        win = smooth(win, window_len=window_len, window=window)
        isig = []
        # how many points to go forward and backward
        fore = window_len / 2
        aft = window_len - fore
        for i in xrange(len(signal)):
            ambiguous_data_pts = []
            awin = []
            for j in xrange(max(0, i - aft), min(len(signal), i + fore)):
                dj = j - i
                dwin = dj + window_len
                if abs(signal[j] - signal[i]) <= step_size * 1.1:
                    ambiguous_data_pts.append(signal[j] * win[dwin])
                    awin.append(win[dwin])
            isig.append(np.sum(ambiguous_data_pts / np.sum(awin)))
        signal = np.array(isig)
    return np.array(isig)


def findclosest(list, value):
    a = min((abs(x - value), x, i) for i, x in enumerate(list))
    return a[2], a[1]

def julday2(year,month,day,hour=[],min=0):
    from datetime import timedelta,datetime
    from numpy import array
    starting_date = datetime(year,month,day).timetuple()
    jday = starting_date[-2]
    vv = []
    for i in hour:
        t1 = timedelta(days=jday,hours=0)
        t2 = timedelta(days=jday,hours=int(i))
        delta = t2 - t1
        deltaday = delta.days
        deltahour = delta.seconds / 3600. / 24.
        vv.append(jday + deltaday + deltahour)
    return array(vv)


def julday(year, month, day, hour=0., min=0.):
    """
	: THIS PROGRAM RETURNS THE FRACTIONAL DAY OF YEAR
		"""
    import datetime as dtime

    tup = dtime.datetime(year, month, day, hour, min).timetuple()

    #tup is constructed in the following index manner
    # [ year, month, day, hour, min, sec, weekday, doy (note not fractional doy) , tzinfo]
    #doy calculated by doy + hour/24. + min/24./.60
    val = tup[-2] + tup[4] / 24. / 60. + tup[3] / 24.
    return val

