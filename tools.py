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



def findclosest(list, value):
    a = min((abs(x - value), x, i) for i, x in enumerate(list))
    return a[2], a[1]

