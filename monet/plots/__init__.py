from __future__ import absolute_import, print_function

from . import colorbars, plots, taylordiagram
from .colorbars import *
from .mapgen import *
from .plots import *

__all__ = ['colorbars', 'plots', 'taylordiagram', 'mapgen']

# This is the driver for all verify objects


def savefig(fname, loc=1, decorate=True, **kwargs):
    import io
    import os
    import sys
    from PIL import Image
    import matplotlib.pyplot as plt
    try:
        from pydecorate import DecoratorAGG
        pydecorate = True
    except ImportError:
        pydecorate = False
    plt.savefig(fname, **kwargs)
    if pydecorate and decorate:
        img = Image.open(fname)
        dc = DecoratorAGG(img)
        if loc == 1:
            dc.align_bottom()
        elif loc == 2:
            dc.align_bottom()
            dc.align_right()
        elif loc == 3:
            dc.align_right()
        # sys.argv[0])[-5] + 'data/MONET_logo.png'
        print(os.path.basename(__file__))
        logo = os.path.abspath(__file__)[:-17] + 'data/MONET-logo.png'
        print(logo)
        dc.add_logo(logo)
        if fname.split('.')[-1] == 'png':
            img.save(fname, "PNG")
        elif fname.split('.')[-1] == 'jpg':
            img.save(fname, "JPEG")


def sp_scatter_bias(df, col1=None, col2=None, ax=None, map_kwargs={}):
    from scipy.stats import scoreatpercentile as score
    from numpy import around
    if ax is None:
        ax = draw_map(**map_kwargs)
    try:
        if col1 is None or col2 is None:
            print('User must specify col1 and col2 in the dataframe')
            raise ValueError
        else:
            diff = (df[col2] - df[col1])
            top = around(score(diff.abs(), per=95))
            x, y = df.longitude.values, df.latitude.values
            ss = diff.abs() / top * 100.
            ss[ss > 300] = 300.

            ax.scatter(x, y, c=diff, s=ss, vmin=-1 * top, vmax=top, **kwargs)
            return ax
    except ValueError:
        exit
