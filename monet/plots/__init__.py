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
        # print(os.path.basename(__file__))
        logo = os.path.abspath(__file__)[:-17] + 'data/MONET-logo.png'
        # print(logo)
        dc.add_logo(logo)
        if fname.split('.')[-1] == 'png':
            img.save(fname, "PNG")
        elif fname.split('.')[-1] == 'jpg':
            img.save(fname, "JPEG")


def sp_scatter_bias(df,
                    col1=None,
                    col2=None,
                    ax=None,
                    outline=False,
                    tight=True,
                    global_map=True,
                    map_kwargs={},
                    cbar_kwargs={},
                    **kwargs):
    from scipy.stats import scoreatpercentile as score
    from numpy import around
    if ax is None:
        ax = draw_map(**map_kwargs)
    try:
        if col1 is None or col2 is None:
            print('User must specify col1 and col2 in the dataframe')
            raise ValueError
        else:
            dfnew = df[['latitude', 'longitude', col1,
                        col2]].dropna().copy(deep=True)
            dfnew['sp_diff'] = (dfnew[col2] - dfnew[col1])
            top = score(dfnew['sp_diff'].abs(), per=95)
            x, y = df.longitude.values, df.latitude.values
            dfnew['sp_diff_size'] = dfnew['sp_diff'].abs() / top * 100.
            dfnew.loc[dfnew['sp_diff_size'] > 300, 'sp_diff_size'] = 300.
            n = dfnew.plot.scatter(
                x='longitude',
                y='latitude',
                c=dfnew['sp_diff'],
                s=dfnew['sp_diff_size'],
                vmin=-1 * top,
                vmax=top,
                ax=ax,
                colorbar=True,
                **kwargs)
            if ~outline:
                ax.outline_patch.set_alpha(0)
            if global_map:
                plt.xlim([-180, 180])
                plt.ylim([-90, 90])
            if tight:
                plt.tight_layout(pad=0)
            return ax
    except ValueError:
        exit
