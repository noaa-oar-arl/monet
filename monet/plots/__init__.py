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
