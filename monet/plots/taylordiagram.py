"""
Taylor diagram (Taylor, 2001) test implementation.
http://www-pcmdi.llnl.gov/about/staff/Taylor/CV/Taylor_diagram_primer.htm
"""
from __future__ import division, print_function

from builtins import map, object, zip

import matplotlib.pyplot as PLT
import numpy as NP
import seaborn as sns
from past.utils import old_div

__version__ = "Time-stamp: <2012-02-17 20:59:35 ycopin>"
__author__ = "Yannick Copin <yannick.copin@laposte.net>"

colors = ["#DA70D6", "#228B22", "#FA8072", "#FF1493"]
sns.set_palette(sns.color_palette(colors))


class TaylorDiagram(object):
    """Taylor diagram: plot model standard deviation and correlation
    to reference (data) sample in a single-quadrant polar plot, with
    r=stddev and theta=arccos(correlation).
    """

    def __init__(self, refstd, scale=1.5, fig=None, rect=111, label="_"):
        """Set up Taylor diagram axes, i.e. single quadrant polar
        plot, using mpl_toolkits.axisartist.floating_axes. refstd is
        the reference standard deviation to be compared to.
        """

        from matplotlib.projections import PolarAxes
        import mpl_toolkits.axisartist.floating_axes as FA
        import mpl_toolkits.axisartist.grid_finder as GF

        self.refstd = refstd  # Reference standard deviation

        tr = PolarAxes.PolarTransform()

        # Correlation labels
        rlocs = NP.concatenate((old_div(NP.arange(10), 10.0), [0.95, 0.99]))
        tlocs = NP.arccos(rlocs)  # Conversion to polar angles
        gl1 = GF.FixedLocator(tlocs)  # Positions
        tf1 = GF.DictFormatter(dict(list(zip(tlocs, list(map(str, rlocs))))))

        # Standard deviation axis extent
        self.smin = 0
        self.smax = scale * self.refstd
        ghelper = FA.GridHelperCurveLinear(
            tr,
            extremes=(0, old_div(NP.pi, 2), self.smin,
                      self.smax),  # 1st quadrant
            grid_locator1=gl1,
            tick_formatter1=tf1,
        )

        if fig is None:
            fig = PLT.figure()

        ax = FA.FloatingSubplot(fig, rect, grid_helper=ghelper)
        fig.add_subplot(ax)

        # Adjust axes
        ax.axis["top"].set_axis_direction("bottom")  # "Angle axis"
        ax.axis["top"].toggle(ticklabels=True, label=True)
        ax.axis["top"].major_ticklabels.set_axis_direction("top")
        ax.axis["top"].label.set_axis_direction("top")
        ax.axis["top"].label.set_text("Correlation")

        ax.axis["left"].set_axis_direction("bottom")  # "X axis"
        ax.axis["left"].label.set_text("Standard deviation")

        ax.axis["right"].set_axis_direction("top")  # "Y axis"
        ax.axis["right"].toggle(ticklabels=True)
        ax.axis["right"].major_ticklabels.set_axis_direction("left")

        ax.axis["bottom"].set_visible(False)  # Useless

        # Contours along standard deviations
        ax.grid(False)

        self._ax = ax  # Graphical axes
        self.ax = ax.get_aux_axes(tr)  # Polar coordinates

        # Add reference point and stddev contour
        print("Reference std:", self.refstd)
        l, = self.ax.plot([0],
                          self.refstd,
                          "r*",
                          ls="",
                          ms=14,
                          label=label,
                          zorder=10)
        t = NP.linspace(0, old_div(NP.pi, 2))
        r = NP.zeros_like(t) + self.refstd
        self.ax.plot(t, r, "k--", label="_")

        # Collect sample points for latter use (e.g. legend)
        self.samplePoints = [l]

    def add_sample(self, stddev, corrcoef, *args, **kwargs):
        """Add sample (stddev,corrcoeff) to the Taylor diagram. args
        and kwargs are directly propagated to the Figure.plot
        command."""
        l, = self.ax.plot(NP.arccos(corrcoef), stddev, *args,
                          **kwargs)  # (theta,radius)
        self.samplePoints.append(l)

        return l

    def add_contours(self, levels=5, **kwargs):
        """Add constant centered RMS difference contours."""

        rs, ts = NP.meshgrid(
            NP.linspace(self.smin, self.smax), NP.linspace(
                0, old_div(NP.pi, 2)))
        # Compute centered RMS difference
        rms = NP.sqrt(self.refstd**2 + rs**2 -
                      2 * self.refstd * rs * NP.cos(ts))

        contours = self.ax.contour(ts, rs, rms, levels, **kwargs)

        return contours


# if __name__ == '__main__':

#     # Reference dataset
#     x = NP.linspace(0, 4 * NP.pi, 100)
#     data = NP.sin(x)
#     refstd = data.std(ddof=1)  # Reference standard deviation

#     # Models
#     m1 = data + 0.2 * NP.random.randn(len(x))  # Model 1
#     m2 = 0.8 * data + .1 * NP.random.randn(len(x))  # Model 2
#     m3 = NP.sin(x - NP.pi / 10)  # Model 3

#     # Compute stddev and correlation coefficient of models
#     samples = NP.array([[m.std(ddof=1), NP.corrcoef(data, m)[0, 1]]
#                         for m in (m1, m2, m3)])

#     fig = PLT.figure(figsize=(10, 4))

#     ax1 = fig.add_subplot(1, 2, 1, xlabel='X', ylabel='Y')
#     # Taylor diagram
#     dia = TaylorDiagram(refstd, fig=fig, rect=122, label="Reference")

#     colors = PLT.matplotlib.cm.jet(NP.linspace(0, 1, len(samples)))

#     ax1.plot(x, data, 'ko', label='Data')
#     for i, m in enumerate([m1, m2, m3]):
#         ax1.plot(x, m, c=colors[i], label='Model %d' % (i + 1))
#     ax1.legend(numpoints=1, prop=dict(size='small'), loc='best')

#     # Add samples to Taylor diagram
#     for i, (stddev, corrcoef) in enumerate(samples):
#         dia.add_sample(stddev, corrcoef, marker='s', ls='', c=colors[i],
#                        label="Model %d" % (i + 1))

#     # Add RMS contours, and label them
#     contours = dia.add_contours(colors='0.5')
#     PLT.clabel(contours, inline=1, fontsize=10)

#     # Add a figure legend
#     fig.legend(dia.samplePoints,
#                [p.get_label() for p in dia.samplePoints],
#                numpoints=1, prop=dict(size='small'), loc='upper right')

#     PLT.show()
