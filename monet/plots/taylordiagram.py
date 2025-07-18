"""
Taylor diagram (Taylor, 2001) implementation.

A Taylor diagram is a graphical representation of how well a model simulates
an observed pattern. It provides a way to summarize multiple aspects of model
performance, including:
- Correlation coefficient
- Root-mean-square (RMS) difference
- The standard deviation ratio

Reference:
Taylor, K.E., 2001. Summarizing multiple aspects of model performance in a
single diagram. Journal of Geophysical Research, 106(D7), 7183-7192.
http://www-pcmdi.llnl.gov/about/staff/Taylor/CV/Taylor_diagram_primer.htm
"""

import functools

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

__version__ = "Time-stamp: <2012-02-17 20:59:35 ycopin>"
__author__ = "Yannick Copin <yannick.copin@laposte.net>"

colors = ["#DA70D6", "#228B22", "#FA8072", "#FF1493"]


def _sns_context(f):
    """Decorator to apply seaborn color palette to a function."""

    @functools.wraps(f)
    def inner(*args, **kwargs):
        with sns.color_palette(colors):
            return f(*args, **kwargs)

    return inner


class TaylorDiagram:
    """
    :no-index:

    Taylor diagram for visualizing model performance metrics.

    The Taylor diagram displays multiple statistical metrics in a single plot:
    - The radial distance from the origin represents the standard deviation
    - The azimuthal position represents the correlation coefficient
    - The distance from the reference point represents the root-mean-square (RMS) difference

    This class creates a Taylor diagram in a polar plot, where:
    - r = standard deviation
    - θ = arccos(correlation coefficient)

    This provides a comprehensive view of how well a model pattern matches observations.
    """

    @_sns_context
    def __init__(self, refstd, scale=1.5, fig=None, rect=111, label="_"):
        """Initialize the Taylor diagram.

        Parameters
        ----------
        refstd : float
            The reference standard deviation (e.g., from observations or a reference model)
            that other models will be compared against.
        scale : float, default 1.5
            The maximum standard deviation shown on the plot, as a multiple of refstd.
            For example, if refstd=2 and scale=1.5, the maximum standard deviation
            displayed will be 3.0.
        fig : matplotlib.figure.Figure, optional
            Figure to use. If None, a new figure will be created.
        rect : int or tuple, default 111
            Subplot specification (nrows, ncols, index) or 3-digit integer where
            the digits represent nrows, ncols, and index in order.
        label : str, default "_"
            Label for the reference point. An underscore prefix makes the label not
            appear in the legend.
        """

        import mpl_toolkits.axisartist.floating_axes as FA
        import mpl_toolkits.axisartist.grid_finder as GF
        from matplotlib.projections import PolarAxes

        self.refstd = refstd  # Reference standard deviation

        tr = PolarAxes.PolarTransform()

        # Correlation labels
        rlocs = np.concatenate((np.arange(10) / 10.0, [0.95, 0.99]))
        tlocs = np.arccos(rlocs)  # Conversion to polar angles
        gl1 = GF.FixedLocator(tlocs)  # Positions
        tf1 = GF.DictFormatter(dict(list(zip(tlocs, list(map(str, rlocs))))))

        # Standard deviation axis extent
        self.smin = 0
        self.smax = scale * self.refstd
        ghelper = FA.GridHelperCurveLinear(
            tr,
            extremes=(0, np.pi / 2, self.smin, self.smax),
            grid_locator1=gl1,
            tick_formatter1=tf1,
        )  # 1st quadrant

        if fig is None:
            fig = plt.figure()

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
        (l,) = self.ax.plot([0], self.refstd, "r*", ls="", ms=14, label=label, zorder=10)
        t = np.linspace(0, np.pi / 2)
        r = np.zeros_like(t) + self.refstd
        self.ax.plot(t, r, "k--", label="_")

        # Collect sample points for latter use (e.g. legend)
        self.samplePoints = [l]

    @_sns_context
    def add_sample(self, stddev, corrcoef, *args, **kwargs):
        """Add a sample point to the Taylor diagram.

        Parameters
        ----------
        stddev : float
            Standard deviation of the sample to add.
        corrcoef : float
            Correlation coefficient between the sample and reference (-1 to 1).
        *args
            Additional positional arguments passed to matplotlib's plot function.
        **kwargs
            Additional keyword arguments passed to matplotlib's plot function.
            Common options include 'marker', 'markersize', 'color', and 'label'.

        Returns
        -------
        matplotlib.lines.Line2D
            The line object representing the sample in the plot.

        Notes
        -----
        Points closer to the reference point indicate better agreement with
        the reference dataset.
        """
        (l,) = self.ax.plot(np.arccos(corrcoef), stddev, *args, **kwargs)  # (theta,radius)
        self.samplePoints.append(l)

        return l

    @_sns_context
    def add_contours(self, levels=5, **kwargs):
        """Add constant RMS difference contours to the Taylor diagram.

        Parameters
        ----------
        levels : int or array-like, default 5
            If an integer, it defines the number of equally-spaced contour levels.
            If array-like, it explicitly defines the contour levels.
        **kwargs
            Additional keyword arguments passed to matplotlib's contour function.
            Common options include 'colors', 'linewidths', and 'linestyles'.

        Returns
        -------
        matplotlib.contour.QuadContourSet
            The contour set created by the function.

        Notes
        -----
        These contours represent lines of constant RMS difference between the
        reference and sample datasets. They help visualize the combined effect
        of differences in standard deviation and correlation.
        """

        rs, ts = np.meshgrid(np.linspace(self.smin, self.smax), np.linspace(0, np.pi / 2))
        # Compute centered RMS difference
        rms = np.sqrt(self.refstd**2 + rs**2 - 2 * self.refstd * rs * np.cos(ts))

        contours = self.ax.contour(ts, rs, rms, levels, **kwargs)

        return contours


if __name__ == "__main__":
    # Reference dataset
    x = np.linspace(0, 4 * np.pi, 100)
    data = np.sin(x)
    refstd = data.std(ddof=1)  # Reference standard deviation

    # Models
    m1 = data + 0.2 * np.random.randn(len(x))  # Model 1
    m2 = 0.8 * data + 0.1 * np.random.randn(len(x))  # Model 2
    m3 = np.sin(x - np.pi / 10)  # Model 3

    # Compute stddev and correlation coefficient of models
    samples = np.array([[m.std(ddof=1), np.corrcoef(data, m)[0, 1]] for m in (m1, m2, m3)])

    fig = plt.figure(figsize=(10, 4))

    # Taylor diagram
    dia = TaylorDiagram(refstd, fig=fig, rect=122, label="Reference")
    # colors_ = plt.matplotlib.cm.jet(np.linspace(0, 1, len(samples)))
    colors_ = [None] * len(samples)
    with sns.color_palette(colors):
        ax1 = fig.add_subplot(1, 2, 1, xlabel="X", ylabel="Y")
        ax1.plot(x, data, "ko", label="Data")
        for i, m in enumerate([m1, m2, m3]):
            ax1.plot(x, m, c=colors_[i], label="Model %d" % (i + 1))
    ax1.legend(numpoints=1, prop=dict(size="small"), loc="best")

    # Add samples to Taylor diagram
    for i, (stddev, corrcoef) in enumerate(samples):
        dia.add_sample(
            stddev, corrcoef, marker="s", ls="", c=colors_[i], label="Model %d" % (i + 1)
        )

    # Add RMS contours, and label them
    contours = dia.add_contours(colors="0.5")
    plt.clabel(contours, inline=1, fontsize=10)

    # Add a figure legend
    fig.legend(
        dia.samplePoints,
        [p.get_label() for p in dia.samplePoints],
        numpoints=1,
        prop=dict(size="small"),
        loc="upper right",
    )

    fig.tight_layout()

    plt.show()
