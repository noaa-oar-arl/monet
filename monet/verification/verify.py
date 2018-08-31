from __future__ import absolute_import, print_function

from builtins import object

import pandas as pd

from ..plots import plots


class VERIFY(object):
    def __init__(self, input, obs=None, model=None):
        self.dset = input
        self.obs = obs
        self.model = model
        self.default_scatter_args = {'s': 20, 'edgecolors': 'w', 'lw': .25}

    def point(self,
              param,
              plot_type=None,
              label=None,
              title=None,
              ax=None,
              plotargs={},
              fillargs={'alpha': .2},
              marker='o',
              **kwargs):
        if isinstance(self.dset, pd.DataFrame):
            if self.obs.objtype is 'AQS' or self.obs.objtype is 'AirNow':
                df, title = self.subset_epa(self.dset, param, **kwargs)
            elif self.obs.objtype is 'CRN' or self.obs.objtype is 'ISH':
                df, title = self.subset_crn(self.dset, **kwargs)
            else:
                df = self.pair
                if title is not None:
                    title = ''
            df.index = df.time
            print(plot_type)
            if plot_type.lower() == 'timeseries':
                ax = self._point_plot(
                    df,
                    col1='model',
                    label=label,
                    title=title,
                    timeseries=True,
                    plotargs=plotargs,
                    fillargs=fillargs,
                    ax=ax)
                plotargs['color'] = 'darkslategrey'
                fillargs['color'] = 'darkslategrey'
                ax = self._point_plot(
                    df,
                    col1='obs',
                    ax=ax,
                    title=title,
                    timeseries=True,
                    plotargs=plotargs,
                    fillargs=fillargs)
            elif plot_type.lower() == 'scatter':
                kwargs['x'] = 'obs'
                kwargs['y'] = 'model'
                ax = self._point_plot(
                    df,
                    col1='obs',
                    col2='model',
                    label=label,
                    title=title,
                    scatter=True)
            elif plot_type.lower == 'box':
                ax = self._point_plot(
                    df,
                    col1='obs',
                    col2='model',
                    label=label,
                    title=title,
                    box=True,
                    plotargs=plotargs)
            elif plot_type.lower() == 'pdf':
                ax = self._point_plot(
                    df,
                    col1='model',
                    label=label,
                    title=title,
                    pdf=True,
                    plotargs=plotargs,
                    ax=ax)
                plotargs['color'] = 'darkslategrey'
                ax = self._point_plot(
                    df,
                    col1='obs',
                    label=self.obs.objtype,
                    title=title,
                    pdf=True,
                    plotargs=plotargs,
                    ax=ax)
            elif plot_type.lower() == 'taylor':
                ax = self._point_plot(
                    df,
                    col1='model',
                    label=label,
                    title=title,
                    taylor=True,
                    plotargs=plotargs,
                    fillargs=fillargs,
                    marker=marker)
        # elif
        return ax

    def _point_plot(self,
                    df,
                    label=None,
                    title=None,
                    ax=None,
                    plotargs={},
                    fillargs={},
                    timeseries=False,
                    scatter=False,
                    pdf=False,
                    taylor=False,
                    box=False,
                    col1=None,
                    col2=None,
                    marker='o',
                    **kwargs):
        if timeseries:
            ax = plots.timeseries(
                df,
                y=col1,
                title=title,
                label=label,
                ax=ax,
                plotargs=plotargs,
                fillargs=fillargs)
            return ax
        if scatter:
            ax = plots.scatter(
                df, x=col1, y=col2, title=title, label=label, ax=ax, **kwargs)
            return ax
        if pdf:
            ax = plots.kdeplot(
                df[col1], title=title, label=label, ax=ax, **plotargs)
            ax.set_xlabel(df.variable.unique()[0] + ' (' +
                          df.units.unique()[0] + ')')
            return ax
        if taylor:
            if marker is None:
                marker = 'o'
            if ax is None:
                dia = plots.taylordiagram(
                    df, label=label, dia=ax, addon=False, marker=marker)
                return dia
            else:
                dia = plots.taylordiagram(
                    df, label=label, dia=ax, addon=True, marker=marker)
                plt.legend()
                return dia

    def compare_surface(self, **kwargs):
        """Short summary.

        Parameters
        ----------
        **kwargs : type
            Description of parameter `**kwargs`.

        Returns
        -------
        type
            Description of returned object.

        """
        if (self.obs.objtype is 'AQS' or self.obs.objtype is 'AirNow') and (
                self.model.objtype is 'CMAQ' or self.model.objtype is 'CAMX'):
            self.compare_epa(**kwargs)

    def compare_spatial(self, **kwargs):
        """Short summary.

        Parameters
        ----------
        **kwargs : type
            Description of parameter `**kwargs`.

        Returns
        -------
        type
            Description of returned object.

        """
        if (obs.objtype is 'AQS'
                or obs.objtype is 'AIRNOW') and (model.objtype is 'CMAQ'
                                                 or model.objtype is 'CAMX'):
            self.compare_epa_spatial(**kwargs)

    def compare_epa_spatial(self,
                            model_param='O3',
                            param='OZONE',
                            date=None,
                            imshow_args={},
                            scatter_args={
                                's': 20,
                                'edgecolors': 'w',
                                'lw': .25
                            },
                            barbs_args={},
                            barbs=False,
                            Obs=True,
                            ncolors=None,
                            discrete=False,
                            lay=None):
        """Short summary.

        Parameters
        ----------
        model_param : type
            Description of parameter `model_param` (the default is 'O3').
        param : type
            Description of parameter `param` (the default is 'OZONE').
        date : type
            Description of parameter `date` (the default is None).
        imshow_args : type
            Description of parameter `imshow_args` (the default is {}).
        scatter_args : type
            Description of parameter `scatter_args` (the default is {'s': 20).
        'edgecolors': 'w' : type
            Description of parameter `'edgecolors': 'w'`.
        'lw': .25} : type
            Description of parameter `'lw': .25}`.
        barbs_args : type
            Description of parameter `barbs_args` (the default is {}).
        barbs : type
            Description of parameter `barbs` (the default is False).
        Obs : type
            Description of parameter `Obs` (the default is True).
        ncolors : type
            Description of parameter `ncolors` (the default is None).
        discrete : type
            Description of parameter `discrete` (the default is False).
        lay : type
            Description of parameter `lay` (the default is None).

        Returns
        -------
        type
            Description of returned object.

        :param param: variable Parameter: Acceptable variable: 'OZONE' 'PM2.5' 'CO' 'NOY' 'SO2' 'SO2' 'NOX'
        :param region: EPA Region: 'Northeast', 'Southeast', 'North_Central', 'South_Central', 'Rockies', 'Pacific'
        :param date: If not supplied will plot all time.  Put in 'YYYY-MM-DD HH:MM' for single time
        :return:
        """
        if Obs:
            try:
                g = df.groupby('variable')
                df2 = g.get_group(param)
            except KeyError:
                print(param + ' variable not available!!!!')
                exit
        param = param.upper()
        v = self.model.get_var(param=model_param, lay=lay)
        m = self.cmaq.map
        dts = v.time.to_index()
        if isinstance(date, type(None)):
            index = where(dts == dts[0])[0][0]
        else:
            index = where(dts.isin([date]))[0][0]
        f, ax, c, cmap, vmin, vmax = plots.make_spatial_plot2(
            cmaq[index, :, :].squeeze(),
            m,
            plotargs=imshow_args,
            ncolors=ncolors,
            discrete=discrete)
        plt.tight_layout()
        if Obs:
            scatter_args['vmin'] = vmin
            scatter_args['vmax'] = vmax
            scatter_args['cmap'] = cmap
            df2 = df2.loc[df2.datetime == dts[index]]
            plots.spatial_scatter(df2, m, plotargs=scatter_args)
            c.set_label(param + ' (' + g.get_group(param).Units.unique()[0] +
                        ')')

    @staticmethod
    def subset_epa(df,
                   param,
                   site=None,
                   city=None,
                   state=None,
                   region=None,
                   epa_region=None):
        from ..obs.epa_util import get_epa_location_df
        if site is None and city is None and state is None and region is None and epa_region is None:
            df2 = df.copy()
            title = ' '
        else:
            df2, title = get_epa_location_df(
                df.copy(),
                param,
                site=site,
                city=city,
                state=state,
                region=region,
                epa_region=epa_region)
        return df2, title

    def compare_epa(self,
                    param='OZONE',
                    site='',
                    city='',
                    state='',
                    epa_region='',
                    region='',
                    timeseries=False,
                    scatter=False,
                    pdfs=False,
                    diffscatter=False,
                    diffpdfs=False,
                    timeseries_rmse=False,
                    timeseries_mb=False,
                    taylordiagram=False,
                    ax=None,
                    label=None,
                    footer=False,
                    dia=None,
                    marker=None):
        """Short summary.

        Parameters
        ----------
        param : type
            Description of parameter `param` (the default is 'OZONE').
        site : type
            Description of parameter `site` (the default is '').
        city : type
            Description of parameter `city` (the default is '').
        state : type
            Description of parameter `state` (the default is '').
        epa_region : type
            Description of parameter `epa_region` (the default is '').
        region : type
            Description of parameter `region` (the default is '').
        timeseries : type
            Description of parameter `timeseries` (the default is False).
        scatter : type
            Description of parameter `scatter` (the default is False).
        pdfs : type
            Description of parameter `pdfs` (the default is False).
        diffscatter : type
            Description of parameter `diffscatter` (the default is False).
        diffpdfs : type
            Description of parameter `diffpdfs` (the default is False).
        timeseries_rmse : type
            Description of parameter `timeseries_rmse` (the default is False).
        timeseries_mb : type
            Description of parameter `timeseries_mb` (the default is False).
        taylordiagram : type
            Description of parameter `taylordiagram` (the default is False).
        ax : type
            Description of parameter `ax` (the default is None).
        label : type
            Description of parameter `label` (the default is None).
        footer : type
            Description of parameter `footer` (the default is False).
        dia : type
            Description of parameter `dia` (the default is None).
        marker : type
            Description of parameter `marker` (the default is None).

        Returns
        -------
        type
            Description of returned object.

        """
        from numpy import NaN
        from ..obs.epa_util import get_epa_location_df
        df2, title = get_epa_location_df(
            self.dset.copy(),
            param,
            site=site,
            city=city,
            state=state,
            region=region,
            epa_region=epa_region)
        df2 = df2.groupby('variable').get_group(param)
        if timeseries:
            if ax is None:
                ax = plots.timeseries_param(
                    df2,
                    col='Obs',
                    title=title,
                    label=label,
                    ax=ax,
                    plotargs={'color': 'darkslategrey'},
                    fillargs={
                        'color': 'darkslategrey',
                        'alpha': .2
                    })
            ax = plots.timeseries_param(
                df2,
                col='model',
                title=title,
                label=label,
                ax=ax,
                fillargs={'alpha': .2})
        if scatter:
            plots.scatter_param(
                df2, title=title, label=label, fig=fig, footer=footer)
        if pdfs:
            plots.kdeplots_param(
                df2, title=title, label=label, fig=fig, footer=footer)
        if diffscatter:
            plots.diffscatter_param(df2, title=title)
        if diffpdfs:
            plots.diffpdfs_param(
                df2, title=title, label=label, fig=fig, footer=footer)
        if timeseries_rmse:
            plots.timeseries_rmse_param(
                df2, title=title, label=label, fig=fig, footer=footer)
        if timeseries_mb:
            plots.timeseries_mb_param(
                df2, title=title, label=label, fig=fig, footer=footer)
        if taylordiagram:
            if marker is None:
                marker = 'o'
            if fig is None:
                dia = plots.taylordiagram(
                    df2, label=label, dia=dia, addon=False, marker=marker)
                return dia
            else:
                dia = plots.taylordiagram(
                    df2, label=label, dia=dia, addon=True, marker=marker)
                plt.legend()
                return dia
