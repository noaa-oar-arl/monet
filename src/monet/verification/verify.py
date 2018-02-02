from __future__ import absolute_import, print_function

from builtins import object

from ..plots import plots


class VERIFY(object):
    def __init__(self, model=None, obs=None, dset=None):
        self.model = model
        self.obs = obs
        self.dset = dset

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
        if (obs.objtype is 'AQS' or obs.objtype is 'AIRNOW') and (model.objtype is 'CMAQ' or model.objtype is 'CAMX'):
            self.compare_epa_spatial(**kwargs)

    def compare_epa_spatial(self, model_param='O3', param='OZONE', date=None, imshow_args={},
                            scatter_args={'s': 20, 'edgecolors': 'w', 'lw': .25}, barbs_args={}, barbs=False, Obs=True,
                            ncolors=None, discrete=False, lay=None):
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

        :param param: Species Parameter: Acceptable Species: 'OZONE' 'PM2.5' 'CO' 'NOY' 'SO2' 'SO2' 'NOX'
        :param region: EPA Region: 'Northeast', 'Southeast', 'North_Central', 'South_Central', 'Rockies', 'Pacific'
        :param date: If not supplied will plot all time.  Put in 'YYYY-MM-DD HH:MM' for single time
        :return:
        """
        if Obs:
            try:
                g = df.groupby('Species')
                df2 = g.get_group(param)
            except KeyError:
                print(param + ' Species not available!!!!')
                exit
        param = param.upper()
        v = self.model.get_var(param=model_param, lay=lay)
        m = self.cmaq.map
        dts = v.time.to_index()
        if isinstance(date, type(None)):
            index = where(dts == dts[0])[0][0]
        else:
            index = where(dts.isin([date]))[0][0]
        f, ax, c, cmap, vmin, vmax = plots.make_spatial_plot2(cmaq[index, :, :].squeeze(), m, plotargs=imshow_args,
                                                              ncolors=ncolors, discrete=discrete)
        plt.tight_layout()
        if Obs:
            scatter_args['vmin'] = vmin
            scatter_args['vmax'] = vmax
            scatter_args['cmap'] = cmap
            df2 = df2.loc[df2.datetime == dts[index]]
            plots.spatial_scatter(df2, m, plotargs=scatter_args)
            c.set_label(param + ' (' + g.get_group(param).Units.unique()[0] + ')')

    def compare_epa(self, param='OZONE', site='', city='', state='', epa_region='', region='', timeseries=False,
                    scatter=False, pdfs=False, diffscatter=False, diffpdfs=False, timeseries_rmse=False,
                    timeseries_mb=False,
                    taylordiagram=False, ax=None, label=None, footer=False, dia=None, marker=None):
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
        df2, title = get_epa_location_df(self.dset.copy(), param, site=site, city=city, state=state, region=region,
                                         epa_region=epa_region)
        df2 = df2.groupby('Species').get_group(param)
        if timeseries:
            if ax is None:
                ax = plots.timeseries_param(df2, col='Obs', title=title, label=label, ax=ax,
                                            plotargs={'color': 'darkslategrey'},
                                            fillargs={'color': 'darkslategrey', 'alpha': .2})
            ax = plots.timeseries_param(df2, col='model', title=title, label=label, ax=ax, fillargs={'alpha': .2})
        if scatter:
            plots.scatter_param(df2, title=title, label=label, fig=fig, footer=footer)
        if pdfs:
            plots.kdeplots_param(df2, title=title, label=label, fig=fig, footer=footer)
        if diffscatter:
            plots.diffscatter_param(df2, title=title)
        if diffpdfs:
            plots.diffpdfs_param(df2, title=title, label=label, fig=fig, footer=footer)
        if timeseries_rmse:
            plots.timeseries_rmse_param(df2, title=title, label=label, fig=fig, footer=footer)
        if timeseries_mb:
            plots.timeseries_mb_param(df2, title=title, label=label, fig=fig, footer=footer)
        if taylordiagram:
            if marker is None:
                marker = 'o'
            if fig is None:
                dia = plots.taylordiagram(df2, label=label, dia=dia, addon=False, marker=marker)
                return dia
            else:
                dia = plots.taylordiagram(df2, label=label, dia=dia, addon=True, marker=marker)
                plt.legend()
                return dia
