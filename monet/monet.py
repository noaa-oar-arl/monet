from __future__ import absolute_import, print_function

from builtins import object

# This is the new driver for MONET
# import verify


class MONET(object):
    def __init__(self):
        self.combined = None
        self.verify = None

    def add_model(self, model='CMAQ', **kwargs):
        """Short summary.

        Parameters
        ----------
        model : type
            Description of parameter `model` (the default is 'CMAQ').
        **kwargs : type
            Description of parameter `**kwargs`.

        Returns
        -------
        type
            Description of returned object.

        """
        """ adds model to monet object
                 for more information on kwargs see: self.add_cmaq
                                           self.add_camx
         """
        if model.upper() == 'CMAQ':
            from .models.cmaq import CMAQ
            mmm = CMAQ()
        if model.upper() == 'CAMX':
            from .models.camx import CMAQ
            mmm = CAMx()
        if model.upper() == 'HYSPLIT':
            from .models.hysplit import HYSPLIT
            mmm = HYSPLIT()
        mmm.open_files(**kwargs)
        return mmm

    def add_obs(self, obs='AirNOW', **kwargs):
        """Short summary.

        Parameters
        ----------
        obs : type
            Description of parameter `obs` (the default is 'AirNOW').
        **kwargs : type
            Description of parameter `**kwargs`.

        Returns
        -------
        type
            Description of returned object.

        """
        if obs.upper() == 'AIRNOW':
            o = self.add_airnow(**kwargs)
        if obs.upper() == 'AERONET':
            o = self.add_aeronet(**kwargs)
        if obs.upper() == 'TOLNET':
            o = self.add_tolnet(**kwargs)
        if obs.upper() == 'AQS':
            o = self.add_aqs(**kwargs)
        if obs.upper() == 'ISH':
            o = self.add_ish(**kwargs)
        if obs.upper() == 'CRN':
            o = self.add_crn(**kwargs)
        if obs.upper() == 'IMPROVE':
            o = self.add_improve(**kwargs)
        return o

    def add_airnow(self, dates=[], **kwargs):
        """Short summary.

        Parameters
        ----------
        dates : type
            Description of parameter `dates` (the default is []).

        Returns
        -------
        type
            Description of returned object.

        """
        from .obs.airnow import AirNow
        airnow = AirNow()
        airnow.dates = dates
        airnow.aggragate_files(**kwargs)
        return airnow

    def add_aqs(self, dates=None, param=None, network=None, daily=False, download=False):
        """Short summary.

        Parameters
        ----------
        dates : type
            Description of parameter `dates` .
        daily : type
            Description of parameter `daily` (the default is False).

        Returns
        -------
        type
            Description of returned object.

        """
        from .obs.aqs import AQS
        aqs = AQS()
        aqs.add_data(dates, param=param, daily=daily, network=network, download=download)
        return aqs

    def add_aeronet(self, dates=[], latlonbox=None):
        """Short summary.

        Parameters
        ----------
        dates : type
            Description of parameter `dates` (the default is []).
        latlonbox : type
            Description of parameter `latlonbox` (the default is None).

        Returns
        -------
        type
            Description of returned object.

        """
        from .obs.aeronet import AERONET
        aeronet = AERONET()
        aeronet.dates = dates
        aeronet.latlonbox = latlonbox
        aeronet.build_url()
        aeronet.read_aeronet()
        return aeronet

    def add_tolnet(self, fname=None):
        """Short summary.

        Parameters
        ----------
        fname : type
            Description of parameter `fname` (the default is None).

        Returns
        -------
        type
            Description of returned object.

        """
        from .obs.tolnet import TOLNET
        tolnet = TOLNet()
        if fname is not None:
            tolnet.open_data(fname)
        return tolnet

    def add_ish(self):
        print('this is a dummy right now')

    def add_crn(self):
        print('this is a dummy right now')

    def add_improve(self):
        print('this is a dummy right now')

    def combine(self, model=None, obs=None, **kwargs):
        """Short summary.

        Parameters
        ----------
        model : type
            Description of parameter `model` (the default is None).
        obs : type
            Description of parameter `obs` (the default is None).
        **kwargs : type
            Description of parameter `**kwargs`.

        Returns
        -------
        type
            Description of returned object.

        """
        if model is not None and obs is not None:
            from .verification.combine import combine as pair
            if obs.objtype == 'TOLNET':
                dset, combined = pair(model=model, obs=obs, **kwargs)
                return dset, combined
            else:
                combined = pair(model=model, obs=obs, **kwargs)
                from .verification.verify import VERIFY
                verify = VERIFY(combined, model=model, obs=obs)
                return combined, verify
