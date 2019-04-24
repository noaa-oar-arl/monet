# server = 'ftp.star.nesdis.noaa.gov'
# base_dir = https://gsce-dtn.sdstate.edu/index.php/s/e8wPYPOL1bGXk5z/download?path=%2F20190318&files=meanFRP.20190318.FV3.C384Grid.tile6.bin
base_dir = "https://gsce-dtn.sdstate.edu/index.php/s/e8wPYPOL1bGXk5z/download?path=%2F"


def download_data(date, ftype="meanFRP"):
    import requests as rq
    from datetime import datetime
    from numpy import arange

    if isinstance(date, datetime):
        year = date.strftime("%Y")
        yyyymmdd = date.strftime("%Y%m%d")
    else:
        from pandas import Timestamp

        date = Timestamp(date)
        year = date.strftime("%Y")
        yyyymmdd = date.strftime("%Y%m%d")
    url_ftype = "&files={}.".format(ftype)
    for i in arange(1, 7, dtype=int).astype(str):
        tile = ".FV3C384Grid.tile{}.bin".format(i)
        url = "{}{}{}{}{}".format(base_dir, yyyymmdd, url_ftype, yyyymmdd,
                                  tile)
        fname = "{}.{}.FV3.C384Grid.tile{}.bin".format(ftype, yyyymmdd, i)
        print("Retrieving file:", fname)
        r = rq.get(url)
        with open(fname, "wb") as f:
            f.write(r.content)
