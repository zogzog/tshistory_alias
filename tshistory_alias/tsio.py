from sqlalchemy import exists, select

from tshistory.tsio import TimeSerie as BaseTs

from tshistory_alias import schema

class TimeSerie(BaseTs):

    def get_bounds(self, cn, name, revision_date=None, remove_outliers=False):

        ts = super(TimeSerie, self).get(cn, name, revision_date)

        outliers = schema.outliers
        presence = exists().where(outliers.c.serie == name)
        if not remove_outliers or not cn.execute(select([presence])).scalar():
            return ts

        mini, maxi = cn.execute(select([outliers.c.min, outliers.c.max]
                          ).where(outliers.c.serie==name)).fetchall()[0]
        if mini:
            ts = ts[ts >= mini]
        if maxi:
            ts = ts[ts <= maxi]

        return ts
