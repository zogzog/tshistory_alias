from sqlalchemy import exists, select
import pandas as pd

from tshistory.tsio import TimeSerie as BaseTs

from tshistory_alias import schema

class TimeSerie(BaseTs):

    def get_bounds(self, cn, name, from_value_date=None, to_value_date=None,
                    revision_date=None, remove_outliers=False):

        ts = super(TimeSerie, self).get(cn, name, revision_date,
                                        from_value_date=from_value_date,
                                        to_value_date=to_value_date
                                        )

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

    # from data_hub.tsio...
    def _apply_priority(self, ts_result, ts_new, remove, index=None):
        if index is not None:
            ts_new = ts_new[index]
        if remove:
            ts_new = ts_new[:-remove]
        combine = self._apply_diff(ts_result, ts_new)
        return combine

    def get_priority(self, cn, alias,
                     revision_date=None,
                     from_value_date=None,
                     to_value_date=None):

        df = pd.read_sql('''select * from alias.priority as pr where pr.alias = '{}' order by priority desc'''.format(alias), cn)
        ts_values = pd.Series()
        ts_origins = pd.Series()

        for row in df.itertuples():
            name = row.serie
            prune = row.prune
            ts = super(TimeSerie, self).get(cn, name, revision_date,
                                            from_value_date=from_value_date,
                                            to_value_date=to_value_date
                                            )
            if ts is None:
                continue
            if (not pd.isnull(row.coefficient) and
                ts is not None and
                ts.dtype != 'O'):
                ts = ts * row.coefficient
            ts_id = pd.Series(name, index=ts.index)

            ts_values = self._apply_priority(ts_values, ts, prune)
            ts_origins = self._apply_priority(ts_origins, ts_id, prune)

            ts_values.name = alias
            ts_origins.name = alias

        return ts_values, ts_origins

    def get_arithmetic(self, cn, alias,
                        from_value_date=None,
                        to_value_date=None):
        df = pd.read_sql('''select * from alias.arithmetic where alias = '{}' '''.format(alias), cn)
        first_iteration=True
        for row in df.itertuples():
            ts = self.get(cn, row.serie,
                          from_value_date=from_value_date,
                          to_value_date=to_value_date)
            if ts is None:
                raise Exception('{} is needed to calculate {} and does not exist'.format(
                                    row.serie, alias))
            if first_iteration:
                df_result = ts.to_frame() * row.coefficient
                first_iteration=False
                continue
            df_result = df_result.join(ts * row.coefficient, how='inner')
        ts_result = df_result.sum(axis=1)
        ts_result.name = alias

        return ts_result