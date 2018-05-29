from sqlalchemy import exists, select
import pandas as pd

from tshistory.util import SeriesServices
from tshistory_supervision.tsio import TimeSerie as BaseTs
from tshistory_alias import schema

service = SeriesServices()

class TimeSerie(BaseTs):

    def insert(self, cn, newts, name, author=None, _insertion_date=None,
               manual=False):

        serie_type = self._typeofserie(cn, name)
        if serie_type is None or serie_type == 'primary':
            diff = super(TimeSerie, self).insert(cn, newts, name,
                                          author=author,
                                          _insertion_date=_insertion_date,
                                          manual=manual)
            return diff
        else:
            raise Exception('Serie {} is trying to be inserted, but is of type {}'.format(
                name, serie_type
            ))

    def get(self, cn, name, revision_date=None, delta=None,
            from_value_date=None, to_value_date=None, _keep_nans=False):

        serie_type = self._typeofserie(cn, name)
        ts = None
        if serie_type == 'primary':
            if not delta:
                ts = super(TimeSerie, self).get(cn, name, revision_date,
                                                from_value_date=from_value_date,
                                                to_value_date=to_value_date,
                                                _keep_nans=_keep_nans
                                                )
            else:
                ts = super(TimeSerie, self).get_delta(cn, name,
                                                from_value_date=from_value_date,
                                                to_value_date=to_value_date,
                                                delta=delta
                                                )
        elif serie_type == 'priority':
            ts, _ = self.get_priority(cn, name, revision_date,
                                            delta=delta,
                                            from_value_date=from_value_date,
                                            to_value_date=to_value_date,
                                            )
        elif serie_type == 'arithmetic':
            ts = self.get_arithmetic(cn, name, revision_date,
                                            delta=delta,
                                            from_value_date=from_value_date,
                                            to_value_date=to_value_date,
                                            )

        if ts is not None:
            ts = self.apply_bounds(cn, ts)

        return ts

    def apply_bounds(self, cn, ts):

        name = ts.name
        outliers = schema.outliers
        presence = exists().where(outliers.c.serie == name)
        if not cn.execute(select([presence])).scalar():
            return ts

        mini, maxi = cn.execute(select([outliers.c.min, outliers.c.max]
                          ).where(outliers.c.serie==name)).fetchall()[0]
        if mini:
            ts = ts[ts >= mini]
        if maxi:
            ts = ts[ts <= maxi]

        return ts

    def get_priority(self, cn, alias,
                     revision_date=None,
                     delta=None,
                     from_value_date=None,
                     to_value_date=None):

        df = pd.read_sql('''select * from alias.priority as pr where pr.alias = '{}' order by priority desc'''.format(alias), cn)
        ts_values = pd.Series()
        ts_origins = pd.Series()

        for row in df.itertuples():
            name = row.serie
            prune = row.prune
            ts = self.get(cn, name, revision_date,
                          delta=delta,
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
                        revision_date=None,
                        delta=None,
                        from_value_date=None,
                        to_value_date=None):

        df = pd.read_sql('''select * from alias.arithmetic where alias = '{}' '''.format(alias), cn)
        first_iteration=True
        for row in df.itertuples():
            ts = self.get(cn, row.serie, revision_date,
                          delta=delta,
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

    def _typeofserie(self, cn, name):
        presence = exists().where(schema.priority.c.alias == name)
        if cn.execute(select([presence])).scalar():
            return 'priority'
        presence = exists().where(schema.arithmetic.c.alias == name)
        if cn.execute(select([presence])).scalar():
            return 'arithmetic'
        if self.exists(cn, name):
            return 'primary'
        return None

    # from data_hub.tsio...
    def _apply_priority(self, ts_result, ts_new, remove, index=None):
        if index is not None:
            ts_new = ts_new[index]
        if remove:
            ts_new = ts_new[:-remove]
        combine = service.patch(ts_result, ts_new)
        return combine
