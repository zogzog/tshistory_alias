from sqlalchemy import exists, select
import pandas as pd

from tshistory_supervision.tsio import TimeSerie as BaseTs
from tshistory_alias import schema


KIND = {}  # ts name to kind


class TimeSerie(BaseTs):

    def insert(self, cn, newts, name, author, **kw):
        serie_type = self._typeofserie(cn, name)
        if serie_type != 'primary':
            raise Exception('Serie {} is trying to be inserted, but is of type {}'.format(
                name, serie_type)
            )

        return super(TimeSerie, self).insert(cn, newts, name, author=author, **kw)

    def get(self, cn, name, revision_date=None, delta=None,
            from_value_date=None, to_value_date=None, _keep_nans=False):

        serie_type = self._typeofserie(cn, name)
        ts = None
        if serie_type == 'primary':
            if not delta:
                ts = super(TimeSerie, self).get(
                    cn, name, revision_date,
                    from_value_date=from_value_date,
                    to_value_date=to_value_date,
                    _keep_nans=_keep_nans
                )
            else:
                ts = self.get_delta(cn, name,
                                    from_value_date=from_value_date,
                                    to_value_date=to_value_date,
                                    delta=delta
                )
        elif serie_type == 'priority':
            ts, _ = self.get_priority(
                cn, name, revision_date,
                delta=delta,
                from_value_date=from_value_date,
                to_value_date=to_value_date,
            )
        elif serie_type == 'arithmetic':
            ts = self.get_arithmetic(
                cn, name, revision_date,
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
        bounded = exists().where(outliers.c.serie == name)
        if not cn.execute(select([bounded])).scalar():
            return ts

        mini, maxi = cn.execute(
            select([outliers.c.min, outliers.c.max]
        ).where(
            outliers.c.serie==name)
        ).fetchone()
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

        df = pd.read_sql("select * from alias.priority as pr "
                         "where pr.alias = '{}' order by priority desc".format(alias),
                         cn)
        ts_values = pd.Series()
        ts_origins = pd.Series()

        for row in df.itertuples():
            name = row.serie
            prune = row.prune
            ts = self.get(
                cn, name, revision_date,
                delta=delta,
                from_value_date=from_value_date,
                to_value_date=to_value_date
            )
            if ts is None:
                continue

            if (not pd.isnull(row.coefficient) and
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
        df = pd.read_sql("select * from alias.arithmetic where alias = '{}'".format(alias), cn)
        first_iteration = True
        for row in df.itertuples():
            ts = self.get(
                cn, row.serie, revision_date,
                delta=delta,
                from_value_date=from_value_date,
                to_value_date=to_value_date
            )
            if ts is None:
                raise Exception('{} is needed to calculate {} and does not exist'.format(
                    row.serie, alias)
                )
            if first_iteration:
                df_result = ts.to_frame() * row.coefficient
                first_iteration=False
                continue
            df_result = df_result.join(ts * row.coefficient, how='inner')
        ts_result = df_result.sum(axis=1)
        ts_result.name = alias

        return ts_result

    def _typeofserie(self, cn, name):
        if name in KIND:
            return KIND[name]

        priority = exists().where(schema.priority.c.alias == name)
        arith = exists().where(schema.arithmetic.c.alias == name)
        if cn.execute(select([priority])).scalar():
            KIND[name] = 'priority'
        elif cn.execute(select([arith])).scalar():
            KIND[name] = 'arithmetic'
        else:
            KIND[name] = 'primary'

        return KIND[name]

    def _apply_priority(self, ts_result, ts_new, remove, index=None):
        if index is not None:
            ts_new = ts_new[index]
        if remove:
            ts_new = ts_new[:-remove]
        combine = self.patch(ts_result, ts_new)
        return combine
