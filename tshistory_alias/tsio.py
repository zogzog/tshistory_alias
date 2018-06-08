from sqlalchemy import exists, select
from sqlalchemy.dialects.postgresql import insert

import pandas as pd

from tshistory_supervision.tsio import TimeSerie as BaseTs
from tshistory_alias.schema import alias_schema


KIND = {}  # ts name to kind
BOUNDS = {}


class TimeSerie(BaseTs):
    alias_schema = None

    def __init__(self, namespace='tsh'):
        super().__init__(namespace)
        self.alias_schema = alias_schema(namespace)

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
                ts = self.get_delta(cn, name, delta=delta)
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
            ts = self.apply_bounds(cn, ts, name)

        return ts

    def apply_bounds(self, cn, ts, name):
        outliers = self.alias_schema.outliers
        bounded = exists().where(outliers.c.serie == name)

        if name not in BOUNDS:
            mini_maxi = cn.execute(
                select([outliers.c.min, outliers.c.max]
                ).where(
                    outliers.c.serie==name)
            ).fetchone()
            BOUNDS[name] = mini_maxi

        mini_maxi = BOUNDS[name]

        if not mini_maxi:
            return ts

        mini, maxi = mini_maxi
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

        df = pd.read_sql(
            'select * from "{}-alias".priority as prio '
            'where prio.alias = %(alias)s '
            'order by priority desc'.format(self.namespace),
            cn, params={'alias': alias}
        )
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
        df = pd.read_sql(
            'select * from "{}-alias".arithmetic where alias = %(alias)s'.format(self.namespace),
                         cn, params={'alias': alias}
        )
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

        priority = exists().where(self.alias_schema.priority.c.alias == name)
        arith = exists().where(self.alias_schema.arithmetic.c.alias == name)
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

    # alias definition/construction

    def canuse_alias(self, cn, alias, warning=False):
        msg = None
        if self._get_ts_table(cn, alias) is not None:
            msg ='{} already used as a primary name'.format(alias)

        table = self.alias_schema.priority
        presence = exists().where(table.c.alias == alias)
        if cn.execute(select([presence])).scalar():
            msg = '{} already used as a priority alias'.format(alias)

        table = self.alias_schema.arithmetic
        presence = exists().where(table.c.alias == alias)
        if cn.execute(select([presence])).scalar():
            msg = '{} already used as an arithmetic alias'.format(alias)

        if msg:
            if warning:
                Warning(msg)
                return False
            else:
                raise Exception(msg)
        return True

    def add_bounds(self, cn, name, min=None, max=None):
        if min is None and max is None:
            return

        BOUNDS.pop(name, None)
        value = {
            'serie': name,
            'min': min,
            'max': max
        }
        insert_sql = insert(self.alias_schema.outliers).values(value)
        insert_sql = insert_sql.on_conflict_do_update(
            index_elements = ['serie'],
            set_= {'min': min, 'max': max}
        )
        cn.execute(insert_sql)
        print('insert {} in outliers table'.format(name))

    def build_priority(self, cn, alias, names, map_prune=None, map_coef=None):
        if not self.canuse_alias(cn, alias):
            return

        table = self.alias_schema.priority
        for priority, name in enumerate(names):
            values = {
                'alias': alias,
                'serie': name,
                'priority': priority
            }
            if map_prune and name in map_prune:
                values['prune'] = map_prune[name]
            if map_coef and name in map_coef:
                values['coefficient'] = map_coef[name]
            cn.execute(table.insert(values))

    def build_arithmetic(self, cn, alias, map_coef):
        if not self.canuse_alias(cn, alias):
            return

        for sn, coef in map_coef.items():
            value = {
                'alias': alias,
                'serie': sn,
                'coefficient': coef
            }
            cn.execute(self.alias_schema.arithmetic.insert().values(value))
