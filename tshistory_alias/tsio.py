from sqlalchemy import exists, select
from sqlalchemy.dialects.postgresql import insert

import pandas as pd

from tshistory.tsio import timeseries as basets
from tshistory_alias.schema import alias_schema



class AliasError(Exception):
    pass


class timeseries(basets):
    alias_schema = None
    alias_types = ('priority', 'arithmetic')

    def type(self, cn, name):
        # cache-filling
        base = (f'select alias from "{self.namespace}".{{}} '
                'where alias = %(name)s '
                'limit 1')
        priority = base.format('priority')
        arith = base.format('arithmetic')
        if cn.execute(priority, name=name).scalar():
            kind = 'priority'
        elif cn.execute(arith, name=name).scalar():
            kind = 'arithmetic'
        else:
            kind = super().type(cn, name)
        return kind

    def exists(self, cn, name):
        if self.type(cn, name) in self.alias_types:
            return True

        return super().exists(cn, name)

    def insert(self, cn, newts, name, author, **kw):
        serie_type = self.type(cn, name)
        if serie_type in self.alias_types:
            raise AliasError('Serie {} is trying to be inserted, but is of type {}'.format(
                name, serie_type)
            )

        return super().insert(cn, newts, name, author=author, **kw)

    def get(self, cn, name, revision_date=None, delta=None,
            from_value_date=None, to_value_date=None, _keep_nans=False):

        serie_type = self.type(cn, name)
        ts = None
        if serie_type not in self.alias_types:
            if delta is None:
                ts = super().get(
                    cn, name, revision_date=revision_date,
                    from_value_date=from_value_date,
                    to_value_date=to_value_date,
                    _keep_nans=_keep_nans
                )
            else:
                ts = self.staircase(cn, name, delta=delta,
                                    from_value_date=from_value_date,
                                    to_value_date=to_value_date
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
            ts = self.apply_bounds(cn, ts, name)

        return ts

    def apply_bounds(self, cn, ts, name):
        sql = (f'select min, max from "{self.namespace}".outliers '
               'where serie = %(name)s')
        mini_maxi = cn.execute(
            sql,
            name=name
        ).fetchone()
        if not mini_maxi:
            return ts

        mini, maxi = mini_maxi
        if mini is not None and not pd.isnull(mini):
            ts = ts[ts >= mini]
        if maxi is not None and not pd.isnull(maxi):
            ts = ts[ts <= maxi]

        return ts

    def get_priority(self, cn, alias,
                     revision_date=None,
                     delta=None,
                     from_value_date=None,
                     to_value_date=None):

        res = cn.execute(
            f'select serie, prune, coefficient '
            f'from "{self.namespace}".priority as prio '
            f'where prio.alias = %(alias)s '
            f'order by priority desc',
            alias=alias
        )

        ts_values = pd.Series()
        ts_origins = pd.Series()

        for row in res.fetchall():
            name = row.serie
            prune = row.prune
            ts = self.get(
                cn, name,
                revision_date=revision_date,
                delta=delta,
                from_value_date=from_value_date,
                to_value_date=to_value_date
            )
            if ts is None:
                continue

            if ts.dtype != 'O' and row.coefficient != 1:
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
        res = cn.execute(
            f'select serie, fillopt, coefficient '
            f'from "{self.namespace}".arithmetic '
            f'where alias = %(alias)s',
            alias=alias
        )

        first_iteration = True
        df_result = None
        ts_with_fillopt = {}

        for row in res.fetchall():
            ts = self.get(
                cn, row.serie,
                revision_date=revision_date,
                delta=delta,
                from_value_date=from_value_date,
                to_value_date=to_value_date
            )
            if ts is None:
                raise AliasError(
                    f'{row.serie} is needed to calculate {alias} and does not exist'
                )

            if row.fillopt:
                ts_with_fillopt[ts.name] = row.fillopt

            if first_iteration:
                if row.coefficient != 1:
                    ts = ts * row.coefficient
                df_result = ts.to_frame()
                first_iteration = False
                continue

            if row.coefficient != 1:
                ts = ts * row.coefficient
            df_result = df_result.join(ts, how='outer')

        for ts, fillopt in ts_with_fillopt.items():
            if fillopt.startswith('fill='):
                filler = float(fillopt.split('=')[1])
                df_result[ts] = df_result[ts].fillna(filler)
                continue
            for method in fillopt.split(','):
                df_result[ts] = df_result[ts].fillna(method=method.strip())

        df_result = df_result.dropna()

        ts_result = df_result.sum(axis=1)
        ts_result.name = alias

        return ts_result

    def _apply_priority(self, ts_result, ts_new, remove, index=None):
        if index is not None:
            ts_new = ts_new[index]
        if remove:
            ts_new = ts_new[:-remove]
        combine = self.patch(ts_result, ts_new)
        return combine

    # alias definition/construction

    def add_bounds(self, cn, name, min=None, max=None):
        if min is None and max is None:
            return

        insert_sql = (f'insert into "{self.namespace}".outliers '
                      '(serie, min, max) '
                      'values (%(serie)s, %(min)s, %(max)s) '
                      'on conflict (serie) do update '
                      'set min = %(min)s, max = %(max)s')
        cn.execute(
            insert_sql,
            serie=name,
            min=min,
            max=max
        )
        print('insert {} in outliers table'.format(name))

    def _handle_conflict(self, cn, alias, override):
        kind = self.type(cn, alias)
        if kind in ('arithmetic', 'priority'):
            if override:
                print('overriding serie {} ({})'.format(alias, kind))
                cn.execute(f'delete from "tsh".{kind} as al where al.alias = %(alias)s',
                           {'alias': alias})
        elif self.exists(cn, alias):
            print('{} serie {} already exists'.format(kind, alias))
            return False
        return True

    def build_priority(self, cn, alias, names, map_prune=None, map_coef=None, override=False):
        if not self._handle_conflict(cn, alias, override):
            return

        for priority, name in enumerate(names):
            values = {
                'alias': alias,
                'serie': name,
                'priority': priority,
                'coef': 1,
                'prune': None
            }
            if map_prune and name in map_prune:
                values['prune'] = map_prune[name]
            if map_coef and name in map_coef:
                values['coef'] = map_coef[name]
            sql = (f'insert into "{self.namespace}".priority '
                   '(alias, serie, priority, coefficient, prune) '
                   'values (%(alias)s, %(serie)s, %(priority)s, %(coef)s, %(prune)s)')
            cn.execute(sql, **values)

    def build_arithmetic(self, cn, alias, map_coef, map_fillopt=None, override=False):
        if not self._handle_conflict(cn, alias, override):
            return

        for sn, coef in map_coef.items():
            values = {
                'alias': alias,
                'serie': sn,
                'coef': coef,
                'fillopt': None
            }
            if map_fillopt and sn in map_fillopt:
                values['fillopt'] = map_fillopt[sn]

            sql = (f'insert into "{self.namespace}".arithmetic '
                   '(alias, serie, coefficient, fillopt) '
                   'values (%(alias)s, %(serie)s, %(coef)s, %(fillopt)s)')
            cn.execute(sql, **values)
