from sqlalchemy import exists, select
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import numpy as np

from tshistory_alias import schema, tsio
from tshistory.schema import tsschema


def add_bounds(cn, name, min=None, max=None):
    if min is None and max is None:
        return

    tsio.BOUNDS.pop(name, None)
    value = {
        'serie': name,
        'min': min,
        'max': max
    }
    insert_sql = insert(schema.outliers).values(value)
    insert_sql = insert_sql.on_conflict_do_update(
        index_elements = ['serie'],
        set_= {'min': min, 'max': max}
    )
    cn.execute(insert_sql)
    print('insert {} in outliers table'.format(name))


def avaibility_alias(cn, alias, warning=False):
    original_schema = tsschema('tsh')
    table = original_schema.registry

    presence = exists().where(table.c.name == alias)
    msg = None
    if cn.execute(select([presence])).scalar():
        msg ='{} already used as a primary name'.format(alias)

    table = schema.priority
    presence = exists().where(table.c.alias == alias)
    if cn.execute(select([presence])).scalar():
        msg = '{} already used as a priority alias'.format(alias)

    table = schema.arithmetic
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


def build_priority(cn, alias, names, map_prune=None, map_coef=None):
    avaibility_alias(cn, alias)

    table = schema.priority
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


def register_priority(cn, path):
    df = pd.read_csv(path)
    aliases = np.unique(df['alias'])
    map_prune = {}
    map_coef = {}
    for alias in aliases:
        if not avaibility_alias(cn, alias, warning=True):
            continue
        sub_df = df[df['alias'] == alias]
        sub_df = sub_df.sort_values(by='priority')
        list_names = sub_df['serie']
        for row in sub_df.itertuples():
            if not pd.isnull(row.prune):
                map_prune[row.serie] = row.prune
            if not pd.isnull(row.coefficient):
                map_coef[row.serie] = row.coefficient
        build_priority(cn, alias, list_names, map_prune, map_coef)


def build_arithmetic(cn, alias, map_coef):
    avaibility_alias(cn, alias)
    for sn, coef in map_coef.items():
        value = {
            'alias': alias,
            'serie': sn,
            'coefficient': coef
        }
        cn.execute(schema.arithmetic.insert().values(value))


def register_arithmetic(cn, path):
    df = pd.read_csv(path)
    aliases = np.unique(df['alias'])
    for alias in aliases:
        if not avaibility_alias(cn, alias, warning=True):
            continue
        sub_df = df[df['alias'] == alias]
        map_coef = {
            row.serie: row.coefficient
            for row in sub_df.itertuples()
        }
        build_arithmetic(cn, alias, map_coef)
