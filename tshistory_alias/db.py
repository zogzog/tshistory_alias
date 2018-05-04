from sqlalchemy import exists, select, MetaData
from sqlalchemy.dialects.postgresql import insert

from tshistory_alias import schema
from tshistory.schema import tsschema


def add_bounds(cn, sn, min=None, max=None):

    if min is None and max is None:
        return
    value = {
        'serie': sn,
        'min': min,
        'max': max
    }
    insert_sql = insert(schema.outliers).values(value)
    insert_sql = insert_sql.on_conflict_do_update(
        index_elements = ['serie'],
        set_= {'min': min, 'max': max}
    )
    cn.execute(insert_sql)
    print('insert {} in outliers table'.format(sn))


def avaibility_alias(cn, alias):

    original_schema = tsschema('tsh')
    table = original_schema.registry

    presence = exists().where(table.c.name == alias)
    if cn.execute(select([presence])).scalar():
        raise Exception('{} already used as a primary name'.format(alias))

    table = schema.priority
    presence = exists().where(table.c.alias == alias)
    if cn.execute(select([presence])).scalar():
        raise Exception('{} already used as a priority alias'.format(alias))

    table = schema.arithmetic
    presence = exists().where(table.c.alias == alias)
    if cn.execute(select([presence])).scalar():
        raise Exception('{} already used as an arithmetic alias'.format(alias))



def build_priority(cn, alias, list_names,
                   map_prune=None,
                   map_read_only=None,
                   map_coef=None):

    avaibility_alias(cn, alias)
    table = schema.priority
    for priority, name in enumerate(list_names):
        values = {'alias': alias,
                  'serie': name,
                  'priority': priority,
                  }
        if map_prune and name in map_prune:
            values['prune'] = map_prune[name]
        if map_read_only and name in map_read_only:
            values['read_only'] = map_read_only[name]
        if map_coef and name in map_coef:
            values['coefficient'] = map_coef[name]

        insert_sql = table.insert(values)
        cn.execute(insert_sql)

def build_arithmetic(cn, alias, map_coef):

    for sn, coef in map_coef.items():
        value = {'alias': alias,
                 'serie': sn,
                 'coefficient': coef
        }
        cn.execute(insert(schema.arithmetic).values(value))

