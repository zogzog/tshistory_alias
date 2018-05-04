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



def build_priority(cn, alias, list_names):

    avaibility_alias(cn, alias)
    table = schema.priority
    for priority, name in enumerate(list_names):
        insert_sql = table.insert(values={'alias':alias,
                                          'serie': name,
                                          'priority': priority})
        cn.execute(insert_sql)


