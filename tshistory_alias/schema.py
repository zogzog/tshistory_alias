from sqlalchemy import Table, Column, Integer, String, Float
from sqlalchemy.schema import CreateSchema

from tshistory.schema import delete_schema
from tshistory_supervision.schema import init as tshinit, reset as tshreset

outliers = None
priority = None
arithmetic = None


def define_schema(meta, namespace='tsh-alias'):
    _outliers = Table(
        'outliers', meta,
        Column('serie', String, primary_key=True, unique=True),
        Column('min', Float),
        Column('max', Float),
        schema=namespace
    )

    _priority = Table(
        'priority', meta,
        Column('id', Integer, primary_key=True),
        Column('alias', String, nullable=False, index=True),
        Column('serie', String, nullable=False, index=True),
        Column('priority', Integer, nullable=False),
        Column('coefficient', Float),
        Column('prune', Integer, default=0),
        schema=namespace
    )

    _arithmetic = Table(
        'arithmetic', meta,
        Column('id', Integer, primary_key=True),
        Column('alias', String, nullable=False, index=True),
        Column('serie', String, nullable=False, index=True),
        Column('coefficient', Float, default=1),
        schema=namespace
    )

    global outliers, priority, arithmetic
    outliers = _outliers
    priority = _priority
    arithmetic = _arithmetic


def init(engine, meta, basens='tsh'):
    tshinit(engine, meta, basens)
    ns = '{}-alias'.format(basens)
    define_schema(meta, ns)
    engine.execute(CreateSchema(ns))
    for table in (outliers, priority, arithmetic):
        table.create(engine)


def reset(engine, basens='tsh'):
    tshreset(engine, basens)
    delete_schema(engine, '{}-alias'.format(basens))
