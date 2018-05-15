from sqlalchemy import Table, Column, Integer, String, DateTime, UniqueConstraint, Date, Float, Boolean
from tshistory.schema import delete_schema, init as tshinit, reset as tshreset
from sqlalchemy.schema import CreateSchema

outliers = None
priority = None
arithmetic = None


def define_schema(meta):
    _outliers = Table(
        'outliers', meta,
        Column('serie', String, primary_key=True, unique=True),
        Column('min', Float),
        Column('max', Float),
        schema='alias'
    )

    _priority = Table(
        'priority', meta,
        Column('id', Integer, primary_key=True),
        Column('alias', String, nullable=False, index=True),
        Column('serie', String, nullable=False, index=True),
        Column('priority', Integer, nullable=False),
        Column('coefficient', Float),
        Column('prune', Integer, default=0),
        schema='alias'
    )

    _arithmetic = Table(
        'arithmetic', meta,
        Column('id', Integer, primary_key=True),
        Column('alias', String, nullable=False, index=True),
        Column('serie', String, nullable=False, index=True),
        Column('coefficient', Float, default=1),
        schema='alias'
    )

    global outliers, priority, arithmetic
    outliers = _outliers
    priority = _priority
    arithmetic = _arithmetic


def init(engine, meta):
    tshinit(engine, meta)
    tshinit(engine, meta, 'tsh-automatic')
    tshinit(engine, meta, 'tsh-manual')
    define_schema(meta)
    engine.execute(CreateSchema('alias'))
    for table in (outliers, priority, arithmetic):
        table.create(engine)


def reset(engine):
    tshreset(engine)
    delete_schema(engine, 'tsh-automatic')
    delete_schema(engine, 'tsh-manual')
    engine.execute('drop schema if exists alias cascade')
