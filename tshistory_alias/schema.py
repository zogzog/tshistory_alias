from sqlalchemy import Table, Column, Integer, String, Float, Boolean, MetaData
from sqlalchemy.schema import CreateSchema

from tshistory.schema import tsschema, delete_schema, meta
from tshistory_supervision.schema import init as tshinit, reset as tshreset

SCHEMAS = {}


class alias_schema():
    namespace = 'tsh-alias'
    meta = None
    outliers = None
    priority = None
    arithmetic = None

    def __new__(cls, basenamespace='tsh'):
        ns = '{}-alias'.format(basenamespace)
        if ns in SCHEMAS:
            return SCHEMAS[ns]
        return super().__new__(cls)

    def __init__(self, basenamespace='tsh'):
        self.namespace = '{}-alias'.format(basenamespace)

    def define(self, meta=MetaData()):
        if self.namespace in SCHEMAS:
            return
        self.outliers = Table(
            'outliers', meta,
            Column('serie', String, primary_key=True, unique=True),
            Column('min', Float),
            Column('max', Float),
            schema=self.namespace,
            keep_existing=True
        )

        self.priority = Table(
            'priority', meta,
            Column('id', Integer, primary_key=True),
            Column('alias', String, nullable=False, index=True),
            Column('serie', String, nullable=False, index=True),
            Column('priority', Integer, nullable=False),
            Column('coefficient', Float, default=1),
            Column('prune', Integer, default=0),
            schema=self.namespace,
            keep_existing=True
        )

        self.arithmetic = Table(
            'arithmetic', meta,
            Column('id', Integer, primary_key=True),
            Column('alias', String, nullable=False, index=True),
            Column('serie', String, nullable=False, index=True),
            Column('coefficient', Float, default=1),
            Column('fillopt', String),
            schema=self.namespace,
            keep_existing=True
        )
        SCHEMAS[self.namespace] = self

    def exists(self, engine):
        return engine.execute('select exists(select schema_name '
                              'from information_schema.schemata '
                              'where schema_name = %(name)s)',
                              name=self.namespace
                              ).scalar()

    def create(self, engine):
        if self.exists(engine):
            return
        engine.execute(CreateSchema(self.namespace))
        self.outliers.create(engine)
        self.priority.create(engine)
        self.arithmetic.create(engine)


def init(engine, meta, basens='tsh'):
    tshinit(engine, meta, basens)
    aliasschema = alias_schema(basens)
    aliasschema.define(meta)
    aliasschema.create(engine)


def reset(engine, basens='tsh'):
    tshreset(engine, basens)
    delete_schema(engine, '{}-alias'.format(basens))
