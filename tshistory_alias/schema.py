from sqlalchemy import Table, Column, Integer, String, Float, Boolean, MetaData
from sqlalchemy.schema import CreateSchema

from tshistory.schema import (
    _delete_schema,
    register_schema
)


class alias_schema:
    namespace = 'tsh-alias'
    meta = None
    outliers = None
    priority = None
    arithmetic = None
    SCHEMAS = {}

    def __new__(cls, basenamespace='tsh'):
        ns = '{}-alias'.format(basenamespace)
        if ns in cls.SCHEMAS:
            return cls.SCHEMAS[ns]
        return super().__new__(cls)

    def __init__(self, basenamespace='tsh'):
        self.namespace = '{}-alias'.format(basenamespace)
        register_schema(self)

    def define(self, meta=MetaData()):
        if self.namespace in self.SCHEMAS:
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
        self.SCHEMAS[self.namespace] = self

    def exists(self, engine):
        return engine.execute(
            'select exists('
            '  select schema_name '
            '  from information_schema.schemata '
            '  where schema_name = %(name)s'
            ')',
            name=self.namespace
        ).scalar()

    def create(self, engine):
        if self.exists(engine):
            return
        engine.execute(CreateSchema(self.namespace))
        self.outliers.create(engine)
        self.priority.create(engine)
        self.arithmetic.create(engine)

    def destroy(self, engine):
        if not self.exists(engine):
            return
        _delete_schema(engine, self.namespace)
        self.SCHEMAS.pop(self.namespace, None)

