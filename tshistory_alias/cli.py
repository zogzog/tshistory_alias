import click

from sqlalchemy import create_engine, MetaData

from tshistory_alias import schema, db


@click.command(name='register-priorities')
@click.argument('dburi')
@click.argument('priority-file')
def register_priorities(dburi, priority_file):
    " register priorities timeseries aliases "
    engine = create_engine(dburi)
    meta = MetaData()
    schema.define_schema(meta)
    with engine.connect() as cn:
        db.register_priority(cn, priority_file)


@click.command(name='register-arithmetic')
@click.argument('dburi')
@click.argument('arithmetic-file')
def register_arithmetic(dburi, arithmetic_file):
    " register arithmetic timeseries aliases "
    engine = create_engine(dburi)
    meta = MetaData()
    schema.define_schema(meta)
    with engine.connect() as cn:
        db.register_arithmetic(cn, arithmetic_file)
