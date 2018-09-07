import click

from sqlalchemy import create_engine

from tshistory_alias import db


@click.command(name='register-priorities')
@click.argument('dburi')
@click.argument('priority-file')
@click.option('--override', is_flag=True, default=False)
def register_priorities(dburi, priority_file, override=False):
    " register priorities timeseries aliases "
    engine = create_engine(dburi)
    with engine.begin() as cn:
        db.register_priority(cn, priority_file, override)


@click.command(name='register-arithmetic')
@click.argument('dburi')
@click.argument('arithmetic-file')
@click.option('--override', is_flag=True, default=False)
def register_arithmetic(dburi, arithmetic_file, override):
    " register arithmetic timeseries aliases "
    engine = create_engine(dburi)
    with engine.begin() as cn:
        db.register_arithmetic(cn, arithmetic_file, override)


@click.command(name='register-outliers')
@click.argument('dburi')
@click.argument('outliers-file')
@click.option('--override', is_flag=True, default=False)
def register_outliers(dburi, outliers_file, override):
    " register outlier definitions "
    engine = create_engine(dburi)
    with engine.begin() as cn:
        db.register_outliers(cn, outliers_file, override)


@click.command(name='remove-alias')
@click.argument('dburi')
@click.argument('alias_type')
@click.argument('alias')
@click.option('--namespace', default='tsh')
def remove_alias(dburi, alias_type, alias, namespace='tsh'):
    "remove singe alias"
    engine = create_engine(dburi)
    table = '"{}-alias".{}'.format(namespace, alias_type)
    sql = "delete from {} where alias = %(alias)s".format(table)
    with engine.begin() as cn:
        cn.execute(sql, alias=alias)


TABLES = ('outliers', 'priority', 'arithmetic')
@click.command(name='reset-aliases')
@click.argument('dburi')
@click.option('--only', type=click.Choice(TABLES))
@click.option('--namespace', default='tsh')
def reset_aliases(dburi, only=None, namespace='tsh'):
    " remove aliases wholesale (all or per type using --only) "
    if only is None:
        tables = TABLES
    else:
        assert only in TABLES
        tables = [only]

    engine = create_engine(dburi)
    for table in tables:
        with engine.begin() as cn:
            cn.execute('delete from "{}-alias"."{}"'.format(namespace, table))

