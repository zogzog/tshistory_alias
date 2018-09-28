from collections import defaultdict
import click

from sqlalchemy import create_engine

from tshistory_alias import db, tsio, helpers


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
            cn.execute(f'delete from "{namespace}-alias"."{table}"')


@click.command(name='audit-aliases')
@click.argument('dburi')
@click.option('--alias', help='specific alias name (all by default)')
@click.option('--namespace', default='tsh')
def audit_aliases(dburi, alias=None, namespace='tsh'):
    " perform a visual audit of aliases "
    engine = create_engine(dburi)

    aliases = []
    if alias:
        # verify
        for kind in ('priority', 'arithmetic'):
            sql = (f'select exists(select id from "{namespace}-alias".{kind} '
                   '               where alias = %(alias)s)')
            if engine.execute(sql, alias=alias).scalar():
                aliases.append(alias)
        assert len(aliases) == 1
    else:
        for kind in ('priority', 'arithmetic'):
            aliases = [alias for alias, in engine.execute(
                f'select distinct alias from "{namespace}-alias".{kind}').fetchall()
            ]
    tsh = tsio.TimeSerie(namespace=namespace)

    trees = []

    for idx, alias in enumerate(aliases):
        trees.append(helpers.buildtree(engine, tsh, alias, []))

    # now, display shit
    for tree in trees:
        print('-' * 70)
        helpers.showtree(tree)


@click.command(name='verify-aliases')
@click.argument('dburi')
@click.option('--only', type=click.Choice(TABLES))
@click.option('--namespace', default='tsh')
def verify_aliases(dburi, only=None, namespace='tsh'):
    " verify aliases wholesale (all or per type using --only) "
    if only is None:
        tables = TABLES
    else:
        assert only in TABLES
        tables = [only]

    engine = create_engine(dburi)
    tsh = tsio.TimeSerie(namespace=namespace)
    for table in tables:
        colname = 'serie' if table == 'outliers' else 'alias'
        for row in engine.execute(
                f'select {colname} from "{namespace}-alias"."{table}"'
        ).fetchall():
            name = row[0]
            try:
                series = tsh.get(engine, name)
            except tsio.AliasError as err:
                print(err)
            else:
                if not series.index.is_monotonic_increasing:
                    print(name, 'is non monotonic')

            print(name, len(series))


@click.command(name='migrate-alias-0.1-to-0.2')
@click.argument('dburi')
@click.option('--namespace', default='tsh')
def migrate_dot_one_to_dot_two(dburi, namespace='tsh'):
    engine = create_engine(dburi)

    sql = f'alter table "{namespace}-alias".priority alter column coefficient set default 1'
    with engine.begin() as cn:
        cn.execute(sql)

    sql = f'update "{namespace}-alias".priority set coefficient = 1 where coefficient is NULL'
    with engine.begin() as cn:
        cn.execute(sql)

