from pathlib import Path
from collections import defaultdict

import click
from sqlalchemy import create_engine
import pandas as pd

from tshistory.util import find_dburi
from tshistory_alias import db, tsio, helpers


@click.command(name='register-priorities')
@click.argument('dburi')
@click.argument('priority-file')
@click.option('--override', is_flag=True, default=False)
def register_priorities(dburi, priority_file, override=False):
    " register priorities timeseries aliases "
    engine = create_engine(find_dburi(dburi))
    with engine.begin() as cn:
        db.register_priority(cn, priority_file, override)


@click.command(name='register-arithmetic')
@click.argument('dburi')
@click.argument('arithmetic-file')
@click.option('--override', is_flag=True, default=False)
def register_arithmetic(dburi, arithmetic_file, override):
    " register arithmetic timeseries aliases "
    engine = create_engine(find_dburi(dburi))
    with engine.begin() as cn:
        db.register_arithmetic(cn, arithmetic_file, override)


@click.command(name='register-outliers')
@click.argument('dburi')
@click.argument('outliers-file')
@click.option('--override', is_flag=True, default=False)
def register_outliers(dburi, outliers_file, override):
    " register outlier definitions "
    engine = create_engine(find_dburi(dburi))
    with engine.begin() as cn:
        db.register_outliers(cn, outliers_file, override)


@click.command(name='remove-alias')
@click.argument('dburi')
@click.argument('alias_type')
@click.argument('alias')
@click.option('--namespace', default='tsh')
def remove_alias(dburi, alias_type, alias, namespace='tsh'):
    "remove singe alias"
    engine = create_engine(find_dburi(dburi))
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

    engine = create_engine(find_dburi(dburi))
    for table in tables:
        with engine.begin() as cn:
            cn.execute(f'delete from "{namespace}-alias"."{table}"')


def _alias_kind(alias):
    for kind in ('priority', 'arithmetic'):
        sql = (f'select exists(select id from "{namespace}-alias".{kind} '
               '               where alias = %(alias)s)')
        if engine.execute(sql, alias=alias).scalar():
            return kind
    return None


@click.command(name='audit-aliases')
@click.argument('dburi')
@click.option('--alias', help='specific alias name (all by default)')
@click.option('--namespace', default='tsh')
def audit_aliases(dburi, alias=None, namespace='tsh'):
    " perform a visual audit of aliases "
    engine = create_engine(find_dburi(dburi))

    aliases = []
    if alias:
        # verify
        if _alias_kind(alias) is not None:
            aliases.append(alias)
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


@click.command('export-aliases')
@click.argument('dburi')
@click.argument('aliases', nargs=-1)
@click.option('--namespace', default='tsh')
def export_aliases(dburi, aliases, namespace='tsh'):
    engine = create_engine(find_dburi(dburi))
    tsh = tsio.TimeSerie(namespace=namespace)

    trees = []
    for alias in aliases:
        trees.append(helpers.buildtree(engine, tsh, alias, []))

    data = {
        'primary': set(),
        'arithmetic': set(),
        'priority': set()
    }

    def collect(tree):
        if isinstance(tree, str):
            if tree.startswith('unknown'):
                print(f'skipping {tree}')
            else:
                data['primary'].add(tree)
            return

        for (alias, kind), subtrees in tree.items():
            data[kind].add(alias)
            for subtree in subtrees:
                collect(subtree)

    for tree in trees:
        collect(tree)

    Path('primary.csv').write_bytes(
        '\n'.join(data['primary']).encode('utf-8')
    )

    arith = []
    for name in data['arithmetic']:
        out = engine.execute(
            f'select alias, serie, coefficient, fillopt '
            f'from "{namespace}-alias".arithmetic '
            f'where alias = %(name)s '
            f'order by alias',
            name=name
        ).fetchall()
        arith.extend(dict(row) for row in out)

    df = pd.DataFrame(arith)
    df.to_csv(
        Path('arith.csv'),
        columns=('alias', 'serie', 'coefficient', 'fillopt'),
        index=False
    )

    prio = []
    for name in data['priority']:
        out = engine.execute(
            f'select alias, serie, priority, coefficient, prune '
            f'from "{namespace}-alias".priority '
            f'where alias = %(name)s '
            f'order by alias, priority asc',
            name=name
        ).fetchall()
        prio.extend(dict(row) for row in out)

    df = pd.DataFrame(prio)
    df.to_csv(
        Path('prio.csv'),
        columns=('alias', 'serie', 'priority', 'coefficient', 'prune'),
        index=False
    )


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

    engine = create_engine(find_dburi(dburi))
    tsh = tsio.TimeSerie(namespace=namespace)
    for table in tables:
        colname = 'serie' if table == 'outliers' else 'alias'
        for row in engine.execute(
                f'select distinct {colname} from "{namespace}-alias"."{table}"'
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
    engine = create_engine(find_dburi(dburi))

    sql = f'alter table "{namespace}-alias".priority alter column coefficient set default 1'
    with engine.begin() as cn:
        cn.execute(sql)

    sql = f'update "{namespace}-alias".priority set coefficient = 1 where coefficient is NULL'
    with engine.begin() as cn:
        cn.execute(sql)

