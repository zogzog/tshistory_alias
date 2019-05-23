from pathlib import Path
import pytest

from sqlalchemy import create_engine

from pytest_sa_pg import db

from tshistory_alias.schema import alias_schema
from tshistory_alias.tsio import timeseries


DATADIR = Path(__file__).parent / 'test' / 'data'


@pytest.fixture(scope='session')
def engine(request):
    port = 5433
    db.setup_local_pg_cluster(request, DATADIR, port, {
        'timezone': 'UTC',
        'log_timezone': 'UTC'
    })
    uri = 'postgresql://localhost:{}/postgres'.format(port)
    e = create_engine(uri)
    sch = alias_schema()
    sch.create(e)
    return e


@pytest.fixture(scope='session')
def tsh(request, engine):
    return timeseries()


def pytest_addoption(parser):
    parser.addoption('--refresh-refs', action='store_true', default=False,
                     help='refresh reference outputs')


@pytest.fixture
def refresh(request):
    return request.config.getoption('--refresh-refs')
