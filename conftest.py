from pathlib import Path
import pytest

from sqlalchemy import create_engine, MetaData

from pytest_sa_pg import db

from tshistory.schema import (
    reset_schemas,
    init_schemas,
    tsschema
)
from tshistory_alias.schema import alias_schema
from tshistory_alias.tsio import TimeSerie

DATADIR = Path(__file__).parent / 'test' / 'data'


@pytest.fixture(scope='session')
def engine(request):
    port = 5433
    db.setup_local_pg_cluster(request, DATADIR, port)
    uri = 'postgresql://localhost:{}/postgres'.format(port)
    e = create_engine(uri)
    tsschema()
    alias_schema()
    reset_schemas(e)
    init_schemas(e, MetaData())
    return e


@pytest.fixture(scope='session')
def tsh(request, engine):
    tsh = TimeSerie()
    tsh._testing = True
    return tsh


def pytest_addoption(parser):
    parser.addoption('--refresh-refs', action='store_true', default=False,
                     help='refresh reference outputs')


@pytest.fixture
def refresh(request):
    return request.config.getoption('--refresh-refs')
