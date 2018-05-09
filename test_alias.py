from datetime import datetime

import pytest

from tshistory.testutil import genserie
from tshistory_alias.db import add_bounds, build_priority, build_arithmetic


def assert_df(expected, df):
    assert expected.strip() == df.to_string().strip()


def test_outliers(engine, tsh):

    add_bounds(engine, 'serie1', min=5, max=10)
    add_bounds(engine, 'serie2', max=10)
    add_bounds(engine, 'serie3', min=5)
    add_bounds(engine, 'serie4', min=50)



    ts = genserie(datetime(2015,1,1), 'D', 15)
    tsh.insert(engine, ts, 'serie1', 'test')
    tsh.insert(engine, ts, 'serie2', 'test')
    tsh.insert(engine, ts, 'serie3', 'test')
    tsh.insert(engine, ts, 'serie4', 'test')

    ts1 = tsh.get(engine, 'serie1')
    ts2 = tsh.get(engine, 'serie2',
                  from_value_date=datetime(2015,1,2),
                  to_value_date=datetime(2015, 1, 13),
                 )
    ts3 = tsh.get(engine, 'serie3')
    ts4 = tsh.get(engine, 'serie4')

    assert """
2015-01-06     5.0
2015-01-07     6.0
2015-01-08     7.0
2015-01-09     8.0
2015-01-10     9.0
2015-01-11    10.0""".strip() == ts1.to_string().strip()

    assert 10 == max(ts2)
    assert 1 == min(ts2)

    assert 14 == max(ts3)
    assert 5 == min(ts3)

    assert 0 == len(ts4)

    #upsert:
    add_bounds(engine, 'serie4', min=-50)
    ts4 = tsh.get(engine, 'serie4')

    assert 15 == len(ts4)


def test_combine(engine, tsh):
    ts_real = genserie(datetime(2010, 1, 1), 'D', 10, [1])
    ts_nomination = genserie(datetime(2010, 1, 1), 'D', 12, [2])
    ts_forecast = genserie(datetime(2010, 1, 1), 'D', 20, [3])

    tsh.insert(engine, ts_real, 'realised', 'test')
    tsh.insert(engine, ts_nomination, 'nominated', 'test')
    tsh.insert(engine, ts_forecast, 'forecasted', 'test')

    build_priority(engine, 'serie5',
                   ['realised', 'nominated', 'forecasted'])

    values, origin = tsh.get_priority(engine,'serie5')

    assert_df("""
2010-01-01    1.0
2010-01-02    1.0
2010-01-03    1.0
2010-01-04    1.0
2010-01-05    1.0
2010-01-06    1.0
2010-01-07    1.0
2010-01-08    1.0
2010-01-09    1.0
2010-01-10    1.0
2010-01-11    2.0
2010-01-12    2.0
2010-01-13    3.0
2010-01-14    3.0
2010-01-15    3.0
2010-01-16    3.0
2010-01-17    3.0
2010-01-18    3.0
2010-01-19    3.0
2010-01-20    3.0""", values)

    assert_df("""
2010-01-01      realised
2010-01-02      realised
2010-01-03      realised
2010-01-04      realised
2010-01-05      realised
2010-01-06      realised
2010-01-07      realised
2010-01-08      realised
2010-01-09      realised
2010-01-10      realised
2010-01-11     nominated
2010-01-12     nominated
2010-01-13    forecasted
2010-01-14    forecasted
2010-01-15    forecasted
2010-01-16    forecasted
2010-01-17    forecasted
2010-01-18    forecasted
2010-01-19    forecasted
2010-01-20    forecasted
    """, origin)

    # we remove the last value of the 2 first series which are considered as bogus

    build_priority(engine, 'serie6',
                   ['realised', 'nominated', 'forecasted'],
                   map_prune={'realised': 1, 'nominated': 1, 'forecasted': 0})

    values, origin = tsh.get_priority(engine,'serie6')

    assert_df("""
2010-01-01    1.0
2010-01-02    1.0
2010-01-03    1.0
2010-01-04    1.0
2010-01-05    1.0
2010-01-06    1.0
2010-01-07    1.0
2010-01-08    1.0
2010-01-09    1.0
2010-01-10    2.0
2010-01-11    2.0
2010-01-12    3.0
2010-01-13    3.0
2010-01-14    3.0
2010-01-15    3.0
2010-01-16    3.0
2010-01-17    3.0
2010-01-18    3.0
2010-01-19    3.0
2010-01-20    3.0
    """, values)

    assert_df("""
2010-01-01      realised
2010-01-02      realised
2010-01-03      realised
2010-01-04      realised
2010-01-05      realised
2010-01-06      realised
2010-01-07      realised
2010-01-08      realised
2010-01-09      realised
2010-01-10     nominated
2010-01-11     nominated
2010-01-12    forecasted
2010-01-13    forecasted
2010-01-14    forecasted
2010-01-15    forecasted
2010-01-16    forecasted
2010-01-17    forecasted
2010-01-18    forecasted
2010-01-19    forecasted
2010-01-20    forecasted
    """, origin)


    build_priority(engine, 'serie7',
                   ['realised', 'nominated', 'forecasted'],
                   map_prune={'realised': 1, 'nominated': 3, 'forecasted': 0})

    values, origin = tsh.get_priority(engine,'serie7')


    assert_df("""
2010-01-01      realised
2010-01-02      realised
2010-01-03      realised
2010-01-04      realised
2010-01-05      realised
2010-01-06      realised
2010-01-07      realised
2010-01-08      realised
2010-01-09      realised
2010-01-10    forecasted
2010-01-11    forecasted
2010-01-12    forecasted
2010-01-13    forecasted
2010-01-14    forecasted
2010-01-15    forecasted
2010-01-16    forecasted
2010-01-17    forecasted
2010-01-18    forecasted
2010-01-19    forecasted
2010-01-20    forecasted
    """, origin)
    # nice


def test_arithmetic(engine, tsh):

    ts_toto = genserie(datetime(2010, 1, 1), 'D', 7, [1])
    ts_tata = genserie(datetime(2010, 1, 3), 'D', 7, [2])

    tsh.insert(engine, ts_toto, 'toto', 'test')
    tsh.insert(engine, ts_tata, 'tata', 'test')

    build_arithmetic(engine, 'sum', {'toto':1,
                                     'tata':1})

    build_arithmetic(engine, 'difference', {'toto':1,
                                            'tata':-1})

    build_arithmetic(engine, 'mean', {'toto':0.5,
                                      'tata':0.5})

    build_arithmetic(engine, 'bogus', {'toto':0.5,
                                      'unknown':0.5})

    values = tsh.get_arithmetic(engine, 'sum')

    assert_df("""
2010-01-03    3.0
2010-01-04    3.0
2010-01-05    3.0
2010-01-06    3.0
2010-01-07    3.0""", values)

    #NB: there are only data at the intersection of the index

    values = tsh.get_arithmetic(engine, 'difference')

    assert_df("""
2010-01-03   -1.0
2010-01-04   -1.0
2010-01-05   -1.0
2010-01-06   -1.0
2010-01-07   -1.0""", values)

    tsh.get_arithmetic(engine, 'mean')

    assert_df("""
2010-01-03   -1.0
2010-01-04   -1.0
2010-01-05   -1.0
2010-01-06   -1.0
2010-01-07   -1.0""", values)

    with pytest.raises(Exception) as err:
        tsh.get_arithmetic(engine, 'bogus')

    assert 'unknown is needed to calculate bogus and does not exist' ==  str(err.value)

    # tester prune sur une série unique et/ou sur la série la moins prioritaire

def test_dispatch_get(engine, tsh):

    ts_real = genserie(datetime(2010, 1, 1), 'D', 5, [1])
    ts_nomination = genserie(datetime(2010, 1, 1), 'D', 6, [2])
    ts_forecast = genserie(datetime(2010, 1, 1), 'D', 7, [3])

    tsh.insert(engine, ts_real, 'realised0', 'test')
    tsh.insert(engine, ts_nomination, 'nominated0', 'test')
    tsh.insert(engine, ts_forecast, 'forecasted0', 'test')

    build_priority(engine, 'composite_serie',
                   ['realised0', 'nominated0', 'forecasted0'])

    build_arithmetic(engine, 'sum_serie', {'realised0':1,
                                           'forecasted0':1})


    assert 'primary' == tsh._typeofserie(engine, 'realised0')
    assert 'priority' == tsh._typeofserie(engine, 'composite_serie')
    assert 'arithmetic' == tsh._typeofserie(engine, 'sum_serie')
    assert tsh._typeofserie(engine, 'serie_not_defined') is None

    assert_df("""
2010-01-01    2.0
2010-01-02    2.0
2010-01-03    2.0
2010-01-04    2.0
2010-01-05    2.0
2010-01-06    2.0
    """, tsh.get(engine, 'nominated0'))

    assert_df("""
2010-01-01    1.0
2010-01-02    1.0
2010-01-03    1.0
2010-01-04    1.0
2010-01-05    1.0
2010-01-06    2.0
2010-01-07    3.0""",  tsh.get(engine, 'composite_serie'))

    assert_df("""
2010-01-01    4.0
2010-01-02    4.0
2010-01-03    4.0
2010-01-04    4.0
2010-01-05    4.0""", tsh.get(engine,'sum_serie'))


def test_micmac(engine, tsh):

    tsh.insert(engine, genserie(datetime(2010, 1, 1), 'D', 5, [1]), 'micmac1', 'test')
    tsh.insert(engine, genserie(datetime(2010, 1, 2), 'D', 1, [1000]), 'micmac1', 'test') # bogus data"

    tsh.insert(engine, genserie(datetime(2010, 1, 1), 'D', 6, [2]), 'micmac2', 'test')

    tsh.insert(engine, genserie(datetime(2010, 1, 1), 'D', 6, [3]), 'micmac3', 'test')
    tsh.insert(engine, genserie(datetime(2010, 1, 1), 'D', 7, [15]), 'micmac4', 'test')


    build_priority(engine, 'prio1',
                   ['micmac1', 'micmac2'],
                   map_prune={'micmac1': 2, 'micmac2': 1})

    build_arithmetic(engine, 'arithmetic1', {'prio1':1,
                                            'micmac3':1})

    build_priority(engine, 'final',
                   ['arithmetic1', 'micmac4'],
                   map_prune={'arithmetic1': 1, 'micmac4': 0})

    add_bounds(engine, 'micmac1', min=0, max=100)

    tsh.get(engine, 'micmac1')
    tsh.get(engine, 'micmac2')
    tsh.get(engine, 'prio1')
    tsh.get(engine, 'micmac3')
    tsh.get(engine, 'arithmetic1')
    tsh.get(engine, 'micmac4')

    assert_df("""
2010-01-01     4.0
2010-01-02     5.0
2010-01-03     4.0
2010-01-04     5.0
2010-01-05    15.0
2010-01-06    15.0
2010-01-07    15.0""", tsh.get(engine, 'final'))


def test_errors(engine, tsh):

    tsh.insert(engine, genserie(datetime(2010, 1, 1), 'D', 5, [1]), 'primary_series', 'test')

    build_arithmetic(engine, 'arithmetic2', {'toto':1,
                                            'tata':1})

    build_priority(engine, 'priority2', ['toto', 'tata'])

    with pytest.raises(Exception) as err:
        build_priority(engine, 'arithmetic2', ['toto', 'tata'])
    assert 'arithmetic2 already used as an arithmetic alias' ==  str(err.value)

    with pytest.raises(Exception) as err:
        build_arithmetic(engine, 'priority2',{'toto':1,'tata':1})
    assert 'priority2 already used as a priority alias' ==  str(err.value)

    with pytest.raises(Exception) as err:
        build_priority(engine, 'primary_series', ['toto', 'tata'])
    assert 'primary_series already used as a primary name' ==  str(err.value)

    with pytest.raises(Exception) as err:
        build_arithmetic(engine, 'primary_series',{'toto':1,
                                                  'tata':1})
    assert 'primary_series already used as a primary name' ==  str(err.value)

    with pytest.raises(Exception) as err:
        tsh.insert(engine, genserie(datetime(2010, 1, 1), 'D', 5, [1]), 'arithmetic2', 'test')
    assert 'Serie arithmetic2 is trying to be inserted, but is of type arithmetic' ==  str(err.value)


    with pytest.raises(Exception) as err:
        tsh.insert(engine, genserie(datetime(2010, 1, 1), 'D', 5, [1]), 'priority2', 'test')
    assert 'Serie priority2 is trying to be inserted, but is of type priority' ==  str(err.value)


def test_historical(engine, tsh):

    assert False