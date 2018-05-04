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

    ts1 = tsh.get_bounds(engine, 'serie1', remove_outliers=True)
    ts2 = tsh.get_bounds(engine, 'serie2', remove_outliers=True,
                         from_value_date=datetime(2015,1,2),
                         to_value_date=datetime(2015, 1, 13),
                         )
    ts3 = tsh.get_bounds(engine, 'serie3', remove_outliers=True)
    ts4 = tsh.get_bounds(engine, 'serie4', remove_outliers=True)

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
    ts4 = tsh.get_bounds(engine, 'serie4', remove_outliers=True)

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

    # Now, after our import, we manualy change some values that we want to insert
    # in the proper place, with the same prune parameter
    ts_pushed = values.copy()
    ts_pushed['2010-01-05'] = 1.1
    ts_pushed['2010-01-16'] = 3.1
    ts_pushed = ts_pushed[2:19]  # some size reduction, for the sake of entropy

    tsh.insert_priority(engine,
                         ts_pushed,
                         'serie6',
                         'author_test',
                         )

    # here are the result values
    values, origin = tsh.get_priority(engine,'serie6')


    assert_df("""
2010-01-01    1.0
2010-01-02    1.0
2010-01-03    1.0
2010-01-04    1.0
2010-01-05    1.1
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
2010-01-16    3.1
2010-01-17    3.0
2010-01-18    3.0
2010-01-19    3.0
2010-01-20    3.0
    """, values)

    # assert_df("""
    # 2010-01-01    False
    # 2010-01-02    False
    # 2010-01-03    False
    # 2010-01-04    False
    # 2010-01-05     True
    # 2010-01-06    False
    # 2010-01-07    False
    # 2010-01-08    False
    # 2010-01-09    False
    # 2010-01-10    False
    # 2010-01-11    False
    # 2010-01-12    False
    # 2010-01-13    False
    # 2010-01-14    False
    # 2010-01-15    False
    # 2010-01-16     True
    # 2010-01-17    False
    # 2010-01-18    False
    # 2010-01-19    False
    # 2010-01-20    False
    # """, marker)

    # we try a bogus prune of data, i.e. remove more than there are data...

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

    # read only test:

    build_priority(engine, 'serie8',
                   ['realised', 'nominated', 'forecasted'],
                   map_prune={'realised': 1, 'nominated': 1, 'forecasted': 0},
                   map_read_only = {'realised': True, 'nominated': False, 'forecasted': True})


    ts_modif =  genserie(datetime(2010, 1, 1), 'D', 20, [-10])
    tsh.insert_priority(engine, ts_modif, 'serie8', 'test')
    values, origin =  tsh.get_priority(engine,'serie8')

    assert_df("""
2010-01-01     1.0
2010-01-02     1.0
2010-01-03     1.0
2010-01-04     1.0
2010-01-05     1.1
2010-01-06     1.0
2010-01-07     1.0
2010-01-08     1.0
2010-01-09     1.0
2010-01-10   -10.0
2010-01-11   -10.0
2010-01-12     3.0
2010-01-13     3.0
2010-01-14     3.0
2010-01-15     3.0
2010-01-16     3.1
2010-01-17     3.0
2010-01-18     3.0
2010-01-19     3.0
2010-01-20     3.0
        """, values)

    # coef:
    build_priority(engine, 'serie9',
                   ['realised', 'nominated', 'forecasted'],
                   map_prune={'realised': 1, 'nominated': 1, 'forecasted': 0},
                   map_read_only = {'realised': True},
                   map_coef = {'nominated': 2, 'forecasted': 3})

    values, origin =  tsh.get_priority(engine,'serie9')
    assert_df("""
2010-01-01     1.0
2010-01-02     1.0
2010-01-03     1.0
2010-01-04     1.0
2010-01-05     1.1
2010-01-06     1.0
2010-01-07     1.0
2010-01-08     1.0
2010-01-09     1.0
2010-01-10   -20.0
2010-01-11   -20.0
2010-01-12     9.0
2010-01-13     9.0
2010-01-14     9.0
2010-01-15     9.0
2010-01-16     9.3
2010-01-17     9.0
2010-01-18     9.0
2010-01-19     9.0
2010-01-20     9.0
            """, values)

    ts_modif =  genserie(datetime(2010, 1, 1), 'D', 20, [-1])
    tsh.insert_priority(engine, ts_modif, 'serie9', 'test')

    values, origin =  tsh.get_priority(engine,'serie9')

    assert_df("""
2010-01-01    1.0
2010-01-02    1.0
2010-01-03    1.0
2010-01-04    1.0
2010-01-05    1.1
2010-01-06    1.0
2010-01-07    1.0
2010-01-08    1.0
2010-01-09    1.0
2010-01-10   -1.0
2010-01-11   -1.0
2010-01-12   -1.0
2010-01-13   -1.0
2010-01-14   -1.0
2010-01-15   -1.0
2010-01-16   -1.0
2010-01-17   -1.0
2010-01-18   -1.0
2010-01-19   -1.0
2010-01-20   -1.0
            """, values)


    # more test to be found and may be adapted from data_hub

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


def test_errors():

    # inserér une nouvelle série primaire dont le nom est déjà utilisé par un alias
    assert False