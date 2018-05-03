from datetime import datetime

from tshistory.testutil import genserie
from tshistory_alias.db import add_bounds

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
    ts2 = tsh.get_bounds(engine, 'serie2', remove_outliers=True)
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
    assert 0 == min(ts2)

    assert 14 == max(ts3)
    assert 5 == min(ts3)

    assert 0 == len(ts4)

