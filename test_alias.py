from datetime import datetime

from tshistory.testutil import genserie
from tshistory_alias.db import add_bounds, build_priority


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

    values = tsh.get_priority(engine,'serie5')

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


    import pdb; pdb.set_trace()

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
    values, origin, marker, enrichement = tsh.get_many(
        engine,
        ['realised', 'nominated', 'forecasted'],
        map_prune={'realised': 1, 'nominated': 1, 'forecasted': 0}
    )

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

    tsh.insert_from_many(engine,
                         ts_pushed,
                         'author_test',
                         ['realised', 'nominated', 'forecasted'],
                         map_prune={'realised': 1, 'nominated': 1, 'forecasted': 0})

    # here are the result values
    values, origin, marker, enrichement = tsh.get_many(
        engine,
        ['realised', 'nominated', 'forecasted'],
        map_prune={'realised': 1, 'nominated': 1, 'forecasted': 0}
    )

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

    assert_df("""
    2010-01-01    False
    2010-01-02    False
    2010-01-03    False
    2010-01-04    False
    2010-01-05     True
    2010-01-06    False
    2010-01-07    False
    2010-01-08    False
    2010-01-09    False
    2010-01-10    False
    2010-01-11    False
    2010-01-12    False
    2010-01-13    False
    2010-01-14    False
    2010-01-15    False
    2010-01-16     True
    2010-01-17    False
    2010-01-18    False
    2010-01-19    False
    2010-01-20    False
    """, marker)

    # we try a bogus prune of data, i.e. remove more than there are data...

    values, origin, marker, enrichement = tsh.get_many(engine,
                                                       ['realised', 'nominated', 'forecasted'],
                                                       map_prune={'realised': 1, 'nominated': 3, 'forecasted': 0})

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

    #   once more, with enrichment

    ts_real = genserie(datetime(2010, 1, 1), 'D', 5, [1])

    ts_comment = pd.Series(['comment__real'], index=[datetime(2010, 1, 3)])
    dico_real = {'realised_rich_comment': ts_comment}
    ts_hyper = pd.Series(['hyper', 'hyper_pruned'],
                         index=[datetime(2010, 1, 4),
                                datetime(2010, 1, 5)])
    dico_real['realised_rich_hyper'] = ts_hyper
    dico_real['realised_rich'] = ts_real
    tsh.insert_group(engine, dico_real, 'test')

    ts_comment = pd.Series(['comment__forecast_1', 'comment__forecast_2'],
                           index=[datetime(2010, 1, 4),
                                  datetime(2010, 1, 7)])

    ts_forecast = genserie(datetime(2010, 1, 1), 'D', 10, [3])
    dico_forecast = {'forecasted_rich_comment': ts_comment}
    ts_formula = pd.Series(['formula'],
                           index=[datetime(2010, 1, 2)])
    dico_forecast['forecasted_rich_formula'] = ts_formula
    dico_forecast['forecasted_rich'] = ts_forecast
    tsh.insert_group(engine, dico_forecast, 'test')

    values, origin, marker, enrichement = tsh.get_many(engine,
                                                       ['realised_rich', 'forecasted_rich'],
                                                       map_prune={'realised_rich': 1, 'forecasted_rich': 1}
                                                       )

    # enrichement['formula'] give a false response as long
    # as there is no erasing procedure in apply_diff
    enrichement['comment']['2010-01-03'] = 'new_comment_real'
    enrichement['comment']['2010-01-07'] = 'new_comment_forecast'

    tsh.insert_from_many(engine,
                         values,
                         'author_test',
                         ['realised_rich', 'forecasted_rich'],
                         map_prune={'realised_rich': 1, 'forecasted_rich': 1},
                         enrichement=enrichement)

    values, marker, enrichement_result = tsh.get_all(engine, 'realised_rich')
    assert "new_comment_real" == enrichement_result['comment']['2010-01-03']

    values, marker, enrichement_result = tsh.get_all(engine, 'forecasted_rich')
    assert "new_comment_forecast" == enrichement_result['comment']['2010-01-07']

    # test when only one serie

    ts_only_one = pd.Series([1] * 5)
    ts_only_one.index = pd.date_range(start=datetime(2010, 1, 1), freq='D', periods=5)

    tsh.insert_from_many(engine,
                         ts_only_one,
                         'test',
                         ['only_one'])

    values, origin, marker, enrichement = tsh.get_many(engine,
                                                       ['only_one'])

    assert_df("""
    2010-01-01    1.0
    2010-01-02    1.0
    2010-01-03    1.0
    2010-01-04    1.0
    2010-01-05    1.0
    """, values)

    # get missing data

    values, origin, marker, enrichement = tsh.get_many(engine,
                                                       ['missing'])
    assert 0 == len(values)

    values, origin, marker, enrichement = tsh.get_many(engine,
                                                       ['only_one', 'missing'])
    assert_df("""
    2010-01-01    1.0
    2010-01-02    1.0
    2010-01-03    1.0
    2010-01-04    1.0
    2010-01-05    1.0
    """, values)

    values, origin, marker, enrichement = tsh.get_many(engine,
                                                       ['missing', 'only_one'])
    assert_df("""
    2010-01-01    1.0
    2010-01-02    1.0
    2010-01-03    1.0
    2010-01-04    1.0
    2010-01-05    1.0
    """, values)

    # now, we insert data outside of the existing one with an intersection:
    ts_only_one = genserie(datetime(2010, 1, 1), 'D', 10, [1])

    tsh.insert_from_many(engine,
                         ts_only_one,
                         'author_test',
                         ['only_one'])

    values = tsh.get(engine, 'only_one')
    assert values.iloc[9] == 1

    ts_preums = genserie(datetime(2010, 1, 1), 'D', 5, [1])

    ts_second = genserie(datetime(2010, 1, 7), 'D', 5, [2])

    ts_new_value = genserie(datetime(2010, 1, 1), 'D', 14, [3])

    tsh.insert(engine, ts_preums, 'uno', 'test')
    tsh.insert(engine, ts_preums, 'dos', 'test')

    tsh.insert_from_many(engine,
                         ts_new_value,
                         'author_test',
                         ['uno', 'dos'])

    values, origin, marker, enrichement = tsh.get_many(engine,
                                                       ['uno', 'dos'])
    assert_df("""
    2010-01-01    uno
    2010-01-02    uno
    2010-01-03    uno
    2010-01-04    uno
    2010-01-05    uno
    2010-01-06    dos
    2010-01-07    dos
    2010-01-08    dos
    2010-01-09    dos
    2010-01-10    dos
    2010-01-11    dos
    2010-01-12    dos
    2010-01-13    dos
    2010-01-14    dos
    """, origin)

    # The new numerical values are affected to the last series given by the user

    # side effect: creation of series with insert_from_many with multiple series:
    # the numerical values will only be inserted on the last series
    ts_preums = genserie(datetime(2010, 1, 1), 'D', 5, [1])
    ts_second = genserie(datetime(2010, 1, 7), 'D', 5, [2])

    tsh.insert_from_many(engine,
                         pd.concat([ts_preums, ts_second]),
                         'author_test',
                         ['preums', 'second'])
    values, origin, marker, enrichement = tsh.get_many(engine, ['preums', 'second'])

    assert 'second' == np.unique(origin)
