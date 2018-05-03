from tshistory_alias import schema


def add_bounds(cn, sn, min=None, max=None):

    if min is None and max is None:
        return
    value = {
        'serie': sn,
        'min': min,
        'max': max
    }
    cn.execute(schema.outliers.insert().values(value))
    print('insert {} in outliers table'.format(sn))
