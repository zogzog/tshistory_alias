import pandas as pd


def buildtree(engine, tsh, alias, ancestors, depth=0):
    kind = tsh._typeofserie(engine, alias)
    if kind == 'primary':
        if not tsh.exists(engine, alias, 'primary'):
            return f'unknown `{alias}`'
        return alias

    ancestors.append(alias)

    series = [name for name, in engine.execute(
        f'select serie from "{tsh.namespace}-alias".{kind} '
        'where alias = %(alias)s', alias=alias).fetchall()
    ]
    leaves = []
    for name in series:
        if name in ancestors:
            print(name, 'in ancestors', ancestors)
            raise Exception('Loop')
        leaves.append(buildtree(engine, tsh, name, ancestors, depth+1))

    ancestors.pop()
    return {(alias, kind): leaves}


def sortkey(item):
    if isinstance(item, str):
        return item
    return f'ZZZ-{item}'


def showtree(tree, depth=0, printer=print):
    if isinstance(tree, str):
        printer('    ' * depth, '-', tree)
        return
    for (alias, kind), children in tree.items():
        printer('    ' * depth, f'* {kind} `{alias}`')
        for child in sorted(children, key=sortkey):
            showtree(child, depth + 1, printer)


def alias_table(engine, tsh, id_serie, fromdate=None, todate=None,
                author=None, additionnal_info=None, url_base_pathname=''):
    '''
    function used as callback for tseditor to handle aliases
    '''
    import dash_html_components as html
    from math import log10

    MAX_LENGTH = 15
    kind = tsh._typeofserie(engine, id_serie)
    if not kind or kind == 'primary':
        return None

    sql = f'select * from "{tsh.namespace}-alias".{kind} where alias = \'{id_serie}\''
    if kind == 'priority':
        sql += ' order by priority'
    result = list(engine.execute(sql).fetchall())
    df = tsh.get(
        engine, id_serie,
        from_value_date=fromdate,
        to_value_date=todate
    ).to_frame()

    infos = {id_serie: ['type : {}'.format(kind)]}
    for row in result:
        ts = tsh.get(
            engine, row.serie,
            from_value_date=fromdate,
            to_value_date=todate
        )
        if ts is None:
            ts = pd.Series(name=row.serie)
        df = df.join(ts, how='outer')
        spec = '(prio={})'.format(row.priority) if kind == 'priority' else row.fillopt
        stype = tsh._typeofserie(engine, row.serie)
        infos[row.serie] = [
            f'type : {stype}', spec, 'x ' + str(row.coefficient)
        ]

    def build_url(col):
        url = url_base_pathname + '?name=%s' % col
        if fromdate:
            url = url + '&startdate=%s' % fromdate
        if todate:
            url = url + '&enddate=%s' % todate
        if author:
            url = url + '&author=%s' % author
        return url

    def short_div(content):
        if len(content) > MAX_LENGTH:
            shortcontent = content[:MAX_LENGTH] + '(…)'
            return html.Div(shortcontent, title=content, style = {'font-size':'small'})
        else:
            return html.Div(content, style = {'font-size':'small'})

    def build_div_header(col):
        add = [
            html.Div(info, style={'font-size':'small'})
            for info in infos[col]
        ]
        name = [
            html.A(href=build_url(col), children=col, target="_blank",
                   style={'font-size':'small', 'word-wrap': 'break-word'})
        ]
        header = name + add
        if additionnal_info is not None:
            info_metadata = additionnal_info(engine, col)
            if info_metadata:
                metadata = [
                    short_div(info.lower().capitalize())
                    for _, info in info_metadata.items()
                ]
                header = metadata + header
        return html.Div(header)

    def build_str_formula(col, value, product):
        if col == id_serie:
            return value
        coef = infos[col][2]
        if coef == 'x 1.0':
            return value
        return f'{value} {coef} = {product}'

    df_coef = pd.DataFrame(
        [[1] + [row.coefficient for row in result]] * len(df),
        index=df.index,
        columns=df.columns
    )
    df_mulitplied = df * df_coef
    df_round = pd.DataFrame(index=df.index, columns=df.columns)

    for col in df.columns:
        avg = df[col].mean()
        if avg == 0 or pd.isnull(avg):
            df_round[col] = df[col]
        else:
            df_round[col] = df[col].round(max(2, int(-log10(abs(avg)))))

    header_css = {
        'max-width': '%sex'%(MAX_LENGTH+3),
        'min-width': '%sex'%(MAX_LENGTH+3),
        'width': '%sex'%(MAX_LENGTH+3),
        'position': 'sticky',
        'top': '0',
        'background-color': 'white'
    }
    corner_css = {
        'zIndex': '9999',
        'position': 'sticky',
        'left': '0',
        'top':'0',
        'background-color':'white'
    }
    dates_css = {
        'position': 'sticky',
        'left': '0',
        'background-color': 'white'
    }

    corner = html.Th('', style = corner_css)
    header = html.Tr([corner] + [
        html.Th(build_div_header(col), style=header_css)
        for col in df.columns
    ])

    list_for_table = [header]
    for i in range(len(df)):
        new_line = [html.Th(df.index[i], style=dates_css)]
        for col in df.columns:
            new_line.append(
                html.Td(
                    df_round.iloc[i][col],
                    title=build_str_formula(
                        col,
                        df.iloc[i][col],
                        df_mulitplied.iloc[i][col])
                )
            )
        list_for_table.append(html.Tr(new_line))

    return html.Table(list_for_table)



