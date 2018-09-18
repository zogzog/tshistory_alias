import pandas as pd

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
    sql = '''select * from "{}-alias".{} where alias = '{}' '''.format(tsh.namespace, kind, id_serie)
    result = pd.read_sql(sql, engine)
    result.loc[result['coefficient'].isnull(), 'coefficient'] = 1
    if kind == 'priority':
        result = result.sort_values(by='priority')
    df = tsh.get(engine, id_serie, from_value_date=fromdate, to_value_date=todate).to_frame()
    infos = {id_serie: ['type : {}'.format(kind)]}
    for idx, row in result.iterrows():
        ts = tsh.get(engine, row['serie'], from_value_date=fromdate, to_value_date=todate)
        if ts is None:
            ts = pd.Series(name=row['serie'])
        df = df.join(ts, how='outer')
        spec = '(prio={})'.format(row['priority']) if kind == 'priority' else row['fillopt']
        infos[row['serie']] = ['type : {}'.format(tsh._typeofserie(engine, row['serie'])
                                                  ), spec, 'x ' + str(row['coefficient'])]

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
        add = [html.Div(info, style = {'font-size':'small'}) for info in infos[col]]
        name = [html.A(href=build_url(col), children=col, target="_blank",
                       style={'font-size':'small', 'word-wrap': 'break-word'})]
        header = name + add
        if additionnal_info is not None:
            info_metadata = additionnal_info(engine, col)
            if info_metadata:
                metadata = [short_div(info.lower().capitalize()) for _, info in info_metadata.items()]
                header = metadata + header
        return html.Div(header)

    def build_str_formula(col, value, product):
        if col == id_serie:
            return value
        coef = infos[col][2]
        if coef == 'x 1.0':
            return value
        return '{} {} = {}'.format(value, coef, product)

    df_coef = pd.DataFrame([[1] + result['coefficient'].tolist()] * len(df),
                           index=df.index, columns=df.columns)
    df_mulitplied = df * df_coef

    df_round = pd.DataFrame(index=df.index, columns=df.columns)
    for col in df.columns:
        avg = df[col].mean()
        if avg == 0 or pd.isnull(avg):
            df_round[col] = df[col]
        else:
            df_round[col] = df[col].round(max(2, int(-log10(abs(avg)))))

    header_css = {'max-width': '%sex'%(MAX_LENGTH+3),
                  'min-width': '%sex'%(MAX_LENGTH+3),
                  'width': '%sex'%(MAX_LENGTH+3),
                  'position': 'sticky',
                  'top': '0',
                  'background-color': 'white'
                  }
    corner_css = {'zIndex': '9999',
                  'position': 'sticky',
                  'left': '0',
                  'top':'0',
                  'background-color':'white'
                  }
    dates_css = {'position': 'sticky',
                 'left': '0',
                 'background-color': 'white'
                 }

    corner = html.Th('', style = corner_css)
    header = html.Tr([corner] + [html.Th(build_div_header(col), style=header_css) for col in df.columns])

    list_for_table = []
    for i in range(len(df)):
        new_line = [html.Th(df.index[i], style=dates_css)]
        for col in df.columns:
            new_line.append(html.Td(df_round.iloc[i][col],
                                    title=build_str_formula(col,
                                                            df.iloc[i][col],
                                                            df_mulitplied.iloc[i][col])))
        list_for_table.append(html.Tr(new_line))

    list_for_table = [header] + list_for_table

    return html.Table(list_for_table)



