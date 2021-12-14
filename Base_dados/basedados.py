import psycopg2 as pg
import pandas as pd
from datetime import datetime
import requests
import dataframe_image as dfi
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.dates as mdates
import numpy as np

#credenciais do postgresql hospedado na vm da aws


database = ""
password = "2Zy%tUuZnQnA"
user = "postgres"
host = "52.67.232.85"


###########################- FUNÇÕES BASE DE DADOS - ####################################################################


def create_table(nome):
    """
    comando postgresql para criar uma tabela
    :param nome: nome da tabela a ser criada
    :return:
    """
    c.execute(f""" CREATE TABLE {nome}(
                    acao VARCHAR,
                    data date,
                    abertura_ajustado NUMERIC,
                    fech_ajustado NUMERIC,
                    min_ajustado NUMERIC,
                    max_ajustado NUMERIC,
                    vol_mm_r$ NUMERIC
                    )""")

def delete_table(name):
    """
    através de comandos do postgresq, deleta uma tabela
    :param name: nome da tabela que será deletada
    :return:
    """
    table = name
    drop_table = "DROP TABLE %s;" %table
    c.execute(drop_table)


def insert_data(table):
    """
    através de comandos do postgresql, insere dados
    na tabela especificada
    a quantidade de %s depende do núemro de colunas da tabela
    :param table: nome da tabela que receberá os dados
    :return:
    """
    c.execute(f"""
        INSERT INTO {table}
#cada %s equivale a uma coluna da tabela
        VALUES (%s, %s, %s, %s);
        """,
        ('423.855.288-19', datetime.date(2005, 11, 18), 1.98, 20000000))



def add_column(tabela, coluna):
    """
    através de comandos do postgresql, será adicionada
    uma coluna na tabela
    :param tabela: nome da tabela
    :param coluna: nome da coluna que será adicionada
    :return:
    """
    c.execute(f"""ALTER TABLE {tabela} ADD COLUMN {coluna} VARCHAR""")

def delete_all(tabela):
    """
    através de comandos do postgresql, deleta
    todos os dados de um tabela
    :param tabela: nome da tabela
    :return:
    """
    c.execute(f"DELETE FROM {tabela}")








def actions_database(database, password, user, host):
    """
    essa função serve como uma forma prática de executar algum comando
    do postgresql, entretanto, o comando tem que ser colocado toda vez, o que
    necessita uma constante alteração no código dessa função

    :param database: nome da database
    :param password: senha
    :param user: usuário
    :param host: host
    :return:
    """
    conn = pg.connect(dbname="", user="postgres", password="2Zy%tUuZnQnA", host="52.67.232.85")
    c = conn.cursor()

    #colocar entre os parênteses a instrução SQL
    c.execute("""""")

    conn.commit()
    c.close()
    conn.close()





def execute_mogrify( df, table, content):
    """
    Using cursor.mogrify() to build the bulk insert query
    then cursor.execute() to execute the query

    Essa função serve para utilizar um método de enviar dados de forma mais rápida para
    alguma tabela do postgresql. Para isso, tem como input um DataFrame

    :param df: DataFrame
    :param table: nome da tabela do postgresql que receberá os dados
    """
    conn = pg.connect(dbname="", user="postgres", password="2Zy%tUuZnQnA", host="52.67.232.85")
    c = conn.cursor()
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    if content == 'ação':
        values = [c.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
    elif content == 'índice':
        values = [c.mogrify("(%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
    query = "INSERT INTO %s(%s) VALUES " % (table, cols) + ",".join(values) + "ON CONFLICT DO NOTHING"

    try:
        c.execute(query, tuples)
        conn.commit()
    except (Exception, pg.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        c.close()
        return 1
    print("execute_mogrify() done")
    c.close()


def fetch_database():
    """
    a função tem como objetivo trazer, em formato de dicionário,
    todas os ticker(key) de ações disponíveis na base de dados pos postgresql
    junto com os seus ids(value)
    :return:
    """
    dic = {}
    conn = pg.connect(dbname="", user="postgres", password="2Zy%tUuZnQnA", host="52.67.232.85")
    c = conn.cursor()

    c.execute("""SELECT id, nome FROM ações ORDER BY id""")
    for i in c.fetchall():
        identidade, nome = i
        dic[nome] = identidade
    conn.commit()
    c.close()
    conn.close()
    return dic




def tickers(dic):
    """
    Essa função serve para, através de um dic com os ticker e seus ids, puxar os dados dos ticker
    lá da Comdinheiro e depois enviar esses mesmos dados para o postgresql
    :param dic: dicionário com os nomes dos ticker e seus ids
    :return:
    """
    datafinal = datetime.today().strftime('%d%m%Y')
    for key, value in dic.items():
        df = extrair_cotacoes(key, datafinal)

        df['data'] = pd.to_datetime(df['data'], dayfirst=True)

        ticker = df.iloc[1, 1]
        identidade = dic[ticker]
        df['acao_id'] = identidade
        df = df[['data', 'acao_id', 'nome', 'maxima', 'minima', 'abertura', 'fechamento', 'volume']]

        print(execute_mogrify(df, 'dados_diários', 'ação'))
        print(key)






####################-FUNÇÕES QUE AJUDAM O BOT-##########################################################################

def get_ticker(ticker):
    """

    :param ticker:
    :return:
    """
    engine = create_engine('postgresql://postgres:2Zy%tUuZnQnA@52.67.232.85:5432/')

    df = pd.read_sql_query(f"""select * from dados_diários where nome = {ticker} order by "data" desc limit 1""", con=engine)

    #dfi.export(df, r'C:\Users\Lucas\Desktop\FEA.dev\Quant\Base de Dados\df.png')
    dfi.export(df, '/home/ubuntu/arquivos/df.png')




def get_index(ticker):
    """

    :param ticker:
    :return:
    """
    engine = create_engine('postgresql://postgres:2Zy%tUuZnQnA@52.67.232.85:5432/')

    df = pd.read_sql_query(f"""select * from índices_diários where nome = {ticker} order by "data" desc limit 1""", con=engine)

    #dfi.export(df, r'C:\Users\Lucas\Desktop\FEA.dev\Quant\Base de Dados\df.png')
    dfi.export(df, '/home/ubuntu/arquivos/df.png')






def list_tickers():
    engine = create_engine('postgresql://postgres:2Zy%tUuZnQnA@52.67.232.85:5432/')

    df = pd.read_sql_query("""select empresas_b3.nome as nome_empresa, ações.nome as ação
                                from empresas_b3, ações
                                where ações.empresa_id = empresas_b3.id """, con=engine)

    #df.to_excel(r'C:\Users\Lucas\Desktop\FEA.dev\Quant\Base de Dados\df.xlsx')
    df.to_excel('/home/ubuntu/arquivos/df.xlsx')




def list_index():
    engine = create_engine('postgresql://postgres:2Zy%tUuZnQnA@52.67.232.85:5432/')

    df = pd.read_sql_query("""select * from índices""", con=engine)

    #df.to_excel(r'C:\Users\Lucas\Desktop\FEA.dev\Quant\Base de Dados\df.xlsx')
    df.to_excel('/home/ubuntu/arquivos/df.xlsx')






def list_companies():
    engine = create_engine('postgresql://postgres:2Zy%tUuZnQnA@52.67.232.85:5432/')

    df = pd.read_sql_query("""select * from empresas_b3""", con=engine)

    #df.to_excel(r'C:\Users\Lucas\Desktop\FEA.dev\Quant\Base de Dados\df.xlsx')
    df.to_excel('/home/ubuntu/arquivos/df.xlsx')




def ticker_daily(ticker, datainicial, datafinal, formato):
    engine = create_engine('postgresql://postgres:2Zy%tUuZnQnA@52.67.232.85:5432/')

    df = pd.read_sql_query(f"""select * from dados_diários where data between {datainicial} and {datafinal} and nome = {ticker}""" , con=engine)

    if formato == "xlsx":
        #df.to_excel(r'C:\Users\Lucas\Desktop\FEA.dev\Quant\Base de Dados\df_daily.xlsx')
        df.to_excel('/home/ubuntu/arquivos/df_daily.xlsx')
    elif formato == "csv":
       #df.to_csv(r'C:\Users\Lucas\Desktop\FEA.dev\Quant\Base de Dados\df_daily.csv')
       df.to_csv('/home/ubuntu/arquivos/df_daily.csv')
    elif formato == "img":
        if len(df) <=100:
            #dfi.export(df, r'C:\Users\Lucas\Desktop\FEA.dev\Quant\Base de Dados\df_daily.png')
            dfi.export(df, '/home/ubuntu/arquivos/df_daily.png')
    return df




def index_daily(ticker, datainicial, datafinal, formato):
    engine = create_engine('postgresql://postgres:2Zy%tUuZnQnA@52.67.232.85:5432/')

    df = pd.read_sql_query(f"""select * from índices_diários where data between {datainicial} and {datafinal} and nome = {ticker}""" , con=engine)

    if formato == "xlsx":
        #df.to_excel(r'C:\Users\Lucas\Desktop\FEA.dev\Quant\Base de Dados\df_daily.xlsx')
        df.to_excel('/home/ubuntu/arquivos/df_daily.xlsx')
    elif formato == "csv":
        #df.to_csv(r'C:\Users\Lucas\Desktop\FEA.dev\Quant\Base de Dados\df_daily.csv')
        df.to_csv('/home/ubuntu/arquivos/df_daily.csv')
    elif formato == "img":
        if len(df) <=100:
            #dfi.export(df, r'C:\Users\Lucas\Desktop\FEA.dev\Quant\Base de Dados\df_daily.png')
            dfi.export(df, '/home/ubuntu/arquivos/df_daily.png')
    return df




def drawdown(df, cdi, legend='lower left'):
    """
    given a fund's and a benchmark's DataFrame
    creates a drawdown graph save
    :df:fund's DataFrame

    :legend: legend's position
    """
    nome = df['nome'][1]
    # set style
    plt.style.use('seaborn')
    # set Data column as index
    df.index = df['data']
    # calculates the drawdown
    wealth = df['fechamento'].pct_change()
    wealth = wealth.fillna(0)
    wealth = pd.DataFrame((wealth + 1).cumprod())
    top = wealth.cummax()
    ddown = (wealth - top) / top


    #calculate cdi's drawdown
    filtered = cdi.data.isin(df.data).astype(bool)
    bench_df = cdi[filtered]
    bench_df.index = bench_df['data']
    bench_df = pd.DataFrame((bench_df['preco'] + 1).cumprod())
    bench_top = bench_df.cummax()
    bench_ddown = (bench_df - bench_top) / bench_top


    fig, ax = plt.subplots(figsize=(30, 15))

    # set a transparent background(must come here, in the beginning)
    fig.patch.set_alpha(0)
    # set font style
    plt.rc('font', family='Bw Modelica')
    # creates legend
    label1 = nome
    ax.plot(ddown.index, ddown, color='gold', label=label1)
    label2 = 'CDI'
    ax.plot(bench_ddown.index, bench_ddown, color='dimgrey', label=label2)
    # set legend
    ax.legend(loc=legend, frameon=False, prop={'size': 14, 'weight': 800})
    # set % symbol y ax
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1, decimals=None, symbol='%', is_latex=False))
    # set the plot lines with borders
    plt.margins(0)
    # set tick format as, for example, February 19
    monthyearFmt = mdates.DateFormatter('%b %y')
    ax.xaxis.set_major_formatter(monthyearFmt)
    # set tick configs
    for tick in ax.get_xticklabels():
        tick.set_fontname("Bw Modelica")
        tick.set_weight(800)
        tick.set_size(20)
        # tick.set_rotation(95)
    for tick in ax.get_yticklabels():
        tick.set_fontname("Bw Modelica")
        tick.set_weight(800)
        tick.set_size(20)
    # fill inside line plots
    plt.fill_between(ddown.index, np.array(ddown['fechamento']), facecolor='gold', alpha=0.20)
    plt.fill_between(bench_ddown.index, np.array(bench_ddown['preco']), facecolor='dimgrey', alpha=0.20)

    ax.set_title('Drawdown (Eixo Y) e Período Mês Ano(Eixo X)', fontname='Bw Modelica', size=30, weight=600)

    print(ddown.index)
    #return plt.savefig(r'C:\Users\Lucas\Desktop\FEA.dev\Quant\Base de Dados\Drawdown.png', dpi=72, bbox_inches='tight')
    return plt.savefig('/home/ubuntu/arquivos/Drawdown.png', dpi=72, bbox_inches='tight')




def volatility(fund_df, legend='upper left'):
    """
        given a fund's DataFrame
        creates a rolling volatility(126 and 21 days) graph save
        :df:fund's DataFrame
        :legend: legend's position
    """

    # add style
    nome = fund_df['nome'][1]
    plt.style.use('seaborn')
    fund_close = fund_df['fechamento'].pct_change()
    fund_close.index = fund_df['data']
    fund_close1 = pd.DataFrame(fund_close)
    # calculate the rolling volatility
    fund_close1['Volatility21'] = fund_close1.rolling(21).std()
    fund_close2 = pd.DataFrame(fund_close)
    fund_close2['Volatility126'] = fund_close2.rolling(126).std()
    # creatibg the figure
    f1, ax = plt.subplots(figsize=(30, 15))
    # set transparent background(needs to come in this position to trnasparent = True
    # only the bakcground and not the whole figure)
    f1.patch.set_alpha(0)
    plt.rc('font', family='Bw Modelica')
    # creates legend
    label1 = "Vol 21 dias"
    # set the axis data
    # annualize the volatility
    ax.plot(fund_close.index, fund_close1['Volatility21'] * np.sqrt(252), color='dimgrey', label=label1)
    label2 = "Vol 126 dias"
    ax.plot(fund_close.index, fund_close2['Volatility126'] * np.sqrt(252), color='gold', label=label2, linewidth=4)
    # set legend
    ax.legend(loc=legend, frameon=False, prop={'size': 14, 'weight': 800})
    # %format, y ax
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1, decimals=None, symbol='%', is_latex=False))
    # stick together the graph's line with the borders
    plt.margins(0)
    # format ex: 21 fevereiro
    monthyearFmt = mdates.DateFormatter('%b %y')
    ax.xaxis.set_major_formatter(monthyearFmt)
    # set axis features
    for tick in ax.get_xticklabels():
        tick.set_fontname("Bw Modelica")
        tick.set_weight(800)
        tick.set_size(20)
        # tick.set_rotation(95)
    for tick in ax.get_yticklabels():
        tick.set_fontname("Bw Modelica")
        tick.set_weight(800)
        tick.set_size(20)
    ax.set_title('Volatilidade: Janelas Móveis de 21 e 126 Dias', fontname='Bw Modelica', size=30, weight=600)

    #return plt.savefig(r"C:\Users\Lucas\Desktop\FEA.dev\Quant\Base de Dados\Volatility.png", dpi=72,bbox_inches='tight')
    return plt.savefig('/home/ubuntu/arquivos/Volatility.png', dpi=72,
                       bbox_inches='tight')





def returns(df, cdi, legend='upper left'):
    """
        given a fund's and a benchmark's DataFrame
        creates a retunrs graph save
        :fund_df:fund's DataFrame
        :legend: legend's position

    """

    nome = df['nome'][1]
    plt.style.use('seaborn')
    df.index = df['data']
    filtered = cdi.data.isin(df.data).astype(bool)
    # clear the first data to start the acumulated from 0
    # calcultes the acumulated
    df = df['fechamento'].pct_change()
    # df = df.dropna()
    df = pd.DataFrame((df + 1).cumprod() - 1)
    df = df.replace(np.nan, 0)

    bench_df = cdi[filtered]
    bench_df.index = bench_df['data']
    bench_df = pd.DataFrame((bench_df['preco'] + 1).cumprod() - 1)
    bench_df = bench_df.replace(np.nan, 0)


    # set the figure
    fig, ax1 = plt.subplots(figsize=(30, 15))
    # set transparent background
    fig.patch.set_alpha(0)
    plt.rc('font', family='Bw Modelica')
    ax1.plot(df.index, df['fechamento'], color='gold', label=nome, linewidth=4)
    ax1.plot(bench_df.index, bench_df['preco'], color='dimgrey', label='CDI', linewidth=4)
    ax1.legend(loc=legend, frameon=False, prop={'size': 14, 'weight': 800})

    ax1.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1, decimals=None, symbol='%', is_latex=False))
    for tick in ax1.get_xticklabels():
        tick.set_fontname('Bw Modelica')
        tick.set_weight(800)
        tick.set_size(20)
    for tick in ax1.get_yticklabels():
        tick.set_fontname('Bw Modelica')
        tick.set_weight(800)
        tick.set_size(20)

    monthyearFmt = mdates.DateFormatter('%b %y')
    ax1.xaxis.set_major_formatter(monthyearFmt)
    ax1.set_title('Retorno Acumulado', fontname='Bw Modelica', size=30, weight=600)
    plt.margins(0)
    plt.tight_layout()

    print(df.index)
    #return plt.savefig(r"C:\Users\Lucas\Desktop\FEA.dev\Quant\Base de Dados\Return.png", dpi=72)
    return plt.savefig('/home/ubuntu/arquivos/Return.png', dpi=72)





def return_application(ret_df):
    plt.style.use('seaborn')
    nome = ret_df['nome'][1]
    df = pd.DataFrame()
    lista = [(ret_df['fechamento'].iloc[-1] / i) - 1 for i in
             ret_df['fechamento']]
    df['Retorno'] = lista
    df.index = ret_df['data']

    f1, ax = plt.subplots(figsize=(30, 15))
    f1.patch.set_alpha(0)
    plt.rc('font', family='Bw Modelica')
    ax.plot(df.index, df['Retorno'], color='dimgrey', label=nome)
    ax.fill_between(df.index, 0, df['Retorno'], facecolor = 'dimgrey',alpha=0.2)
    ax.legend(loc='upper left', frameon=False, prop={'size': 20, 'weight': 800})
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1, decimals=None, symbol='%', is_latex=False))
    monthyearFmt = mdates.DateFormatter('%b %y')
    ax.xaxis.set_major_formatter(monthyearFmt)
    for tick in ax.get_xticklabels():
        tick.set_fontname("Bw Modelica")
        tick.set_weight(800)
        tick.set_size(20)
    for tick in ax.get_yticklabels():
        tick.set_fontname("Bw Modelica")
        tick.set_weight(800)
        tick.set_size(20)
    plt.margins(0)
    # f1.tight_layout()
    ax.set_title('Retorno Acumulado até o Vencimento por Aplicação(Absoluto)', fontname='Bw Modelica', size=30,
                 weight=600)

    #return plt.savefig(r'C:\Users\Lucas\Desktop\FEA.dev\Quant\Base de Dados\Return_application.png', dpi=72,bbox_inches='tight')
    return plt.savefig('/home/ubuntu/arquivos/Return_application.png', dpi=72,
                       bbox_inches='tight')


#####################- FUNÇÕES COMDINHEIRO -##########################################

def extrair_cotacoes(ticker, datafinal, datainicial='01011993'):
    """
    serve para extrair os dados díarios de ações
    utlizando a api do comdinheiro e gerar output
    de um dataframe formatado adequadamente para ser inserido
    no databse do postgresql

    :param ticker: ticker da ação
    :param datainicial: data do início do periodo
    :param datafinal: data final do período

    """
    url = "https://www.comdinheiro.com.br/Clientes/API/EndPoint001.php"

    querystring = {"code": "import_data"}

    payload = "username=feausp&password=2020feausp&URL=HistoricoCotacaoAcao001-" + ticker + "-" + datainicial + "-" + datafinal + "-1-1&format=json2"
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    response = requests.request("POST", url, data=payload, headers=headers, params=querystring)

    dados = response.text
    df = pd.DataFrame(response.json()["resposta"]["tab-p0"]["linha"])

    # formatação do dataframe
    # apenas filtra as colunas indicadas
    df = df[['data', 'abertura_ajustado', 'fech_ajustado', 'min_ajustado', 'max_ajustado', 'vol_(mm_r$)']]

    df.rename(columns={'vol_(mm_r$)': 'vol_mm_r$'}, inplace=True)

    # servirá para descartar linhas que não possuem dado na abertura
    b1 = df['abertura_ajustado'] != ""

    new_df = df[b1]
    new_df = pd.DataFrame(new_df)

    # servirá para descartar linhas que possuem 'nd' na abertura
    #b1 = new_df['abertura_ajustado'] != 'nd'
    #new_df = new_df[b1]
    #new_df

    #Nova solução para o bloco de cima
    new_df.replace('nd', np.nan, inplace=True)
    new_df.ffill(axis=0, inplace=True)

    # cria coluna com o nome da ação
    new_df['acao'] = ticker

    new_df = new_df[['data', 'acao', 'max_ajustado', 'min_ajustado', 'abertura_ajustado', 'fech_ajustado', 'vol_mm_r$']]
    new_df.columns = ['data', 'nome', 'maxima', 'minima', 'abertura', 'fechamento', 'volume']



    # formata de str para float
    new_df['maxima'] = new_df['maxima'].str.replace(',', '.').astype(float)
    new_df['minima'] = new_df['minima'].str.replace(',', '.').astype(float)
    new_df['abertura'] = new_df['abertura'].str.replace(',', '.').astype(float)
    new_df['fechamento'] = new_df['fechamento'].str.replace(',', '.').astype(float)
    new_df['volume'] = new_df['volume'].str.replace(',', '.').astype(float)

    # arredonda as casas decimais
    new_df = new_df.round({'maxima': 4, 'minima': 4, 'abertura': 4, 'fechamento': 4, 'volume': 2})

    # df['data'] = pd.to_datetime(df['data'], dayfirst=True)

    #o dropna é colocado pois utilizo intervalo de data desde 94, independente da acao, assim, retornando dados vazios em muitos casos
    return new_df.dropna()




def extrair_indice(indice, datafinal, datainicial='01011993'):

    url = "https://www.comdinheiro.com.br/Clientes/API/EndPoint001.php"

    cdi = 0
    ibov = 0
    ptax = 0
    ipca = 0
    querystring = {"code": "import_data"}
    if indice == "cdi":
        payload = "username=feausp&password=2020feausp&URL=HistoricoCotacao002.php%3F%26x%3DCDI%26data_ini%3D" + datainicial + "%26data_fim%3D" + datafinal + "%26pagina%3D1%26d%3DMOEDA_ORIGINAL%26g%3D0%26m%3D0%26info_desejada%3Dretorno%26retorno%3Ddiscreto%26tipo_data%3Ddu_br%26tipo_ajuste%3Dtodosajustes%26num_casas%3D2%26enviar_email%3D0%26ordem_legenda%3D1%26cabecalho_excel%3Dmodo1%26classes_ativos%3D18ce53un18f%26ordem_data%3D0%26rent_acum%3Drent_acum%26minY%3D%26maxY%3D%26deltaY%3D%26preco_nd_ant%3D0%26base_num_indice%3D100%26flag_num_indice%3D0%26eixo_x%3DData%26startX%3D0%26max_list_size%3D20%26line_width%3D2%26titulo_grafico%3D%26legenda_eixoy%3D%26tipo_grafico%3Dline&format=json2"
        cdi = 1
    elif indice == "ibov":
        payload = "username=feausp&password=2020feausp&URL=HistoricoCotacao002.php%3F%26x%3DIBOV%2B%26data_ini%3D" + datainicial + "%26data_fim%3D" + datafinal + "%26pagina%3D1%26d%3DMOEDA_ORIGINAL%26g%3D0%26m%3D0%26info_desejada%3Dretorno%26retorno%3Ddiscreto%26tipo_data%3Ddu_br%26tipo_ajuste%3Dtodosajustes%26num_casas%3D2%26enviar_email%3D0%26ordem_legenda%3D1%26cabecalho_excel%3Dmodo1%26classes_ativos%3D18ce53un18f%26ordem_data%3D0%26rent_acum%3Drent_acum%26minY%3D%26maxY%3D%26deltaY%3D%26preco_nd_ant%3D0%26base_num_indice%3D100%26flag_num_indice%3D0%26eixo_x%3DData%26startX%3D0%26max_list_size%3D20%26line_width%3D2%26titulo_grafico%3D%26legenda_eixoy%3D%26tipo_grafico%3Dline&format=json2"
        ibov = 1
    elif indice == "ptax":
        payload = "username=feausp&password=2020feausp&URL=HistoricoCotacao002.php%3F%26x%3DPTAXC%26data_ini%3D" + datainicial + "26data_fim%3D" + datafinal + "%26pagina%3D1%26d%3DMOEDA_ORIGINAL%26g%3D0%26m%3D0%26info_desejada%3Dretorno%26retorno%3Ddiscreto%26tipo_data%3Ddu_br%26tipo_ajuste%3Dtodosajustes%26num_casas%3D2%26enviar_email%3D0%26ordem_legenda%3D1%26cabecalho_excel%3Dmodo1%26classes_ativos%3Djpia9pm8jr3%26ordem_data%3D0%26rent_acum%3Drent_acum%26minY%3D%26maxY%3D%26deltaY%3D%26preco_nd_ant%3D0%26base_num_indice%3D100%26flag_num_indice%3D0%26eixo_x%3DData%26startX%3D0%26max_list_size%3D20%26line_width%3D2%26titulo_grafico%3D%26legenda_eixoy%3D%26tipo_grafico%3Dline&format=json2"
        ptax = 1
    elif indice == "ipca":
        payload = "username=feausp&password=2020feausp&URL=HistoricoCotacao002.php%3F%26x%3DIBGE_IPCAd%2B%26data_ini%3D" + datainicial + "%26data_fim%3D" + datafinal + "%26pagina%3D1%26d%3DMOEDA_ORIGINAL%26g%3D0%26m%3D0%26info_desejada%3Dretorno%26retorno%3Ddiscreto%26tipo_data%3Ddu_br%26tipo_ajuste%3Dtodosajustes%26num_casas%3D8%26enviar_email%3D0%26ordem_legenda%3D1%26cabecalho_excel%3Dmodo1%26classes_ativos%3Djpia9pm8jr3%26ordem_data%3D0%26rent_acum%3Drent_acum%26minY%3D%26maxY%3D%26deltaY%3D%26preco_nd_ant%3D0%26base_num_indice%3D100%26flag_num_indice%3D0%26eixo_x%3DData%26startX%3D0%26max_list_size%3D20%26line_width%3D2%26titulo_grafico%3D%26legenda_eixoy%3D%26tipo_grafico%3Dline&format=json2"
        ipca = 1

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    response = requests.request("POST", url, data=payload, headers=headers, params=querystring)

    dados = response.text
    df = pd.DataFrame(response.json()["resposta"]["tab-p1"]["linha"])

    if cdi==1:
        b1 = df['cdi'] != 'nd'
        df = df[b1]


        df['nome'] = "CDI"
        df['indice_id'] = 1

        df = df[['data', 'indice_id', 'nome', 'cdi']]
        df.columns = ['data', 'indice_id', 'nome', 'preco']


        df['preco'] = df['preco'].str.replace(',', '.').astype(float)
        df['data'] = pd.to_datetime(df['data'], dayfirst=True)




    if ibov==1:
        b1 = df['ibov'] != 'nd'
        df = df[b1]

        df['nome'] = 'IBOV'
        df['indice_id'] = 2

        df = df[['data', 'indice_id', 'nome', 'ibov']]
        df.columns = ['data', 'indice_id', 'nome', 'preco']

        df['preco'] = df['preco'].str.replace(',', '.').astype(float)
        df['data'] = pd.to_datetime(df['data'], dayfirst=True)


    if ptax == 1:
        b1 = df['ptax'] != 'nd'
        df = df[b1]

        df['nome'] = 'PTAX'
        df['indice_id'] = 3

        df = df[['data', 'indice_id', 'nome', 'ptax']]
        df.columns = ['data', 'indice_id', 'nome', 'preco']

        df['preco'] = df['preco'].str.replace(',', '.').astype(float)
        df['data'] = pd.to_datetime(df['data'], dayfirst=True)


    if ipca == 1:
        b1 = df['ibge_ipcad'] != 'nd'
        df = df[b1]

        df['nome'] = 'IPCA'
        df['indice_id'] = 4

        df = df[['data', 'indice_id', 'nome', 'ibge_ipcad']]
        df.columns = ['data', 'indice_id', 'nome', 'preco']

        df['preco'] = df['preco'].str.replace(',', '.').astype(float)
        df['data'] = pd.to_datetime(df['data'], dayfirst=True)

    return df.dropna()








############### EXECUÇÂO CORRIQUEIRA ####################################

if __name__ == '__main__':
    #dic = fetch_database()
    dic = {'AALR3':1,}
    tickers(dic)
    #df = extrair_indice('ipca', '06112021')
    #execute_mogrify(df, 'índices_diários', 'índice')