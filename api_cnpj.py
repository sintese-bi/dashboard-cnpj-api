from urllib.parse import quote_plus
import datetime
from flask import Flask, json, jsonify, request
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
import psycopg2 as pg
import pandas as pd
from flask import Flask
from flask_caching import Cache
import re
from datetime import datetime, timedelta




db_password = "2023@Tag"
#db_password = "saulodados"
db_password_encoded = quote_plus(db_password)


# Atualize a string de conexão com o banco de dados substituindo a senha codificada
DB_CONFIG = f"postgresql://postgres:{db_password_encoded}@159.65.42.225:5432/comercial_BI"
#DB_CONFIG = f"postgresql://postgres:{db_password_encoded}@134.209.223.235/comercial_BI"

app = Flask(__name__)
cache = Cache(app, config={'CACHE_TYPE': 'simple'})
app.config["DEBUG"] = True
app.config['ENV'] = 'production'
app.config['SQLALCHEMY_DATABASE_URI'] = DB_CONFIG
app.config['SQLALCHEMY_POOL_SIZE'] = 200
app.config['SQLALCHEMY_MAX_OVERFLOW'] = 10
app.config['TIMEOUT'] = 300
db = SQLAlchemy(app)
CORS(app)

VERSION = "v2"

def connectionDataBase():
    connection = pg.connect(user="postgres", password="2023@Tag", host="159.65.42.225", port=5432, database="comercial_BI")
    return connection


def find (lista):
    new_list=[]
    for element in lista:
        if "'" not in element:
            new_list.append(element)
    return new_list,list(set(lista)-set(new_list))


@app.route(f'/{VERSION}/activities', methods=['GET'])
def activities():

    connection = connectionDataBase()

    cursor=connection.cursor()

    query = cursor.execute("SELECT DISTINCT  cna_name from cnae_cnaes_")

    query = cursor.fetchall()

    print(query)

    json_data = {"activites":[x[0].capitalize() for x in query ]}


    return json_data

@app.route(f'/{VERSION}/natjuri', methods=['GET'])
def natjuri():

    connection = connectionDataBase()

    cursor=connection.cursor()

    query = cursor.execute("SELECT DISTINCT  nat_juri from nat_juri")

    query = cursor.fetchall()

    print(query)

    json_data = {"activites":[x[0].capitalize() for x in query ]}


    return json_data

@app.route(f'/{VERSION}/cnaeCode', methods=['GET'])
def cnaeCode():

    connection = connectionDataBase()

    cursor=connection.cursor()

    query = cursor.execute("SELECT DISTINCT  cna_subclass from cnae_cnaes_")

    query = cursor.fetchall()

    print(query)

    json_data = {"activites":[x[0] for x in query ]}


    return json_data


@app.route(f'/{VERSION}/states', methods=['GET'])
def states():
    connection = connectionDataBase()

    cursor=connection.cursor()

    query = cursor.execute("SELECT DISTINCT states from st_states")

    query = cursor.fetchall()

    print(query)

    json_data = {"states":[x[0].upper() for x in query ]}


    return json_data

@app.route(f'/{VERSION}/mei', methods=['GET'])
def mei():
    year = request.args.get('year')
    state = request.args.get('state')

    if year in ['2017','2016','2015']:
        query = f"SELECT * FROM mei_{year} WHERE uf='{state}'"
    elif year in ['2020','2019','2018']:
        query = f"SELECT * FROM mei_{year} WHERE uf='{state}'"
    else:
        query = f"SELECT * FROM mei_{year} WHERE uf='{state}'"

    print(query)
    result_mei = db.session.execute(text(query)).fetchall()

    df = pd.DataFrame(result_mei)
    print(df)

    df_dict = df.to_dict(orient='records')

    json_data = {'mei':df_dict}


    return json.dumps(json_data,indent=4)




@app.route(f'/{VERSION}/cities', methods=['GET'])
def cities():
    state = request.args.get('state')
    connection = connectionDataBase()

    cursor=connection.cursor()

    query = cursor.execute(f"SELECT DISTINCT sm.muni_name from state_muni sm WHERE sm.state='{state}'")

    query = cursor.fetchall()

    print(query)

    json_data = {"cities":[x[0].upper() for x in query ]}


    return json_data

def find_int(string):
    padrao = re.compile(r'\d+')
    correspondencias = padrao.findall(string)
    
    if correspondencias:
        return int(correspondencias[0])
    else:
        return None


@app.route(f'/{VERSION}/cnpj', methods=['POST'])
def cnpj():
    data = request.get_json() 
   
    response = json.loads(request.data)
    cities = response.get("municipio")
    states = response.get("states")
    dates = response.get("dateRange")
    size= response.get("porte")
    cod_nat = response.get("cod_nat")
    print(dates)
    cnae_names = response.get('activities')
    if len(cnae_names)==1:
        query_cnaes = f"SELECT cna_subclass FROM cnae_cnaes_ WHERE cna_name IN ('{cnae_names[0]}') AND cna_subclass <> '' ORDER BY cna_subclass"
    else:
        query_cnaes = f'''SELECT cna_subclass FROM cnae_cnaes_ WHERE cna_name IN {tuple(cnae_names)} AND cna_subclass <> '' ORDER BY cna_subclass'''
    


    result = db.session.execute(text(query_cnaes)).fetchall()
    values = [row[0] for row in result]
    result = [value for value in values]
    if len(cnae_names)==1:
        query_cna = f"IN ('{result[0]}')"
    else:
        query_cna = f"IN {tuple(result)}"
    print(len(states))
    if cities is not None:
        print('none')
        municipio=find(cities)[0]
    print(result)
    if len(states)==1:
        if len(municipio)==1:
            query_cnpj = f'''SELECT distinct tt.cnpj_ as cnpj,tt.nome_fantasia,tt.idade,tt.cna_name,ee.razao_social,ee.porte,ee.capital_social,ee.cod_nat_juri_,ee.qual_respons_,tt.uf,tt.data_situacao_cadastral,tt.data_incio_atividade,tt.telefone, tt.muni_name,tt.logradouro ,tt.tipo_logradouro ,tt.complemento,tt.bairro ,tt.cep,tt.cep_lat,tt.cep_long
                        FROM (
                        SELECT cc.cnpj_,cc.cnpj,cna.cna_name,cc.nome_fantasia,cc.idade,cc.uf,cc.data_situacao_cadastral,cc.data_incio_atividade,cc.telefone, mm.muni_name,cc.logradouro ,cc.tipo_logradouro ,cc.complemento,cc.bairro ,cc.cep,cp.cep_lat,cp.cep_long from cnp_cnpj cc
                        left join mun_municipio mm on mm.muni_cod = cc.muncipio
                        left join cnae_cnaes_ cna on cna.cna_subclass = cc.cnae_principal
                        left join cep_lat_long cp on cp.cep=cc.cep 
                        WHERE cc.cnae_principal  {query_cna} AND cc.uf='{states[0]}') AS tt 
                        left join em_empresas ee on ee.cnpj = tt.cnpj 
                        where tt.muni_name = '{cities[0]}' 
                        '''
        else:
            print('else')
            #municipio=find(cities)[0]
            query_cnpj = f'''SELECT distinct tt.cnpj_ as cnpj,tt.nome_fantasia,tt.idade,tt.cna_name,tt.razao_social,tt.porte,tt.capital_social,tt.cod_nat_juri_,tt.qual_respons_,tt.uf,tt.data_situacao_cadastral,tt.data_incio_atividade,tt.telefone, tt.muni_name,tt.logradouro ,tt.tipo_logradouro ,tt.complemento,tt.bairro ,tt.cep,tt.cep_lat,tt.cep_long
                        FROM (
                        SELECT cc.cnpj_,cc.cnpj,cna.cna_name,cc.nome_fantasia,cc.idade,ee.razao_social,ee.porte,ee.capital_social,ee.cod_nat_juri_,ee.qual_respons_,cc.uf,cc.data_situacao_cadastral,cc.data_incio_atividade,cc.telefone, mm.muni_name,cc.logradouro ,cc.tipo_logradouro ,cc.complemento,cc.bairro ,cc.cep,cp.cep_lat,cp.cep_long from cnp_cnpj cc
                        left join mun_municipio mm on mm.muni_cod = cc.muncipio
                        left join em_empresas ee on ee.cnpj = cc.cnpj
                        left join cep_lat_long cp on cp.cep=cc.cep
                        left join cnae_cnaes_ cna on cna.cna_subclass = cc.cnae_principal
                        WHERE cc.cnae_principal {query_cna} AND cc.uf='{states[0]}') AS tt where tt.muni_name IN {tuple(municipio)}'''
    else:
        query_cnpj = f'''SELECT distinct tt.cnpj_ as cnpj,tt.nome_fantasia,tt.idade,tt.cna_name,tt.razao_social,tt.porte,tt.capital_social,tt.cod_nat_juri_,tt.qual_respons_,tt.uf,tt.data_situacao_cadastral,tt.data_incio_atividade,tt.telefone, tt.muni_name,tt.logradouro ,tt.tipo_logradouro ,tt.complemento,tt.bairro ,tt.cep,tt.cep_lat,tt.cep_long
                        FROM (
                        SELECT cc.cnpj_,cc.cnpj,cna.cna_name,cc.nome_fantasia,cc.idade,ee.razao_social,ee.porte,ee.capital_social,ee.cod_nat_juri_,ee.qual_respons_,cc.uf,cc.data_situacao_cadastral,cc.data_incio_atividade,cc.telefone, mm.muni_name,cc.logradouro ,cc.tipo_logradouro ,cc.complemento,cc.bairro ,cc.cep,cp.cep_lat,cp.cep_long from cnp_cnpj cc
                        left join mun_municipio mm on mm.muni_cod = cc.muncipio
                        left join em_empresas ee on ee.cnpj = cc.cnpj
                        left join cep_lat_long cp on cp.cep=cc.cep
                        left join cnae_cnaes_ cna on cna.cna_subclass = cc.cnae_principal
                        WHERE cc.cnae_principal {query_cna} AND cc.uf IN {tuple(states)}) AS tt '''
        
    print(query_cnpj)


    count_cnae = len(cnae_names)
    result_cnpj = db.session.execute(text(query_cnpj)).fetchall()
    df = pd.DataFrame(result_cnpj)
    df['data_incio_atividade']=pd.to_datetime(df['data_incio_atividade'].astype(str),format='%Y-%m-%d')
    dfo=df.copy()
    dfo['dif'] = dfo['data_incio_atividade'].dt.to_period('M')
    dft = pd.DataFrame(dfo[['cna_name','dif']].groupby('dif').count()).reset_index()
    dft['dif']=pd.to_datetime(dft['dif'].astype(str) + '-01')
    dft['dif']=pd.to_datetime(dft['dif'] + pd.offsets.MonthEnd(0),format='%Y-%m-%d')
    dft.sort_values(by='dif',ascending=True,inplace=True)
    
    hoje = datetime.now().date()
    tres_mes_atras = hoje - pd.DateOffset(months=3)
    seis_meses_atras = hoje - pd.DateOffset(months=6)
    um_ano_atras = hoje - pd.DateOffset(years=1)
    cinco_anos_atras = hoje - pd.DateOffset(years=5)
    dez_anos_atras = hoje - pd.DateOffset(years=10)
    df_tres_mes_atras = dft[dft['dif'] > tres_mes_atras]['cna_name'].sum()
    df_seis_meses_atras = dft[dft['dif'] > seis_meses_atras]['cna_name'].sum()
    df_um_ano_atras = dft[dft['dif'] > um_ano_atras]['cna_name'].sum()
    df_cinco_anos_atras = dft[dft['dif'] > cinco_anos_atras]['cna_name'].sum()
    df_dez_anos_atras = dft[dft['dif'] > dez_anos_atras]['cna_name'].sum()
    print(df_dez_anos_atras)
    scroll = {'tres_meses':str(df_tres_mes_atras),'seis_meses':str(df_seis_meses_atras),'um_ano':str(df_um_ano_atras),'cinco_anos':str(df_cinco_anos_atras),'dez_anos':str(df_dez_anos_atras)}

    if len(df)>0:
        df['data_situacao_cadastral']=pd.to_datetime(df['data_situacao_cadastral'].astype(str),format='%Y-%m-%d')
        df['data_situacao_cadastral']=df['data_situacao_cadastral'].astype(str)
        df['data_incio_atividade']=pd.to_datetime(df['data_incio_atividade'].astype(str),format='%Y-%m-%d')
        df=df.sort_values(by='data_incio_atividade')
        df_dates = df[(df['data_incio_atividade']>=f'{dates[0]}') & (df['data_incio_atividade']<f'{dates[1]}')]
        df_size_ = df_dates[df_dates['porte'].isin(size)]
        df_size =df_size_[df_size_['cod_nat_juri_'].isin(cod_nat)]

        try:
            mkt_rate_dict = []
            list_razao = []
            for cnae in df_size['cna_name'].unique().tolist():
                df2=df_size[df_size['cna_name']==cnae].sort_values('data_incio_atividade',ascending=False)
                df2['Dif_Meses'] =df2['data_incio_atividade'].dt.to_period('M')

                df_qtd = pd.DataFrame(df2[['cna_name','Dif_Meses']].groupby('Dif_Meses').count()).reset_index()
                p_valor = df_qtd['cna_name'].iloc[0]
                r_valor = df_qtd['cna_name'].iloc[1:].sum()

                razao_s = r_valor - p_valor
                list_razao.append(razao_s)
                df_qtd['Dif_Meses'] = pd.to_datetime(df_qtd['Dif_Meses'].astype(str) + '-01')
                df_qtd['Dif_Meses']=pd.to_datetime(df_qtd['Dif_Meses'] + pd.offsets.MonthEnd(0),format='%Y-%m-%d')
                df_qtd['Dif_Meses']=df_qtd['Dif_Meses'].astype(str)
                df_qtd.columns=['Dif_Meses','count']

                print(df_qtd)

                mkt_rate_dict.append({f'{cnae}':df_qtd.to_dict(orient='records')})
            razao=sum(list_razao)
        except Exception as e:
            print(e)
            mkt_rate_dict='0'
            razao = 0

        count_cnpj = len(df_size)
        df_qtd = df[['cna_name','data_incio_atividade']].groupby('cna_name').count()
        print(pd.DataFrame(df_qtd))

        df_size.loc[df_size['capital_social'].isnull(), 'capital_social'] = 1
        df_size['capital_social']=df_size['capital_social'].astype(float)
        map_fat = {'Micro Empresa':'Igual ou inferior a R$360.000','Empresa de Pequeno porte':'Igual ou inferior a R$4.800.000,00 e superior a R$360.000,00','Demais':'Faturamento Não informado na Receita'}


        df_size['Faturamento']=df_size['porte'].map(map_fat)
        df_size['idade_']=df_size['idade'].apply(find_int)

        mean_age = df_size['idade_'].mean()

        

        df_quartile=df[['data_incio_atividade']]

        q1,q2,q3=df_quartile['data_incio_atividade'].quantile([0.25, 0.5, 0.75])

        contagem_q1 = df_size[df_size['data_incio_atividade'] <= q1].count()[0]

        contagem_q2 = df_size[(df_size['data_incio_atividade'] > q1) & (df_size['data_incio_atividade'] <= q2)].count()[0]

        contagem_q3 = df_size[(df_size['data_incio_atividade'] > q2) & (df_size['data_incio_atividade'] <= q3)].count()[0]

        maior_quartil = max([contagem_q1,contagem_q2,contagem_q3])

        if maior_quartil==contagem_q1:
            market_trend = 'Alta'
        elif maior_quartil == contagem_q2:
            market_trend = 'Média'
        else:
            market_trend='Baixa'

        mkt_result = []

        for value in df_size['capital_social']:
            if value>1000000000:
                mkt_result.append(0)
            else:
                mkt_result.append(value)    

        market_size = sum(mkt_result)

        df_size['data_incio_atividade']=df_size['data_incio_atividade'].astype(str)
        count_age = df_size['idade'].value_counts().reset_index()
        count_age.columns=['age','count']
        count_age=count_age.sort_values(by='count', ascending=False)


        count_size = df_size['porte'].value_counts().reset_index()
        count_size.columns=['size','count']
        count_size=count_size.sort_values(by='count', ascending=False)

        count_state = df_size['uf'].value_counts().reset_index()
        count_state.columns=['state','count']

        count_state=count_state.sort_values(by='count', ascending=False)

        list_cnpj = df_size.to_dict(orient='records')
        count_age_dict = count_age.to_dict(orient='records')
        count_size_dict = count_size.to_dict(orient='records')
        count_state_dict = count_state.to_dict(orient='records')

        final_dict = {
        'listCnpj': list_cnpj,
        'count_cnae': str(count_cnae),
        'count_cnpj': str(count_cnpj),
        'count_age': count_age_dict,
        'count_size': count_size_dict,
        'count_state': count_state_dict,
        'market_size': str(market_size),
        'market_growth': str(razao),
        'market_trend': market_trend,
        'market_growing':mkt_rate_dict,
        'mean_age':str(round(mean_age,2)),
        'scroll':scroll
                }
        return json.dumps(final_dict,indent=4)
    else:
        final_dict = {'listCnpj':[],'count_cnae':0,'count_cnpj':0,'market_size':0}
        return json.dumps(final_dict,indent=4)
    

@app.route(f'/v2/fcnpj', methods=['POST'])
def fcnpj():
    data = request.get_json() 
   
    response = json.loads(request.data)
    cities = response.get("municipio")
    states = response.get("states")
    size= response.get("porte")

    cnae_names = response.get('activities')
    if len(cnae_names)==1:
        query_cnaes = f"SELECT cna_subclass FROM cnae_cnaes_ WHERE cna_name IN ('{cnae_names[0]}') AND cna_subclass <> '' ORDER BY cna_subclass"
    else:
        query_cnaes = f'''SELECT cna_subclass FROM cnae_cnaes_ WHERE cna_name IN {tuple(cnae_names)} AND cna_subclass <> '' ORDER BY cna_subclass'''
    
    if len(cnae_names)==1:
        query_cna = f"IN ('{result[0]}')"
    else:
        query_cna = f"IN {tuple(result)}"


    print(len(states))

    result = db.session.execute(text(query_cnaes)).fetchall()
    values = [row[0] for row in result]
    result = [value for value in values]
    if cities is not None:
        print('none')
        municipio=find(cities)[0]

    if len(states)==1:
        if len(municipio)==1:
            query_cnpj = f'''SELECT tt.cnpj_ as cnpj,tt.nome_fantasia,tt.idade,tt.cna_name,ee.razao_social,ee.porte,ee.capital_social,ee.cod_nat_juri_,ee.qual_respons_,tt.uf,tt.data_situacao_cadastral,tt.data_incio_atividade,tt.telefone, tt.muni_name,tt.logradouro ,tt.tipo_logradouro ,tt.complemento,tt.bairro ,tt.cep,tt.cep_lat,tt.cep_long
                        FROM (
                        SELECT cc.cnpj_,cc.cnpj,cna.cna_name,cc.nome_fantasia,cc.idade,cc.uf,cc.data_situacao_cadastral,cc.data_incio_atividade,cc.telefone, mm.muni_name,cc.logradouro ,cc.tipo_logradouro ,cc.complemento,cc.bairro ,cc.cep,cp.cep_lat,cp.cep_long from cnp_cnpj cc
                        left join mun_municipio mm on mm.muni_cod = cc.muncipio
                        left join cnae_cnaes_ cna on cna.cna_subclass = cc.cnae_principal
                        left join cep_lat_long cp on cp.cep=cc.cep 
                        WHERE cc.cnae_principal  {query_cna} AND cc.uf='{states[0]}' AND  cc.sit_cadastral = 'Ativa' ) AS tt 
                        left join em_empresas ee on ee.cnpj = tt.cnpj 
                        where tt.muni_name = '{cities[0]} LIMIT 50' 
                        '''
        else:
            print('else')
            #municipio=find(cities)[0]
            query_cnpj = f'''SELECT tt.cnpj_ as cnpj,tt.nome_fantasia,tt.idade,tt.cna_name,tt.razao_social,tt.porte,tt.capital_social,tt.cod_nat_juri_,tt.qual_respons_,tt.uf,tt.data_situacao_cadastral,tt.data_incio_atividade,tt.telefone, tt.muni_name,tt.logradouro ,tt.tipo_logradouro ,tt.complemento,tt.bairro ,tt.cep,tt.cep_lat,tt.cep_long
                        FROM (
                        SELECT cc.cnpj_,cc.cnpj,cna.cna_name,cc.nome_fantasia,cc.idade,ee.razao_social,ee.porte,ee.capital_social,ee.cod_nat_juri_,ee.qual_respons_,cc.uf,cc.data_situacao_cadastral,cc.data_incio_atividade,cc.telefone, mm.muni_name,cc.logradouro ,cc.tipo_logradouro ,cc.complemento,cc.bairro ,cc.cep,cp.cep_lat,cp.cep_long from cnp_cnpj cc
                        left join mun_municipio mm on mm.muni_cod = cc.muncipio
                        left join em_empresas ee on ee.cnpj = cc.cnpj
                        left join cep_lat_long cp on cp.cep=cc.cep
                        left join cnae_cnaes_ cna on cna.cna_subclass = cc.cnae_principal
                        WHERE cc.cnae_principal {query_cna} AND cc.uf='{states[0]}' AND  cc.sit_cadastral = 'Ativa' ) AS tt where tt.muni_name IN {tuple(municipio)} LIMIT 50'''
    else:
        query_cnpj = f'''SELECT tt.cnpj_ as cnpj,tt.nome_fantasia,tt.idade,tt.cna_name,tt.razao_social,tt.porte,tt.capital_social,tt.cod_nat_juri_,tt.qual_respons_,tt.uf,tt.data_situacao_cadastral,tt.data_incio_atividade,tt.telefone, tt.muni_name,tt.logradouro ,tt.tipo_logradouro ,tt.complemento,tt.bairro ,tt.cep,tt.cep_lat,tt.cep_long
                        FROM (
                        SELECT cc.cnpj_,cc.cnpj,cna.cna_name,cc.nome_fantasia,cc.idade,ee.razao_social,ee.porte,ee.capital_social,ee.cod_nat_juri_,ee.qual_respons_,cc.uf,cc.data_situacao_cadastral,cc.data_incio_atividade,cc.telefone, mm.muni_name,cc.logradouro ,cc.tipo_logradouro ,cc.complemento,cc.bairro ,cc.cep,cp.cep_lat,cp.cep_long from cnp_cnpj cc
                        left join mun_municipio mm on mm.muni_cod = cc.muncipio
                        left join em_empresas ee on ee.cnpj = cc.cnpj
                        left join cep_lat_long cp on cp.cep=cc.cep
                        left join cnae_cnaes_ cna on cna.cna_subclass = cc.cnae_principal
                        WHERE cc.cnae_principal {query_cna} AND cc.uf IN {tuple(states)} AND  cc.sit_cadastral = 'Ativa' ) AS tt LIMIT 50 '''
        
    #print(query_cnpj)


    count_cnae = len(cnae_names)
    result_cnpj = db.session.execute(text(query_cnpj)).fetchall()
    df = pd.DataFrame(result_cnpj)
    df['data_situacao_cadastral']=df['data_situacao_cadastral'].astype(str)
    df['data_incio_atividade']=df['data_incio_atividade'].astype(str)
    if len(df)>0:

        df_size = df[df['porte'].isin(size)]
        print(df_size['porte'])

        count_cnpj = len(df_size)
        

        df_size.loc[df_size['capital_social'].isnull(), 'capital_social'] = 1
        df_size['capital_social']=df_size['capital_social'].astype(float)

        

        market_size = sum(df_size['capital_social'])


        count_age = df_size['idade'].value_counts().reset_index()
        count_age.columns=['age','count']
        count_age=count_age.sort_values(by='count', ascending=False)


        count_size = df_size['porte'].value_counts().reset_index()
        count_size.columns=['size','count']
        count_size=count_size.sort_values(by='count', ascending=False)

        count_state = df_size['uf'].value_counts().reset_index()
        count_state.columns=['state','count']

        count_state=count_state.sort_values(by='count', ascending=False)

        list_cnpj = df_size.to_dict(orient='records')
        count_age_dict = count_age.to_dict(orient='records')
        count_size_dict = count_size.to_dict(orient='records')
        count_state_dict = count_state.to_dict(orient='records')

        final_dict = {
        'listCnpj': list_cnpj,
        'count_cnae': count_cnae,
        'count_cnpj': count_cnpj,
        'count_age': count_age_dict,
        'count_size': count_size_dict,
        'count_state': count_state_dict,
        'market_size': market_size
                }
        return json.dumps(final_dict,indent=4)
    else:
        final_dict = {'listCnpj':[],'count_cnae':0,'count_cnpj':0,'market_size':0}
        return json.dumps(final_dict,indent=4)

        


@app.route(f'/v2/munlist', methods=['POST'])
def munlist():
    response = json.loads(request.data)
    state = response.get("states")
    connection = connectionDataBase()

    cursor=connection.cursor()

    query = cursor.execute(f"SELECT DISTINCT st.muni_name from state_muni st  WHERE state='{state[0]}'")

    query = cursor.fetchall()

    print(query)

    final_dict = {"muni_name":[x[0].upper() for x in query ]}
 

    return json.dumps(final_dict,indent=4)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8080, debug=True, threaded=True, processes=1)
    # app.run(host="104.131.163.240", port=8080)
