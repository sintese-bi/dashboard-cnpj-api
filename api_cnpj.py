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





#db_password = 
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
app.config['TIMEOUT'] = 800
db = SQLAlchemy(app)
CORS(app)

VERSION = "v2"

def connectionDataBase():
    connection = "pg string connect")
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

@app.route(f'/{VERSION}/email', methods=['POST'])
def email():
    response = json.loads(request.data)
    cnae_names = response.get('activities')
    query_cnaes = f''' SELECT cc.cnae_principal_,cc.data_incio_atividade from cnp_cnpj_ cc where cc.cnae_principal_ IN {tuple(cnae_names)} 
                and cc.data_incio_atividade >= CURRENT_DATE - INTERVAL '3 months' '''

    result = db.session.execute(text(query_cnaes)).fetchall()

    values = [row[0] for row in result]

    result = [value for value in values]

    df = pd.DataFrame(result)

    df['data_incio_atividade']=pd.to_datetime(df['data_incio_atividade'].astype(str),format='%Y-%m-%d')
    df=df.sort_values(by='data_incio_atividade')
    df_quartile=df[['data_incio_atividade']]

    q1,q2,q3=df_quartile['data_incio_atividade'].quantile([0.25, 0.5, 0.75])

    contagem_q1 = df[df['data_incio_atividade'] <= q1].count()[0]

    contagem_q2 = df[(df['data_incio_atividade'] > q1) & (df['data_incio_atividade'] <= q2)].count()[0]

    contagem_q3 = df[(df['data_incio_atividade'] > q2) & (df['data_incio_atividade'] <= q3)].count()[0]

    maior_quartil = max([contagem_q1,contagem_q2,contagem_q3])


    if maior_quartil==contagem_q1:
        market_trend = 'Alta'
    elif maior_quartil == contagem_q2:
        market_trend = 'Média'
    else:
        market_trend='Baixa'


    df_qtd=pd.DataFrame(df['data_incio_atividade'].value_counts()).reset_index().sort_values(by='data_incio_atividade',ascending=False)
    dicionario = {'market_trend':market_trend}






@app.route(f'/{VERSION}/cities', methods=['GET'])
def cities():
    
    connection = connectionDataBase()

    cursor=connection.cursor()
    state = request.args.getlist('state')
    print(state)
    if len(state)==1:
        query = cursor.execute(f"SELECT DISTINCT sm.state_mun from state_muni sm WHERE sm.state='{state[0]}'")

        query = cursor.fetchall()

        print(query)

        json_data = {"cities":[x[0].upper() for x in query ]}
    else:
        query = cursor.execute(f"SELECT DISTINCT sm.state_mun from state_muni sm WHERE sm.state IN {tuple(state)}")

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
    cod_nat = [x.lower() for x in cod_nat]
    print(dates)
    cnae_names = response.get('activities')
    cnae_names=[item for item in cnae_names if item is not None]
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
    else:
        municipio=[]

    print(result)
    if len(states)==1:

        query_cnpj = f'''SELECT distinct tt.cnpj_ as cnpj,tt.nome_fantasia,tt.sit_cadastral,tt.idade,tt.cna_name,ee.razao_social,ee.porte,ee.capital_social,ee.cod_nat_juri_,ee.qual_respons_,tt.uf,tt.data_situacao_cadastral,tt.data_incio_atividade,tt.telefone,tt.email, tt.muni_name,tt.logradouro ,tt.tipo_logradouro ,tt.complemento,tt.bairro ,tt.cep,tt.cep_lat,tt.cep_long
                        FROM (
                        SELECT cc.cnpj_,cc.cnpj,cna.cna_name,cc.nome_fantasia,cc.sit_cadastral,cc.idade,cc.uf,cc.data_situacao_cadastral,cc.data_incio_atividade,cc.telefone, mm.muni_name,cc.logradouro ,cc.tipo_logradouro ,cc.complemento,cc.bairro ,cc.email,cc.cep,cp.cep_lat,cp.cep_long from cnp_cnpj_ cc
                        left join mun_municipio mm on mm.muni_cod = cc.muncipio
                        left join cnae_cnaes_ cna on cna.cna_subclass = cc.cnae_principal
                        left join cep_lat_long cp on cp.cep=cc.cep 
                        WHERE cc.cnae_principal  {query_cna} AND cc.uf='{states[0]}' ) AS tt  
                        left join em_empresas ee on ee.cnpj = tt.cnpj limit 5000
                        '''

        # print('else')
        #     #municipio=find(cities)[0]
        #     query_cnpj = f'''SELECT distinct tt.cnpj_ as cnpj,tt.nome_fantasia,tt.idade,tt.cna_name,tt.razao_social,tt.porte,tt.capital_social,tt.cod_nat_juri_,tt.qual_respons_,tt.uf,tt.data_situacao_cadastral,tt.data_incio_atividade,tt.telefone, tt.muni_name,tt.logradouro ,tt.tipo_logradouro ,tt.complemento,tt.bairro ,tt.cep,tt.cep_lat,tt.cep_long
        #                 FROM (
        #                 SELECT cc.cnpj_,cc.cnpj,cna.cna_name,cc.nome_fantasia,cc.idade,ee.razao_social,ee.porte,ee.capital_social,ee.cod_nat_juri_,ee.qual_respons_,cc.uf,cc.data_situacao_cadastral,cc.data_incio_atividade,cc.telefone, mm.muni_name,cc.logradouro ,cc.tipo_logradouro ,cc.complemento,cc.bairro ,cc.cep,cp.cep_lat,cp.cep_long from cnp_cnpj cc
        #                 left join mun_municipio mm on mm.muni_cod = cc.muncipio
        #                 left join em_empresas ee on ee.cnpj = cc.cnpj
        #                 left join cep_lat_long cp on cp.cep=cc.cep
        #                 left join cnae_cnaes_ cna on cna.cna_subclass = cc.cnae_principal
        #                 WHERE cc.cnae_principal {query_cna} AND cc.uf='{states[0]}') AS tt ''
    else:
        query_cnpj = f'''SELECT distinct tt.cnpj_ as cnpj,tt.nome_fantasia,tt.sit_cadastral,tt.idade,tt.cna_name,tt.razao_social,tt.porte,tt.capital_social,tt.cod_nat_juri_,tt.qual_respons_,tt.uf,tt.data_situacao_cadastral,tt.data_incio_atividade,tt.telefone, tt.email,tt.muni_name,tt.logradouro ,tt.tipo_logradouro ,tt.complemento,tt.bairro ,tt.cep,tt.cep_lat,tt.cep_long
                        FROM (
                        SELECT cc.cnpj_,cc.cnpj,cna.cna_name,cc.nome_fantasia,cc.sit_cadastral,cc.idade,ee.razao_social,ee.porte,ee.capital_social,ee.cod_nat_juri_,ee.qual_respons_,cc.uf,cc.data_situacao_cadastral,cc.data_incio_atividade,cc.telefone,cc.email, mm.muni_name,cc.logradouro ,cc.tipo_logradouro ,cc.complemento,cc.bairro ,cc.cep,cp.cep_lat,cp.cep_long from cnp_cnpj_ cc
                        left join mun_municipio mm on mm.muni_cod = cc.muncipio
                        left join em_empresas ee on ee.cnpj = cc.cnpj
                        left join cep_lat_long cp on cp.cep=cc.cep
                        left join cnae_cnaes_ cna on cna.cna_subclass = cc.cnae_principal
                        WHERE cc.cnae_principal {query_cna} AND cc.uf IN {tuple(states)}) AS tt limit 5000 '''
        
    print(query_cnpj)


    count_cnae = len(cnae_names)
    result_cnpj = db.session.execute(text(query_cnpj)).fetchall()
    df = pd.DataFrame(result_cnpj)
    if len(df)>0:
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

    
        df['data_situacao_cadastral']=pd.to_datetime(df['data_situacao_cadastral'].astype(str),format='%Y-%m-%d')
        df['data_situacao_cadastral']=df['data_situacao_cadastral'].astype(str)
        df['data_incio_atividade']=pd.to_datetime(df['data_incio_atividade'].astype(str),format='%Y-%m-%d')
        df=df.sort_values(by='data_incio_atividade')
        df_dates = df[(df['data_incio_atividade']>=f'{dates[0]}') & (df['data_incio_atividade']<=f'{dates[1]}')]
        df_size__ = df_dates[df_dates['porte'].isin(size)]
        df_size__['cod_nat_juri_'] = df_size__['cod_nat_juri_'].str.lower()
        df_size_ =df_size__[df_size__['cod_nat_juri_'].isin(cod_nat)]
        print(df_size_.columns)
        df_size_['st_muni']=df_size_['muni_name'] +' - ' + df_size_['uf']
        
        if len(municipio)>0:
            df_size = df_size_[df_size_['st_muni'].isin(municipio)]
        else:
            df_size = df_size_

        print(df_size_['st_muni'])

        try:
            mkt_rate_dict = []
            list_razao = []
            list_init = []
            list_sitCadastral=[]
            for cnae in df_size['cna_name'].unique().tolist():
                df2=df_size[df_size['cna_name']==cnae]
                df_qtd=pd.DataFrame(df2['data_incio_atividade'].value_counts()).reset_index().sort_values(by='data_incio_atividade',ascending=False)

                df_sitCadastral=pd.DataFrame(df_size['sit_cadastral'].value_counts()).reset_index()
                print(df_sitCadastral)
                list_sitCadastral.append({f'{cnae}':df_sitCadastral.to_dict(orient='records')})
                mkt_rate_dict.append({f'{cnae}':df_qtd.to_dict(orient='records')})
                list_razao.append(df_qtd['count'].sum())
                list_init.append(df_qtd['count'][0])
            
            razao=round((sum(list_razao)-sum(list_init))/sum(list_razao)*100,2)
            print(razao)
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
        print(razao)
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
        'sit_cadastral':list_sitCadastral,
        'mean_age':str(round(mean_age,2)),
        'scroll':scroll
                }
        return json.dumps(final_dict,indent=4)
    else:
        scroll = {'tres_meses':'0','seis_meses':'0','um_ano':'0','cinco_anos':'0','dez_anos':'0'}
        final_dict = {
        'listCnpj': [],
        'count_cnae': '0',
        'count_cnpj': '0',
        'count_age': [],
        'count_size': [],
        'count_state': [],
        'market_size': '0',
        'market_growth': '0',
        'market_trend': '-',
        'market_growing':[],
        'sit_cadastral':[],
        'mean_age':'0',
        'scroll':scroll
                }
        return json.dumps(final_dict,indent=4)
    

@app.route(f'/v2/scnpj', methods=['POST'])
def scnpj():
    try:
        response = json.loads(request.data)
        cnae_names = response.get('activities')
        cnae_names=[item for item in cnae_names if item is not None]
        query = f'''select cc.cnpj_  as CNPJ, ee.porte,ee.cod_nat_juri_,ee.capital_social,CC.nome_fantasia as NOME_FANTASIA, cc.cnae_principal_ as ATIVIDADE, cc.idade,cc.data_incio_atividade,cc.email as EMAIL ,cc.telefone as TELEFONE ,cc.cep as CEP ,cc.logradouro as LOGRADOURO  ,cc.tipo_logradouro as TIPO ,cc.uf as ESTADO ,cc.muncipio_ as MUNICIPIO  
                from cnp_cnpj_ cc 
                left join em_empresas ee on ee.cnpj = cc.cnpj where cc.cnae_principal_ like '%{cnae_names[0]}%' and cc.sit_cadastral ='Ativa' and cc.telefone <>'00nan00000'    '''
        result = db.session.execute(text(query)).fetchall()
        
        df = pd.DataFrame(result)
        df['data_incio_atividade']=pd.to_datetime(df['data_incio_atividade'].astype(str),format='%Y-%m-%d')
        list_cnpj = df.to_dict(orient='records')
        final_dict = {'listCnpj': list_cnpj}
        return json.dumps(final_dict,indent=4)
    except:
        final_dict = {'listCnpj':[]}
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
