import requests


url1 = 'https://api5.comercialbi.com.br/v2/activities'
url2 = 'https://api5.comercialbi.com.br/v2/states'

def getActivities():
    requests.get(url1)
    response = requests.get(url1)
    data = response.json()["activites"]
    return data

def getStates():
    requests.get(url1)
    response = requests.get(url2)
    data = response.json()["states"]
    return data


# Lista de estados brasileiros e suas respectivas siglas
estados_brasileiros = {
    'Acre': 'AC', 'Alagoas': 'AL', 'Amapá': 'AP', 'Amazonas': 'AM', 'Bahia': 'BA',
    'Ceará': 'CE', 'Distrito Federal': 'DF', 'Espírito Santo': 'ES', 'Goiás': 'GO',
    'Maranhão': 'MA', 'Mato Grosso': 'MT', 'Mato Grosso do Sul': 'MS', 'Minas Gerais': 'MG',
    'Pará': 'PA', 'Paraíba': 'PB', 'Paraná': 'PR', 'Pernambuco': 'PE', 'Piauí': 'PI',
    'Rio de Janeiro': 'RJ', 'Rio Grande do Norte': 'RN', 'Rio Grande do Sul': 'RS',
    'Rondônia': 'RO', 'Roraima': 'RR', 'Santa Catarina': 'SC', 'São Paulo': 'SP',
    'Sergipe': 'SE', 'Tocantins': 'TO'
}

# Função que recebe o nome de um estado e retorna a sigla em letras maiúsculas
def obter_sigla_estado(nome_estado):
    sigla = estados_brasileiros.get(nome_estado)
    if sigla:
        return sigla.upper()
    else:
        return "Estado não encontrado"

# Exemplo de uso
estado = "Minas Gerais"
print(f"A sigla de {estado} é {obter_sigla_estado(estado)}")

import psycopg2 as pg
import pandas as pd

def connectionDataBase():
    connection = pg.connect(user="postgres", password="2023@Tag", host="159.65.42.225", port=5432, database="comercial_BI_Relational")
    print(connection)
    return connection
conn = connectionDataBase()

cursor = conn.cursor()

def dataframefull(ativi):
    query = f'''select cc.cnpj_  as CNPJ, CC.nome_fantasia as NOME_FANTASIA, cc.cnae_principal_ as CNAE, cc.email ,cc.telefone ,cc.cep ,cc.logradouro ,cc.tipo_logradouro ,cc.uf ,cc.muncipio_  from cnp_cnpj_ cc where cc.cnae_principal_ like '%{ativi}%' and cc.sit_cadastral ='Ativa' and cc.telefone <>'00nan00000' '''
    print(query)
    query=cursor.execute(query)
    query=cursor.fetchall()

    nomes_colunas = [desc[0] for desc in cursor.description]
    df_size = pd.DataFrame(query,columns=nomes_colunas)
    print(df_size.head())
    return df_size

def dataframe(ativi,state):
    query = f'''select cc.cnpj_  as CNPJ, CC.nome_fantasia as NOME_FANTASIA, cc.cnae_principal_ as CNAE, cc.email ,cc.telefone ,cc.cep ,cc.logradouro ,cc.tipo_logradouro ,cc.uf ,cc.muncipio_  from cnp_cnpj_ cc where cc.cnae_principal_ like '%{ativi}%' and cc.sit_cadastral ='Ativa' and cc.telefone <>'00nan00000' and cc.uf = '{state}' '''
    print(query)
    query=cursor.execute(query)
    query=cursor.fetchall()

    nomes_colunas = [desc[0] for desc in cursor.description]
    df_size = pd.DataFrame(query,columns=nomes_colunas)
    print(df_size.head())
    return df_size