import yfinance as yf
import  pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os 

load_dotenv()

commodities = ['CL=F','GC=F','SI=F'] #lista para guardar os ativos 

# variáveis para conexão com o banco de dados Postgres utilizando o Render
DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD') 
DB_SCHEMA= os.getenv('DB_SCHEMA_PROD')

# url de conexão
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

#criação de uma engine para conectar ao banco
engine = create_engine(DATABASE_URL)

#função que pega as informações das Commodities
def buscar_dados_commodities(ticker, periodo='5d', intervalo='1d'):
    yf_ticker = yf.Ticker(ticker)
   ## ticker = yf.Ticker('CL=F') # retorna as informações do ticker
    dados = yf_ticker.history(period=periodo, interval=intervalo)[['Close']] # retorna um data frame com os dados dos parametros
    dados['ticker'] = ticker # criando uma coluna nova para pegar a informação do tickeer
    return dados

# função para pegar todos os dados dos tickers
def buscar_todos_dados_commodities(commodities):
    todos_dados = [] # variável do tipo lista que vai guardar todos os dados
    for ticker in commodities:
        dados = buscar_dados_commodities(ticker)
        todos_dados.append(dados)
    return pd.concat(todos_dados)

# função para salvar no banco de dados
def salvar_no_postgres (df, schema='public'):
    df.to_sql('commodities', engine, if_exists='replace', index=True, index_label='Date', schema=schema) # parametros do banco: tabela, engine, qual tipo de inserção...etc

if __name__ == "__main__":
    dados_concatenados = buscar_todos_dados_commodities(commodities)
    #print(dados_concatenados) #usado na construção do código para visualizar os dados
    
    # salvar no BD
    salvar_no_postgres (dados_concatenados, schema='public')
