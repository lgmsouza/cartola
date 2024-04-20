import requests
import pandas as pd
import sqlalchemy
from sqlalchemy import text
import json
import get_atleta_id

with open('utils.json','r') as f:
    utils = json.load(f)
    
ACCESS_TOKEN = utils['token']
PASSWORD = utils['password']

class CartolaFC(object):
    
    access_token = None
    base_url = "https://api.cartola.globo.com"
    
    def __init__(self,access_token,password,*args, **kwargs):
        super().__init__(*args, **kwargs)
        self.access_token = access_token
        self.password = password
        
    def get_headers_auth(self):
        access_token = self.access_token
        
        return {
                "Content-Type": "application/json",
                "X-GLB-Auth": "oidc",
                "X-GLB-Tag": "1502374855",
                "Authorization" : f"Bearer {access_token}"
    }
    
    def get_headers(self):
        
        return {
                'Content-type': 'application/json'
    }

    
    def get_info_time(self):
                
        base_url = self.base_url
        endpoint = f"{base_url}/auth/time"
        headers = self.get_headers_auth()        
        r = requests.get(endpoint, headers=headers)
        return r.json()        
    
    def get_players_database(self):
        
        password = self.password
        engine = sqlalchemy.create_engine(f'mysql://root:{password}@localhost/my_database')
        query = "SELECT * FROM cartola_fc.atletas;"
        with engine.connect() as con:
            return pd.read_sql(text(query), con)
        
    def check_rodada(self):
        
        rodada_banco = self.get_players_database().rodada_id.max()
        rodada_atual = self.get_info_time()['rodada_atual']
        if not (rodada_banco == rodada_atual-1):
            get_atleta_id.run()     
            return self.check_rodada()
        return True
        
    def get_esquema(self):
        return 3
            
    def get_titulares(self):
        
        time={1:[{'gatito':'botafogo'}],
              2:[{'cuiabano':'gremio'},{'juninho':'bragantino'}],
              3:[{'kaique':'atletico'},{'geromel':'gremio'}],
              4:[{'lima':'fluminense'},{'erick':'atletico'},{'cristaldo':'gremio'}],      
              5:[{'soteldo':'gremio'},{'canobbio':'atletico'},{'cano':'fluminense'}],
              6:[{'gaucho':'gremio'}]}
        
        df_atletas = self.get_players_database()
        rodada = self.get_info_time()['rodada_atual']-1
        lista_atleta = list()
        preco_jogadores = 0
        preco_min = {1:100,2:100,3:100,4:100,5:100,6:100}
        for posicao,jogador in time.items():
            for i in jogador:
                jogador=list(i.keys())[0]
                clube=i.get(jogador)
                id_posicao=posicao
                query=df_atletas.loc[(df_atletas.slug.str.contains(jogador))&
                                    (df_atletas.posicao_id==id_posicao)&
                                    (df_atletas.slug_clube.str.contains(clube))&
                                    (df_atletas.rodada_id==rodada)]
                lista_atleta.append(query['atleta_id'].item())
                preco_jogadores += (query['preco_num'].item())
                if preco_min[posicao] > (query['preco_num'].item()):
                    preco_min[posicao] = (query['preco_num'].item())
                  
        return lista_atleta,preco_jogadores,preco_min
    
    def get_capitao(self):
        
        df_atletas = self.get_players_database()
        rodada = self.get_info_time()['rodada_atual']-1
        jogador = 'cano'
        id_posicao = 5
        clube = 'fluminense'
        query=df_atletas.loc[(df_atletas.slug.str.contains(jogador))&
                             (df_atletas.posicao_id==id_posicao)&
                             (df_atletas.slug_clube.str.contains(clube))&
                             (df_atletas.rodada_id==rodada)]
        
        return query['atleta_id'].item()
    
    def get_reservas(self):
        
        df_atletas = self.get_players_database()
        rodada = self.get_info_time()['rodada_atual']-1
        lista_atleta,_,preco_min = self.get_titulares()
        reservas = dict()
        
        if (self.get_esquema()==1)|(self.get_esquema()==2):
            posicoes = [1,3,4,5]
            
        posicoes = [1,2,3,4,5]     
        
        for posicao in posicoes:
            query=df_atletas.loc[(df_atletas.posicao_id==posicao)&
                                 (df_atletas.preco_num<=preco_min[posicao])&
                                 (df_atletas.rodada_id==rodada)&
                                 (~df_atletas.atleta_id.isin(lista_atleta))]
            posicao_str = str(posicao)
            reservas[posicao_str] = query.sort_values('preco_num',ascending=False).head(1)['atleta_id'].item()
            
        return reservas
    
    def get_patrimonio(self):
        
        data = self.get_info_time()
        
        return data['patrimonio']
    
    def check_cartoletas(self):
        
        _, valor_jogadores,_ = self.get_titulares()
        patrimonio = self.get_patrimonio()
        
        if valor_jogadores < patrimonio:
            return True
        return False
    
    def post_time(self):
        
        if not self.check_rodada():
            return "Atualize a Base de Jogadores!"
        
        if not self.check_cartoletas():
            return 'Cartoletas Insuficientes'
           
        esquema = self.get_esquema()
        titulares,_,_ = self.get_titulares()
        capitao = self.get_capitao()
        reservas = self.get_reservas()
        headers = self.get_headers_auth()
        base_url = self.base_url
        endpoint = f'{base_url}/auth/time/salvar'            
        json = {"esquema":esquema,
                "atletas":titulares,
                "capitao":capitao,
                "reservas":reservas}
        r = requests.post(endpoint,json=json,headers=headers)
        return r.json()
    

cartola = CartolaFC(ACCESS_TOKEN,PASSWORD)

print(cartola.post_time())