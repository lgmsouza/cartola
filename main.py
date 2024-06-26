import requests
import pandas as pd
import json
from selenium import webdriver
from bs4 import BeautifulSoup
# import chromedriver_autoinstaller

with open('utils.json','r') as f:
    utils = json.load(f)
    
ACCESS_TOKEN = utils['token']

class CartolaFC(object):
    
    access_token = None
    base_url = "https://api.cartola.globo.com"
    
    def __init__(self,access_token,*args, **kwargs):
        super().__init__(*args, **kwargs)
        self.access_token = access_token
        
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
    
    def get_atleta_info(self):
        
        base_url = self.base_url
        headers = self.get_headers()
        endpoint = f'{base_url}/atletas/mercado'
        r = requests.get(endpoint,headers=headers)
        data = r.json()['atletas']        
        aux=0
        for i in range(len(data)):
            dados_atleta=data[i]
            df2=pd.DataFrame(dados_atleta)
            df3=df2[["minimo_para_valorizar", "jogos_num", "atleta_id", "rodada_id",
                     "clube_id", "posicao_id", "status_id", "pontos_num",
                     "media_num", "variacao_num", "preco_num", "entrou_em_campo",
                     "slug", "apelido", "apelido_abreviado", "nome"]].copy()           
            if aux == 0:
                df=df3.copy()
            else:
                df = pd.concat([df,df3],ignore_index=True)
            aux+=1
            
        df['clube_id'] = df.clube_id.astype('str')                  
        return df.drop_duplicates()
        
    def get_players_database(self):
         
        base_url = self.base_url
        headers = self.get_headers()
        endpoint = f'{base_url}/clubes'
        df = self.get_atleta_info()
        r = requests.get(endpoint,headers=headers)
        data = r.json()      
        aux=0
        for i in df.clube_id.unique():
            df_clube = pd.DataFrame(data[i])
            df_clube2 = df_clube[['nome','abreviacao','slug','id']]
            if aux == 0:
                dfclube=df_clube2.copy()
            else:
                dfclube = pd.concat([dfclube,df_clube2],ignore_index=True)
            aux+=1
            
        df_clube = dfclube.rename(columns={'nome':'clube',
                                           'abreviacao':'abreviacao_clube',
                                           'slug':'slug_clube',
                                           'id':'clube_id'}).drop_duplicates()
        df_clube['clube_id'] = df_clube.clube_id.astype('str')        
         
        return df.merge(df_clube, on='clube_id', how='left').drop_duplicates()
        
    def get_esquema(self):
        return 3
    
    def get_escalacao(self):
        
        driver = webdriver.Chrome()
        driver.get('https://gurudocartola.com/selecao-guru-cartola')
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        infos = soup.find_all('p')
        lista = list()
        # print(infos)
        for info in infos:
            lista.append((info.text).strip())            
        jogadores = lista[1:-6]
        dict_posicao = {'GOL':1,'LAT':2,'ZAG':3,'MEI':4,'ATA':5,'TEC':6}
        time = {1:[],2:[],3:[],4:[],5:[],6:[]}
        for jogador in jogadores:
            nome_jogador = jogador[:-6]
            posicao_jogador = jogador[-4:-1]
            cod = dict_posicao[posicao_jogador]
            time[cod].append(nome_jogador)
            
        return time

    def get_titulares(self):
        
        time = self.get_escalacao()
        
        df_atletas = self.get_players_database()
        rodada = self.get_info_time()['rodada_atual']-1
        lista_atleta = list()
        preco_jogadores = 0
        preco_min = {1:100,2:100,3:100,4:100,5:100,6:100}
        for posicao,jogadores in time.items():
            for jogador in jogadores:
                id_posicao=posicao
                query=df_atletas.loc[(df_atletas.apelido.str.contains(jogador))&
                                     (df_atletas.posicao_id==id_posicao)&
                                     (df_atletas.rodada_id==rodada)].head(1)

                lista_atleta.append(query['atleta_id'].item())
                preco_jogadores += (query['preco_num'].item())
                if preco_min[posicao] > (query['preco_num'].item()):
                    preco_min[posicao] = (query['preco_num'].item())
                  
        return lista_atleta,preco_jogadores,preco_min

    
    def get_capitao(self):
        
        df_atletas = self.get_players_database()
        # rodada = self.get_info_time()['rodada_atual']-1
        jogador = 'Cano'
        id_posicao = 5
        clube = 'fluminense'
        query=df_atletas.loc[(df_atletas.apelido.str.contains(jogador))&
                             (df_atletas.posicao_id==id_posicao)&
                             (df_atletas.slug_clube.str.contains(clube))].head(1)
              
        return query['atleta_id'].item()
    
    def get_reservas(self,titulares,preco_min):
        
        df_atletas = self.get_players_database()
        rodada = self.get_info_time()['rodada_atual']-1
        lista_atleta = titulares
        preco_min = preco_min
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
    
    def check_cartoletas(self,preco_jogadores):
        
        patrimonio = self.get_patrimonio()
        print(preco_jogadores)
        print(patrimonio)
        
        if preco_jogadores < patrimonio:
            return True
        return False
    
    def post_time(self):
        
                 
        esquema = self.get_esquema()
        titulares,preco_jogadores,preco_min = self.get_titulares()
        
        if not self.check_cartoletas(preco_jogadores):
            return 'Cartoletas Insuficientes'
        
        capitao = self.get_capitao()
        reservas = self.get_reservas(titulares,preco_min)
        headers = self.get_headers_auth()
        base_url = self.base_url
        endpoint = f'{base_url}/auth/time/salvar'            
        json = {"esquema":esquema,
                "atletas":titulares,
                "capitao":capitao,
                "reservas":reservas}
        print(json)
        r = requests.post(endpoint,json=json,headers=headers)
        return r.text
    

if __name__ == '__main__':
        
    cartola = CartolaFC(ACCESS_TOKEN)
    print(cartola.post_time())
    # print(cartola.get_info_time())