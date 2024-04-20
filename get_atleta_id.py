import requests
import json
import sqlalchemy
import pandas as pd

def get_atleta_info(data):
    aux=0
    for i in range(len(data)):
        dados_atleta=data[i]
        df2=pd.DataFrame(dados_atleta)
        df3 = df2[["minimo_para_valorizar", "jogos_num", "atleta_id", "rodada_id",
                            "clube_id", "posicao_id", "status_id", "pontos_num",
                            "media_num", "variacao_num", "preco_num", "entrou_em_campo",
                            "slug", "apelido", "apelido_abreviado", "nome"]].copy()
        if aux == 0:
            df=df3.copy()
        else:
            df = pd.concat([df,df3],ignore_index=True)
        aux+=1
        
    return df

def get_clube_info(data,df):    
    aux=0
    for i in df.clube_id.unique():
        df_clube = pd.DataFrame(data[i])
        df_clube2 = df_clube[['nome','abreviacao','slug','id']]
        if aux == 0:
            dfclube=df_clube2.copy()
        else:
            dfclube = pd.concat([dfclube,df_clube2],ignore_index=True)
        aux+=1
    return dfclube
    
def update_table(df,PASSWORD): 

    engine = sqlalchemy.create_engine(f'mysql://root:{PASSWORD}@localhost/my_database')
    with engine.begin() as connection:    
        df.to_sql('atletas', con=connection,schema='cartola_fc',if_exists='append', index=False)
        
def run():
    with open('utils.json','r') as f:
        utils = json.load(f)    
    PASSWORD = utils['password']
    
    r = requests.get('https://api.cartola.globo.com/atletas/mercado')
    r2 = requests.get('https://api.cartola.globo.com/clubes')

    data = r.json()['atletas']
    data2 = r2.json()
    
    df = get_atleta_info(data)   
    df = df.drop_duplicates()
    df['clube_id'] = df.clube_id.astype('str')
    
    dfclube = get_clube_info(data2,df) 

    df_clube = dfclube.drop_duplicates().rename(columns={'nome':'clube',
                                                         'abreviacao':'abreviacao_clube',
                                                         'slug':'slug_clube',
                                                         'id':'clube_id'})
    
    df_clube['clube_id'] = df_clube.clube_id.astype('str')
    df_final = df.merge(df_clube, on='clube_id', how='left')
    
    update_table(df_final,PASSWORD)
    
if __name__ == '__main__':
    run()
        
    