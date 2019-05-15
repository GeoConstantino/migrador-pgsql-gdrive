import os
import re
import sys
import shutil
import logging
import logging.config
import unicodedata
import psycopg2 as pg
from pydrive.auth import GoogleAuth #credentials.json, settings.yaml, client_secrets.json
from pydrive.drive import GoogleDrive
from pandas.io import sql as psql
from decouple import config #settings.ini

#ID da pasta MP em Mapas - Dados no Google Drive
ID_MP = "1cvbybdy4dtZfO4o2dSq36qUxVadfUWxb"

try:
    ONE_VIEW = sys.argv[1]
except:
    ONE_VIEW = None


logging.config.fileConfig("simple_logging.ini")

logger = logging.getLogger()


def auth():

    return GoogleDrive(GoogleAuth().LocalWebserverAuth())


def upload_file(path, folder, filename, drive):

    if not os.path.exists(path):
        logger.error('Arquivo não encontrado: {}'.format(filename))
        return
    #print('Arquivo encontrado: {}'.format(path))
    
    id_file = find_files(filename,drive)

    id_folder_destiny = find_folder(folder,drive)
    file_metadata = {'id':id_file,'title': filename, 
        'parents': [{'kind': 'drive#fileLink',
        'id': id_folder_destiny}]}
    file = drive.CreateFile(file_metadata)
       
    try:
        file.SetContentFile(path)
        file.Upload()
    except NameError:
        logger.warning('Arquivo não encontrado.')
        return
    except :
        logger.warning('Erro de conexão na API do Google Drive.')
        return
    

def create_folder(foldername,drive):
# se a pasta não existir, ele a criará, se já existir, ele não
# apaga o conteúdo  
    if find_folder(foldername,drive):
        return

    try:
        folder_metadata = {'title':foldername, 'mimeType':'application/vnd.google-apps.folder', 'parents':  [{'kind': 'drive#fileLink','id': ID_MP}]}
        folder = drive.CreateFile(folder_metadata)
        folder.Upload()
    
    except drive.Erro as error:
        logger.error('Erro na criação da pasta: {}'.format(error))
        pass
        

def find_folder(name,drive):

    parameters = "'{}' in parents and trashed=false".format(ID_MP) 
    for file_list in drive.ListFile({'q':parameters}): 
        for file1 in file_list: 
            if file1['title'] == name:
                return (file1['id'])
    #print('ID não encontrado para {}'.format(name))


def find_files(name,drive):

    parameters = "'{}' in parents and trashed=false".format(ID_MP) 
    
    file_list = drive.ListFile({'q':parameters}).GetList()
    for file in file_list:
        parameters2 = "'{}' in parents and trashed=false".format(file['id'])
        file_list2 = drive.ListFile({'q':parameters2}).GetList()
        for file2 in file_list2:
            #import ipdb; ipdb.set_trace()
            if file2['title'] == name:
                return (file2['id'])
   

def salva_xlsx(df,folder,filename):

    directory = "out/" + folder
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    name = directory + "/" + filename + ".xlsx"
    
    try:
        df.to_excel(name, index=False,float_format="%.2f")
        logger.debug('Arquivo {} salvo com sucesso.'.format(name))
    except FileNotFoundError as error:
        logger.error('Erro ao salvar o arquivo: {}'.format(name))


def db_connect():
# return a cursor of the connection
    try:
        conn = pg.connect(user = config('USERDB'), 
                    password = config('PASSWORD'),
                    host = config('HOST'),
                    port = config('PORT'),
                    database = config('DATABASE'))
        return (conn)
    except pg.Error as erro:
        logger.error("Erro ao conectar no banco de dados: {}".format(erro))
        sys.exit()
    

def get_list_views(view=None):

    if view == None:
        sql = "select distinct area_mae, nome_tabela_pgadmin from datapedia.temas;"
    else:
        sql = "select distinct area_mae, nome_tabela_pgadmin from datapedia.temas where nome_tabela_pgadmin = '{}';".format(view)

    df = psql.read_sql(sql, db_connect())
    
    if not df.empty:

        df.columns = ['area','view']
        df.dropna(subset=['view'], inplace=True)
        df['area'] = (df['area'].str.lower().map(lambda x: unicodedata.normalize('NFKD',x).encode('ASCII','ignore').decode()))
        return df
        
    else:
        logger.error('view {} indisponível.'.format(view))
        return
    


if __name__ == '__main__':


    drive = auth()

    if not os.path.exists('out/'):
        os.makedirs('out')
    
    if not os.path.exists('log/'):
        os.makedirs('log')




    try:        
        connection = db_connect()
        cursor = connection.cursor()

        list_views = get_list_views(ONE_VIEW)

        for index, row in list_views.iterrows():
            try:
                sql = "SELECT * FROM {};".format(row['view'])
                try:
                    df = psql.read_sql(sql,connection)
                    #print('arquivo:',row['area'])
                    #print(df.head(2))
                    salva_xlsx(df,row['area'],row['view'])
                    logger.debug('leitura de view: "{}/{}"'.format(row['area'],row['view']))
                except:
                    logger.error("erro ao salvar arquivo para upload: '{}/{}'".format(row['area'],row['view']))
                    continue
            except (pg.Error, EnvironmentError) as error:
                logger.error("view não encontrada no DB: {}".format(row['view']))
                continue    
             
        for index, row in list_views.iterrows():
            #print('Tentando upload do arquivo {}'.format(row['view']))
            try:
                create_folder(row['area'],drive)
                path = 'out/{}/{}.xlsx'.format(row['area'],row['view'])
                upload_file(path,row['area'],row['view'],drive)
                logger.debug("upload concluído: '{}/{}'".format(row['area'],row['view']))

            except:
                logger.error('erro de upload no google drive: "{}/{}"'.format(row['area'], row['view']))
                continue

    except (pg.Error, NameError, AttributeError) as error:
        logger.error("Error while connecting to PostgreSQL.", error)

    finally:
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
        
        print('Fim do Processamento.')
        logger.error('Fim do Processamento')
        shutil.rmtree('out')

        