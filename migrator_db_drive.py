import argparse
import os
import logging
import re
import sys
import shutil
import unicodedata

from logging.handlers import RotatingFileHandler

import psycopg2 as pg

from decouple import config #settings.ini
from pandas.io import sql as psql
from pydrive.auth import GoogleAuth #credentials.json, settings.yaml, client_secrets.json
from pydrive.drive import GoogleDrive

#ID da pasta MP em Mapas - Dados no Google Drive
ID_MP = "1cvbybdy4dtZfO4o2dSq36qUxVadfUWxb"

log_formatter = logging.Formatter('%(asctime)s:::%(levelname)s:::%(filename)s:::%(lineno)s:::%(message)s')

logFile = 'log/migrator.log'

my_handler = RotatingFileHandler(logFile, mode='a', maxBytes=1024*1024, 
                                 backupCount=None, encoding=None, delay=0)

my_handler.setFormatter(log_formatter)
my_handler.setLevel(logging.WARNING)

logger = logging.getLogger('root')
logger.setLevel(logging.WARNING)

logger.addHandler(my_handler)


def auth():

    return GoogleDrive(GoogleAuth().LocalWebserverAuth())


def upload_file(path, folder, filename, drive):

    if not os.path.exists(path):
        logger.warning('Arquivo não encontrado: {}'.format(filename))
        return
    
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
    except:
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
    sql = """
        SELECT distinct area_mae, nome_tabela_pgadmin 
        FROM datapedia.temas;"""
    if view is not None:
        sql = """
        SELECT distinct area_mae, nome_tabela_pgadmin 
        FROM datapedia.temas 
        WHERE nome_tabela_pgadmin = '{}';""".format(view)
    
    df = psql.read_sql(sql, db_connect())
    
    if not df.empty:
        df.columns = ['area','view']
        df.dropna(subset=['view'], inplace=True)
        df['area'] = (df['area'].str.lower().map(lambda x: unicodedata.normalize('NFKD',x).encode('ASCII','ignore').decode()))
        return df
    else:
        logger.error('view {} indisponível.'.format(view))
        return
    

def check_files():

    essential_files = ('settings.ini', 'settings.yaml')

    for file in essential_files:
        if not os.path.isfile(file):
            print('Arquivo de configuração inexistente: {}.'.format(file))
            
            
    for file in essential_files:
        if not os.path.isfile(file):
            sys.exit(0)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--keep',
        help='"True": Não deleta a pasta temporária com o arquivo gerado'
    )
    parser.add_argument(
        '--view',
        help='"tabela.view": Nome da view para execução unitária'
    )
    
    args = parser.parse_args()
    one_view = args.view
    keep_view = args.keep

    check_files()

    drive = auth()

    if not os.path.exists('out/'):
        os.makedirs('out')
    
    if not os.path.exists('log/'):
        os.makedirs('log')


    try:        
        connection = db_connect()
        cursor = connection.cursor()

        list_views = get_list_views(one_view)

        try:
            for index, row in list_views.iterrows():
                try:
                    sql = "SELECT * FROM {};".format(row['view'])
                    try:
                        df = psql.read_sql(sql,connection)
                        salva_xlsx(df,row['area'],row['view'])
                        logger.debug('leitura de view: "{}/{}"'.format(row['area'],row['view']))
                    except:
                        logger.warning("erro ao salvar arquivo para upload: '{}/{}'".format(row['area'],row['view']))
                        continue
                except (pg.Error, EnvironmentError) as error:
                    logger.warning("view não encontrada no DB: {}".format(row['view']))
                    continue    

            for index, row in list_views.iterrows():
                logger.debug('Tentando upload do arquivo {}'.format(row['view']))
                create_folder(row['area'],drive)
                path = 'out/{}/{}.xlsx'.format(row['area'],row['view'])
                if os.path.isfile(path):
                    upload_file(path,row['area'],row['view'],drive)
                    logger.debug("upload concluído: '{}/{}'".format(row['area'],row['view']))
                else:
                    logger.error('arquivo esperado inexistente: "{}/{}"'.format(row['area'], row['view']))
                    continue
        
        except AttributeError as error:
            logger.warning('DF vazio. View não encontrada: {}'.format(sys.argv[1]))

    except (pg.Error, NameError, AttributeError) as error:
        logger.error("Error while connecting to PostgreSQL.", error)

    finally:
        if(connection):
            cursor.close()
            connection.close()
            logger.debug("PostgreSQL connection is closed")
        
        logger.debug('Fim do Processamento')
        
        if keep_view == "True":
            print('Comando keep: Pasta out não removida')
        else:
            shutil.rmtree('out')