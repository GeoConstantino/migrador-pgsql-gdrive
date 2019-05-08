import os
import re
import sys
import unicodedata
import psycopg2 as pg
from pydrive.auth import GoogleAuth #credentials.json, settings.yaml, client_secrets.json
from pydrive.drive import GoogleDrive
from pandas.io import sql as psql
from decouple import config #settings.ini

#ID da pasta MP em Mapas - Dados no Google Drive
ID_MP = "1cvbybdy4dtZfO4o2dSq36qUxVadfUWxb"

def auth():
    return GoogleDrive(GoogleAuth().LocalWebserverAuth())


def upload_file(path, folder, filename, drive):
    if not os.path.exists(path):
        print('Arquivo não encontrado: {}'.format(filename))
        return
    #print('Arquivo encontrado: {}'.format(path))
    id_folder_destiny = get_id_from_gdrive(folder)
    file_metadata = {'title': filename, 
        'parents': [{'kind': 'drive#fileLink',
        'id': id_folder_destiny}]}
    file = drive.CreateFile(file_metadata)
       
    try:
        file.SetContentFile(path)
        file.Upload()
    
    except NameError:
        print('Arquivo não encontrado.')

    except :
        print('Erro de conexão na API do Google Drive.')
    

def create_folder(foldername,drive):
# se a pasta não existir, ele a criará, se já existir, ele não
# apaga o conteúdo  
    if get_id_from_gdrive(foldername):
        return

    try:
        folder_metadata = {'title':foldername, 'mimeType':'application/vnd.google-apps.folder', 'parents':  [{'kind': 'drive#fileLink','id': ID_MP}]}
        folder = drive.CreateFile(folder_metadata)
        folder.Upload()
    
    except drive.Erro as error:
        print('Erro na criação da pasta: {}'.format(error))
        pass
        

def get_id_from_gdrive(name):
    parameters = "'{}' in parents and trashed=false".format(ID_MP) 
    for file_list in auth().ListFile({'q':parameters}): 
        for file1 in file_list: 
            if file1['title'] == name:
                return (file1['id'])
    #print('ID não encontrado para {}'.format(name))

def salva_xlsx(df,folder,filename):
    directory = "out/" + folder
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    name = directory + "/" + filename + ".xlsx"
    
    try:
        df.to_excel(name, index=False,float_format="%.2f")
        print('Arquivo {} salvo com sucesso.'.format(name))
    except FileNotFoundError as error:
        print('Erro ao salvar o arquivo: {}'.format(name))


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
        print("Erro ao conectar no banco de dados: {}".format(erro))
        sys.exit()
    

def get_list_views():

    sql = "select distinct area_mae, nome_tabela_pgadmin from datapedia.temas;"

    df = psql.read_sql(sql, db_connect())

    df.columns = ['area','view']
    
    df.dropna(subset=['view'], inplace=True)
    df['area'] = (df['area'].str.lower().map(lambda x: unicodedata.normalize('NFKD',x).encode('ASCII','ignore').decode()))   
    
    return df


if __name__ == '__main__':

    drive = auth()

    try:        
        connection = db_connect()
        cursor = connection.cursor()

        list_views = get_list_views()

        for index, row in list_views.iterrows():
            try:
                sql = "SELECT * FROM {};".format(row['view'])
                try:
                    df = psql.read_sql(sql,connection)
                except psql.DatabaseError as error:
                    print("View não encontrada no banco de dados: {}".format(row['view']))
                    continue
                print('arquivo:',row['area'])
                print(df.head(2))
                salva_xlsx(df,row['area'],row['view'])

            except pg.Error as error:
                continue    
                print("Erro no Banco de Dados: {}".format(error))
           
            except EnvironmentError as error:
                continue
                print('XLSX(erro) view: {}'.format(row['view']))
                print(error)

        for index, row in list_views.iterrows():
            print('Tentando upload do arquivo {}'.format(row['view']))
            try:
                create_folder(row['area'],drive)
                path = 'out/{}/{}.xlsx'.format(row['area'],row['view'])
                upload_file(path,row['area'],row['view'],drive)

            except:
                print('GOOGLE DRIVE(erro) view: {}, area: {}'.format(row['view'],row['area']))
                continue

    except pg.Error as error:
        print ("Error while connecting to PostgreSQL.", error)

    except NameError as error:
        print ("Error while connecting to PostgreSQL.", error)
    
    except AttributeError as error:
        print ("Error while connecting to PostgreSQL.", error)

    finally:
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")