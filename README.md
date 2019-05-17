# migrador-pgsql-gdrive
Capta views no PostgreSQL e importa como xlsx no Google Drive

Modo de usar:
parâmetros: 1º para uma view específica, passando um nome de view. 2º para manter os arquivos gerados, passando "keep"

Ex.: python migrador-pgsql-gdrive "datapedia.base_educ" "keep"


Arquivos necessários:
----------------------------------------------------------------------
'settings.ini'

Usado para configurar o acesso ao BD PostgreSQL, abstraído com o PYTHON-DECOUPLE

Padrão de preenchimento:

[settings]
USERDB=nome_usuario
PASSWORD=senha
HOST=host_servidor
PORT=porta
DATABASE=nome_database

----------------------------------------------------------------------
'client_secrets.json'

chave extraída do seu projeto na API do Google, para acessar a API é necessário obter este arquivo direto com a página da google.

----------------------------------------------------------------------
'settings.yaml'

Configuração de Acesso da biblioteca PyDrive. 

Padrão de preenchimento:

client_config_backend: settings

client_config:
  client_id: ??????????????????????????.apps.googleusercontent.com
  client_secret: ??????????????
  auth_uri: https://accounts.google.com/o/oauth2/auth
  token_uri: https://oauth2.googleapis.com/token
  redirect_uri: urn:ietf:wg:oauth:2.0:oob
  

save_credentials: True
save_credentials_backend: file
save_credentials_file: credentials.json

get_refresh_token: True

----------------------------------------------------------------------
'credentials.json'
Usado no "client_config" para armazenar a credencial usada. 
Gerado automaticamente quando realizado login.


