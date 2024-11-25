

import pyodbc
import json
import snowflake.connector as sf
import random
import string
import pytz

from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from azure.identity import ClientSecretCredential
from msgraph.core import GraphClient 
from pyspark.sql.dataframe import DataFrame
from datetime import datetime


sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

ENV_RGS = {
  "databricks-rg-dbricks-n-we-caar-uat--workers": "UAT",
  "databricks-rg-dbricks-we-caar-nft-workers": "NFT",
  "databricks-rg-dbricaarew-jyuj6r3mkcsks": "PRO"
}

SCOPES = {
  "UAT" : "keyvaultcaarew",
  "NFT" : "key-vault-secrets",
  "PRO" : "dbri-scope-caar-ew"
}

def check_permissions(group: str, scope: str, environment: str, authorized: bool, graph_client: GraphClient, group_id: str):
  if not authorized:
    user = get_db_utils(spark).notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

    if group == 'CAAR_ADMIN':
      members = graph_client.get(f'/groups/{group_id}/members?$select=mail')

      mails = [item['mail'] for item in members.json()['value']]
    else:
      members = graph_client.get(f'/groups/{group_id}/members?')
      members_admin = graph_client.get(f'/groups/0b269fac-ff83-4082-aa3b-80ecb0252e79/members?$select=mail')

      mails = [item['mail'] for item in members.json()['value'] if item['@odata.type'] == '#microsoft.graph.user'] + [item['mail'] for item in members_admin.json()['value']]
      group_children_id = [item['id'] for item in members.json()['value'] if item['@odata.type'] == '#microsoft.graph.group']

      for item in group_children_id:
        members = graph_client.get(f'/groups/{item}/members?$select=mail')
        mails = mails + [item['mail'] for item in members.json()['value'] if item['@odata.type'] == '#microsoft.graph.user']

    if 'jobType' in  get_db_utils(spark).notebook.entry_point.getDbutils().notebook().getContext().toJson():
      df_run = get_db_utils(spark).notebook.entry_point.getDbutils().notebook().getContext().tags().apply('jobType')
    else:
      df_run = None

    if group == 'CAAR_ADMIN' and environment == 'UAT':
      return False
    else:
      if user in mails or (df_run in ('EPHEMERAL','WORKFLOW') and user in ('a80ef1a5-bd19-485f-beeb-22862e2e76c4','x47024ha@gexterno.es')):
        authorized = True
        return True
      else:
        return False
  else:
    return True
        
  
def load_json(file: str) -> json:
  with open(f'/dbfs/databricks/admin/{file}.json') as f:
    return json.load(f)
  
def get_db_utils(spark):
  dbutils = None
      
  if spark.conf.get("spark.databricks.service.client.enabled") == "true":
    
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
      
  else:
    import IPython
    dbutils = IPython.get_ipython().user_ns["dbutils"]
      
  return dbutils    

class Connection:
  
  def __init__(self, group, environment = None):
    if environment:
      print('Provided deprecated "environment" argument, ignoring it')

    authorized = False
    environment = ENV_RGS.get(spark.conf.get("spark.databricks.clusterUsageTags.managedResourceGroup"))
    
    group_sp_name = group if environment == 'UAT' else 'CAAR_ADMIN'
    array = group_sp_name.lower().split('_')
    sp_name = array[0] + 'sp' + ''.join(map(str, array[1:]))
    
    client_secret = get_db_utils(spark).secrets.get(SCOPES.get(environment), 'caarspadmin')
    credentials = ClientSecretCredential(tenant_id = 'da060e56-5e46-475d-8b74-5fb187bd2177', client_id = 'a80ef1a5-bd19-485f-beeb-22862e2e76c4', client_secret = client_secret) 
    graph_client = GraphClient(credential = credentials)
    
    group_id = graph_client.get(f"/groups?$filter=startswith(displayName, 'CAA')&$top=999")
    group_id = [item['id'] for item in group_id.json()['value'] if item['displayName'] == f'{group}'][0]
    
    sp_id = graph_client.get(f"/servicePrincipals?$filter=startswith(displayName, 'caa')&$top=999")
    sp_id = [item['appId'] for item in sp_id.json()['value'] if item['displayName'] == f"{array[0] + '_sp_' + '_'.join(map(str, array[1:]))}"][0]
    
    self.sql_server = 'auxserver.database.windows.net'
    self.port = '1433'
    self.database = 'sqldwpochayd'
    self.driver = '{ODBC Driver 17 for SQL Server}'
    self.user = get_db_utils(spark).notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
    self.group = group if environment == 'UAT' else 'CAAR_ADMIN_NFT' if environment == 'NFT' else 'CAAR_ADMIN_PRO'
    self.scope = SCOPES.get(environment)
    self.endpoint = 'da060e56-5e46-475d-8b74-5fb187bd2177'
    self.sp_name = sp_name
    self.sp_id = sp_id
    self.group_id = group_id
    self.environment = environment
    self.authorized = authorized
    self.graph_client = graph_client

    #self.__mount_spark_abfss('dlgen2caaruatew')
    #self.__mount_spark_abfss('dlgen2caarnftew')
    #self.__mount_spark_abfss('dlgen2caarproew')

  def __mount_spark_abfss(self, storage_account):
    raise NotImplementedError('Use mount points instead.')
    #spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
    #spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    #spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", self.sp_id)
    #spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", get_db_utils(spark).secrets.get(self.scope, self.sp_name))
    #spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{self.endpoint}/oauth2/token")
    
  def get_parameters(self):
    return self.environment, f'dlgen2caar{self.environment.lower()}ew', f'dlgen2caar{self.environment.lower()}ew'
    
  def get_jdbc(self, query: str, predicates = None, lower_bound: int = None, upper_bound: int = None, num_partitions: int = 1, verbose: bool = False):
    if check_permissions(self.group, self.scope, self.environment, self.authorized, self.graph_client, self.group_id):
      password = get_db_utils(spark).secrets.get(self.scope, self.sp_name + '-db')

      jdbc_url = 'jdbc:sqlserver://{0}:{1};database={2}'.format(self.sql_server, self.port, self.database)
      connection_properties = {
        "user" : self.sp_name,
        "password" : password,
        "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      }
      
      if verbose == True:
        print('Query executed --> ' + query)
        
      df = spark.read.jdbc(url=jdbc_url, table = f'({query}) t', predicates = predicates, 
                           lowerBound = lower_bound, upperBound = upper_bound, numPartitions = num_partitions, 
                           properties = connection_properties)

      return df
    else:
      raise ValueError(f'Error, you do not have permissions to access the group {self.group}')
      
  def read_polybase(self, query: str):
    if check_permissions(self.group, self.scope, self.environment, self.authorized, self.graph_client, self.group_id):
      
      spark.conf.set("fs.azure.account.key.rgpochayddiag770.blob.core.windows.net",get_db_utils(spark).secrets.get(self.scope,'rgpochayddiag770-key'))
      
      password = get_db_utils(spark).secrets.get(self.scope, self.sp_name + '-db')
      jdbc_url = f'jdbc:sqlserver://{self.sql_server}:{self.port};database={self.database};user={self.sp_name};password={password};encrypt=true;'
        
      df = spark.read           .format("com.databricks.spark.sqldw")           .option("url", jdbc_url)           .option("tempDir","wasbs://dwhtemp@rgpochayddiag770.blob.core.windows.net/tempDirs")           .option("forwardSparkAzureStorageCredentials", "true")           .option("query", query)           .load()

      return df
    else:
      raise ValueError(f'Error, you do not have permissions to access the group {self.group}')
      
  def write_jdbc(self, dataframe, table: str, mode: str = 'overwrite', colums_type=[]):
    if check_permissions(self.group, self.scope, self.environment, self.authorized, self.graph_client, self.group_id):
      password = get_db_utils(spark).secrets.get(self.scope, self.sp_name + '-db')
      
      jdbc_url = 'jdbc:sqlserver://{0}:{1};database={2}'.format(self.sql_server, self.port, self.database)
      connection_properties = {
        "user" : self.sp_name,
        "password" : password,
        "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      }
      
      if mode == 'overwrite':
        columns_name = list(map(lambda x: x.upper(), dataframe.columns))
        columns = ','.join([i[0]+' '+i[1] for i in [[columns_name[i], colums_type[i]] for i in range(0,len(columns_name))]])
        connection_properties['createTableColumnTypes'] = columns
              
      dataframe.write.jdbc(url=jdbc_url, table = table, mode = mode, properties = connection_properties)
    
    else:
      raise ValueError(f'Error, you do not have permissions to access the group {self.group}')
      
  def write_polybase(self, dataframe, table: str, mode: str = 'overwrite', truncate: bool = False):
    if check_permissions(self.group, self.scope, self.environment, self.authorized, self.graph_client, self.group_id):
      spark.conf.set("fs.azure.account.key.rgpochayddiag770.blob.core.windows.net",get_db_utils(spark).secrets.get(self.scope,'rgpochayddiag770-key'))
      
      password = get_db_utils(spark).secrets.get(self.scope, self.sp_name + '-db')
      jdbc_url = f'jdbc:sqlserver://{self.sql_server}:{self.port};database={self.database};user={self.sp_name};password={password};encrypt=true;'

      dataframe.write.format("com.databricks.spark.sqldw")          .option("url", jdbc_url)          .option("forwardSparkAzureStorageCredentials", "true")          .option("dbtable", table)          .option("tempdir", "wasbs://dwhtemp@rgpochayddiag770.blob.core.windows.net/tempDirs")          .option("truncate", truncate)          .mode(mode)          .save()
    else:
      raise ValueError(f'Error, you do not have permissions to access the group {self.group}')
  
  def get_pyodbc(self):
    if check_permissions(self.group, self.scope, self.environment, self.authorized, self.graph_client, self.group_id):
      password = get_db_utils(spark).secrets.get(self.scope, self.sp_name + '-db')

      conn = pyodbc.connect( DRIVER = self.driver,
                           SERVER = self.sql_server,
                           DATABASE = self.database,
                           UID = self.sp_name,
                           PWD = password)           

      conn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1') 
      conn.autocommit = True

      return conn
    else:
      raise ValueError(f'Error, you do not have permissions to access the group {self.group}')
      
  def export_parquet(self, schema: str, table: str, location: str, environment: str = 'UAT', date_var: str = None, date_ini: str = None, date_end: str = None):
    if check_permissions(self.group, self.scope, self.environment, self.authorized, self.graph_client, self.group_id):
      user_admin = 'caarspadmin'
      password_admin = get_db_utils(spark).secrets.get(self.scope, user_admin + '-db')
      query = f"select distinct string_agg([Object], ',') as SCHEMAS                from (                     select                         class_desc as [Type]                       , case when class = 0 then DB_NAME()                              when class = 1 then OBJECT_NAME(major_id)                              when class = 3 then SCHEMA_NAME(major_id) end [Object]                       , USER_NAME(grantee_principal_id) [User]                       , permission_name as [Permission]                       , state_desc as [State]                     from sys.database_permissions                 ) as a                 where [User] in ('{self.group}') and [State] = 'GRANT' and [Type] = 'SCHEMA' and Permission in ('SELECT', 'CONTROL')"

      jdbc_url = 'jdbc:sqlserver://{0}:{1};database={2}'.format(self.sql_server, self.port, self.database)
      connection_properties = {
          "user" : user_admin,
          "password" : password_admin,
          "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      }
      df = spark.read.jdbc(url=jdbc_url, table = f'({query}) t', properties = connection_properties)

      schemas = df.collect()[0][0]

      if schema in schemas or self.group == 'CAAR_ADMIN':
        if environment == 'UAT' or self.group == 'CAAR_ADMIN':
          password = get_db_utils(spark).secrets.get(self.scope, user_admin + '-db')
          conn = pyodbc.connect(DRIVER = self.driver,
                               SERVER = self.sql_server,
                               DATABASE = self.database,
                               UID = user_admin,
                               PWD = password)           

          conn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1') 
          conn.autocommit = True

          list_params = [environment, schema, table, location, date_var, date_ini, date_end]
          string_execute = f"exec stg_admin.EXPORT_PARQUET_DS {', '.join(['?' for param in list_params])}"

          cursor = conn.cursor()
          cursor.execute(string_execute, tuple(list_params))
          
          print(f'The table {schema}.{table} has been exported in the path {location} of the {environment} environment')
          
        else:
          raise ValueError(f'Error, you do not have permissions to write in environment {environment}')
      else:
        raise ValueError(f'Error, you do not have permissions to access the schema {schema}')
    else:
      raise ValueError(f'Error, you do not have permissions to access the group {self.group}')
      
  def get_abfss(self, environment=None):
    raise NotImplementedError('Use mount points instead.')
    #if environment not in ('UAT', 'NFT', 'PRO'):
    #  environment = self.environment
    #return f'abfss://dlgen2caar{environment.lower()}ew@dlgen2caar{environment.lower()}ew.dfs.core.windows.net'

  def get_mount_point(self, storage: str, container: str, path_adls: str, path_mnt: str = None) -> str:
    if self.environment == 'UAT' and path_mnt is not None and not path_mnt.startswith('/mnt/CAAR/'):
      raise ValueError(f"Invalid path_mnt value '{path_mnt}'. Variable value should start with '/mnt/CAAR/'.")

    if check_permissions(self.group, self.scope, self.environment, self.authorized, self.graph_client, self.group_id):
      
      configs = {"fs.azure.account.auth.type": "OAuth",
             "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
             "fs.azure.account.oauth2.client.id": self.sp_id,
             "fs.azure.account.oauth2.client.secret": get_db_utils(spark).secrets.get(self.scope, self.sp_name),
             "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{self.endpoint}/oauth2/token"}
      
      if path_mnt is None:
        letters = string.ascii_lowercase
        path_mnt = '/mnt/CAAR/' + datetime.now(pytz.timezone('Europe/Madrid')).strftime('%Y%m%d%H%M%S') + ''.join(random.choice(letters) for i in range(20)) + '/'
      else:
        print('''WARNING DEPRECATED:
          Don't use:
              get_mount_point(storage, container, path_adls, path_mnt)
          Use this syntax instead:
              path_mnt = get_mount_point(storage, container, path_adls)
          ''')
      
      try:
        get_db_utils(spark).fs.unmount(path_mnt)
        get_db_utils(spark).fs.mount(
          source = f"abfss://{container}@{storage}.dfs.core.windows.net/" + path_adls,
          mount_point = path_mnt,
          extra_configs = configs)
      except Exception as e:
        get_db_utils(spark).fs.mount(
          source = f"abfss://{container}@{storage}.dfs.core.windows.net/" + path_adls,
          mount_point = path_mnt,
          extra_configs = configs)
      print(path_mnt + ' has been created')
      return path_mnt
    else:
      raise ValueError(f'Error, you do not have permissions to access the group {self.group}')
  
  def unmount_point(self, path_mnt: str):
    if check_permissions(self.group, self.scope, self.environment, self.authorized, self.graph_client, self.group_id):
      try:
        get_db_utils(spark).fs.unmount(path_mnt)
      except:
        print('Mount point is not created')
    else:
      raise ValueError(f'Error, you do not have permissions to access the group {self.group}')
      
  def read_snowflake(self, schema: str = '', table: str = None, query: str = None, database: str = 'EBDW', warehouse: str = 'DEV_WH', role: str = None) -> DataFrame:
    if check_permissions(self.group, self.scope, self.environment, self.authorized, self.graph_client, self.group_id):
      if role is None:
        role = self.group

      options = {
        "sfUrl": 'https://bx74413.west-europe.privatelink.snowflakecomputing.com',
        "sfUser": self.sp_name,
        "sfPassword": get_db_utils(spark).secrets.get(self.scope, self.sp_name + '-db'),
        "sfDatabase": database,
        "sfSchema": schema,
        "sfWarehouse": warehouse,
        "sfRole": role
      }

      if query is None and table is not None:
        return spark.read.format("snowflake").options(**options).option("dbtable", table).load()
      elif query is not None and table is None:
        return spark.read.format("snowflake").options(**options).option("query", query).load()
    else:
      raise ValueError(f'Error, you do not have permissions to access the group {self.group}')

  def write_snowflake(self, schema: str, table: str, dataframe: DataFrame, mode: str = 'overwrite', database: str = 'EBDW', warehouse: str = 'DEV_WH', role: str = None):
    if check_permissions(self.group, self.scope, self.environment, self.authorized, self.graph_client, self.group_id):
      if role is None:
        role = self.group

      options = {
        "sfUrl": 'https://bx74413.west-europe.privatelink.snowflakecomputing.com',
        "sfUser": self.sp_name,
        "sfPassword": get_db_utils(spark).secrets.get(self.scope, self.sp_name + '-db'),
        "sfDatabase": database,
        "sfSchema": schema,
        "sfWarehouse": warehouse,
        "sfRole": role
      }

      dataframe.write.format("snowflake").options(**options).option("dbtable", table).mode(mode).save()
    else:
        raise ValueError(f'Error, you do not have permissions to access the group {self.group}')

  def cursor_snowflake(self, schema: str, database: str = 'EBDW', warehouse: str = 'DEV_WH', role: str = None) -> sf.cursor.SnowflakeCursor:
    if check_permissions(self.group, self.scope, self.environment, self.authorized, self.graph_client, self.group_id):
      if role is None:
        role = self.group

      ctx = sf.connect(
        user = self.sp_name,
        account = 'bx74413.west-europe.privatelink',
        password = get_db_utils(spark).secrets.get(self.scope, self.sp_name + '-db'),
        warehouse = warehouse,
        database=  database,
        schema = schema,
        role = role
        )

      return ctx.cursor()
    else:
      raise ValueError(f'Error, you do not have permissions to access the group {self.group}')

  def session_snowflake(self, schema: str = 'MAESTRAS', database: str = 'EBDW', warehouse: str = 'DEV_WH', role: str = None):
    if check_permissions(self.group, self.scope, self.environment, self.authorized, self.graph_client, self.group_id):
      if role is None:
        role = self.group

      session_obj = sf.connect(
        user = self.sp_name,
        account = 'bx74413.west-europe.privatelink',
        password = get_db_utils(spark).secrets.get(self.scope, self.sp_name + '-db'),
        warehouse = warehouse,
        database=  database,
        schema = schema,
        role = role
        )

      return session_obj
    else:
      raise ValueError(f'Error, you do not have permissions to access the group {self.group}')

