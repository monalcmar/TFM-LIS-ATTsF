import pandas as pd
from sqlalchemy import create_engine, text

"""
# Establecer la conexión a la base de datos PostgreSQL
engine = create_engine('postgresql://postgres:postgres@3.69.128.37:5432/postgres')

# Crea un objeto de la clase Connection
conn = engine.connect()

# Define la sentencia SQL que deseas ejecutar
sql = text("SELECT fecha FROM attsf.tbl_prueba ORDER BY fecha DESC LIMIT 1")

# Ejecuta la sentencia SQL utilizando el objeto Connection
result = conn.execute(sql)

# Recupera la última fila de la columna fecha
last_row = result.fetchone()

engine.dispose()

fechas =['2000-01-01', '2001-01-01', '1970-01-01']
fechas = pd.to_datetime(fechas)

# crea una lista de objetos date a partir de los objetos Timestamp
fechas_fecha = [fecha.date() for fecha in fechas]

datos = {'nombre': ['Juan', 'Maria', 'Pedro'], 'edad': [25, 30, 28], 'fecha': fechas_fecha}

df = pd.DataFrame(datos)

# Seleccionar las filas que tienen fecha posterior a la fecha de referencia
df_filtro = df.loc[df['fecha'] > last_row[0]]

########################################################## DBEAVER #################################################################

# Establecer la conexión a la base de datos PostgreSQL
engine = create_engine('postgresql://postgres:postgres@3.69.128.37:5432/postgres')

df_filtro.to_sql(name='tbl_prueba', con=engine, schema='attsf', if_exists='append', index=False)

engine.dispose()
"""


################################################## CONEXION BASE DE DATOS #########################################################

# Establecer la conexión a la base de datos PostgreSQL
engine = create_engine('postgresql://postgres:postgres@3.69.128.37:5432/postgres')

# Crea un objeto de la clase Connection
conn = engine.connect()

############# MAESTROS ###############
distribucion = text('SELECT * FROM attsf.tbl_camion')

df_distribucion = pd.read_sql_query(distribucion, con=conn)


conn.close()
engine.dispose()

print(df_distribucion)