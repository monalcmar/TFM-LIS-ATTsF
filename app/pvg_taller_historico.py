import pandas as pd
from sqlalchemy import create_engine, text
import dependencies
from db.connection import engine, conn #se importa directamente la conexion con la base de datos

# Carpetas donde se saca y guarda informacion
path_input = dependencies.rootFolder / 'data' / 'raw'
path_output = dependencies.rootFolder / 'data' / 'processed'


################################################## CONEXION BASE DE DATOS #########################################################

# # Establecer la conexión a la base de datos PostgreSQL
# engine = create_engine('postgresql://postgres:postgres@3.69.128.37:5432/postgres')

# # Crea un objeto de la clase Connection
# conn = engine.connect()

############## MAESTROS ###############
tipo_ot = text('SELECT * FROM attsf.tbl_tipo_ot')
df_tipo_ot = pd.read_sql_query(tipo_ot, con=conn)

frecuencia = text('SELECT * FROM attsf.tbl_frecuencia')
df_frecuencia = pd.read_sql_query(frecuencia, con=conn)

tipo_taller = text('SELECT * FROM attsf.tbl_tipo_taller')
df_tipo_taller = pd.read_sql_query(tipo_taller, con=conn)

averia = text('SELECT * FROM attsf.tbl_averia')
df_averia = pd.read_sql_query(averia, con=conn)

taller = text('SELECT id_taller, taller FROM attsf.tbl_taller')
df_taller = pd.read_sql_query(taller, con=conn)

repuesto = text('SELECT * FROM attsf.tbl_repuesto')
df_repuesto = pd.read_sql_query(repuesto, con=conn)

mecanico = text('SELECT * FROM attsf.tbl_mecanico')
df_mecanico = pd.read_sql_query(mecanico, con=conn)

camion = text('SELECT id_camion, nombre_attsf FROM attsf.tbl_camion')
df_camion = pd.read_sql_query(camion, con=conn)

#Para tomar la ultima fecha anhadida a la base de datos
ot = text('SELECT fecha_inicio FROM attsf.tbl_ot ORDER BY fecha_inicio DESC LIMIT 1')

result = conn.execute(ot)

# Recupera la última fila de la columna fecha_inicio
last_row = result.fetchone()

engine.dispose()


#FUNCIONES
def limpiar_capitalize(cadena):
    cadena_limpia = cadena.strip()
    cadena_capitalizada = cadena_limpia.capitalize()
    return cadena_capitalizada

######################################################### PREVENTIVO ##########################################################
#Extraemos la información del excel de ot preventivo
df_prev = pd.read_excel(
            path_input / 'OTP DE ',
            sheet_name='OTP',
            usecols='A, C:E, I, K, P:AD, AE, AH',
            skiprows=5,
            parse_dates=['fecha_inicio', 'fecha_fin'],
            names=['camion', 'ot', 'frecuencia', 'mecanico', 'fecha_inicio', 'fecha_fin', 'aceite', 'anticongelante', 'embrague', 
            'h direccion', 'baterias', 'l freno', 'a. caja', 'a/destil', 'ruedas', 'lamparas', 'd/tacgfo', 's. lava',
            'j. carroceria', 'calcho', 'grasa', 'descripcion', 'observacion']
        )

# Seleccionar las filas que tienen fecha posterior a la fecha de referencia
df_ot_prev = df_prev.loc[df_prev['fecha_inicio'] > last_row[0]] #OJOOOO, si hay dos fechas iguales o asi

#Se eliminan las filas con valores nan en todas las columnas
df_ot_prev.dropna(how='all', inplace=True)
df_ot_prev.dropna(subset=['ot'], inplace=True) #si no tiene numero de ot tampoco tiene informacion
df_ot_prev.dropna(subset=['camion'], inplace=True) #camiones que son vacios y su informacion tambien

#Se cambian los nan por un tipo de frecuencia 'No frecuencia'
df_ot_prev['frecuencia'] = df_ot_prev['frecuencia'].fillna('No frecuencia')

#Se limpian los datos
df_ot_prev['ot'] = df_ot_prev['ot'].astype(int)

df_ot_prev['camion'] = df_ot_prev['camion'].apply(limpiar_capitalize)

df_ot_prev['mecanico'] = df_ot_prev['mecanico'].str.apply(limpiar_capitalize)

df_ot_prev['frecuencia'] = df_ot_prev['frecuencia'].apply(limpiar_capitalize)

df_ot_prev['descripcion'] = df_ot_prev['descripcion'].apply(limpiar_capitalize)

df_ot_prev['observacion'] = df_ot_prev['observacion'].apply(limpiar_capitalize)

#Se quitan los duplicatos OJO
df_ot_prev = df_ot_prev.drop_duplicates()

#Se resetea el index por los valores eliminados
df_ot_prev= df_ot_prev.reset_index()

#Crear una columna de tipo_ot = 'Preventivo'
df_ot_prev['tipo_ot'] = 'Preventivo'

#Se define el dataframe de ot repuesto (se usara mas adelante)
df_ot_repuesto = df_ot_prev[['ot', 'aceite', 'anticongelante', 'embrague', 
            'h direccion', 'baterias', 'l freno', 'a. caja', 'a/destil', 'ruedas', 'lamparas', 'd/tacgfo', 's. lava',
            'j. carroceria', 'calcho', 'grasa']]

#Se toman las columnas deseadas
df_ot_prev = df_ot_prev[['ot', 'camion', 'frecuencia', 'tipo_ot', 'fecha_inicio', 'fecha_fin', 'descripcion', 'observacion']]


######################################################### CORRECTIVO ###########################################################
#Extraemos la información del excel de ot correctivo
df_corr = pd.read_excel(
            path_input / 'Base de Trabajos correctivas de ',
            sheet_name='BaseDatosCorrectiva',
            usecols='A, C, E, I, K, R:AD, AF, AG',
            skiprows=4,
            parse_dates=['fecha_inicio', 'fecha_fin'],
            names=['camion', 'ot', 'mecanico', 'fecha_inicio', 'fecha_fin', 'chasis', 'carroceria', 'ruedas', 
            'mecanica', 'elec, vehiculos', 'obra civil', 'agua y combustible', 'herramientas',
            'informatica', 'exteriores', 'aire', 'maquinaria', 'electricidad', 'descripcion', 'observacion']
        )

# Seleccionar las filas que tienen fecha igual o posterior a la fecha de referencia
df_ot_corr = df_corr.loc[df_corr['fecha_inicio'] > last_row[0]] #OJOOOO, si hay dos fechas iguales o asi

#Se eliminan las filas con valores nan en todas las columnas
df_ot_corr.dropna(how='all', inplace=True)
df_ot_corr.dropna(subset=['ot'], inplace=True) #si no tiene numero de ot tampoco tiene informacion
df_ot_corr.dropna(subset=['camion'], inplace=True) #camiones que son vacios y su informacion tambien

#Se limpian los datos
df_ot_corr['ot'] = df_ot_corr['ot'].astype(int)

df_ot_corr['camion'] = df_ot_corr['camion'].str.apply(limpiar_capitalize)

df_ot_corr['mecanico'] = df_ot_corr['mecanico'].str.apply(limpiar_capitalize)

df_ot_corr['descripcion'] = df_ot_corr['descripcion'].str.apply(limpiar_capitalize)

df_ot_corr['observacion'] = df_ot_corr['observacion'].str.apply(limpiar_capitalize)

#Eliminar los duplicados
df_ot_corr = df_ot_corr.drop_duplicates() 

#Se resetea el index por los valores eliminados
df_ot_corr = df_ot_corr.reset_index()

#Crear una columna de frecuencia = 'No frecuencia'
df_ot_corr['frecuencia'] = 'No frecuencia'

#Crear una columna de tipo_ot = 'Correctivo'
df_ot_corr['tipo_ot'] = 'Correctivo'

#Se define el dataframe de ot averia (se usara mas adelante)
df_ot_averia = df_ot_corr[['ot', 'chasis', 'carroceria', 'ruedas', 
            'mecanica', 'elec, vehiculos', 'obra civil', 'agua y combustible', 'herramientas',
            'informatica', 'exteriores', 'aire', 'maquinaria', 'electricidad']]

#Se toman las columnas deseadas
df_ot_corr = df_ot_corr[['ot', 'camion', 'frecuencia', 'tipo_ot', 'fecha_inicio', 'fecha_fin', 'descripcion', 'observacion']]


######################################################### UNION ################################################################
#Concat de ambos dataframes
df_ot = pd.concat([df_ot_corr, df_ot_prev], axis=0)

#Se toman las columnas deseadas
df_ot = df_ot[['ot', 'camion', 'frecuencia', 'tipo_ot', 'fecha_inicio', 'fecha_fin', 'descripcion', 'observacion']]

df_ot = df_ot.sort_values(by='fecha_inicio') #Ordenar por fecha

#Se quitan los indices anteriores ya que se repiten cada anho
df_ot.reset_index(names='id_attsf', inplace=True)
df_ot = df_ot.drop('id_attsf', axis=1)

#Columna de indices
df_ot.index += 1
df_ot.reset_index(names='id_ot', inplace=True)

########################################################### MERGES ##############################################################
#Merge de ot e id_camion
#df_ot = pd.merge(df_ot, df_camion, how='left', on='camion')
#Eliminar columna camion
#df_ot = df_ot.drop('camion', axis=1)

#Merge de ot e id_taller
#df_ot = pd.merge(df_ot, df_taller.loc[:, ['id_taller', 'taller']], how='left', on='taller')
#Eliminar columna taller
#df_ot = df_ot.drop('taller', axis=1)

#Merge de ot e id_tipo_ot
df_ot = pd.merge(df_ot, df_tipo_ot, how='left', on='tipo_ot')
#Eliminar columna tipo_ot
df_ot = df_ot.drop('tipo_ot', axis=1)

#Merge de ot e id_frecuencia
df_ot = pd.merge(df_ot, df_frecuencia, how='left', on='frecuencia')
#Eliminar columna frecuencia
df_ot = df_ot.drop('frecuencia', axis=1)

#Columna de id_mecanico
df_ot = pd.merge(df_ot, df_mecanico, how='left', on='mecanico')
#Eliminar columna frecuencia
df_ot = df_ot.drop('mecanico', axis=1)

#Se reordenan las columnas
df_ot = df_ot[['id_ot', 'ot', 'camion', 'id_taller', 'id_tipo_ot', 'id_frecuencia', 'id_mecanico', 'fecha_inicio', 'fecha_fin', 
                'descripcion', 'observacion']]


############################################################# OT AVERIA ########################################################
#Se crea la lista de averias
df_tipo_averia = df_averia.drop('id_averia', axis=1)
df_tipo_averia['averia'] = df_tipo_averia['averia'].str.lower()
averias = df_tipo_averia['averia'].tolist()

#Se crea la tabla con el ot y las averias
df_otc_averia = df_ot_averia.rename(columns={'NºOTC': 'ot'}).dropna(subset='ot')

#Se juntan e invierten columnas
df_ot_averia = pd.melt(
    df_otc_averia,
    id_vars='ot',
    value_vars=averias
).dropna(subset='value').loc[:, ['ot', 'variable']].rename(columns={'variable': 'averia'})

#Se limpian los datos
df_ot_averia['averia'] = df_ot_averia['averia'].apply(limpiar_capitalize)

#Merge ot
df_ot_averia = pd.merge(df_ot_averia, df_ot.loc[:, ['id_ot', 'ot']], how='left', on='ot')
df_ot_averia.dropna(subset=['id_ot'], inplace=True) #hay averias que no tienen ot porque solo he cogido los camiones con numero
df_ot_averia['id_ot'] = df_ot_averia['id_ot'].astype(int)
#Se elimina la columna ot
df_ot_averia = df_ot_averia.drop('ot', axis=1)

#Merge averia
df_ot_averia = pd.merge(df_ot_averia, df_averia, how='left', on='averia')
#Se elimina la columna averia
df_ot_averia = df_ot_averia.drop('averia', axis=1)

############################################################ OT REPUESTO ###############################################################
#Se crea la lista de repuestos
df_tipo_repuesto = df_repuesto.drop('id_repuesto', axis=1)
df_tipo_repuesto['repuesto'] = df_tipo_repuesto['repuesto'].str.lower()
repuestos = df_tipo_repuesto['repuesto'].tolist()

#Se crea la tabla con el ot y los repuestos
df_otc_repuesto = df_ot_repuesto.rename(columns={'NºOTC': 'ot'}).dropna(subset='ot')

#Se juntas e invierten columnas
df_ot_repuesto = pd.melt(
    df_otc_repuesto,
    id_vars='ot',
    value_vars=repuestos
).dropna(subset='value').loc[:, ['ot', 'variable']].rename(columns={'variable': 'repuesto'})

#Se limpian los datos
df_ot_repuesto['repuesto'] = df_ot_repuesto['repuesto'].apply(limpiar_capitalize)

#Merge ot
df_ot_repuesto = pd.merge(df_ot_repuesto, df_ot.loc[:, ['id_ot', 'ot']], how='left', on='ot')
df_ot_repuesto.dropna(subset=['id_ot'], inplace=True)
#Se elimina la columna ot
df_ot_repuesto = df_ot_repuesto.drop('ot', axis=1)
df_ot_repuesto['id_ot'] = df_ot_repuesto['id_ot'].astype(int)

#Merge repuesto
df_ot_repuesto = pd.merge(df_ot_repuesto, df_repuesto, how='left', on='repuesto')
#Se elimina la columna repuesto
df_ot_repuesto = df_ot_repuesto.drop('repuesto', axis=1)


########################################################## DBEAVER #################################################################

# Establecer la conexión a la base de datos PostgreSQL
engine = create_engine('postgresql://postgres:postgres@3.69.128.37:5432/postgres')

df_ot.to_sql(name='tbl_ot', con=engine, schema='attsf', if_exists='append', index=False)
df_ot_averia.to_sql(name='tbl_ot_averia', con=engine, schema='attsf', if_exists='append', index=False)
df_ot_repuesto.to_sql(name='tbl_ot_repuesto', con=engine, schema='attsf', if_exists='append', index=False)

engine.dispose()

