import pandas as pd
import re

# crear un ejemplo de dataframe con una columna de fecha y hora
df = pd.DataFrame({'fecha_hora': ['1900-01-16T00:00:00.000', '1900-01-17T01:23:45.678', '00:00:00.000', '01:23:45']})

# convertir la columna a tipo de datos de fecha y hora
df['fecha_hora'] = pd.to_datetime(df['fecha_hora'])

# verificar si cada fila está en el formato correcto o no
def check_format(s):
    regex = r'^\d{2}:\d{2}:\d{2}\.\d{3}$'
    return bool(re.match(regex, s))

# convertir la columna al formato deseado
df['fecha_hora'] = df['fecha_hora'].apply(lambda x: x.strftime('%H:%M:%S.%f') if not check_format(x.strftime('%H:%M:%S.%f')) else x.strftime('%H:%M:%S.%f'))

# mostrar el resultado
print(df)