import pandas as pd
import dependencies as dp


dp.logger.info('Iniciamos el proceso')

path_input = dp.rootFolder / 'data' / 'raw'

try:
    df_camiones = pd.read_excel(
        path_input / 'BASE DE DATO 2022 PRUEBA.xlsx',
        sheet_name='Lista de camiones',
        usecols='A:C'
    )
    dp.logger.info('Leido el fichero BASE DE DATOS 2022 PRUEBA.xlsx')

except Exception as e:
        dp.logger.exception(e, exc_info=True)



