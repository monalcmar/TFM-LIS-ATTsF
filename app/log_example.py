import pandas as pd
import numpy as np
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

except FileNotFoundError as e:
    dp.logger.error(f'Fichero no encontrado: {e}')
    try:
        dp.logger.info('Generando otro df porque el otro no se encuentra')
        df = pd.DataFrame(
            np.array(
                [
                    [1, 2, 3],
                    [4, 5, 6],
                    [7, 8, 9]
                ]
            ),
            columnas=['a', 'b', 'c']
        )
    except TypeError as e:
        dp.logger.error(f'Hay un error en los parámetros de lectura: {e}')
    except Exception as e:
        dp.logger.error(e)

except ImportError as e:
    dp.logger.error(f'Faltan dependencias: {e}')
except Exception as e:
   dp.logger.error(e)