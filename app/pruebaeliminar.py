import pandas as pd
import dependencies as dp
import glob

path_input = dp.rootFolder / 'data' / 'prueba'
path_output = dp.rootFolder / 'data' / 'processed'

path = path_input / 'Base de Trabajo correctivas  de 20**.xlsx'
file_name = glob.glob(path)[0]

df_corr = pd.read_excel(
    file_name,
    sheet_name="BaseDatosCorrectiva 2020",
    usecols='A, C, E, I, K, R:AD, AF, AG',
    skiprows=4,
    parse_dates=['fecha_inicio', 'fecha_fin'],
    names=['camion', 'ot', 'mecanico', 'fecha_inicio', 'fecha_fin', 'chasis',
            'carroceria', 'ruedas', 'mecanica', 'elec, vehiculos', 'obra civil',
            'agua y combustible', 'herramientas', 'informatica', 'exteriores',
            'aire', 'maquinaria', 'electricidad', 'descripcion', 'observacion']
)

print(df_corr)
