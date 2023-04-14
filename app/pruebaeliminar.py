import pandas as pd
import dependencies as dp
import glob
import os
import re


path_input = dp.rootFolder / 'data' / 'prueba'
path_output = dp.rootFolder / 'data' / 'processed'


# Expresión regular para buscar archivos cuyo nombre empieza por "20" seguido de dos dígitos
patron = r"Base de Trabajo correctivas  de 20\d{2}\.xlsx$"

print(patron)

nombre_archivo = path_input / patron

df_corr = pd.read_excel(
    nombre_archivo,
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
