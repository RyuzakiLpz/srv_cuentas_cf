import pandas as pd


def valida_nombre_hoja(archivo, nombre_hoja):
    if nombre_hoja is None or nombre_hoja == "":
        df_xlsx = pd.read_excel(archivo, dtype=str)
    else:
        df_xlsx = pd.read_excel(archivo, sheet_name=nombre_hoja, dtype=str)

    return df_xlsx

