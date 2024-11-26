import pandas as pd
from datetime import date
from src.resources.configlog import log_trace, configure_logging
from src.dao.connectionBD import conexion_sql
from src.resources.common.level import Nivel
from src.utils.util import valida_nombre_hoja
from src.dao.processMongoDB import obtener_resultados_cuentas, segmentar_operaciones, cuentas_segmentadas_final
from src.dao.connectionMongoDB import conexion_mongodb
from loadProperties import leer_propiedades

configure_logging()


def frame_mongo():
    try:
        conexion_cuentas = conexion_mongodb(leer_propiedades("coleccion.cuentas"))
        resultados = obtener_resultados_cuentas(conexion_cuentas)

        conexion_segmentos = conexion_mongodb(leer_propiedades("coleccion.segmentos"))
        segmentos_tipos = segmentar_operaciones(conexion_segmentos)
        union_segmentos = set().union(*segmentos_tipos.values())

        cuentas_segmentadas = cuentas_segmentadas_final(resultados, conexion_segmentos, union_segmentos)

        df_mongo = pd.DataFrame(cuentas_segmentadas, dtype=str)
        df_mongo['Cuenta_10'] = df_mongo['_id'].astype(str).str[-10:]
        df_mongo.to_excel(f'{leer_propiedades('archivos.xlsx.mongo')}mongo {date.today()}.xlsx')

        return df_mongo

    except Exception as e:
        log_trace(Nivel.ERROR, str(e))
        return


def frame_sql():
    try:
        sql_query = "SELECT * FROM tableCuentas;"
        with conexion_sql().connect() as conn:
            df_bd = pd.read_sql(sql_query, conn, dtype=str)
            df_bd.replace(['None', 'nan'], "", inplace=True)

            return df_bd
    except Exception as e:
        log_trace(Nivel.ERROR, str(e))


def merge_xlsx_bd(archivo_xlsx, cuenta_xlsx, nombre_hoja):
    df_xlsx = valida_nombre_hoja(archivo_xlsx, nombre_hoja)

    df_xlsx['Cuenta_10'] = df_xlsx[cuenta_xlsx].astype(str).str[-10:]
    df_resultado = pd.merge(df_xlsx, frame_sql(), left_on='Cuenta_10', right_on='cuenta_10', how='outer',
                            indicator=True)
    df_resultado['Coincide'] = df_resultado['_merge'] == 'both'
    log_trace(Nivel.INFORMATIVO, 'Se crea dataframe general')

    return df_resultado


def merge_mongo_sql():
    try:
        df_resultado = pd.merge(frame_mongo(), frame_sql(), left_on='Cuenta_10', right_on='cuenta_10', how='outer',
                                indicator=True)
        df_resultado['Coincide'] = df_resultado['_merge'] == 'both'

        return df_resultado
    except Exception as e:
        log_trace(Nivel.ERROR, str(e))


def compara_bd_xlsx(archivo_xlsx, cuenta_xlsx, nombre_hoja):
    df_xlsx = valida_nombre_hoja(archivo_xlsx, nombre_hoja)

    df_xlsx['Cuenta_10'] = df_xlsx[cuenta_xlsx].astype(str).str[-10:]
    df_resultado = pd.merge(df_xlsx, frame_sql(), left_on='Cuenta_10', right_on='cuenta_10', how='outer',
                            indicator=True)
    df_resultado['Coincide'] = df_resultado['_merge'] == 'both'

    df_excel_con_coincidentes = pd.merge(
        left=df_xlsx,
        right=df_resultado[
            [
                cuenta_xlsx, 'cuentas_', 'Grupo_1', 'Grupo_2', 'Grupo_3',
                'operacionesOtroSegmento', 'Coincide', '_merge'
            ]
        ],
        how='left',
        on=cuenta_xlsx
    )
    return df_excel_con_coincidentes
