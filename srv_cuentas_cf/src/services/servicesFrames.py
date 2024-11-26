import pandas as pd
from loadProperties import leer_propiedades
from src.dao.connectionMongoDB import conexion_mongodb
from src.resources.common.level import Nivel
from src.resources.configlog import log_trace, configure_logging
from src.utils.frames import frame_sql

configure_logging()


def rpt_agrupacion():
    df_bd = frame_sql()

    coleccion_agrupacion = conexion_mongodb(leer_propiedades('coleccion.agrupacion'))
    df_agrupaciones = pd.DataFrame(coleccion_agrupacion.find(), columns=["GRUPO", "SEGMENTOS"])

    try:
        for index, row in df_agrupaciones.iterrows():
            nombre_grupo = row['GRUPO']
            segmentos_grupo = [seg.strip() for seg in row['SEGMENTOS']]

            df_bd[nombre_grupo] = ""

            for i, segmento_bd in df_bd.iterrows():
                if pd.notna(segmento_bd['segmento']):
                    segmentos_bd_list = [seg.strip() for seg in segmento_bd['segmento'].split(', ')]
                    segmentos_coincidentes = [seg for seg in segmentos_grupo if seg in segmentos_bd_list]

                    if segmentos_coincidentes:
                        df_bd.at[i, nombre_grupo] = ', '.join(segmentos_coincidentes)
        log_trace(Nivel.INFORMATIVO, f'Ejecución de {rpt_agrupacion} completada con éxito')
        return df_bd
    except Exception as e:
        log_trace(Nivel.ERROR, f"Error durante la ejecución de {rpt_agrupacion}: {e}")


def agrupa_segmetos(df_mongo):

    coleccion_agrupacion = conexion_mongodb(leer_propiedades('coleccion.agrupacion'))
    df_agrupaciones = pd.DataFrame(coleccion_agrupacion.find(), columns=["GRUPO", "SEGMENTOS"])

    try:
        for index, row in df_agrupaciones.iterrows():
            nombre_grupo = row['GRUPO']
            segmentos_grupo = [seg.strip() for seg in row['SEGMENTOS']]

            df_mongo[nombre_grupo] = ""

            for i, segmento_bd in df_mongo.iterrows():
                if pd.notna(segmento_bd['segmento']):
                    segmentos_bd_list = [seg.strip() for seg in segmento_bd['segmento'].split(', ')]
                    segmentos_coincidentes = [seg for seg in segmentos_grupo if seg in segmentos_bd_list]

                    if segmentos_coincidentes:
                        df_mongo.at[i, nombre_grupo] = ', '.join(segmentos_coincidentes)
        log_trace(Nivel.INFORMATIVO, f'Ejecución de actualiza_segmentaciones completada con éxito')
        return df_mongo
    except Exception as e:
        log_trace(Nivel.ERROR, f"Error durante la ejecución de {rpt_agrupacion}: {e}")
