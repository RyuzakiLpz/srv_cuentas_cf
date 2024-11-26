import pandas as pd
from sqlalchemy import text
from src.dao.connectionBD import execute_session, execute_procedure_batch
from src.resources.common.level import Nivel
from src.resources.configlog import log_trace, configure_logging
from src.resources.common.constants import CONSCONSTANTS

configure_logging()


def inserta_allin(df_no_coincidentes):
    batch_size = 5000
    params_batch = []

    log_trace(Nivel.INFORMATIVO, 'INICIA inserta_allin ...')

    result_counter = execute_procedure_batch(CONSCONSTANTS.SP_COUNTER)
    log_trace(Nivel.INFORMATIVO, f'Registros actuales en la BD: {result_counter[0]}')

    try:
        for index, row in df_no_coincidentes.iterrows():
            cuenta = row['_id']
            tipos_de_operacion = row['tipos_de_operacion_x']
            alnovaID = row['cuentaID_x']
            subProducto = row['subProducto_x']
            descripcion = row['descripcion_x']
            segmento = row['segmento_x']
            operacionesOtroSegmento = row['operaciones_no_coincidentes']
            cuenta_10 = row['Cuenta_10']

            operacionesOtroSegmento = '' if pd.isna(operacionesOtroSegmento) else str(operacionesOtroSegmento)

            params_batch.append((cuenta, tipos_de_operacion, alnovaID, subProducto, descripcion, segmento,
                                 operacionesOtroSegmento, cuenta_10))

            if len(params_batch) >= batch_size:
                execute_procedure_batch(CONSCONSTANTS.SP_INSERT_ALLIN, params_batch)
                params_batch = []
                log_trace(Nivel.INFORMATIVO, f"Se han insertado {index + 1} filas.")

        if params_batch:
            execute_procedure_batch(CONSCONSTANTS.SP_INSERT_ALLIN, params_batch)
            log_trace(Nivel.INFORMATIVO, f"Se han insertado {len(df_no_coincidentes)} filas en total.")

        result_counter = execute_procedure_batch(CONSCONSTANTS.SP_COUNTER)
        log_trace(Nivel.INFORMATIVO, f'Registros finales en la BD: {result_counter[0]}')

    except Exception as e:
        log_trace(Nivel.ERROR, f"Error durante la ejecución de inserta_allin(): {e}")
        return
