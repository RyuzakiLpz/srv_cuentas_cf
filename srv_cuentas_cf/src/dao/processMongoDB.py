from src.resources.common.level import Nivel
from src.resources.configlog import log_trace, configure_logging

configure_logging()


def pipeline_cuentas():
    try:
        add_set = "$addToSet"
        pipeline = [
            {
                "$group": {
                    "_id": "$CUENTA",
                    "tipos_de_operacion": {add_set: "$TIPO DE OPERACION"},
                    "cuentaID": {add_set: "$CUENTA ID"},
                    "subProducto": {add_set: "$SUBPRODUCTO"},
                    "descripcion": {add_set: "$DESCRIPCION SUBPRODUCTO"}
                }
            }
        ]
        return pipeline
    except Exception as e:
        log_trace(Nivel.ERROR, f"Error obteniendo resultados de pipeline: {e}")
        return


def obtener_resultados_cuentas(conexion_cuentas):
    try:
        return list(conexion_cuentas.aggregate(pipeline_cuentas()))
    except Exception as e:
        log_trace(Nivel.ERROR, f"Error obteniendo resultados de cuentas: {e}")
        return


def segmentar_operaciones(conexion_segmentos):
    try:
        segmentos_tipos_operacion = {}
        for documento_segmento in conexion_segmentos.find():
            segmento = documento_segmento.get("SEGMENTO")
            tipos_operacion = documento_segmento.get("TIPO_DE_OPERACION", [])
            segmentos_tipos_operacion[segmento] = tipos_operacion
        return segmentos_tipos_operacion
    except Exception as e:
        log_trace(Nivel.ERROR, f'Error obteniendo tipos de operación de segmentos: {e}')
        return


def cuentas_segmentadas_final(resultados, conexion_segmentos, union_segmentos):
    resultados_finales = []
    try:
        for resultado in resultados:
            banderaSegmento = False
            listaResultado = []
            operacionesNoCoincidentes = []

            if not all(tipo_operacion in union_segmentos for tipo_operacion in resultado.get("tipos_de_operacion", [])):
                banderaSegmento = True
                operacionesNoCoincidentes.extend(tipo_operacion for tipo_operacion in resultado.get("tipos_de_operacion", []) if tipo_operacion not in union_segmentos)

            for documento_segmento in conexion_segmentos.find():
                segmento = documento_segmento.get("SEGMENTO")
                tipos_operacion = documento_segmento.get("TIPO_DE_OPERACION", [])
                coincidir = False

                for tipo_operacion in resultado.get("tipos_de_operacion", []):
                    if tipo_operacion in tipos_operacion and not coincidir:
                        coincidir = True
                        listaResultado.append(segmento)

            if listaResultado:
                resultado["segmento"] = ', '.join(listaResultado)

            if banderaSegmento:
                if operacionesNoCoincidentes:
                    listaResultado.append("otroSegmento")
                    resultado["segmento"] = ', '.join(listaResultado)
                resultado["operaciones_no_coincidentes"] = operacionesNoCoincidentes

            resultado["tipos_de_operacion"] = ', '.join(map(str, resultado.get("tipos_de_operacion", [])))
            resultado["cuentaID"] = ', '.join(map(str, resultado.get("cuentaID", [])))
            resultado["subProducto"] = ', '.join(map(str, resultado.get("subProducto", [])))
            resultado["descripcion"] = ', '.join(map(str, resultado.get("descripcion", [])))
            operacionesNoCoincidentes = ', '.join(map(str, operacionesNoCoincidentes))

            resultados_finales.append(resultado)
        log_trace(Nivel.INFORMATIVO, 'cuentas_segmentadas_final finalizado con éxito')
        return resultados_finales
    except Exception as e:
        log_trace(Nivel.ERROR, f'Error procesando cuentas segmentadas: {e}')
        return
