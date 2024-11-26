from sqlalchemy import text
from src.dao.connectionBD import execute_session
from src.resources.common.level import Nivel
from src.resources.configlog import log_trace, configure_logging

configure_logging()


def crea_campo_bd(df_coincidentes, campo_nuevo):
    session = execute_session()

    if session:
        try:
            if campo_nuevo not in df_coincidentes.columns:
                session.execute(text(f"ALTER TABLE tableCuentas ADD {campo_nuevo} VARCHAR(255);"))
                session.commit()

            contador = 0
            for index, row in df_coincidentes.iterrows():
                cuenta_10 = row['Cuenta_10']
                contenido_servicio = ''  # Este campo varía, puede ser de row[campo_nuevo] o valor directo
                session.execute(
                    text(
                        f"UPDATE tableCuentas SET {campo_nuevo} = :contenido_servicio WHERE cuenta_10 = :cuenta_10"),
                    {
                        'contenido_servicio': contenido_servicio,
                        'cuenta_10': cuenta_10
                    }
                )
                contador += 1
            session.commit()

            log_trace(Nivel.INFORMATIVO, f"Se actualizaron {contador} registros.")
        except Exception as e:
            session.rollback()
            log_trace(Nivel.ERROR, f"Error durante la ejecución: {e}")
        finally:
            session.close()
    else:
        log_trace(Nivel.ERROR, 'No se pudo establecer la sesión')
