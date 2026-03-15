"""" MÓDULO: DataConsilidator ( COnsolidador del Dataset Maestro)

Este componente actúa como el núcleo de integración de pipeline de datos.
Su objetivo es unificar múltiple fuentes atómicas (archivo por liga y temporada) en un único Dataset Maestro

UTILIDAD ESTRATËGICA:
    1. Inyeccion de metadatos: Enriquece registros con 'liga' y 'temporada' extraídos del sistema de archivos.
    2. Integridad Temporal: Asegura que el flujo de datos sea cronológicamente coherentes por modelos de series temporales
    3. Normalización: Estandariza tipos de datos para evitar errores en la fase de entrenamiento de Marchine Learning.
"""

import logging
from pathlib import Path
import pandas as pd
from modelo_futbol.configuracion.direcciones import DIR_PROCESSED, DIR_FINAL

logger = logging.getLogger(__name__)

class DataConsolidator:
    """
    Clase responsable de la agregación y enriquecimiento de datasets procesados.
    
    El flujo de ejecución sigue el patrón: 
    LECTURA -> PARSING DE METADATOS -> CONCATENACIÓN -> ORDENAMIENTO -> PERSISTENCIA.
    """

    def __init__(self):
        # Funcion que inicia la clase
        pass

    
    # ==========================================================
    # EXTRACCIÓN DE TEMPORADA Y LIGA
    # ==========================================================
    def _extraer_metadata(self, ruta: Path):
        """ Se va a encargar de extraer la liga y los temporadas desde el nombre del arhcivo"""
        nombre = ruta.stem
        partes = nombre.split("_")

        if len(partes) < 2:
            raise ValueError(f"No se pudo extraer metadata en {ruta.name}")

        liga = partes[0]
        temporada = partes[1]

        return liga, temporada


    # ==========================================================
    # GUARDAR EL DATAFRAME DE LAS LIGAS EN LA RUTA
    # ==========================================================

    def _guardar_dataset(self, df: pd.DataFrame):
        """" Guarda el dataset_maestro en la carpeta final. """
        DIR_FINAL.mkdir(parents=True, exist_ok=True)

        ruta_final = DIR_FINAL / "final"
        
        df.to_csv(ruta_final, index= False)

        logger.info(f"Dataset maestro guardado en {ruta_final}")


    # ==========================================================
    # EJECUCION
    # ==========================================================
    def ejecucion(self):
        logger.info("Iniciando consolidación del dataset.")

        # variable que va a contener la lista de todos los archivos .csv dentro de processed
        archivos = list(DIR_PROCESSED.glob("*.csv"))

        if not archivos:
            logger.warning("No se encontraron archivos procesados.")
            return

        dataframes = []

        for archivo in archivos:
            try:
                logger.info(f"Consolidando {archivo.name}")

                df = pd.read_csv(archivo)

                liga, temporada = self._extraer_metadata(archivo)

                df["liga"] = liga
                df["temporada"] = temporada

                dataframes.append(df)
            
            except Exception as e:
                logger.error(f"Error consolidando {archivo.name}: {e}")

        if not dataframes:
            logger.error(f"No se pudo consolidar ningún archivo.")
            return
        
        df_final = pd.concat(dataframes, ignore_index=True)

        # Asegurar formato de fecha
        df_final["fecha"] = pd.to_datetime(df["Date"], errors="coerce")

        # Orden cronologico
        df_final = df_final.sort_values("fecha").reset_index(drop=True)

        self._guardar_dataset(df_final)

        logger.info("Consolidación finalizada correctamente.")