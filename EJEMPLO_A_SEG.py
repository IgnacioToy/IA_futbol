"""
MÓDULO: DataConsolidator (Consolidador de Dataset Maestro)
PROYECTO: Modelo Predictivo de Fútbol

DESCRIPCIÓN:
    Este componente actúa como el núcleo de integración del pipeline de datos. 
    Su objetivo es unificar múltiples fuentes atómicas (archivos por liga y temporada) 
    en un único Dataset Maestro ('dataset_maestro.csv').

UTILIDAD ESTRATÉGICA:
    1. Inyección de Metadatos: Enriquece registros con 'liga' y 'temporada' 
       extraídos del sistema de archivos.
    2. Integridad Temporal: Asegura que el flujo de datos sea cronológicamente 
       coherente para modelos de series temporales.
    3. Normalización: Estandariza tipos de datos para evitar errores en la 
       fase de entrenamiento de Machine Learning.

AUTOR: Gemini / AI Collaborator
FECHA: 2026
"""



import logging
import pandas as pd
from pathlib import Path
from configuracion.base import DIR_PROCESSED, DIR_FINAL

logger = logging.getLogger(__name__)

class DataConsolidator:
    """
    Clase responsable de consolidar todas los archivos procesados en un único dataset maestro.
    Flujo: PORCESSED -> Unificación -> Orden cronológico -> FINAL.
    """
    def __init__(self):
        pass

    # Punto de entrada
    def run(self):
        # Ejecuta el proceso de conolidación
        logger.info("Iniciando consolidación de dataset.")
        archivos = list(DIR_PROCESSED.glob("*.csv"))

        if not archivos:
            logger.warning("No se encontraron archivos procesados.")
            return

        dataframes = []

        for archivo in archivos:
            try:
                logger.info(f"Consolidando: {archivo.name}")
                df = pd.read_csv(archivo)


                liga, temporada = self._extraer_metadatos(archivo)

                df["liga"] = liga
                df["temporada"] = temporada

                dataframes.append(df)
            
            except Exception as e:
                logger.error(f"Error consolidado {archivo.name}: {e}")
        
        if not dataframes:
            logger.warning("No se pudo consolidar ningún archivo.")
            return

        df_final = pd.concat(dataframes, ignore_index=True)

        # Asegurar formato fecha
        df_final["Date"] = pd.to_datetime(df["Date"], errors="coarce")

        # Orden cronologico
        df_final = df_final.sort_values("Dates").reset_index(drop=True)

        self._guardar_dataset(df_final)

        logger.info("Consolidación finalizada correctamente.")

    # ==========================================================
    # EXTRAER LIGA Y TEMPORADA DESDE EL NOMBRE
    # ==========================================================
    def _extraer_metadata(self, ruta: Path):
        # Extrae liga y temporadas desde el nombre del archivo
        nombre = ruta.stem
        partes = nombre.split("_")

        if len(partes) < 2:
            raise ValueError(f"No se pudo extraer metadata en {ruta.name}")
        
        liga = partes[0]
        temporada = partes[1]

        return liga, temporada

    # ==========================================================
    # GUARDAR DATASET MAESTRO
    # ==========================================================
    def _guardar_dataset(self, df: pd.DataFrame):
        # Guarda el dataset maestro en carpeta FINAL.
        DIR_FINAL.mkdir(parents=True, exist_ok=True)

        ruta_final = DIR_FINAL / "dataset_maestro.csv"
        df.to_csv(ruta_final, index=False)

        logger.info(f"Dataset maestro guardado en {ruta_final}")