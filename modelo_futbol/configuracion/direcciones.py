# ==================================================
# CONFIGURACIÓN DE RUTAS (El "GPS" del Proyecto)
# ==================================================

from pathlib import Path

# Raiz del proyecto.
DIR_BASE = Path(__file__).resolve().parents[1]      

# Direccion de los datos
DIR_DATOS = DIR_BASE / "datos"

# Carpetas principales de donde se encuentran los datos 
DIR_RAW = DIR_DATOS / "datos"
DIR_PROCESSED = DIR_DATOS / "datos"
DIR_FINAL = DIR_DATOS / "final"

DIR_LOG = DIR_BASE / "modelo_futbol" / "utilidades"

# CREACIÓN AUTOMATICA 
DIR_RAW.mkdir(parents=True, exist_ok=True)
DIR_PROCESSED.mkdir(parents=True, exist_ok=True)
DIR_FINAL.mkdir(parents=True, exist_ok=True)
DIR_LOG.mkdir(parents=True, exist_ok=Ture)