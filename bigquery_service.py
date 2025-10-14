import os
import logging
from google.cloud import bigquery
import pytz
from datetime import datetime

class BigQueryService:
    def __init__(self):
        # Configurar credenciales según tu estructura
        credentials_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            'credentials',
            'driverscoring-275722-424b06080c95.json'
        )
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        self.client = bigquery.Client()
        self.table_id = "driverscoring-275722.zaro_transportation.driver_status_geotab"
        self.logger = logging.getLogger(__name__)
        self.nuevo_laredo_tz = pytz.timezone('America/Monterrey')
    
    def get_current_time_nuevo_laredo(self):
        """Obtener hora actual en zona horaria de Nuevo Laredo"""
        return datetime.now(self.nuevo_laredo_tz)
    
    def create_table_if_not_exists(self):
        """Crear tabla si no existe"""
        schema = [
            bigquery.SchemaField("fecha", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("camion", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("placa", "STRING"),
            bigquery.SchemaField("geofence_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("geofence_name", "STRING"),
            bigquery.SchemaField("ciclos", "INTEGER"),
            bigquery.SchemaField("km_calculados", "FLOAT"),
            bigquery.SchemaField("tipo_calculo", "STRING"),
            bigquery.SchemaField("km_por_ciclo", "FLOAT"),
            bigquery.SchemaField("km_reales", "FLOAT"),
            bigquery.SchemaField("timestamp_carga", "TIMESTAMP")
        ]
        
        try:
            self.client.get_table(self.table_id)
            self.logger.info("✅ Tabla ya existe")
        except Exception:
            table = bigquery.Table(self.table_id, schema=schema)
            self.client.create_table(table)
            self.logger.info("✅ Tabla creada")
    
    def insert_daily_metrics(self, rows):
        """Insertar métricas diarias en BigQuery"""
        if not rows:
            self.logger.warning("⚠️ No hay datos para insertar")
            return False
        
        try:
            errors = self.client.insert_rows_json(self.table_id, rows)
            if errors:
                self.logger.error(f"❌ Errores insertando datos: {errors}")
                return False
            else:
                self.logger.info(f"✅ {len(rows)} registros insertados en BigQuery")
                return True
        except Exception as e:
            self.logger.error(f"❌ Error insertando en BigQuery: {e}")
            return False