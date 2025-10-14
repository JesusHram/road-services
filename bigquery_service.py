import os
import json
import logging
import base64
from google.cloud import bigquery
from google.oauth2 import service_account
import pytz
from datetime import datetime

class BigQueryService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        credentials = None

        # 1. Intentar desde variable de entorno (GitHub Actions)
        credentials_b64 = os.getenv('BIGQUERY_SERVICE_ACCOUNT_KEY')
        if credentials_b64:
            try:
                # Decodificar Base64 a JSON
                credentials_json = base64.b64decode(credentials_b64).decode('utf-8')
                credentials_info = json.loads(credentials_json)
                credentials = service_account.Credentials.from_service_account_info(credentials_info)
                self.logger.info("✅ Credenciales BigQuery cargadas desde variable Base64 (entorno)")
            except Exception as e:
                self.logger.error(f"❌ Error cargando credenciales desde variable Base64: {e}")

        # 2. Intentar desde archivo local (para entorno de desarrollo)
        if not credentials:
            try:
                credentials_path = os.path.join(os.getcwd(), 'credentials', 'driverscoring-275722-424b06080c95.json')
                if os.path.exists(credentials_path):
                    credentials = service_account.Credentials.from_service_account_file(credentials_path)
                    self.logger.info(f"✅ Credenciales BigQuery cargadas desde archivo: {credentials_path}")
                else:
                    self.logger.warning(f"⚠️ Archivo de credenciales no encontrado en: {credentials_path}")
            except Exception as e:
                self.logger.error(f"❌ Error cargando credenciales desde archivo: {e}")

        # 3. Fallback: usar credenciales por defecto
        if not credentials:
            self.logger.warning("⚠️ Usando credenciales por defecto de BigQuery")
            credentials = None

        # Crear cliente
        self.client = bigquery.Client(credentials=credentials)
        self.table_id = "driverscoring-275722.zaro_transportation.driver_status_geotab"
        self.nuevo_laredo_tz = pytz.timezone('America/Monterrey')

        self.logger.info("✅ Cliente BigQuery inicializado correctamente")

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
