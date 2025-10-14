import os
import logging
import pytz
from datetime import datetime
from google.cloud import bigquery
from google.api_core import exceptions

class BigQueryService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

        try:
            # La librería buscará automáticamente las credenciales.
            # No necesitas pasarle nada.
            self.client = bigquery.Client()
            
            # Es una buena práctica hacer una llamada simple para verificar la conexión
            # y la autenticación al inicio.
            self.client.list_datasets(max_results=1) 
            self.logger.info("✅ Cliente BigQuery inicializado y autenticado correctamente.")

        except exceptions.DefaultCredentialsError:
            self.logger.error("❌ ERROR DE AUTENTICACIÓN: No se encontraron las credenciales.")
            self.logger.error("Asegúrate de haberte autenticado. En local, usa 'gcloud auth application-default login'.")
            self.logger.error("En GitHub Actions, asegúrate de que el paso 'google-github-actions/auth' se ejecutó correctamente.")
            raise # Detiene la ejecución si no hay credenciales

        except Exception as e:
            self.logger.error(f"❌ Fallo al inicializar el cliente BigQuery: {e}")
            raise

        self.table_id = "driverscoring-275722.zaro_transportation.camiones_metrics"
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
