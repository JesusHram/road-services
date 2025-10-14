import os
import logging
from datetime import datetime, timedelta
from bigquery_service import BigQueryService
# --- CORRECCIÓN 1: Importar la función para la conexión ---
from geofence_calculator import analyze_weekly_geofence_data, get_us_vehicles, get_geotab_connection

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ID del grupo US en Geotab (Asegúrate de que esté definido aquí o en el otro archivo)
US_GROUP_ID = "b27A3"

# --- CORRECCIÓN 2: La función ahora acepta la conexión 'api' como parámetro ---
def prepare_daily_data(api, fecha):
    """Preparar datos del día para BigQuery usando una conexión API existente."""
    
    # Esta función ahora también debería aceptar el objeto 'api' para ser 100% eficiente,
    # pero por ahora el cambio principal es en el bucle.
    us_vehicles = get_us_vehicles(api)
    
    if not us_vehicles:
        logger.warning("⚠️ No se encontraron vehículos del grupo US")
        return []
    
    all_rows = []
    processed_vehicles = 0
    
    for vehicle in us_vehicles:
        vehicle_id = vehicle['id'] # Necesitamos el ID para la llamada a la API
        vehicle_name = vehicle['name']
        placa = vehicle.get('licensePlate', '')
        
        logger.info(f"📊 Procesando {vehicle_name} (Placa: {placa}) para {fecha}")
        
        try:
            # --- CORRECCIÓN 3: Pasamos la conexión 'api' y el 'vehicle_id' ---
            results = analyze_weekly_geofence_data(api, vehicle_name, vehicle_id, fecha, fecha)
            
            # (El resto de tu lógica para crear las filas es correcta)
            for geofence_id, data in results.items():
                if data['total_km'] > 0:
                    row = {
                        "fecha": fecha,
                        "camion": vehicle_name,
                        "placa": placa,
                        "geofence_id": geofence_id,
                        "geofence_name": geofence_id,
                        "ciclos": data.get('cycles', 0),
                        "km_calculados": round(data['total_km'], 2),
                        "tipo_calculo": data['type'],
                        "km_por_ciclo": data.get('km_per_cycle', 0),
                        "km_reales": round(data.get('distance_km', 0), 2),
                        "timestamp_carga": datetime.now().isoformat()
                    }
                    all_rows.append(row)
            
            processed_vehicles += 1
                    
        except Exception as e:
            logger.error(f"❌ Error procesando {vehicle_name}: {e}")
    
    logger.info(f"✅ Procesados {processed_vehicles}/{len(us_vehicles)} vehículos")
    return all_rows

def generate_summary(rows):
    """Generar resumen de los datos procesados (Esta función es correcta)."""
    if not rows:
        return "No hay datos para resumir"
    summary = {}
    total_km = 0
    for row in rows:
        camion = row['camion']
        if camion not in summary:
            summary[camion] = 0
        summary[camion] += row['km_calculados']
        total_km += row['km_calculados']
    summary_text = f"📈 Resumen - Total: {total_km:.2f} km\n"
    for camion, km in summary.items():
        summary_text += f"   {camion}: {km:.2f} km\n"
    return summary_text

def main():
    """Función principal - se ejecuta diariamente"""
    try:
        bq_service = BigQueryService()
        
        # --- CORRECCIÓN 4: Crear UNA SOLA conexión al inicio ---
        logger.info("🔌 Creando conexión única a la API de Geotab...")
        geotab_api = get_geotab_connection()
        geotab_api.authenticate()
        logger.info("   -> Conexión exitosa.")

        fecha = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        logger.info(f"🚀 Iniciando carga para {fecha}")
        
        # --- CORRECCIÓN 5: Eliminar la inicialización duplicada de BigQuery ---
        # bq_service = BigQueryService() # Esta línea era redundante

        bq_service.create_table_if_not_exists()
        
        # --- CORRECCIÓN 6: Pasamos la conexión 'geotab_api' a la función ---
        rows = prepare_daily_data(geotab_api, fecha)
        
        if rows:
            success = bq_service.insert_daily_metrics(rows)
            if success:
                summary = generate_summary(rows)
                logger.info(f"\n{summary}")
            else:
                logger.error("❌ Falló la inserción en BigQuery")
        else:
            logger.warning("⚠️ No hay datos para guardar")
        
        logger.info("🎯 Proceso completado!")
        
    except Exception as e:
        logger.error(f"💥 Error en el proceso principal: {e}")
        raise

if __name__ == "__main__":
    main()