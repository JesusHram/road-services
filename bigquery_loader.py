import os
import logging
from datetime import datetime, timedelta
from bigquery_service import BigQueryService
from geofence_calculator import analyze_weekly_geofence_data, get_us_vehicles

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ID del grupo US en Geotab
US_GROUP_ID = "b27A3"

def prepare_daily_data(fecha):
    """Preparar datos del día para BigQuery"""
    
    # Obtener vehículos del grupo US
    us_vehicles = get_us_vehicles()
    
    if not us_vehicles:
        logger.warning("⚠️ No se encontraron vehículos del grupo US")
        return []
    
    all_rows = []
    processed_vehicles = 0
    
    for vehicle in us_vehicles:
        vehicle_name = vehicle['name']
        placa = vehicle.get('licensePlate', '')
        
        logger.info(f"📊 Procesando {vehicle_name} (Placa: {placa}) para {fecha}")
        
        try:
            # Analizar datos del día
            results = analyze_weekly_geofence_data(vehicle_name, fecha, fecha)
            
            for geofence_id, data in results.items():
                if data['total_km'] > 0:  # Solo guardar geocercas con actividad
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
    """Generar resumen de los datos procesados"""
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
        # Inicializar servicio BigQuery
        bq_service = BigQueryService()
        
        # Usar fecha de ayer (datos del día actual pueden no estar completos)
        fecha = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        logger.info(f"🚀 Iniciando carga para {fecha}")
        logger.info("=" * 50)
        logger.info(f"🔐 BIGQUERY_SERVICE_ACCOUNT_KEY definida: {'BIGQUERY_SERVICE_ACCOUNT_KEY' in os.environ}")
        
        #incializar servicio BigQuery
        bq_service = BigQueryService()

        # Crear tabla si no existe
        bq_service.create_table_if_not_exists()
        
        # Preparar datos
        rows = prepare_daily_data(fecha)
        
        # Insertar en BigQuery
        if rows:
            success = bq_service.insert_daily_metrics(rows)
            
            if success:
                # Mostrar resumen
                summary = generate_summary(rows)
                logger.info(f"\n{summary}")
            else:
                logger.error("❌ Falló la inserción en BigQuery")
        else:
            logger.warning("⚠️ No hay datos para guardar")
        
        logger.info("=" * 50)
        logger.info("🎯 Proceso completado!")
        
    except Exception as e:
        logger.error(f"💥 Error en el proceso principal: {e}")
        raise

if __name__ == "__main__":
    main()