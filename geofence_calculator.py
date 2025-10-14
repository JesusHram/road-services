import os
import logging
import streamlit as st
from mygeotab import API as GeotabAPI
from geofence_core import GeofenceAnalyzer

logger = logging.getLogger(__name__)

# ID del grupo US en Geotab
US_GROUP_ID = "b27A3"

db_credentials = st.secrets["geotab"]

def get_geotab_connection():
    """Obtener conexión a Geotab desde variables de entorno"""
    return GeotabAPI(
        username=db_credentials["GEOTAB_USERNAME"],
        password=db_credentials["GEOTAB_PASSWORD"],
        database=db_credentials["GEOTAB_DATABASE"],
        server="my.geotab.com"
    )

def get_geotab_gps_data(vehicle_name, start_date, end_date):
    """Obtener datos GPS para un vehículo específico"""
    try:
        api = get_geotab_connection()
        api.authenticate()
        
        # Buscar el vehículo por nombre
        vehicles = api.get("Device", search={"name": vehicle_name})
        if not vehicles:
            logger.warning(f"⚠️ Vehículo {vehicle_name} no encontrado")
            return []
        
        vehicle_id = vehicles[0]["id"]
        
        logger.info(f"📍 Obteniendo datos GPS para {vehicle_name} ({start_date} a {end_date})")
        
        log_records = api.get('LogRecord', {
            'deviceSearch': {'id': vehicle_id},
            'fromDate': f"{start_date} 00:00:00",
            'toDate': f"{end_date} 23:59:59"
        })
        
        gps_points = []
        for log in log_records:
            if log.get('latitude') and log.get('longitude'):
                gps_points.append({
                    'lat': float(log['latitude']),
                    'lon': float(log['longitude']),
                    'timestamp': log.get('dateTime', '')
                })
        
        logger.info(f"   📍 {len(gps_points)} puntos GPS obtenidos")
        return gps_points
        
    except Exception as e:
        logger.error(f"❌ Error obteniendo datos GPS para {vehicle_name}: {e}")
        return []

def get_us_vehicles():
    """Obtener solo los vehículos del grupo US"""
    try:
        api = get_geotab_connection()
        api.authenticate()
        
        # Obtener todos los vehículos
        all_vehicles = api.get('Device')
        
        us_vehicles = []
        for vehicle in all_vehicles:
            vehicle_groups = vehicle.get('groups', [])
            is_us_vehicle = any(group.get('id') == US_GROUP_ID for group in vehicle_groups)
            if is_us_vehicle:
                us_vehicles.append({
                    'id': vehicle.get('id'),
                    'name': vehicle.get('name'),
                    'licensePlate': vehicle.get('licensePlate', '')
                })
        
        logger.info(f"✅ Encontrados {len(us_vehicles)} vehículos del grupo US")
        return us_vehicles
        
    except Exception as e:
        logger.error(f"❌ Error obteniendo vehículos US: {e}")
        return []

def analyze_weekly_geofence_data(vehicle_name, start_date, end_date):
    """
    Función principal para análisis de geocercas
    
    Args:
        vehicle_name: Nombre del vehículo
        start_date: Fecha inicio (YYYY-MM-DD)
        end_date: Fecha fin (YYYY-MM-DD)
    
    Returns:
        Dict con resultados de geocercas
    """
    # 1. Obtener datos GPS
    gps_points = get_geotab_gps_data(vehicle_name, start_date, end_date)
    
    if not gps_points:
        return {}
    
    # 2. Inicializar analizador
    analyzer = GeofenceAnalyzer("geofences.json")
    
    # 3. Analizar datos
    results = analyzer.analyze_vehicle_data(vehicle_name, gps_points)
    
    return results

# Función para debug
def debug_vehicle_groups():
    """Función de debug para ver grupos de vehículos"""
    try:
        api = get_geotab_connection()
        api.authenticate()
        
        all_vehicles = api.get('Device')
        
        logger.info("🚗 Lista de vehículos y sus grupos:")
        logger.info("=" * 60)
        
        for vehicle in all_vehicles:
            vehicle_name = vehicle.get('name', 'Sin nombre')
            groups = vehicle.get('groups', [])
            group_ids = [group.get('id', 'Sin ID') for group in groups]
            
            logger.info(f"{vehicle_name}:")
            logger.info(f"  Grupos: {group_ids}")
            logger.info(f"  ID: {vehicle.get('id')}")
            logger.info("-" * 40)
            
    except Exception as e:
        logger.error(f"❌ Error en debug: {e}")

if __name__ == "__main__":
    # Ejecutar debug si se llama directamente
    debug_vehicle_groups()