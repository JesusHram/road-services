import os
import logging
import streamlit as st
from mygeotab import API as GeotabAPI
from geofence_core import GeofenceAnalyzer

logger = logging.getLogger(__name__)

# ID del grupo US en Geotab
US_GROUP_ID = "b27A3"

def get_geotab_connection():
    """Obtener conexión a Geotab - funciona en Streamlit y GitHub Actions"""
    if os.getenv('GEOTAB_USERNAME'):
        return GeotabAPI(
            username=os.getenv('GEOTAB_USERNAME'),
            password=os.getenv('GEOTAB_PASSWORD'),
            database=os.getenv('GEOTAB_DATABASE'),
            server="my.geotab.com"
        )
    else:
        import streamlit as st
        db_credentials = st.secrets["geotab"]
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
        
        gps_points = [{'lat': float(log['latitude']), 'lon': float(log['longitude']), 'timestamp': log.get('dateTime', '')} for log in log_records if log.get('latitude') and log.get('longitude')]
        logger.info(f"   📍 {len(gps_points)} puntos GPS obtenidos")
        return gps_points
        
    except Exception as e:
        logger.error(f"❌ Error obteniendo datos GPS para {vehicle_name}: {e}")
        return []

def get_us_vehicles():
    """
    Obtiene los vehículos de US, primero filtrando por tipo de asset (vehículo)
    y luego por el grupo específico de US.
    """
    try:
        api = get_geotab_connection()
        api.authenticate()

        # --- PASO 1: Obtener SOLO los assets que son vehículos ---
        # Hacemos una llamada para traer todos los dispositivos que pertenecen al grupo de vehículos.
        # Esto excluye las 'cajas' y reduce drásticamente el tamaño de la respuesta.
        logger.info("Paso 1: Obteniendo todos los dispositivos del grupo 'GroupVehicleId'...")
        all_vehicles = api.get('Device', search={
            'groups': [{'id': 'GroupVehicleId'}] # Asegúrate de que este ID sea correcto
        })
        logger.info(f"   -> Se encontraron {len(all_vehicles)} vehículos en total.")

        # --- PASO 2: Filtrar esa lista para encontrar los de US ---
        # Ahora, de esa lista de solo vehículos, buscamos los que también están en el grupo de US.
        logger.info("Paso 2: Filtrando la lista de vehículos para encontrar los del grupo US...")
        us_vehicles_filtered = []
        for vehicle in all_vehicles:
            vehicle_groups = vehicle.get('groups', [])
            # Verificamos si AL MENOS UNO de sus grupos es el de US
            is_us_vehicle = any(group.get('id') == US_GROUP_ID for group in vehicle_groups)
            
            if is_us_vehicle:
                us_vehicles_filtered.append({
                    'id': vehicle.get('id'),
                    'name': vehicle.get('name'),
                    'licensePlate': vehicle.get('licensePlate', '')
                })
        
        logger.info(f"✅ Encontrados {len(us_vehicles_filtered)} vehículos de US después del filtrado.")
        return us_vehicles_filtered
        
    except Exception as e:
        logger.error(f"❌ Error obteniendo vehículos US con el método de dos pasos: {e}")
        return []

def analyze_weekly_geofence_data(vehicle_name, start_date, end_date):
    """Función principal para análisis de geocercas"""
    gps_points = get_geotab_gps_data(vehicle_name, start_date, end_date)
    if not gps_points:
        return {}
    analyzer = GeofenceAnalyzer("geofences.json")
    results = analyzer.analyze_vehicle_data(vehicle_name, gps_points)
    return results

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
    debug_vehicle_groups()