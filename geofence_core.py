import json
import logging
from shapely.geometry import Point, Polygon
from math import radians, sin, cos, sqrt, atan2

logger = logging.getLogger(__name__)

# ===============================
# CONFIGURACIÓN DE KMS POR GEOCERCA
# ===============================
GEOCERCA_KM_VALUES = {
    "aduana_420": 37,    # 37 km por cada entrada/salida completa
    "colombia": 60,      # 60 km por cada entrada/salida completa  
    "nuevo_laredo": None  # Se calculan km reales (no fijos)
}

# ===============================
# SERVICIO DE GEOCERCAS
# ===============================
class GeofenceService:
    def __init__(self, geofence_file="geofences.json"):
        self.geofences = self._load_geofences(geofence_file)
        self.km_values = GEOCERCA_KM_VALUES
    
    def _load_geofences(self, file_path):
        """Cargar geocercas y sus configuraciones"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            geofences = {}
            for feature in data['features']:
                properties = feature['properties']
                geometry = feature['geometry']
                
                geofence_id = properties['id']
                name = properties['name']
                coordinates = geometry['coordinates'][0]
                
                polygon = Polygon(coordinates)
                
                geofences[geofence_id] = {
                    'name': name,
                    'polygon': polygon,
                    'properties': properties
                }
                logger.info(f"✅ Geocerca cargada: {name} (ID: {geofence_id})")
            
            return geofences
            
        except Exception as e:
            logger.error(f"❌ Error cargando geocercas: {e}")
            return {}
    
    def find_geofence(self, lat, lon):
        """Encontrar en qué geocerca está un punto"""
        point = Point(lon, lat)
        
        for geofence_id, geofence_data in self.geofences.items():
            if geofence_data['polygon'].contains(point):
                return {
                    'geofence_id': geofence_id,
                    'geofence_name': geofence_data['name'],
                    'inside': True
                }
        
        return {'inside': False}

# ===============================
# CALCULADORA DE DISTANCIAS
# ===============================
def calculate_distance(lat1, lon1, lat2, lon2):
    """Calcular distancia entre dos puntos GPS en kilómetros"""
    R = 6371  # Radio de la Tierra en km
    
    # Convertir a radianes
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    
    # Diferencias
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    # Fórmula de Haversine
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    
    return R * c

def calculate_real_distances(gps_points, geofence_service):
    """Calcular distancias reales recorridas dentro de cada geocerca"""
    distances = {}
    prev_point = None
    current_geofence = None
    
    for point in gps_points:
        lat, lon = point['lat'], point['lon']
        geofence_info = geofence_service.find_geofence(lat, lon)
        
        if geofence_info['inside']:
            geofence_id = geofence_info['geofence_id']
            
            # Si estamos en la misma geocerca que el punto anterior, calcular distancia
            if prev_point and current_geofence == geofence_id:
                distance_km = calculate_distance(
                    prev_point['lat'], prev_point['lon'],
                    lat, lon
                )
                
                if geofence_id not in distances:
                    distances[geofence_id] = 0
                distances[geofence_id] += distance_km
            
            current_geofence = geofence_id
        else:
            current_geofence = None
        
        prev_point = point
    
    return distances

# ===============================
# CONTADOR DE ENTRADAS/SALIDAS
# ===============================
class EntryExitCounter:
    def __init__(self, geofence_service):
        self.geofence_service = geofence_service
        self.vehicle_states = {}  # {vehicle_id: current_geofence}
    
    def count_entry_exit_cycles(self, vehicle_id, gps_points):
        """
        Contar ciclos completos de entrada/salida por geocerca
        
        Returns:
            {
                'aduana_420': 2,
                'colombia': 1,
                'nuevo_laredo': 3
            }
        """
        cycles_count = {geofence_id: 0 for geofence_id in self.geofence_service.geofences}
        current_state = self.vehicle_states.get(vehicle_id, {'current_geofence': None})
        
        for point in gps_points:
            lat, lon = point['lat'], point['lon']
            geofence_info = self.geofence_service.find_geofence(lat, lon)
            
            current_geofence = geofence_info['geofence_id'] if geofence_info['inside'] else None
            
            # Detectar cambio: salida de una geocerca
            if current_state['current_geofence'] and not current_geofence:
                # ¡Salida detectada! Contar un ciclo completo
                cycles_count[current_state['current_geofence']] += 1
                logger.debug(f"🚗 Ciclo completo: {current_state['current_geofence']}")
            
            # Actualizar estado
            current_state['current_geofence'] = current_geofence
        
        # Guardar estado final
        self.vehicle_states[vehicle_id] = current_state
        
        return cycles_count
    
    def calculate_kilometers(self, cycles_count, real_distances):
        """Calcular kilómetros totales basado en ciclos y distancias reales"""
        results = {}
        
        for geofence_id, cycles in cycles_count.items():
            km_value = self.geofence_service.km_values.get(geofence_id)
            
            if km_value is not None:
                # Geocerca con km fijos (Aduana 420, Colombia)
                results[geofence_id] = {
                    'cycles': cycles,
                    'total_km': cycles * km_value,
                    'km_per_cycle': km_value,
                    'type': 'fixed_km'
                }
            else:
                # Geocerca con km reales (Nuevo Laredo)
                real_km = real_distances.get(geofence_id, 0)
                results[geofence_id] = {
                    'cycles': cycles,
                    'total_km': real_km,
                    'distance_km': real_km,
                    'type': 'real_km'
                }
        
        return results

# ===============================
# ANALIZADOR PRINCIPAL
# ===============================
class GeofenceAnalyzer:
    def __init__(self, geofence_file="geofences.json"):
        self.geofence_service = GeofenceService(geofence_file)
        self.entry_exit_counter = EntryExitCounter(self.geofence_service)
    
    def analyze_vehicle_data(self, vehicle_name, gps_points):
        """
        Analizar datos de un vehículo y calcular métricas de geocercas
        
        Args:
            vehicle_name: Nombre del vehículo
            gps_points: Lista de puntos GPS
            
        Returns:
            Dict con resultados por geocerca
        """
        if not gps_points:
            logger.warning(f"⚠️ No hay puntos GPS para {vehicle_name}")
            return {}
        
        logger.info(f"🔍 Analizando {len(gps_points)} puntos GPS para {vehicle_name}")
        
        try:
            # 1. Contar ciclos de entrada/salida
            cycles_count = self.entry_exit_counter.count_entry_exit_cycles(vehicle_name, gps_points)
            
            # 2. Calcular distancias reales (para geocercas sin km fijos)
            real_distances = calculate_real_distances(gps_points, self.geofence_service)
            
            # 3. Calcular kilómetros totales
            results = self.entry_exit_counter.calculate_kilometers(cycles_count, real_distances)
            
            # 4. Log resumen
            total_km = sum(data['total_km'] for data in results.values())
            active_geofences = sum(1 for data in results.values() if data['total_km'] > 0)
            
            logger.info(f"✅ {vehicle_name}: {total_km:.2f} km en {active_geofences} geocercas")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Error analizando {vehicle_name}: {e}")
            return {}