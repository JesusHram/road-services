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

def calculate_real_distance_in_geofence(gps_points, geofence_service, target_geofence_id):
    """
    Suma los segmentos de distancia recorridos exclusivamente DENTRO de una geocerca específica.
    """
    total_distance_km = 0
    prev_point = None

    for point in gps_points:
        lat, lon = point['lat'], point['lon']
        
        # Comprueba si el punto actual está dentro de la geocerca objetivo
        geofence_info = geofence_service.find_geofence(lat, lon)
        is_inside = geofence_info.get('inside', False) and geofence_info.get('geofence_id') == target_geofence_id

        # Si el punto anterior también existió y AMBOS (el anterior y el actual) están dentro,
        # calculamos y sumamos la distancia de ese segmento.
        if prev_point and is_inside and prev_point['is_inside']:
            segment_distance = calculate_distance(
                prev_point['lat'], prev_point['lon'],
                lat, lon
            )
            total_distance_km += segment_distance
        
        # Actualizamos el punto anterior para la siguiente iteración
        prev_point = {'lat': lat, 'lon': lon, 'is_inside': is_inside}

    return total_distance_km

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
            current_geofence = geofence_info.get('geofence_id') # Usar .get() por seguridad
            
            # --- CORRECCIÓN DE LÓGICA AQUÍ ---
            # Detectamos una salida si antes estábamos en una geocerca
            # y ahora estamos en una geocerca DIFERENTE o en NINGUNA.
            if current_state['current_geofence'] and current_state['current_geofence'] != current_geofence:
                # ¡Salida detectada! Contamos un ciclo para la geocerca que acabamos de dejar.
                cycles_count[current_state['current_geofence']] += 1
                logger.debug(f"🚗 Ciclo completo para {vehicle_id}: Salida de {current_state['current_geofence']}")
            
            # Actualizar el estado actual
            current_state['current_geofence'] = current_geofence
        
        self.vehicle_states[vehicle_id] = current_state
        return cycles_count

    def calculate_kilometers(self, cycles_count, real_distances):
        """
        Calcula los kilómetros totales basado en ciclos (para KM fijos)
        y en distancias reales (para geocercas designadas).
        """
        results = {}
        
        for geofence_id, cycles in cycles_count.items():
            km_value = self.geofence_service.km_values.get(geofence_id)
            
            if km_value is not None:
                # --- Lógica para geocercas con KM FIJOS (Aduana, Colombia) ---
                results[geofence_id] = {
                    'cycles': cycles,
                    'total_km': cycles * km_value,
                    'type': 'fixed_km'
                }
            else:
                # --- Lógica para geocercas con KM REALES (Nuevo Laredo) ---
                # Obtenemos la distancia real calculada por separado
                real_km = real_distances.get(geofence_id, 0)
                results[geofence_id] = {
                    'cycles': cycles, # Aún guardamos los ciclos por si son útiles
                    'total_km': real_km,
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
        Analiza datos de un vehículo usando un método híbrido:
        - Cuenta ciclos para geocercas de KM fijos.
        - Calcula distancia real para geocercas específicas.
        """
        if not gps_points:
            logger.warning(f"⚠️ No hay puntos GPS para {vehicle_name}")
            return {}
        
        logger.info(f"🔍 Analizando {len(gps_points)} puntos GPS para {vehicle_name}")
        
        try:
            # 1. Contar ciclos de entrada/salida (como antes)
            cycles_count = self.entry_exit_counter.count_entry_exit_cycles(vehicle_name, gps_points)
            
            # 2. Calcular distancias reales SOLO para las geocercas que lo necesiten
            real_distances = {}
            for geofence_id, km_value in self.geofence_service.km_values.items():
                if km_value is None: # Si es None, significa que necesita cálculo real
                    distance = calculate_real_distance_in_geofence(gps_points, self.geofence_service, geofence_id)
                    real_distances[geofence_id] = distance
                    logger.info(f"🛣️ Distancia real calculada para '{geofence_id}': {distance:.2f} km")

            # 3. Calcular kilómetros totales usando el método híbrido
            results = self.entry_exit_counter.calculate_kilometers(cycles_count, real_distances)
            
            total_km = sum(data['total_km'] for data in results.values())
            logger.info(f"✅ Análisis completo para {vehicle_name}: {total_km:.2f} km totales.")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Error analizando {vehicle_name}: {e}")
            return {}