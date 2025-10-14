from geofence_calculator import get_geotab_connection, get_us_vehicles, debug_vehicle_groups
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_geotab_connection():
    """Probar conexión a Geotab y obtener vehículos US"""
    print("🔍 Probando conexión a Geotab...")
    print("=" * 50)
    
    try:
        # 1. Probar conexión
        api = get_geotab_connection()
        api.authenticate()
        print("✅ Conexión a Geotab exitosa")
        
        # 2. Obtener vehículos US
        us_vehicles = get_us_vehicles()
        
        if us_vehicles:
            print(f"✅ Vehículos US encontrados: {len(us_vehicles)}")
            for vehicle in us_vehicles[:5]:  # Mostrar primeros 5
                print(f"   🚗 {vehicle['name']} (Placa: {vehicle.get('licensePlate', 'N/A')})")
            
            if len(us_vehicles) > 5:
                print(f"   ... y {len(us_vehicles) - 5} más")
        else:
            print("❌ No se encontraron vehículos US")
            
        # 3. Debug de grupos (opcional)
        print("\n" + "=" * 50)
        debug = input("¿Quieres ver debug de todos los grupos? (s/n): ")
        if debug.lower() == 's':
            debug_vehicle_groups()
            
    except Exception as e:
        print(f"❌ Error en conexión Geotab: {e}")
        return False
    
    return True

if __name__ == "__main__":
    test_geotab_connection()