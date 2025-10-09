import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import Error

db_credentials = st.secrets["database"]

@st.cache_resource(ttl=3600)  # Cache de la conexión por 1 hora
def get_database_connection():
    """Establece y devuelve una conexión a la base de datos."""
    try:
        connection = mysql.connector.connect(
            host=db_credentials["host"],
            port=db_credentials["port"],
            user=db_credentials["user"],
            password=db_credentials["password"],
            database=db_credentials["database"],
            connection_timeout=30,
        )
        return connection
    except Error as e:
        st.error(f"Error al conectar con MySQL: {e}")
        return None
    

@st.cache_data(ttl=3600)  # Cache de los datos por 1 hora
def load_data():
    """Carga todos los datos necesarios desde la base de datos."""
    connection = get_database_connection()
    if not connection:
        st.error("No se pudo establecer la conexión con la base de datos.")
        return None, None, None, None
        
    try:
        query_services = "SELECT * FROM road_services"
        df_services = pd.read_sql(query_services, connection)
        
        query_trucks = "SELECT * FROM camiones WHERE intSucursal = 2"
        df_trucks = pd.read_sql(query_trucks, connection)
        
        query_trailers = "SELECT * FROM cajas WHERE inSucursal != 0"
        df_trailers = pd.read_sql(query_trailers, connection)
        
        query_completo = """
            SELECT v.intIdViaje, cam.strCamion, v.intIdChofer, 
                   CONCAT(c.strNombreChofer, ' ', c.strApellidoMaterno) as nombre_chofer,
                   e.dateFechaRecoleccion, e.intIdEmbarque
            FROM viajes v
            JOIN viajes_embarques ve ON v.intIdViaje = ve.intIdViaje
            JOIN embarques e ON e.intIdEmbarque = ve.intIdEmbarque
            JOIN choferes c ON v.intIdChofer = c.intIdChofer
            JOIN camiones cam ON cam.intIdCamion = v.intIdCamion
            WHERE v.intSucursal = 2
        """
        df_viajes_completo = pd.read_sql(query_completo, connection)

        return df_services, df_trucks, df_trailers, df_viajes_completo
    except Error as e:
        st.error(f"Error en la base de datos: {e}")
        return None, None, None, None
    finally:
        if connection and connection.is_connected():
            connection.close()