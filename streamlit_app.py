# -----------------------------------------------------------------------------
# IMPORTS Y CONFIGURACIÓN INICIAL
# -----------------------------------------------------------------------------
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

# (Importa las mismas funciones de conexión de base de datos que usas en tus otras páginas)
# Para este ejemplo, asumiremos que tienes un archivo 'database.py' con esas funciones
# o simplemente las copias aquí también.
from utils import load_data# Reutilizamos la función de carga de datos de la otra página

# --- Configuración de la página ---
# Esto DEBE SER el primer comando de Streamlit en el script.
st.set_page_config(
    page_title="Dashboard Principal | Road Services",
    page_icon="🚛",
    layout="wide"
)

# -----------------------------------------------------------------------------
# BIENVENIDA Y GUÍA DEL USUARIO
# -----------------------------------------------------------------------------

st.title("Dashboard de Operaciones: Road Services 🚛")

st.markdown("""
    ¡Bienvenido al dashboard interactivo de Road Services!
    
    Esta plataforma centraliza los datos más importantes de nuestras operaciones para facilitar
    el análisis y la toma de decisiones.
    
    **👈 Utiliza el menú en la barra lateral** para navegar entre los diferentes análisis disponibles.
""")

# Mensaje en la barra lateral para guiar al usuario
st.sidebar.success("Selecciona un análisis para comenzar.")
st.sidebar.markdown("---") # Separador visual

# -----------------------------------------------------------------------------
# MÉTRICAS PRINCIPALES (KPIs)
# -----------------------------------------------------------------------------

st.markdown("### Resumen General del Último Mes")

# Cargar los datos (usando el cache de la función)
all_data = load_data()

if all_data and all_data[0] is not None:
    df_services, df_trucks, _, _ = all_data
    
    # Definir el rango del último mes
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    # Filtrar servicios del último mes
    df_services['created_at'] = pd.to_datetime(df_services['created_at'])
    last_month_mask = (df_services['created_at'] >= start_date) & (df_services['created_at'] <= end_date)
    services_last_month = df_services[last_month_mask]
    
    # Calcular KPIs
    kpi1_value = services_last_month['work_order'].nunique()
    kpi2_value = df_trucks['strCamion'].nunique()
    kpi3_value = services_last_month['truck'].nunique()

    # Mostrar KPIs en columnas
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            label="Servicios Únicos (Últimos 30 días)",
            value=f"{kpi1_value}"
        )

    with col2:
        st.metric(
            label="Total de Camiones en Flota",
            value=f"{kpi2_value}"
        )
    
    with col3:
        st.metric(
            label="Camiones con Actividad (Últimos 30 días)",
            value=f"{kpi3_value}"
        )
else:
    st.warning("No se pudieron cargar los datos para mostrar las métricas. Revisa la conexión a la base de datos.")

st.markdown("---") # Separador visual

# -----------------------------------------------------------------------------
# SECCIÓN ADICIONAL (OPCIONAL)
# -----------------------------------------------------------------------------
st.subheader("Acerca de esta Aplicación")
st.info("""
    - **Datos:** Conectados directamente a la base de datos de operaciones en tiempo real.
    - **Tecnología:** Construido con Python y Streamlit.
    - **Objetivo:** Ofrecer una vista clara y actualizada del rendimiento de los servicios en carretera.
""")