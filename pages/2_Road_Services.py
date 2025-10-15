# -----------------------------------------------------------------------------
# 1. IMPORTS Y CONFIGURACIÓN INICIAL
# -----------------------------------------------------------------------------
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector
from mysql.connector import Error
from datetime import datetime
from st_aggrid import JsCode, AgGrid, GridOptionsBuilder, GridUpdateMode
from streamlit_modal import Modal
import json

if "selected_operator_data" not in st.session_state:
    st.session_state.selected_operator_data = None

# --- Configuración de la página ---
# ESTO DEBE SER EL PRIMER COMANDO DE STREAMLIT EN EL SCRIPT
st.set_page_config(
    page_title="Análisis RoadServices",
    layout="wide"
)

# -----------------------------------------------------------------------------
# 2. CARGA DE DATOS Y CONEXIÓN A LA BASE DE DATOS
# -----------------------------------------------------------------------------

# --- Carga de credenciales seguras ---
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

# -----------------------------------------------------------------------------
# 3. FUNCIONES DE CÁLCULO Y LÓGICA
# -----------------------------------------------------------------------------

def filter_data_by_date(df, start_date, end_date):
    """Filtra el DataFrame principal por el rango de fechas."""
    df['invoice_date'] = pd.to_datetime(df['invoice_date'])
    mask = (df['invoice_date'].dt.date >= start_date) & (df['invoice_date'].dt.date <= end_date)
    return df.loc[mask].copy()

@st.cache_data(ttl=3600)
def calculate_truck_consistency(filtered_df, start_date):
    """Calcula la consistencia de servicios de los camiones."""
    
    def get_unique_services(df):
        daily_services = df.groupby(['invoice_date', 'truck', 'work_order']).size().reset_index()
        return daily_services.groupby('truck')['work_order'].nunique()

    reference_date = pd.to_datetime(start_date)
    
    # Definir períodos
    one_month_end = reference_date + pd.DateOffset(months=1)
    three_month_end = one_month_end + pd.DateOffset(months=3)
    rest_months_end = three_month_end + pd.DateOffset(months=9)

    # Crear máscaras
    one_month_mask = (filtered_df['invoice_date'] >= reference_date) & (filtered_df['invoice_date'] < one_month_end)
    three_months_mask = (filtered_df['invoice_date'] >= one_month_end) & (filtered_df['invoice_date'] < three_month_end)
    rest_months_mask = (filtered_df['invoice_date'] >= three_month_end) & (filtered_df['invoice_date'] <= rest_months_end)

    # Crear DataFrame comparativo
    comparison_df = pd.DataFrame({
        'Month 1': get_unique_services(filtered_df[one_month_mask]),
        'Months 2-4': get_unique_services(filtered_df[three_months_mask]),
        'Months 5-12': get_unique_services(filtered_df[rest_months_mask])
    }).fillna(0)
    
    comparison_df['Total Services'] = comparison_df.sum(axis=1)
    
    # Separar consistentes de inconsistentes
    consistent_trucks = comparison_df[
        (comparison_df['Month 1'] >= 1) &
        (comparison_df['Months 2-4'] >= 1) &
        (comparison_df['Months 5-12'] >= 1)
    ].sort_values('Total Services', ascending=False)
    
    inconsistent_trucks = comparison_df.drop(consistent_trucks.index).sort_values('Total Services', ascending=False)
    
    return consistent_trucks, inconsistent_trucks

@st.cache_data(ttl=3600)
def calculate_operator_analysis(filtered_df, df_viajes_completo):
    """Realiza el análisis de servicios por operador."""
    
    def find_closest_operator(row, df_viajes):
        road_service_date = row['invoice_date']
        truck = row['truck']
        relevant_trips = df_viajes[df_viajes['strCamion'] == truck]
        if not relevant_trips.empty:
            time_diffs = (relevant_trips['dateFechaRecoleccion'] - road_service_date).abs()
            return relevant_trips.loc[time_diffs.idxmin()]['nombre_chofer']
        return 'Sin operador asignado'

    if filtered_df.empty:
        return pd.DataFrame()

    # Asignamos el operador si no existe
    if 'operador' not in filtered_df.columns:
        filtered_df['operador'] = filtered_df.apply(find_closest_operator, args=(df_viajes_completo,), axis=1)

  

    # 1. Primero, agrupamos los productos por cada orden de servicio
    products_by_order = filtered_df.groupby(['operador', 'truck', 'work_order', 'invoice_date'])['product'].apply(
        lambda x: list(x.dropna().unique())
    ).reset_index()

    # 2. Ahora, creamos la estructura anidada que necesitamos
    # Convertimos cada fila en un diccionario {'work_order': ..., 'products': ...}
    order_details = products_by_order.apply(
        lambda row: {'work_order': row['work_order'], 
                     'products': row['product'], 
                     'invoice_date': row['invoice_date'].strftime('%Y-%m-%d')},
        axis=1
    )

    # 3. Agrupamos estos diccionarios en una lista por cada operador/camión
    detailed_summary = order_details.groupby([products_by_order['operador'], products_by_order['truck']]).apply(list).reset_index(name='detailed_orders')
    
    # 4. Hacemos un merge con el conteo general de servicios
    service_counts = filtered_df.groupby(['operador', 'truck']).agg(
        total_servicios=('work_order', 'count'),
        servicios_unicos=('work_order', 'nunique')
    ).reset_index()

    final_analysis = pd.merge(service_counts, detailed_summary, on=['operador', 'truck'])

    # 5. Convertimos la columna de la estructura detallada a un string JSON
    final_analysis['detailed_orders'] = final_analysis['detailed_orders'].apply(json.dumps)
    
    return final_analysis.sort_values(
    by=['truck', 'total_servicios'], 
    ascending=[False, True]
    )

@st.cache_data(ttl=3600)
def calculate_trailer_analysis(filtered_df, df_trailers):
    """Calcula el número de servicios por remolque."""
    df_merged = pd.merge(df_trailers, filtered_df, left_on='strNumeroEconomico', right_on='trailer', how='left')
    trailer_services = df_merged.groupby('strNumeroEconomico')['work_order'].nunique().fillna(0).reset_index()
    trailer_services.columns = ['Trailer Number', 'Number of Work Orders']
    return trailer_services.sort_values('Number of Work Orders', ascending=False)


# -----------------------------------------------------------------------------
# 4. FUNCIONES DE VISUALIZACIÓN (UI)
# -----------------------------------------------------------------------------

def display_sidebar():
    """Muestra los filtros en la barra lateral y devuelve las selecciones."""
    st.sidebar.header('Filtros ⚙️')
    start_date = st.sidebar.date_input('Fecha de Inicio', datetime(2024, 10, 1))
    end_date = st.sidebar.date_input('Fecha de Fin', datetime.now())
    if start_date and end_date:
        return start_date, end_date
    else:
        # Detiene la ejecución y muestra un mensaje amigable
        st.warning("Por favor, selecciona un rango de fechas válido.")
        st.stop()


def display_truck_consistency_analysis(consistent_trucks, inconsistent_trucks):
    """Muestra el análisis de consistencia de camiones."""
    st.subheader('Análisis de Consistencia de Servicios por Camión')
    col1, col2 = st.columns(2)
    with col1:
        st.info('Camiones con Servicios Consistentes')
        st.dataframe(consistent_trucks, use_container_width=True)
    with col2:
        st.warning('Camiones con Servicios Inconsistentes')
        st.dataframe(inconsistent_trucks, use_container_width=True)

def display_operator_analysis_grid(operator_analysis):
    """
    Muestra la tabla interactiva y abre una nueva ventana con detalles al hacer clic.
    """
    st.subheader('Análisis Detallado por Operador y Unidad (Haz clic en un operador para ver detalles)')
    
    if operator_analysis.empty:
        st.warning("No hay datos de operadores para mostrar en el período seleccionado.")
        return

    # --- CAMBIO CLAVE DENTRO DEL JSCODE ---
    cell_click_js = JsCode("""
    function(params) {
        if (params.data) {
            // 1. "Desempaquetamos" la estructura detallada desde el string JSON
            const detailedOrders = JSON.parse(params.data.detailed_orders);

            // 2. Variable para construir el HTML del desglose
            let breakdownHTML = '<li>No hay órdenes de servicio para mostrar.</li>';

            // 3. Iteramos sobre cada orden de servicio para construir su bloque HTML
            if (Array.isArray(detailedOrders) && detailedOrders.length > 0) {
                breakdownHTML = detailedOrders.map(order => {
                    // Para cada orden, creamos la lista de sus productos
                    let productList = order.products.map(p => `<li>${p}</li>`).join('');
                    
                    // Devolvemos el bloque HTML completo para esta orden
                    return `
                        <div class="work-order-item">
                            <strong>Servicio (work_order): ${order.work_order} , Fecha: ${order.invoice_date} </strong>
                            <ul>
                                ${productList}
                            </ul>
                        </div>
                    `;
                }).join(''); // Unimos todos los bloques de las órdenes
            }

            // 4. Construimos el contenido final de la ventana
            const content = `
                <html>
                <head>
                    <title>Detalles del Operador</title>
                    <style>
                        body { font-family: sans-serif; padding: 25px; }
                        .summary { margin-bottom: 20px; padding-bottom: 10px; border-bottom: 2px solid #eee; }
                        .work-order-item { margin-bottom: 15px; }
                        .work-order-item ul { margin-top: 5px; }
                    </style>
                </head>
                <body>
                    <div class="summary">
                        <h2>🚛 Detalles de: ${params.data.operador}</h2>
                        <p>
                            <strong>Unidad:</strong> ${params.data.truck} | 
                            <strong>Servicios Únicos:</strong> ${params.data.servicios_unicos}
                        </p>
                    </div>
                    
                    <div class="breakdown-container" style="max-height: 400px; overflow-y: auto;">
                        ${breakdownHTML}
                    </div>
                </body>
                </html>`;
            
            const win = window.open("", "_blank", "width=700,height=600,scrollbars=yes,resizable=yes");
            win.document.write(content);
            win.document.close();
            win.focus();
        }
    }
    """)
    
    gb = GridOptionsBuilder.from_dataframe(operator_analysis)
    gb.configure_column("operador", headerName="Operador", onCellClicked=cell_click_js, cellStyle={'cursor': 'pointer'}, filter=True)
    gb.configure_column("truck", headerName="Unidad", filter=True)
    gb.configure_column("total_servicios", headerName="Total Servicios")
    gb.configure_column("servicios_unicos", headerName="Servicios Únicos")
    gb.configure_column("detailed_orders", hide=True)
    
    grid_options = gb.build()
    
    AgGrid(
        operator_analysis,
        gridOptions=grid_options,
        allow_unsafe_jscode=True,
        theme='streamlit',
        height=400
    )


def display_details_modal(modal):
    """
    Muestra el contenido dentro de la ventana modal.
    Esta función se llama cuando el modal está abierto.
    """
    # Usamos los datos guardados en st.session_state
    data = st.session_state.get("selected_operator_data")
    if not data:
        st.warning("No se ha seleccionado ningún operador.")
        return

    with modal.container():
        # ... (todo tu código para mostrar los detalles es correcto y se mantiene igual) ...
        st.markdown(f"### 🚛 Detalles de: **{data.get('operador')}**")
        # ... (resto de métricas y productos) ...

        if st.button("Cerrar"):
            # --- CAMBIO CLAVE ---
            # En lugar de borrar la variable, la ponemos en None.
            st.session_state.selected_operator_data = None
            modal.close()
            # Forzamos un re-run para que el cambio de estado se refleje inmediatamente
            st.experimental_rerun() 
        
        col1, col2 = st.columns(2)
        col1.metric("Unidad", data.get('truck'))
        col2.metric("Total de Servicios", data.get('total_servicios'))

        # Procesar y mostrar la lista de productos
        products = data.get('product', [])
        if products:
            # Aplanar la lista si es una lista de listas y obtener únicos
            flat_products = [item for sublist in products for item in sublist]
            unique_products = sorted(list(set(flat_products)))
            
            st.markdown("#### Productos Atendidos:")
            # Usamos columnas para mostrar la lista de forma más compacta
            num_columns = 3
            cols = st.columns(num_columns)
            for i, product in enumerate(unique_products):
                cols[i % num_columns].write(f"- {product}")

        if st.button("Cerrar"):
            # Al cerrar, limpiamos la selección para que el modal no se vuelva a abrir
            del st.session_state.selected_operator_data
            modal.close()

def display_trailer_analysis_table(trailer_services):
    """Muestra la tabla de análisis de remolques."""
    st.subheader('Análisis de Servicios por Remolque (Caja)')
    st.dataframe(trailer_services, use_container_width=True)

def display_weekday_chart(filtered_df):
    """Muestra un gráfico de barras de servicios por día de la semana."""
    st.subheader('Distribución de Servicios por Día de la Semana')
    weekday_counts = filtered_df['invoice_date'].dt.day_name().value_counts()
    weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    weekday_counts = weekday_counts.reindex(weekday_order)
    
    fig, ax = plt.subplots(figsize=(8, 4))
    weekday_counts.plot(kind='bar', ax=ax, color='skyblue')
    ax.set_title('Órdenes de Trabajo por Día de la Semana')
    ax.set_xlabel('Día de la Semana')
    ax.set_ylabel('Número de Órdenes')
    plt.xticks(rotation=45)
    st.pyplot(fig)


def main():
    """Función principal que orquesta la aplicación."""
    st.title('📊 Análisis de Road Services')
    
    # <-- CAMBIO: Inicializar el objeto Modal al principio
    details_modal = Modal(
        "Detalles del Operador", 
        key="operator_details_modal",
        padding=20,
        max_width=700
    )
    
    all_data = load_data()
    if any(df is None for df in all_data):
        st.error("Fallo al cargar los datos. La aplicación no puede continuar.")
        return
    df_services, df_trucks, df_trailers, df_viajes_completo = all_data
    
    start_date, end_date = display_sidebar()
    filtered_df = filter_data_by_date(df_services, start_date, end_date)
    st.markdown(f"Mostrando datos desde **{start_date}** hasta **{end_date}**.")
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "🚚 Consistencia de Camiones", 
        "👨‍🔧 Análisis por Operador", 
        "📦 Análisis por Remolque", 
        "📅 Análisis Semanal"
    ])
    
    with tab1:
        consistent_trucks, inconsistent_trucks = calculate_truck_consistency(filtered_df, start_date)
        display_truck_consistency_analysis(consistent_trucks, inconsistent_trucks)

    with tab2:
        operator_analysis = calculate_operator_analysis(filtered_df, df_viajes_completo)
        
        # --- PASO 1: Muestra la tabla y captura la interacción del usuario ---
        grid_response = display_operator_analysis_grid(operator_analysis)

        # --- PASO 2: Actualiza el estado SOLO si el usuario ha seleccionado una fila ---
        # Esta condición es clave: solo modificamos el estado si hay una selección activa.
        if grid_response and grid_response['selected_rows']:
            st.session_state.selected_operator_data = grid_response['selected_rows'][0]

        # --- PASO 3: Decide si el modal debe estar abierto ---
        # La condición es simple: si hay datos en nuestro estado, abre el modal.
        if st.session_state.selected_operator_data is not None:
            details_modal.open()

        # --- PASO 4: Muestra el contenido del modal si está abierto ---
        if details_modal.is_open():
            display_details_modal(details_modal)

    with tab3:
        trailer_services = calculate_trailer_analysis(filtered_df, df_trailers)
        display_trailer_analysis_table(trailer_services)

    with tab4:
        display_weekday_chart(filtered_df)

if __name__ == "__main__":
    main()