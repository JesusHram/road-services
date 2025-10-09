import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector
from mysql.connector import Error
from datetime import datetime
from st_aggrid import JsCode, AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode

db_credentials = st.secrets["database"]

@st.cache_resource(ttl=3600)  # Cache for 1 hour
def get_database_connection():
    try:
        connection = mysql.connector.connect(
            host=db_credentials["host"],
            port=db_credentials["port"],
            user=db_credentials["user"],
            password=db_credentials["password"],
            database= db_credentials["database"],
            connection_timeout=30,
            autocommit=True,
            # Remove pool settings to use direct connection
        )
        return connection
    except Error as e:
        st.error(f"Error connecting to MySQL: {e}")
        return None

@st.cache_data(ttl=3600)  # Cache data for 1 hour
def load_data():
    connection = None
    try:
        connection = get_database_connection()
        if not connection:
            st.error("Could not establish database connection")
            return None, None, None, None
            
        # Obtener road services
        query_services = "SELECT * FROM road_services"
        df_services = pd.read_sql(query_services, connection)
        
        # Obtener camiones
        query_trucks = "SELECT * FROM camiones WHERE intSucursal = 2"
        df_trucks = pd.read_sql(query_trucks, connection)
        
        # Obtener trailers
        query_trailers = "SELECT * FROM cajas WHERE inSucursal != 0"
        df_trailers = pd.read_sql(query_trailers, connection)
        
        # Query para viajes_embarques con relación a embarques
        query_completo = """
            SELECT v.intIdViaje, cam.strCamion, v.intIdChofer, 
                    CONCAT( c.strNombreChofer, ' ', c.strApellidoMaterno) as nombre_chofer,
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
        st.error(f"Database error: {e}")
        return None, None, None, None
    finally:
        if connection:
            connection.close()

# Set page config at the very top of the file, before any other Streamlit commands
st.set_page_config(
    page_title="Datos RoadServices",
    layout="wide"
)

def main():
    try:
        # Load data first
        results = load_data()
        
        # Validate data loading results
        if results is None or any(df is None for df in results):
            st.error("Failed to load data from database. Please check your connection and try again.")
            return
            
        df_services, df_trucks, df_trailers, df_viajes_completo = results
        
        # Validate required dataframe and columns
        if df_services is None or df_services.empty or 'created_at' not in df_services.columns:
            st.error("Road services data is missing or invalid.")
            return
            
        st.title('Datos RoadServices')
        
        # Process dates once
        df_services['created_at'] = pd.to_datetime(df_services['created_at'])
        
        # Sidebar para filtros
        st.sidebar.header('Filters')
        start_date = st.sidebar.date_input('Start Date', datetime(2024, 10, 1))
        end_date = st.sidebar.date_input('End Date', datetime(2025, 10, 7))
        
        # Procesar datos
        df_services['created_at'] = pd.to_datetime(df_services['created_at'])
        mask = (df_services['created_at'].dt.date >= start_date) & \
               (df_services['created_at'].dt.date <= end_date)
        filtered_df = df_services.loc[mask].copy()
                
        # Sección 2: Análisis de Camiones
        st.header('Truck Analysis')
        
        #Tabla de Camiones
        col1, col2 = st.columns([1,1])

        def get_unique_services(df, mask):
            period_df = df[mask].copy()
            # Primero agrupamos por fecha y work_order para contar servicios únicos por día
            daily_services = period_df.groupby(['created_at', 'truck', 'work_order']).size().reset_index()
            # Luego contamos work_orders únicos por camión
            return daily_services.groupby('truck')['work_order'].nunique()

        # Definir períodos consecutivos
        reference_date = pd.to_datetime(start_date)
        
        # Primer mes
        one_month_start = reference_date
        one_month_end = reference_date + pd.DateOffset(months=1)
        
        # Meses 2, 3 y 4
        three_month_start = one_month_end
        three_month_end = three_month_start + pd.DateOffset(months=3)
        
        # Meses 5 al 12
        rest_months_start = three_month_end
        rest_months_end = rest_months_start + pd.DateOffset(months=9)
        
        # Crear máscaras para períodos consecutivos
        one_month_mask = (filtered_df['created_at'] >= one_month_start) & \
                         (filtered_df['created_at'] < one_month_end)
        
        three_months_mask = (filtered_df['created_at'] >= three_month_start) & \
                          (filtered_df['created_at'] < three_month_end)
        
        rest_months_mask = (filtered_df['created_at'] >= rest_months_start) & \
                         (filtered_df['created_at'] <= rest_months_end)

        
        # Mostrar rangos de fechas para referencia
        st.info(f"""
        Períodos analizados (consecutivos):
        - Mes 1: {one_month_start.strftime('%Y-%m-%d')} a {one_month_end.strftime('%Y-%m-%d')}
        - Meses 2-4: {three_month_start.strftime('%Y-%m-%d')} a {three_month_end.strftime('%Y-%m-%d')}
        - Meses 5-12: {rest_months_start.strftime('%Y-%m-%d')} a {rest_months_end.strftime('%Y-%m-%d')}
        """)
        
        # Obtener datos para cada período
        one_month_data = get_unique_services(filtered_df, one_month_mask)
        three_months_data = get_unique_services(filtered_df, three_months_mask)
        rest_months_data = get_unique_services(filtered_df, rest_months_mask)
        
        # Crear DataFrame comparativo
        comparison_df = pd.DataFrame({
            'Month 1': one_month_data,
            'Months 2-4': three_months_data,
            'Months 5-12': rest_months_data
        }).fillna(0)
        
        # Agregar totales
        comparison_df['Total Services'] = comparison_df['Month 1'] + comparison_df['Months 2-4'] + comparison_df['Months 5-12']
        
        # Ordenar por total de servicios
        comparison_df = comparison_df.sort_values('Total Services', ascending=False)
        
        # Filtrar camiones con servicios consistentes
        consistent_trucks = comparison_df[
            (comparison_df['Month 1'] >= 1) &
            (comparison_df['Months 2-4'] >= 1) &
            (comparison_df['Months 5-12'] >= 1)
        ].sort_values('Total Services', ascending=False)
        
        # Camiones con servicios inconsistentes
        inconsistent_trucks = comparison_df[
            ~comparison_df.index.isin(consistent_trucks.index) &
            ((comparison_df['Month 1'] >= 1) |
             (comparison_df['Months 2-4'] >= 1) |
             (comparison_df['Months 5-12'] >= 1))
        ].sort_values('Total Services', ascending=False)

        # Agregar búsqueda global en el sidebar
        st.sidebar.header('Global Search')
        global_search = st.sidebar.text_input('Search across all tables')
        
        @st.cache_data(ttl=3600)
        def global_search_filter(df, search_term):
            if not search_term:
                return df
            
            # Check if DataFrame has an index name
            if df.index.name:
                # Reset index to make it searchable
                df_searchable = df.reset_index()
                # Search in all columns including the original index
                mask = df_searchable.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(axis=1)
                # Set index back and return filtered results
                return df_searchable[mask].set_index(df.index.name)
            else:
                # For DataFrames without named index, search directly
                mask = df.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(axis=1)
                return df[mask]

        @st.cache_data(ttl=3600)
        def find_closest_operator(road_service_date, truck):
            mask = (df_viajes_completo['strCamion'] == truck)
            relevant_trips = df_viajes_completo[mask]
            
            if not relevant_trips.empty:
                relevant_trips['time_diff'] = abs(relevant_trips['dateFechaRecoleccion'] - road_service_date)
                closest_trip = relevant_trips.loc[relevant_trips['time_diff'].idxmin()]
                return closest_trip['nombre_chofer']
            return 'Sin operador asignado'
        
        @st.cache_data(ttl=3600)
        def process_operator_analysis(filtered_df):
            filtered_df['operador'] = filtered_df.apply(
                lambda x: find_closest_operator(x['created_at'], x['truck']), 
                axis=1
            )
            
            operator_analysis = filtered_df.groupby(['operador', 'truck'])['work_order'].agg([
                ('total_servicios', 'count'),
                ('servicios_unicos', 'nunique')
            ]).reset_index()
            
            products_by_service = filtered_df.groupby(['operador', 'truck', 'work_order'])['product'].apply(list).reset_index()
            
            products_by_operator = products_by_service.groupby(['operador', 'truck'])['product'].apply(list).reset_index()
            
            operator_analysis = pd.merge(operator_analysis, products_by_operator, on=['operador', 'truck'])
            
            return operator_analysis.sort_values('total_servicios', ascending=False)
        
        # Create operator analysis first
        operator_analysis = process_operator_analysis(filtered_df)
        
        # Create truck services analysis
        df_merged = pd.merge(df_trucks, filtered_df, 
                           left_on='strCamion', 
                           right_on='truck', 
                           how='left')
        
        truck_services = df_merged.groupby('strCamion')['work_order'].nunique().fillna(0).reset_index()
        truck_services.columns = ['Truck Number', 'Number of Work Orders']
        truck_services = truck_services.sort_values('Number of Work Orders', ascending=False)
        
        # Create trailer analysis
        df_merged = pd.merge(df_trailers, filtered_df,
                           left_on='strNumeroEconomico',
                           right_on='trailer',
                           how='left')
        trailer_services = df_merged.groupby('strNumeroEconomico')['work_order'].nunique().fillna(0).reset_index()
        trailer_services.columns = ['Trailer Number', 'Number of Work Orders']
        trailer_services = trailer_services.sort_values('Number of Work Orders', ascending=False)
        
        # Now apply global search after all DataFrames are created
        if global_search:
            consistent_trucks = global_search_filter(consistent_trucks, global_search)
            inconsistent_trucks = global_search_filter(inconsistent_trucks, global_search)
            operator_analysis = global_search_filter(operator_analysis, global_search)
            truck_services = global_search_filter(truck_services, global_search)
            trailer_services = global_search_filter(trailer_services, global_search)
            
        # Display all tables
        with col1:
            st.subheader('Trucks with Consistent Services')
            st.dataframe(consistent_trucks, use_container_width=True)
                
        with col2:
            st.subheader('Trucks with Inconsistent Services')
            st.dataframe(inconsistent_trucks, use_container_width=True)
            
        st.header('Relacion de rds por operadores')   
        
        # Create a clean DataFrame for display (without products column)
        display_df = operator_analysis[['operador', 'truck', 'total_servicios', 'servicios_unicos']].copy()
        
        # Add products to display_df but hide it in the grid
        display_df['product'] = operator_analysis['product']

        # Define the cell click handler first
        cell_click_js = """
        function(params) {
            let productsList = '';
            
            // Verificar si hay productos y crear lista HTML
            if (params.data.product) {
                try {
                    // Intentar convertir la cadena a array si es necesario
                    const products = typeof params.data.product === 'string' 
                        ? params.data.product.split("'").filter(item => item.trim() && item !== '[' && item !== ']' && item !== ', ')
                        : params.data.product;
                    
                    // Crear lista de productos únicos
                    const uniqueProducts = [...new Set(products.flat())];
                    uniqueProducts.forEach(product => {
                        if (product && product.trim()) {
                            productsList += `<li>${product.trim()}</li>`;
                        }
                    });
                } catch (error) {
                    console.error('Error processing products:', error);
                    productsList = '<li>Error al procesar productos</li>';
                }
            }

            let content = `
                <html>
                <head>
                    <style>
                        body { 
                            font-family: Arial, sans-serif; 
                            padding: 20px;
                            background-color: #f5f5f5;
                        }
                        .products-list { 
                            max-height: 200px; 
                            overflow-y: auto;
                            list-style-type: none;
                            padding: 0;
                            background-color: white;
                            border-radius: 5px;
                            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                        }
                        .products-list li {
                            padding: 8px 15px;
                            border-bottom: 1px solid #eee;
                        }
                        .products-list li:last-child {
                            border-bottom: none;
                        }
                    </style>
                </head>
                <body>
                    <h2>🚛 Detalles del Operador</h2>
                    <h3>${params.data.operador}</h3>
                    <p><strong>Unidad:</strong> ${params.data.truck}</p>
                    <p><strong>Total Servicios:</strong> ${params.data.total_servicios}</p>
                    <p><strong>Servicios Únicos:</strong> ${params.data.servicios_unicos}</p>
                    <p><strong>Productos:</strong></p>
                    <ul class="products-list">
                        ${productsList}
                    </ul>
                </body>
                </html>
            `;
            
            const win = window.open('', '_blank', 'width=600,height=500');
            win.document.write(content);
            win.document.close();
            win.focus();
        }
        """

        # Configure the grid with AG Grid
        gb = GridOptionsBuilder.from_dataframe(display_df)
        
        # Configure columns (solo una vez)
        gb.configure_column("operador", 
                          headerName="Operador",
                          onCellClicked=JsCode(cell_click_js))
        gb.configure_column("truck", headerName="Unidad")
        gb.configure_column("total_servicios", headerName="Total Servicios")
        gb.configure_column("servicios_unicos", headerName="Servicios Únicos")
        gb.configure_column("product", headerName="Productos", hide=True)
        
        # Build and display the grid
        grid_options = gb.build()
        AgGrid(
            display_df,
            gridOptions=grid_options,
            allow_unsafe_jscode=True,
            theme='streamlit',
            height=400,
            use_container_width=True
        )

        st.header('Trailer Analysis')
        st.dataframe(trailer_services, use_container_width=True)

        # Sección 1: Gráfico de servicios por día
        st.header('Road Services by Day of Week')
        filtered_df['weekday'] = filtered_df['created_at'].dt.day_name()
        daily_orders = filtered_df.groupby([filtered_df['created_at'].dt.date, 'work_order']).size().reset_index()
        daily_orders['weekday'] = pd.to_datetime(daily_orders['created_at']).dt.day_name()
        weekday_counts = daily_orders.groupby('weekday')['work_order'].count()
        
        weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        weekday_counts = weekday_counts.reindex(weekday_order)

        # Ajustar el tamaño de la figura - modifica los números para cambiar el ancho y alto
        fig, ax = plt.subplots(figsize=(8, 4))  # (ancho, alto) en pulgadas
        
        weekday_counts.plot(kind='bar', ax=ax)
        for i, v in enumerate(weekday_counts):
            ax.text(i, v, str(v), ha='center', va='bottom')
        plt.title('Work Orders by Day of Week')
        plt.xlabel('Day of Week')
        plt.ylabel('Number of Work Orders')
        plt.xticks(rotation=45)
        
        # Opcionalmente, puedes usar el parámetro use_container_width para que se ajuste al ancho del contenedor
        st.pyplot(fig, use_container_width=True)
    
    except Exception as e:
        st.error(f"An error occurred while processing the data: {str(e)}")
        return

if __name__ == "__main__":
    main()
