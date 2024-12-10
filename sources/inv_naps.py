import pandas as pd
from datetime import datetime
import psycopg2
import json

# Función para leer los datos de configuración de la base de datos
def read_db_credentials(config_path):
    """
    Lee las credenciales desde un archivo JSON.
    """
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config['PostgresSQL'], config['radiusmain']

# Función para leer los datos desde el archivo CSV sin header
def leer_csv_sin_header(ruta_csv):
    """
    Lee un archivo CSV sin encabezado y lo convierte en un DataFrame.
    """
    try:
        df = pd.read_csv(ruta_csv, header=None, names=['CODIGO_NAP'])
        df['CODIGO_NAP'] = df['CODIGO_NAP'].astype(str).str.strip().str.upper()  # Normalizar
        return df
    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        return None

# Función para procesar los datos faltantes
def procesar_busqueda_por_faltantes(df_faltantes, ruta_archivo, hoja, postgres_connection, exportar, ruta_salida_base, cluster):
    """
    Procesa los datos faltantes desde un archivo Excel, los cruza con los faltantes,
    y exporta el resultado filtrado.
    """
    try:
        # Leer la hoja principal del archivo Excel
        df = pd.read_excel(ruta_archivo, sheet_name=hoja)
        
        # Normalizar y limpiar los datos
        df['CODIGO_NAP'] = df['CODIGO_NAP'].astype(str).str.strip().str.upper()

        # Filtrar los datos que coinciden con los faltantes
        resultado = df[df['CODIGO_NAP'].isin(df_faltantes['CODIGO_NAP'])]

        # Extraer solo la columna `region` desde la base de datos usando la columna `cluster`
        cursor = postgres_connection.cursor()
        resultado.loc[:, 'region'] = resultado['CLUSTER'].apply(
            lambda cluster: obtener_region_cluster(cluster, cursor)
        )

        # Ajustar las columnas para coincidir con los campos de la tabla SQL
        resultado = resultado.rename(
            columns={
                "HUB": "hub",
                "CLUSTER": "cluster",
                "OLT": "olt",
                "FRAME": "frame",
                "SLOT": "slot",
                "PUERTO": "puerto",
                "CODIGO_NAP": "nap",
                "# PUERTOS NAP": "puertos_nap",
                "LATITUD": "latitud",
                "LONGITUD": "longitud",
            }
        )
        
        # Crear la columna `coordenadas` concatenando longitud y latitud entre paréntesis
        resultado["coordenadas"] = resultado.apply(
            lambda row: f"({row['longitud']}, {row['latitud']})", axis=1
        )
        
        # Agregar columna `fecha_de_liberacion` usando la fecha del sistema
        resultado["fecha_de_liberacion"] = datetime.now().strftime('%Y-%m-%d')

        # Agregar columna `zona` con valor predeterminado `null`
        resultado["zona"] = "[null]"

        # Reorganizar las columnas
        columnas_finales = [
            "hub", "cluster", "olt", "frame", "slot", "puerto", "nap",
            "puertos_nap", "coordenadas", "fecha_de_liberacion", "region", "zona", "latitud", "longitud"
        ]
        resultado = resultado[columnas_finales]
        
        if exportar:
            # Obtener la fecha actual
            fecha_actual = datetime.now().strftime('%Y%m%d')
            # Crear el nombre del archivo con la fecha
            nombre_archivo = f"Registros_Naps/{ruta_salida_base}_{fecha_actual}_{cluster}.xlsx"
            # Exportar a Excel
            resultado.to_excel(nombre_archivo, index=False)
            print(f"Archivo exportado: {nombre_archivo}")
        
        return resultado
    except Exception as e:
        print(f"Error al procesar la búsqueda por faltantes: {e}")
        return None

# Función para obtener solo la región desde la tabla `clusters`
def obtener_region_cluster(cluster, cursor):
    """
    Obtiene la región de un `cluster` desde la tabla `clusters` en la base de datos.
    """
    try:
        query = "SELECT t.region FROM public.clusters t WHERE t.nombre = %s LIMIT 1"
        cursor.execute(query, (cluster,))
        resultado = cursor.fetchone()
        if resultado:
            return resultado[0]
        return "[null]"
    except Exception as e:
        print(f"Error al obtener la región para el cluster {cluster}: {e}")
        return "[null]"

# Función para separar los datos obtenidos en variables independientes
def definicion_variables(df):
    """
    Separa los datos obtenidos en variables independientes.
    """
    try:
        required_columns = [
            'hub', 'cluster', 'olt', 'frame', 'slot', 
            'puerto', 'nap', 'puertos_nap', 'latitud', 'longitud', 'zona', 'region'
        ]
        # Verificar si todas las columnas requeridas están presentes
        if not all(col in df.columns for col in required_columns):
            print(f"Faltan columnas en el DataFrame: {set(required_columns) - set(df.columns)}")
            return None
        
        # Definir las variables
        variables = {col: df[col].values[0] for col in required_columns}
        variables['coordenadas'] = f"({variables['latitud']}, {variables['longitud']})"

        for key, value in variables.items():
            print(f"{key}: {value}")

        return variables
    except Exception as e:
        print(f"Error al definir las variables: {e}")
        return None

def subir_datos_a_bd(variables, radiusmain_credentials):
    """
    Sube los datos obtenidos en variables independientes a la base de datos radiusmain.
    """
    print("--- Subiendo datos a la base de datos RadiusMain ---")
    try:
        # Convertir valores a tipos nativos de Python
        variables = {k: (v.item() if hasattr(v, 'item') else v) for k, v in variables.items()}
        
        # Validar que todos los valores requeridos estén presentes
        required_keys = ['hub', 'cluster', 'olt', 'frame', 'slot', 'puerto', 'nap', 
                         'puertos_nap', 'latitud', 'longitud', 'coordenadas', 'region', 'zona']
        
        for key in required_keys:
            if key not in variables or variables[key] is None:
                variables[key] = '[null]'  # Valor predeterminado

        # Conexión a la base de datos radiusmain
        connection = psycopg2.connect(
            database=radiusmain_credentials['dbname'],
            user=radiusmain_credentials['user'],
            password=radiusmain_credentials['password'],
            host=radiusmain_credentials['host'],
            port=radiusmain_credentials['port']
        )
        cursor = connection.cursor()
        fecha_de_liberacion = datetime.now().strftime('%Y-%m-%d')
        
        # Insertar los datos en la tabla
        query = """
            INSERT INTO public.inv_naps 
            (hub, cluster, olt, frame, slot, puerto, nap, puertos_nap, latitud, longitud, coordenadas, region, fecha_de_liberacion, zona) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (
            variables['hub'], variables['cluster'], variables['olt'], variables['frame'],
            variables['slot'], variables['puerto'], variables['nap'], variables['puertos_nap'],
            variables['latitud'], variables['longitud'], variables['coordenadas'], variables['region'],
            fecha_de_liberacion, variables['zona']
        ))
        connection.commit()
        print("Datos subidos correctamente.")
    except Exception as e:
        print(f"Error al subir los datos a la base de datos RadiusMain: {e}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()


# Función principal
def main():
    """
    Función principal que ejecuta el flujo completo.
    """
    config_path = 'conexion/config.json'
    ruta_archivo = 'data/data.xlsx'
    hoja = 'Correo'
    exportar = True
    ruta_salida_base = 'Resultados_NAP'
    
    # Leer el nombre del cluster desde el archivo de texto
    try:
        with open('cluster_name.txt', 'r') as file:
            cluster = file.read().strip()
    except Exception as e:
        print(f"Error al leer el archivo de nombre del cluster: {e}")
        return

    fecha_actual = datetime.now().strftime('%Y%m%d')
    ruta_csv_faltantes = f'Faltantes/faltante_naps_{fecha_actual}_{cluster}.csv'

    db_credentials, radiusmain_credentials = read_db_credentials(config_path)

    try:
        postgres_connection = psycopg2.connect(
            database=db_credentials['database'],
            user=db_credentials['user'],
            password=db_credentials['password'],
            host=db_credentials['host'],
            port=db_credentials['port']
        )
        print("Conexión exitosa a la base de datos PostgresSQL.")
    except Exception as e:
        print(f"Error al conectar a la base de datos PostgresSQL: {e}")
        return

    df_faltantes = leer_csv_sin_header(ruta_csv_faltantes)

    if df_faltantes is not None and not df_faltantes.empty:
        print("-----Procesando códigos NAP faltantes-----")
        df_resultado = procesar_busqueda_por_faltantes(df_faltantes, ruta_archivo, hoja, postgres_connection, exportar, ruta_salida_base, cluster)
        
        if df_resultado is not None and not df_resultado.empty:
            print("Datos procesados y exportados correctamente.")
            variables = definicion_variables(df_resultado)
            if variables is not None:
                subir_datos_a_bd(variables, radiusmain_credentials)

    else:
        print("Error al cargar los valores faltantes desde el archivo CSV.")

    if postgres_connection:
        postgres_connection.close()

if __name__ == '__main__':
    main()