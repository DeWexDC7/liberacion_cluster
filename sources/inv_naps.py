import pandas as pd
from datetime import datetime
import psycopg2
import json

# Función para leer los datos de configuración de la base de datos
def read_db_credentials(config_path):
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config['PostgresSQL']

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
def procesar_busqueda_por_faltantes(df_faltantes, ruta_archivo, hoja, connection, exportar, ruta_salida_base):
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
        cursor = connection.cursor()
        resultado['region'] = resultado['CLUSTER'].apply(
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
            nombre_archivo = f"Registros_Naps/{ruta_salida_base}_{fecha_actual}.xlsx"
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
            print(f"Datos para cluster {cluster}: {resultado}")  # Depuración
            return resultado[0]  # Retorna solo la región
        print(f"No se encontraron datos para el cluster {cluster}")
        return "[null]"
    except Exception as e:
        print(f"Error al obtener los datos desde la base de datos para {cluster}: {e}")
        return "[null]"

# Función principal
def main():
    """
    Función principal que ejecuta el flujo completo:
    1. Lee credenciales y conecta a la base de datos.
    2. Lee un archivo Excel.
    3. Compara la columna CODIGO_NAP del Excel con los faltantes.
    4. Exporta los valores procesados a un archivo Excel.
    """
    config_path = 'conexion/config.json'
    ruta_archivo = 'data/data.xlsx'
    hoja = 'Correo'
    exportar = True
    ruta_salida_base = 'Resultados_NAP'
    ruta_csv_faltantes = 'Faltantes/faltantes_codigos_nap_en_bd.csv'

    # Leer las credenciales de la base de datos
    db_credentials = read_db_credentials(config_path)

    # Establecer conexión a la base de datos
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_credentials['database'],
            user=db_credentials['user'],
            password=db_credentials['password'],
            host=db_credentials['host'],
            port=db_credentials['port']
        )
        print("Conexión exitosa a la base de datos.")
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return

    # Leer los valores faltantes desde el archivo CSV sin header
    df_faltantes = leer_csv_sin_header(ruta_csv_faltantes)

    if df_faltantes is not None and not df_faltantes.empty:
        print("-----Procesando códigos NAP faltantes-----")
        # Procesar y obtener el DataFrame resultado
        df_resultado = procesar_busqueda_por_faltantes(df_faltantes, ruta_archivo, hoja, connection, exportar, ruta_salida_base)
        
        if df_resultado is not None and not df_resultado.empty:
            print("Datos procesados y exportados correctamente.")
        else:
            print("No se generó ningún resultado para exportar.")
    else:
        print("Error al cargar los valores faltantes desde el archivo CSV. Verifique que el archivo contiene los datos necesarios.")

    # Cerrar conexión a la base de datos
    if connection:
        connection.close()

# Ejecución del script
if __name__ == '__main__':
    main()
