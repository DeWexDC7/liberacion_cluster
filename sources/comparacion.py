import psycopg2
import json
import pandas as pd
from datetime import datetime

# Función para leer credenciales de la base de datos desde un archivo JSON
def read_db_credentials(config_path):
    """
    Lee las credenciales para la conexión a la base de datos desde un archivo JSON.
    """
    try:
        with open(config_path, 'r') as file:
            config = json.load(file)
        return config['PostgresSQL']
    except Exception as e:
        print(f"Error al leer el archivo de configuración: {e}")
        return None

# Función para establecer conexión a la base de datos
def conexion_bd(config_path):
    """
    Establece la conexión a la base de datos PostgreSQL usando credenciales del archivo JSON.
    """
    db_credentials = read_db_credentials(config_path)
    if not db_credentials:
        return None
    try:
        connection = psycopg2.connect(
            database=db_credentials['database'],
            user=db_credentials['user'],
            password=db_credentials['password'],
            host=db_credentials['host'],
            port=db_credentials['port']
        )
        print("Conexión exitosa")
        return connection
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

# Función para leer un archivo Excel y una hoja específica
def leer_archivo(ruta_archivo, hoja):
    """
    Lee un archivo Excel y retorna un DataFrame correspondiente a la hoja especificada.
    """
    try:
        df = pd.read_excel(ruta_archivo, sheet_name=hoja)
        return df
    except Exception as e:
        print(f"Error al leer el archivo: {e}")
        return None

# Función para verificar valores faltantes en la base de datos
def verificar_faltantes(df, connection):
    """
    Compara los valores de la columna CODIGO_NAP del Excel con los existentes en la base de datos.
    """
    try:
        # Normalizar la columna del archivo Excel
        df['CODIGO_NAP'] = df['CODIGO_NAP'].astype(str).str.strip().str.upper()
        df['CLUSTER'] = df['CLUSTER'].astype(str).str.strip().str.upper()
        
        # Filtrar los valores 'nap' y 'NAN'
        df = df[~df['CODIGO_NAP'].isin(['NAP', 'NAN'])]

        # Consulta para obtener los valores existentes en la base de datos por cluster
        cursor = connection.cursor()
        clusters = df['CLUSTER'].unique()

        faltantes_por_cluster = {}

        for cluster in clusters:
            # Filtrar el cluster actual
            df_cluster = df[df['CLUSTER'] == cluster]
            codigos_excel = df_cluster['CODIGO_NAP'].tolist()

            cursor.execute("SELECT nap FROM public.inv_naps WHERE cluster = %s", (cluster,))
            codigos_bd = [row[0].strip().upper() for row in cursor.fetchall()]

            # Identificar los valores que están en el Excel pero no en la base de datos
            faltantes_excel = [codigo for codigo in codigos_excel if codigo not in codigos_bd]

            if faltantes_excel:
                faltantes_por_cluster[cluster] = faltantes_excel
            
            else:
                print("Todos los códigos naps está registrados en la bd")

        return faltantes_por_cluster

    except Exception as e:
        print(f"Error al verificar los faltantes: {e}")
        return {}

# Función principal
def main():
    """
    Función principal que ejecuta el flujo completo:
    1. Lee credenciales y conecta a la base de datos.
    2. Lee un archivo Excel.
    3. Compara la columna CODIGO_NAP del Excel con la base de datos.
    4. Exporta los valores faltantes por cluster a archivos CSV.
    """
    config_path = 'conexion/config.json'  # Ruta al archivo de configuración
    ruta_archivo = 'Data/data.xlsx'       # Ruta al archivo Excel
    hoja = 'Correo'                       # Hoja a procesar

    # Conexión a la base de datos
    connection = conexion_bd(config_path)

    if connection:
        # Leer el archivo Excel
        df = leer_archivo(ruta_archivo, hoja)

        if df is not None and 'CODIGO_NAP' in df.columns and 'CLUSTER' in df.columns:
            print("-----Procesando valores faltantes por cluster-----")
            faltantes_por_cluster = verificar_faltantes(df, connection)

            fecha_actual = datetime.now().strftime('%Y%m%d')

            for cluster, faltantes in faltantes_por_cluster.items():
                # Crear un DataFrame con los valores faltantes
                df_faltantes = pd.DataFrame(faltantes, columns=['nap'])

                # Generar el nombre del archivo CSV
                nombre_archivo = f"Faltantes/faltante_naps_{fecha_actual}_{cluster}.csv"

                # Exportar los faltantes a un archivo CSV sin el header
                df_faltantes.to_csv(nombre_archivo, index=False, header=False)
                print(f"Archivo exportado: {nombre_archivo}")

                # Guardar el nombre del cluster en un archivo de texto
                with open('cluster_name.txt', 'w') as file:
                    file.write(cluster)

        else:
            print("Error: El archivo Excel no contiene las columnas 'CODIGO_NAP' o 'CLUSTER'.")
        
        # Cerrar la conexión a la base de datos
        connection.close()
    else:
        print("Error: No se pudo establecer conexión con la base de datos.")

# Ejecución del script
if __name__ == '__main__':
    main()
