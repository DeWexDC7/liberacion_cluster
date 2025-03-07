import pandas as pd 
import logging
import psycopg2
import json
import time
import os

#configuracion logging
logging.basicConfig(level=logging.DEBUG)

def conexion_bd():
    try:
        with open('conexion/config.json') as f:
            config = json.load(f)
        
        conn = psycopg2.connect(
            host=config['host'],
            database=config['database'],
            user=config['user'],
            password=config['password'],
            port=config['port']
        )
        logging.info("Conexión exitosa a la base de datos")
        return conn
    except (psycopg2.Error, FileNotFoundError) as e:
        logging.error(f"Error de conexión: {e}")
        return None

def get_column_case_insensitive(df, column_name):
    """Obtiene una columna del DataFrame independientemente de mayúsculas/minúsculas"""
    for col in df.columns:
        if col.upper() == column_name.upper():
            return col
    return None

def lectura_naps():
    try:
        df = pd.read_excel("Data/data.xlsx", sheet_name="Correo")

        # Mostrar las columnas disponibles para depuración
        logging.info(f"Columnas disponibles en el Excel: {list(df.columns)}")
        
        # Mapeo de nombres de columnas esperadas a nombres reales
        column_mappings = {}
        expected_columns = ["CODIGO_NAP", "HUB", "CLUSTER", "OLT", "FRAME", "SLOT", 
                            "PUERTO", "# PUERTOS NAP", "LATITUD", "LONGITUD"]
        
        for col in expected_columns:
            actual_col = get_column_case_insensitive(df, col)
            if actual_col:
                column_mappings[col] = actual_col
            else:
                logging.warning(f"Columna '{col}' no encontrada en el Excel. Verificar el nombre exacto.")
        
        # Extraer valores usando los nombres de columnas reales
        codigos_nap = df[column_mappings.get("CODIGO_NAP")].dropna().tolist() if "CODIGO_NAP" in column_mappings else []
        
        # Para cada valor individual, usar iloc[0] solo si hay datos
        if len(df) == 0:
            logging.error("El archivo Excel no contiene datos")
            return None
            
        # Obtener los valores individuales con manejo de errores
        try:
            hub = df[column_mappings.get("HUB")].iloc[0] if "HUB" in column_mappings else "Sin dato"
            cluster = df[column_mappings.get("CLUSTER")].iloc[0] if "CLUSTER" in column_mappings else "Sin dato"
            olt = df[column_mappings.get("OLT")].iloc[0] if "OLT" in column_mappings else "Sin dato"
            frame = df[column_mappings.get("FRAME")].iloc[0] if "FRAME" in column_mappings else 0
            slot = df[column_mappings.get("SLOT")].iloc[0] if "SLOT" in column_mappings else 0
            puerto = df[column_mappings.get("PUERTO")].iloc[0] if "PUERTO" in column_mappings else 0
            puertos_nap = df[column_mappings.get("# PUERTOS NAP")].iloc[0] if "# PUERTOS NAP" in column_mappings else 0
            latitud = df[column_mappings.get("LATITUD")].iloc[0] if "LATITUD" in column_mappings else 0.0
            longitud = df[column_mappings.get("LONGITUD")].iloc[0] if "LONGITUD" in column_mappings else 0.0
            
            # Convertir valores a los tipos correctos
            frame = int(frame) if pd.notna(frame) else 0
            slot = int(slot) if pd.notna(slot) else 0
            puerto = int(puerto) if pd.notna(puerto) else 0
            puertos_nap = int(puertos_nap) if pd.notna(puertos_nap) else 0
            latitud = float(latitud) if pd.notna(latitud) else 0.0
            longitud = float(longitud) if pd.notna(longitud) else 0.0
            
            # Logging para depuración
            logging.debug(f"hub: {hub}, cluster: {cluster}, olt: {olt}, frame: {frame}, slot: {slot}, puerto: {puerto}, puertos_nap: {puertos_nap}, latitud: {latitud}, longitud: {longitud}")

            return hub, cluster, olt, frame, slot, puerto, codigos_nap, puertos_nap, latitud, longitud
        except Exception as e:
            logging.error(f"Error al extraer datos del Excel: {e}")
            return None
            
    except FileNotFoundError as e:
        logging.error(f"Error al leer el archivo Excel: {e}")
        return None
    except Exception as e:
        logging.error(f"Error inesperado al procesar el Excel: {e}")
        return None

def get_region_zone_from_db(cluster):
    """Obtiene la región y zona desde la base de datos según el cluster"""
    conn = conexion_bd()
    region = "Sin dato"
    zona = "Sin dato"
    
    if conn is not None:
        try:
            cursor = conn.cursor()
            # Consultar región y zona basado en el cluster
            cursor.execute("SELECT region, zona FROM inv_naps WHERE cluster = %s LIMIT 1", (cluster,))
            result = cursor.fetchone()
            if result:
                region = result[0] if result[0] else "Sin dato"
                zona = result[1] if result[1] else "Sin dato"
            else:
                # Buscar patrones R1 o R2 en el código del cluster
                if "R1" in str(cluster).upper():
                    region = "R1"
                elif "R2" in str(cluster).upper():
                    region = "R2"
                
            cursor.close()
            conn.close()
        except Exception as e:
            logging.error(f"Error al obtener región y zona de la BD: {e}")
    
    return region, zona

def busqueda_naps_bd():
    result = lectura_naps()
    if result is None:
        return
    
    hub, cluster, olt, frame, slot, puerto, codigos_nap_excel, puertos_nap, latitud, longitud = result

    conn = conexion_bd()
    if conn is not None:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT nap FROM inv_naps")
            codigos_nap_bd = [row[0] for row in cursor.fetchall()]
            
            codigos_faltantes = set(codigos_nap_excel) - set(codigos_nap_bd)
            if codigos_faltantes:
                print("Códigos NAP faltantes en la BD:")
                print(codigos_faltantes)
                
                # Exportar cada NAP faltante
                for nap_faltante in codigos_faltantes:
                    exportacion_data(hub, cluster, olt, frame, slot, puerto, nap_faltante, puertos_nap, latitud, longitud)
                
                return codigos_faltantes
            else:
                print("Todos los códigos NAP del Excel están presentes en la BD.")
        except psycopg2.Error as e:
            logging.error(f"Error al ejecutar la consulta: {e}")
        finally:
            cursor.close()
            conn.close()

def exportacion_data(hub, cluster, olt, frame, slot, puerto, nap, puertos_nap, latitud, longitud):
    # Obtener región y zona de la BD si están vacías o son "Sin dato"
    region, zona = get_region_zone_from_db(cluster)
    
    # Convertir coordenadas a formato numérico si son strings
    try:
        if latitud and isinstance(latitud, str):
            latitud = float(latitud.replace(',', '.'))
        if longitud and isinstance(longitud, str):
            longitud = float(longitud.replace(',', '.'))
    except (ValueError, TypeError):
        logging.warning("Error al convertir coordenadas a formato numérico")
    
    # Formatear coordenadas con más decimales
    if latitud and longitud and latitud != 0 and longitud != 0:
        coordenadas = f"({longitud:.6f}, {latitud:.6f})"
        # Formatear latitud y longitud para el DataFrame con comas como separador decimal
        lat_str = f"{latitud:.6f}".replace('.', ',')
        long_str = f"{longitud:.6f}".replace('.', ',')
    else:
        coordenadas = "(Sin coordenadas)"
        lat_str = ""
        long_str = ""
    
    # Obtener la fecha actual en formato DD/MM/YYYY
    fecha_liberacion = fecha_hoy()
    
    data = {
        "hub": [hub],
        "cluster": [cluster],
        "olt": [olt],
        "frame": [frame],
        "slot": [slot],
        "puerto": [puerto],
        "nap": [nap],  # Ahora este es el NAP específico que falta en la BD
        "puertos_nap": [puertos_nap],
        "coordenadas": [coordenadas],
        "fecha_de_liberacion": [fecha_liberacion],
        "region": [region],
        "zona": [zona],
        "latitud": [lat_str],
        "longitud": [long_str]
    } 
    
    # Crear el directorio si no existe
    os.makedirs("Registros_Naps", exist_ok=True)
    
    df = pd.DataFrame(data)
    # Usar el código NAP específico en el nombre del archivo
    filename = f"Registros_Naps/Inventario_Nap_{nap}.xlsx"
    df.to_excel(filename, index=False)
    logging.info(f"Archivo '{filename}' generado correctamente")
    
def fecha_hoy():
    return time.strftime("%d/%m/%Y")

def presentacion_resultados():
    print("-----------------------------------------------------")
    print("      SISTEMA DE VALIDACIÓN Y REGISTRO DE NAPs       ")
    print("-----------------------------------------------------")
    print("Iniciando validación de NAPs en base de datos...")
    busqueda_naps_bd()
    print("-----------------------------------------------------")
    print("Proceso completado.")

def main():
    presentacion_resultados()

if __name__ == "__main__":
    main()