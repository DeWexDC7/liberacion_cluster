import pandas as pd
from datetime import datetime

def leer_faltantes(ruta_csv):
    try:
        # Leer el archivo CSV
        df = pd.read_csv(ruta_csv)
        return df
    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        return None
        # Leer el archivo CSV
        df = pd.read_csv(ruta_csv)
        return df
    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        return None

def procesar_busqueda_por_faltantes(ruta_archivo, hoja, df_faltantes, exportar, ruta_salida_base):
    try:
        # Leer el archivo Excel
        df = pd.read_excel(ruta_archivo, sheet_name=hoja)
        
        # Filtrar los datos que coinciden con los faltantes
        df_faltantes['CODIGO_NAP'] = df_faltantes['CODIGO_NAP'].astype(str).str.strip().str.upper()
        df['CODIGO_NAP'] = df['CODIGO_NAP'].astype(str).str.strip().str.upper()
        resultado = df[df['CODIGO_NAP'].isin(df_faltantes['CODIGO_NAP'])]
        
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
                "LONGITUD": "longitud"
            }
        )
        
        # Crear la columna `coordenadas`
        resultado["coordenadas"] = resultado.apply(
            lambda row: f"({row['latitud']}, {row['longitud']})", axis=1
        )
        
        # Agregar columna `fecha_de_liberacion` usando la fecha del sistema
        resultado["fecha_de_liberacion"] = datetime.now().strftime('%Y-%m-%d')
        
        # Agregar columna `region` y valores predeterminados
        resultado["region"] = "R2"

        #Agregar columna 'zona' y valores predeterminados
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
            # Obtener el nombre del cluster
            nombre_cluster = resultado['cluster'].iloc[0] if not resultado.empty else 'sin_cluster'
            # Crear el nombre del archivo con la fecha y el nombre del cluster
            nombre_archivo = f"Registros_Naps/{ruta_salida_base}_{fecha_actual}_{nombre_cluster}.xlsx"
            # Exportar a Excel
            resultado.to_excel(nombre_archivo, index=False)
            print(f"Archivo exportado: {nombre_archivo}")
        else:
            print(resultado)
    except Exception as e:
        print(f"Error al procesar la búsqueda por faltantes: {e}")

# Función principal
def main():
    # Configuración
    ruta_archivo = 'data/Data.xlsx'  # Ruta del archivo Excel cargado
    hoja = 'Correo'  # Cambiar a la hoja correcta
    exportar = True  # Cambiar a False si no deseas exportar
    ruta_salida_base = 'Resultados_NAP'  # Base para los nombres de salida

    # Leer los valores faltantes
    ruta_csv = 'Faltantes/faltantes_codigos_nap.csv'  # Ruta del archivo CSV generado
    df_faltantes = leer_faltantes(ruta_csv)

    if df_faltantes is not None and not df_faltantes.empty:
        print("-----Procesando códigos NAP faltantes-----")
        procesar_busqueda_por_faltantes(ruta_archivo, hoja, df_faltantes, exportar, ruta_salida_base)
    else:
        print("Error al cargar el archivo CSV o no hay datos faltantes.")

# Ejecución del script
if __name__ == '__main__':
    main()
