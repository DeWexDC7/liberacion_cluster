import pandas as pd

# Función para leer un archivo xlsx y una hoja específica
def leer_archivo(ruta_archivo, hoja):
    try:
        # Leer la hoja del archivo
        df = pd.read_excel(ruta_archivo, sheet_name=hoja)
        return df
    except Exception as e:
        print(f"Error al leer el archivo: {e}")
        return None

# Función para comparar y mostrar los valores faltantes en la columna 'nap' en relación con 'CODIGO_NAP'
def verificar_faltantes(df):
    try:
        # Normalizar las columnas para garantizar coincidencias exactas
        df['nap'] = df['nap'].astype(str).str.strip().str.upper()
        df['CODIGO_NAP'] = df['CODIGO_NAP'].astype(str).str.strip().str.upper()

        # Identificar los valores que están en 'CODIGO_NAP' pero no en 'nap'
        faltantes = df[~df['CODIGO_NAP'].isin(df['nap'])]
        return faltantes[['CODIGO_NAP']]
    except KeyError as e:
        print(f"Error: Falta una columna clave - {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error al verificar los faltantes: {e}")
        return pd.DataFrame()

# Función principal
def main():
    # Ruta del archivo Excel
    ruta_archivo = 'Data/data.xlsx'  # Cambiar por la ruta correcta
    hoja = 'Hoja3'  # Cambiar por la hoja correspondiente

    # Leer el archivo
    df = leer_archivo(ruta_archivo, hoja)

    # Verificar faltantes
    if df is not None:
        print("-----Valores faltantes en 'nap' en relación con 'CODIGO_NAP'-----")
        faltantes = verificar_faltantes(df)
        print(faltantes)
        
        # Exportar los faltantes a un archivo CSV en la carpeta Faltantes
        faltantes.to_csv('Faltantes/faltantes_codigos_nap.csv', index=False)
    else:
        print("Error al cargar el archivo o la hoja.")

# Ejecución del script
if __name__ == '__main__':
    main()