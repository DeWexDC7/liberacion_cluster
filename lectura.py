import pandas as pd

def lectura_naps():
    try:
        df = pd.read_excel("Data/data.xlsx", sheet_name="Correo")
        # Selecciona las columnas deseadas
        columnas_deseadas = df[["HUB", "CLUSTER"]].iloc[0]
        return columnas_deseadas
    except FileNotFoundError as e:
        print(f"Error de lectura: {e}")
        return None

def presentacion_naps():
    columnas_deseadas = lectura_naps()
    if columnas_deseadas is not None:
        print(columnas_deseadas)
    else:
        print("No se pudo leer el archivo.")

def exportar_xlsx():
    df = pd.read_excel("Data/data.xlsx", sheet_name="Correo")
    # Selecciona todas las filas de las columnas HUB y CLUSTER
    datos = df[["HUB", "CLUSTER"]]
    # Crea un nuevo DataFrame con todos los datos
    df_export = pd.DataFrame(datos, columns=["HUB", "CLUSTER"])
    # Exporta a Excel
    df_export.to_excel("Data/export.xlsx", index=False)

def main():
    presentacion_naps()
    exportar_xlsx()

if __name__ == "__main__":
    main()