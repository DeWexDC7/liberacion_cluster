import psycopg2
import json

def read_db_credentials(config_path):
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config['radiusmain']

config_path = '../conexion/config.json'
db_credentials = read_db_credentials(config_path)

try:
    connection = psycopg2.connect(
        database=db_credentials['database'],  # Changed from dbname to database
        user=db_credentials['user'],
        password=db_credentials['password'],
        host=db_credentials['host'],
        port=db_credentials['port']
    )
except Exception as e:
    print(f"Error al conectar a la base de datos: {e}")
    connection = None

def verificacion_de_conexion():
    if connection is None:
        print("No se pudo establecer la conexión a la base de datos.")
        return

    try:
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print(f"Conexión exitosa a la base de datos: {record}")
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
    finally:
        cursor.close()
        connection.close()

def main():
    verificacion_de_conexion()

if __name__ == '__main__':
    main()