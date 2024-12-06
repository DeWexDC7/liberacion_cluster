import time
import logging
import sys
from sources import comparate, naps  

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ejecutar_scripts():
    """
    Ejecuta los scripts comparate.py y naps.py secuencialmente.
    """
    logging.info('Iniciando ejecución de comparate.py')
    print("\n=== Iniciando ejecución de comparate.py ===")
    comparate.main()
    
    # Pausa para asegurar que el archivo CSV se haya generado
    time.sleep(1)
    
    logging.info('Iniciando ejecución de naps.py')
    print("\n=== Iniciando ejecución de naps.py ===")
    naps.main()

if __name__ == '__main__':
    # Agrega el directorio 'sources' al path de Python
    sys.path.append('./sources') 
    ejecutar_scripts()
