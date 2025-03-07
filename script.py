import time
import logging
import sys
from sources import comparacion, inv_naps  

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ejecutar_scripts():
    """
    Ejecuta los scripts comparacion.py y inv_naps.py secuencialmente.
    """
    logging.info('Iniciando ejecución de comparacion.py')
    print("\n=== Iniciando ejecución de comparacion.py ===")
    comparacion.main()
    
    # Pausa para asegurar que el archivo CSV se haya generado
    time.sleep(1)
    
    logging.info('Iniciando ejecución de inv_naps.py')
    print("\n=== Iniciando ejecución de inv_naps.py ===")
    inv_naps.main()

if __name__ == '__main__':
    # Agrega el directorio 'sources' al path                                                                                                                                      de Python
    sys.path.append('./sources') 
    ejecutar_scripts()
