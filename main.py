import subprocess
import concurrent.futures
import logging
from pathlib import Path

def configurar_logging():
    """
    Configura el sistema de logging
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('proceso.log'),
            logging.StreamHandler()
        ]
    )

def ejecutar_script(script_path):
    """
    Ejecuta un script de Python y espera a que termine.
    """
    try:
        logging.info(f"Iniciando {script_path}...")
        result = subprocess.run(
            ['python', script_path], 
            check=True, 
            capture_output=True, 
            text=True
        )
        logging.info(f"{script_path} completado exitosamente")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Error ejecutando {script_path}: {e.stderr}")
        return False

def verificar_estructura():
    """
    Verifica que existan las carpetas y archivos necesarios
    """
    carpetas_requeridas = [
        'Multisource Categoría Chile',
        'Plantilla'
    ]
    
    archivos_requeridos = [
        'Plantilla/plantilla_peru.xlsx'
    ]
    
    for carpeta in carpetas_requeridas:
        if not Path(carpeta).exists():
            logging.error(f"Carpeta requerida no encontrada: {carpeta}")
            return False
            
    for archivo in archivos_requeridos:
        if not Path(archivo).exists():
            logging.error(f"Archivo requerido no encontrado: {archivo}")
            return False
            
    return True

def main():
    """
    Función principal que ejecuta el proceso completo
    """
    configurar_logging()
    logging.info("Iniciando procesamiento de datos")
    
    if not verificar_estructura():
        logging.error("Verificación de estructura fallida")
        return
    
    scripts = [
        'interp_tricycles.py',
        'merge_cl_comp.py',
        'merge_cl_ph.py',
        'homologate.py'
    ]
    
    # Crear carpetas de salida
    for carpeta in ['data_filipinas', 'data_chile', 'data_merged_cl_fi', 'data_peru_final']:
        Path(carpeta).mkdir(exist_ok=True)
    
    # Ejecutar scripts secuencialmente ya que dependen uno del otro
    for script in scripts:
        if not ejecutar_script(script):
            logging.error(f"Proceso detenido debido a error en {script}")
            return
    
    logging.info("Proceso completado exitosamente")

if __name__ == "__main__":
    main()
