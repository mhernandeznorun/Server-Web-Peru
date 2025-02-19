from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import glob
import os
import unicodedata
from pathlib import Path

def normalizar_texto(texto):
    """
    Normaliza el texto removiendo tildes y convirtiendo a minúsculas.
    También maneja equivalencias español-inglés.
    """
    if pd.isna(texto):
        return None
        
    # Mapeo de términos inglés-español
    equivalencias = {
        'project': 'proyecto',
        'location': 'localizacion',
        'data source': 'fuente de datos',
        'geolocation': 'geolocalizacion',
        'interval': 'intervalo',
        'movement': 'movimiento',
        'person': 'persona'
    }
    
    # Normalizar texto (eliminar tildes y convertir a minúsculas)
    texto_normalizado = ''.join(
        c for c in unicodedata.normalize('NFD', str(texto))
        if unicodedata.category(c) != 'Mn'
    ).lower().strip()
    
    # Aplicar equivalencias si existe
    return equivalencias.get(texto_normalizado, texto_normalizado)

def cargar_mapeos_desde_plantilla(archivo_plantilla):
    """
    Carga los mapeos de PC y nombres de intersecciones desde la plantilla
    """
    try:
        df_mapeo = pd.read_excel(archivo_plantilla, sheet_name=0)
        df_mapeo.columns = [normalizar_texto(col) for col in df_mapeo.columns]
        
        # Debug: imprimir columnas disponibles
        print("\nColumnas en la plantilla:")
        print(df_mapeo.columns.tolist())
        
        # Crear mapeos desde la plantilla
        pc_to_intersection = dict(zip(
            df_mapeo['punto norun'], 
            df_mapeo['nombre para cliente']
        ))
        
        # En este caso, pc_to_s y s_to_intersection son el mismo mapeo
        # ya que vamos directo de PC a nombre de intersección
        return pc_to_intersection, pc_to_intersection
        
    except Exception as e:
        print(f"Error cargando mapeos desde plantilla: {str(e)}")
        raise

def procesar_datos_peatones(archivo_entrada, pc_to_intersection, _):
    """
    Procesa un archivo de datos de peatones
    """
    try:
        # Leer archivo (segunda hoja, índice 1)
        df = pd.read_excel(archivo_entrada, sheet_name=1, skiprows=1)
        
        # Normalizar columnas
        df.columns = [normalizar_texto(col) for col in df.columns]
        
        # Debug: mostrar columnas disponibles
        print(f"\nColumnas en archivo {os.path.basename(archivo_entrada)}:")
        print(df.columns.tolist())
        
        # Aplicar mapeo directo de PC a INTERSECCION
        df['pc'] = df['fuente de datos']
        df['interseccion'] = df['fuente de datos'].map(pc_to_intersection)
        
        # Procesar fechas y horas
        df['start_date'] = pd.to_datetime(
            df['intervalo'].str.split(' - ').str[0],
            format="%m/%d/%Y %H:%M:%S"
        )
        df['end_date'] = pd.to_datetime(
            df['intervalo'].str.split(' - ').str[1],
            format="%m/%d/%Y %H:%M:%S"
        )
        
        # Crear columnas requeridas
        df['fecha'] = df['start_date'].dt.strftime('%d-%m-%Y')
        df['hora inicio'] = df['start_date'].dt.strftime('%H:%M:%S')
        df['hora termino'] = df['end_date'].dt.strftime('%H:%M:%S')
        
        # Calcular cuartos
        df['hour'] = df['start_date'].dt.hour
        df['minute'] = df['start_date'].dt.minute
        df['cuarto'] = df['hour'].astype(str) + ',' + (df['minute'] // 15 + 1).astype(str)
        
        # Seleccionar columnas finales (en minúsculas)
        columnas_finales = ['pc', 'interseccion', 'fecha', 'hora inicio', 
                           'hora termino', 'movimiento', 'cuarto', 'persona']
        
        df_final = df[columnas_finales].copy()
        
        # Ordenar y eliminar duplicados
        df_final = df_final.sort_values(
            by=['pc', 'movimiento', 'fecha', 'hora inicio']
        ).drop_duplicates(
            subset=['pc', 'movimiento', 'fecha', 'hora inicio'],
            keep='first'
        )
        
        # Convertir columnas a mayúsculas para el formato final
        df_final.columns = df_final.columns.str.upper()
        
        return df_final
        
    except Exception as e:
        print(f"Error procesando archivo {archivo_entrada}: {str(e)}")
        raise

def main():
    print("\n" + "="*50)
    print("Iniciando procesamiento de datos peatonales")
    print("="*50 + "\n")
    
    # Crear carpetas de salida
    carpeta_salida = 'data_peatones_final'
    os.makedirs(carpeta_salida, exist_ok=True)
    
    try:
        # Cargar plantilla y mapeos
        archivo_plantilla = 'Plantilla/plantilla_peru.xlsx'
        pc_to_intersection, _ = cargar_mapeos_desde_plantilla(archivo_plantilla)
        
        # Procesar archivos de entrada
        carpeta_entrada = 'Multisource Categoría Peatones'
        archivos = glob.glob(os.path.join(carpeta_entrada, '*.xlsx'))
        
        if not archivos:
            print("No se encontraron archivos para procesar")
            return
            
        # Procesar cada archivo
        dfs = []
        for archivo in archivos:
            try:
                df = procesar_datos_peatones(archivo, pc_to_intersection, None)
                if not df.empty:
                    dfs.append(df)
            except Exception as e:
                print(f"Error en archivo {archivo}: {str(e)}")
                continue
        
        if not dfs:
            print("No se generaron datos válidos para procesar")
            return
            
        # Combinar todos los DataFrames
        df_final = pd.concat(dfs, ignore_index=True)
        
        # Guardar resultado
        archivo_salida = os.path.join(carpeta_salida, 'reporte_final_peatones.xlsx')
        df_final.to_excel(archivo_salida, index=False)
        print(f"\nArchivo guardado en: {archivo_salida}")
        
    except Exception as e:
        print(f"Error en el procesamiento: {str(e)}")
        raise

if __name__ == "__main__":
    main()