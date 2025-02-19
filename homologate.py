'''
Este Script hace lo siguiente:
Toma los archivos procesados de la carpeta data_merged_cl_fi,
los combina en un solo dataframe y realiza las modificaciones
para pasarlo a la categoría de destino (Perú).
'''

from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import glob
import os
import re
import unicodedata

# Mantener solo las definiciones de funciones auxiliares
def normalizar_texto(texto):
    """
    Normaliza el texto removiendo tildes y convirtiendo a minúsculas
    """
    if pd.isna(texto):
        return None
    texto_normalizado = ''.join(
        c for c in unicodedata.normalize('NFD', str(texto))
        if unicodedata.category(c) != 'Mn'
    )
    return texto_normalizado.lower().strip()

def extract_pc_number(text):
    """
    Extrae el número de PC
    """
    if pd.isna(text):
        return None
    match = re.search(r'PC(\d+)', str(text))
    if match:
        return f'PC{match.group(1)}'
    return None

def cargar_mapeo_vehiculos(archivo_mapeo):
    """
    Carga el mapeo de vehículos desde el archivo Excel
    """
    try:
        df_mapeo = pd.read_excel(archivo_mapeo, sheet_name=1)
        df_mapeo.columns = [normalizar_texto(col) for col in df_mapeo.columns]
        
        # Actualizar nombres de columnas para manejar ambos idiomas
        columnas_entrada = ['vehiculo entrada (norun)', 'vehicle input (norun)']
        columnas_salida = ['vehiculos salida (peru)', 'vehicle output (peru)']
        
        # Buscar la columna correcta
        col_entrada = next((col for col in columnas_entrada if col in df_mapeo.columns), None)
        col_salida = next((col for col in columnas_salida if col in df_mapeo.columns), None)
        
        if not col_entrada or not col_salida:
            raise ValueError("No se encontraron las columnas de mapeo necesarias")
            
        df_mapeo[col_entrada] = df_mapeo[col_entrada].apply(normalizar_texto)
        return dict(zip(df_mapeo[col_entrada], df_mapeo[col_salida]))
        
    except Exception as e:
        print(f"Error cargando mapeo de vehículos: {str(e)}")
        raise

def cargar_mapeo_calles(archivo_mapeo):
    """
    Carga el mapeo de puntos de control a nombres de calles
    """
    try:
        df_mapeo = pd.read_excel(archivo_mapeo, sheet_name=0)
        df_mapeo.columns = [normalizar_texto(col) for col in df_mapeo.columns]
        columnas_esperadas = ['punto norun', 'nombre para cliente']
        for col in columnas_esperadas:
            if col not in df_mapeo.columns:
                raise ValueError(f"Columna requerida '{col}' no encontrada en el archivo de mapeo")
        return dict(zip(df_mapeo['punto norun'], df_mapeo['nombre para cliente']))
    except Exception as e:
        print(f"Error cargando mapeo de calles: {str(e)}")
        raise

def get_suma_vehiculos(row, vehiculos_salida):
    """
    Retorna la suma de todos los vehículos en una fila
    """
    return sum(row[col] for col in vehiculos_salida if col in row)

def procesar_datos(df_combined):
    """
    Procesa los datos combinados para generar el formato final
    """
    try:
        # Cargar mapeos desde el archivo de configuración
        archivo_config = 'Plantilla/plantilla_peru.xlsx'
        mapeo_vehiculos = cargar_mapeo_vehiculos(archivo_config)
        mapeo_calles = cargar_mapeo_calles(archivo_config)
        
        print("\nMapeo de vehículos cargado:")
        for k, v in mapeo_vehiculos.items():
            print(f"{k} -> {v}")
        
        # Crear una copia del DataFrame original para debug
        df_debug = df_combined.copy()
        
        # Aplicar transformaciones
        df_combined['PC'] = df_combined['FUENTE DE DATOS'].apply(extract_pc_number)
        df_combined['INTERSECCION'] = df_combined['PC'].map(mapeo_calles)
        
        # Normalizar y crear columnas de vehículos
        columnas_vehiculos = [col for col in df_combined.columns 
                            if normalizar_texto(col) in mapeo_vehiculos.keys()]
        
        print("\nColumnas de vehículos encontradas:")
        print(columnas_vehiculos)
        
        vehiculos_salida = list(dict.fromkeys(mapeo_vehiculos.values()))
        print("\nVehículos de salida únicos:")
        print(vehiculos_salida)
        
        # Crear columnas de vehículos de salida
        for vehiculo_salida in vehiculos_salida:
            columnas_entrada = [col for col in columnas_vehiculos 
                              if mapeo_vehiculos[normalizar_texto(col)] == vehiculo_salida]
            
            print(f"\nProcesando {vehiculo_salida}:")
            print(f"Columnas entrada: {columnas_entrada}")
            
            if columnas_entrada:
                # Guardar valores antes de la suma para debug
                valores_antes = {col: df_combined[col].sum() for col in columnas_entrada}
                
                # Realizar la suma
                df_combined[vehiculo_salida] = df_combined[columnas_entrada].fillna(0).sum(axis=1)
                
                # Verificar resultado
                print(f"Total antes de la suma por columna: {valores_antes}")
                print(f"Total después de la suma: {df_combined[vehiculo_salida].sum()}")
        
        # Procesar fechas y horas
        df_combined['start_date'] = pd.to_datetime(
            df_combined['INTERVALO'].str.split(' - ').str[0],
            format="%m/%d/%Y %H:%M:%S"
        )
        
        # Verificar fechas procesadas
        print("\nRango de fechas:")
        print(f"Fecha mínima: {df_combined['start_date'].min()}")
        print(f"Fecha máxima: {df_combined['start_date'].max()}")
        
        df_combined['end_date'] = pd.to_datetime(
            df_combined['INTERVALO'].str.split(' - ').str[1],
            format="%m/%d/%Y %H:%M:%S"
        )
        
        # Crear columnas de fecha y hora
        df_combined['FECHA'] = df_combined['start_date'].dt.strftime('%d-%m-%Y')
        df_combined['HORA INICIO'] = df_combined['start_date'].dt.strftime('%H:%M:%S')
        df_combined['HORA TERMINO'] = df_combined['end_date'].dt.strftime('%H:%M:%S')
        
        # Calcular cuartos de hora
        df_combined['hour'] = df_combined['start_date'].dt.hour
        df_combined['minute'] = df_combined['start_date'].dt.minute
        df_combined['CUARTO'] = df_combined['hour'].astype(str) + ',' + (df_combined['minute'] // 15 + 1).astype(str)
        
        # Limpiar columnas temporales
        df_combined = df_combined.drop(columns=['hour', 'minute', 'start_date', 'end_date'])
        
        # Antes de seleccionar las columnas finales, verificar duplicados
        columnas_identificadoras = ['PC', 'FECHA', 'HORA INICIO', 'HORA TERMINO', 'MOVIMIENTO', 'CUARTO']
        duplicados = df_combined.duplicated(subset=columnas_identificadoras, keep=False)
        
        if duplicados.any():
            print("\nSe encontraron filas duplicadas:")
            filas_duplicadas = df_combined[duplicados].sort_values(by=columnas_identificadoras)
            print(f"Total de filas duplicadas: {len(filas_duplicadas)}")
            
            # Procesar duplicados grupo por grupo
            grupos_duplicados = df_combined[duplicados].groupby(columnas_identificadoras)
            indices_a_mantener = []
            
            for _, grupo in grupos_duplicados:
                # Si el grupo tiene más de una fila
                if len(grupo) > 1:
                    # Calcular suma de vehículos para cada fila
                    sumas = grupo.apply(lambda row: get_suma_vehiculos(row, vehiculos_salida), axis=1)
                    
                    if (sumas == 0).any() and (sumas > 0).any():
                        # Si hay filas con ceros y otras con valores, mantener la que tiene valores
                        idx_mantener = sumas[sumas > 0].index[0]
                    else:
                        # Si todas tienen valores o todas son ceros, mantener la última
                        idx_mantener = grupo.index[-1]
                    
                    indices_a_mantener.append(idx_mantener)
            
            # Crear máscara para filas a mantener
            mascara = ~duplicados | df_combined.index.isin(indices_a_mantener)
            df_combined = df_combined[mascara]
            
            print(f"Filas después de eliminar duplicados: {len(df_combined)}")
        
        # Seleccionar columnas finales
        columnas_BBDD = ['PC', 'INTERSECCION', 'FECHA', 'HORA INICIO', 'HORA TERMINO', 
                        'MOVIMIENTO', 'CUARTO'] + vehiculos_salida
        
        df_BBDD = df_combined[columnas_BBDD].copy()
        
        # Convertir FECHA a datetime para ordenar cronológicamente
        df_BBDD['_fecha_orden'] = pd.to_datetime(df_BBDD['FECHA'], format='%d-%m-%Y')
        
        # Ordenar el DataFrame
        df_BBDD = df_BBDD.sort_values(
            by=['_fecha_orden', 'PC', 'MOVIMIENTO', 'HORA INICIO']
        )
        
        # Eliminar columna temporal de ordenamiento
        df_BBDD = df_BBDD.drop(columns=['_fecha_orden'])
        
        return df_BBDD
        
    except Exception as e:
        print(f"Error en procesar_datos: {str(e)}")
        return None

def main():
    print("\n" + "="*50)
    print("Iniciando homologación de datos para Perú")
    print("="*50 + "\n")
    
    # Crear carpeta de salida
    carpeta_salida = 'data_peru_final'
    os.makedirs(carpeta_salida, exist_ok=True)
    
    try:
        # Cargar archivos
        print("Cargando archivos...")
        carpeta_merged = 'data_merged_cl_fi'
        dfs = []
        archivos = sorted(glob.glob(os.path.join(carpeta_merged, '*_completo.xlsx')))
        
        print("\nOrden de procesamiento de archivos:")
        for archivo in archivos:
            print(f"Procesando: {os.path.basename(archivo)}")
            try:
                df = pd.read_excel(archivo)
                if not df.empty:
                    # Agregar una columna para mantener el orden
                    df['_orden_original'] = len(dfs)
                    dfs.append(df)
                    print(f"Archivo cargado: {len(df)} filas")
            except Exception as e:
                print(f"Error leyendo archivo {archivo}: {str(e)}")
                continue
        
        if not dfs:
            print("No hay datos válidos para procesar")
            return
            
        # Combinar manteniendo el orden
        df_combinado = pd.concat(dfs, ignore_index=True)
        df_combinado = df_combinado.sort_values('_orden_original')
        df_combinado = df_combinado.drop(columns=['_orden_original'])
        
        print("Columnas del DataFrame combinado:")
        print(df_combinado.columns)
        
        # Procesar datos
        print("Procesando datos...")
        df_resultado = procesar_datos(df_combinado)
        
        if df_resultado is not None and not df_resultado.empty:  # Verificar resultado
            # Guardar resultado
            archivo_salida = os.path.join(carpeta_salida, 'reporte_final_peru.xlsx')
            df_resultado.to_excel(archivo_salida, index=False)
            print(f"\nArchivo guardado en: {archivo_salida}")
        else:
            print("No se generaron datos para guardar")
        
    except Exception as e:
        print(f"Error en el procesamiento: {str(e)}")
        raise
    
    print("\n" + "="*50)
    print("Proceso finalizado exitosamente")
    print("="*50)

if __name__ == "__main__":
    main()