import pandas as pd
import os
from datetime import datetime, timedelta
import re
import glob
import numpy as np
import warnings
import unicodedata

warnings.simplefilter(action='ignore', category=Warning)

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

def normalizar_columnas(df):
    """
    Normaliza los nombres de las columnas del DataFrame
    """
    columnas_normalizadas = {
        col: normalizar_texto(col)
        for col in df.columns
    }
    return df.rename(columns=columnas_normalizadas)

def interpolar_datos_tricycle(df):
    """
    Interpola los datos faltantes en todas las columnas de vehículos
    """
    # Identificar columnas de vehículos (todas excepto las de identificación)
    columnas_no_vehiculos = ['PROYECTO', 'LOCALIZACIÓN', 'FUENTE DE DATOS', 
                            'GEOLOCALIZACIÓN', 'INTERVALO', 'MOVIMIENTO']
    columnas_vehiculos = [col for col in df.columns if col not in columnas_no_vehiculos]
    
    # Agrupar por FUENTE DE DATOS y MOVIMIENTO para interpolar dentro de cada grupo
    grupos = df.groupby(['FUENTE DE DATOS', 'MOVIMIENTO'])
    
    df_resultado = pd.DataFrame()
    for nombre_grupo, grupo in grupos:
        # Ordenar por intervalo
        grupo = grupo.sort_values('INTERVALO')
        
        # Procesar cada columna de vehículos
        for columna in columnas_vehiculos:
            # Convertir a numérico y manejar valores faltantes
            grupo[columna] = pd.to_numeric(grupo[columna], errors='coerce')
            
            if columna == 'TRICYCLE':
                # Para TRICYCLE, simplemente rellenar con 0
                grupo[columna] = grupo[columna].fillna(0)
            else:
                # Para otras columnas, usar interpolación
                grupo[columna] = grupo[columna].interpolate(method='linear', limit_direction='both')
                grupo[columna] = grupo[columna].fillna(method='ffill').fillna(method='bfill')
            
            # Redondear a enteros y asegurar que no haya negativos
            grupo[columna] = grupo[columna].round().clip(lower=0).astype('Int64')
        
        df_resultado = pd.concat([df_resultado, grupo])
    
    # Eliminar duplicados
    df_resultado = df_resultado.drop_duplicates()
    
    return df_resultado

def normalizar_valor_pc(valor):
    """
    Normaliza los valores de PC extrayendo todo lo que está antes del guión
    Ejemplo: 'PC1A3B-A2-722' -> 'PC1A3B', 'PC6B1-B1-461' -> 'PC6B1'
    """
    if pd.isna(valor):
        return valor
    # Eliminar espacios extra
    valor = str(valor).strip()
    # Extraer todo lo que está antes del primer guión
    parts = valor.split('-', 1)
    return parts[0].strip()

def merge_archivos_dia(archivo_chile, archivo_filipinas, carpeta_salida):
    try:
        # Leer archivos
        df_chile = pd.read_excel(archivo_chile)
        df_filipinas = pd.read_excel(archivo_filipinas)
        
        # Convertir todas las columnas a minúsculas para normalizar
        df_chile.columns = df_chile.columns.str.lower()
        df_filipinas.columns = df_filipinas.columns.str.lower()
        
        # Mapeo de columnas esperadas
        columnas_merge = {
            'fuente de datos': ['fuente de datos', 'data source'],
            'intervalo': ['intervalo', 'interval'],
            'movimiento': ['movimiento', 'movement']
        }
        
        # Normalizar nombres de columnas en ambos DataFrames
        for df in [df_chile, df_filipinas]:
            for col_esp, cols_alt in columnas_merge.items():
                col_actual = next((col for col in cols_alt if col in df.columns), None)
                if col_actual:
                    if col_actual != col_esp:
                        df[col_esp] = df[col_actual]
                        df.drop(columns=[col_actual], inplace=True)
                    
                    # Normalizar valores
                    if col_esp == 'fuente de datos':
                        df[col_esp] = df[col_esp].apply(normalizar_valor_pc)
                    elif col_esp in ['movimiento', 'intervalo']:
                        df[col_esp] = df[col_esp].astype(str).str.strip()

        # Verificar los valores únicos antes del merge
        print("\nValores únicos en fuente de datos (Chile):", df_chile['fuente de datos'].unique())
        print("Valores únicos en fuente de datos (Filipinas):", df_filipinas['fuente de datos'].unique())
        print("\nValores únicos en movimiento (Chile):", df_chile['movimiento'].unique())
        print("Valores únicos en movimiento (Filipinas):", df_filipinas['movimiento'].unique())
        
        # Realizar merge
        try:
            df_merged = pd.merge(
                df_chile,
                df_filipinas[['fuente de datos', 'intervalo', 'movimiento', 'tricycle']],
                on=['fuente de datos', 'intervalo', 'movimiento'],
                how='left'
            )
            
            # Verificar resultados del merge
            print(f"\nTotal de registros en Chile: {len(df_chile)}")
            print(f"Total de registros en Filipinas: {len(df_filipinas)}")
            print(f"Total de registros después del merge: {len(df_merged)}")
            print(f"Registros con TRICYCLE > 0: {len(df_merged[df_merged['tricycle'] > 0])}")
            
            if df_merged is not None and not df_merged.empty:
                # Asegurar que la columna TRICYCLE esté presente y con valores válidos
                df_merged['tricycle'] = pd.to_numeric(df_merged['tricycle'], errors='coerce').fillna(0)
                
                # Convertir columnas a mayúsculas para mantener consistencia
                df_merged.columns = df_merged.columns.str.upper()
                
                # Guardar archivo
                archivo_salida = os.path.join(carpeta_salida, f"{os.path.splitext(os.path.basename(archivo_chile))[0]}_completo.xlsx")
                df_merged.to_excel(archivo_salida, index=False)
                print(f"Merge exitoso: {os.path.basename(archivo_salida)}")
                
                return df_merged
            else:
                print("El merge resultó en un DataFrame vacío")
                return None
            
        except Exception as e:
            print(f"Error en el merge inicial: {str(e)}")
            return None

    except Exception as e:
        print(f"Error en merge_archivos_dia: {str(e)}")
        return None

def main():
    print("\n" + "="*50)
    print("Iniciando proceso de merge final Chile-Filipinas")
    print("="*50 + "\n")
    
    # Crear carpeta de salida
    carpeta_salida = 'data_merged_cl_fi'
    os.makedirs(carpeta_salida, exist_ok=True)
    
    # Obtener archivos de Chile
    carpeta_chile = 'data_chile'
    archivos_chile = {}
    for archivo in os.listdir(carpeta_chile):
        if archivo.endswith('_chile.xlsx'):
            match = re.search(r'(\d+)\.', archivo)
            if match:
                numero_dia = match.group(1)
                archivos_chile[numero_dia] = os.path.join(carpeta_chile, archivo)
    
    if not archivos_chile:
        print("No se encontraron archivos de Chile para procesar")
        return
        
    print(f"Archivos de Chile encontrados: {len(archivos_chile)}")
    
    # Verificar si existen archivos de Filipinas
    carpeta_filipinas = 'data_filipinas'
    archivos_filipinas = {}
    if os.path.exists(carpeta_filipinas):
        for archivo in os.listdir(carpeta_filipinas):
            if archivo.endswith('_filipinas.xlsx'):
                match = re.search(r'(\d+)\.', archivo)
                if match:
                    numero_dia = match.group(1)
                    archivos_filipinas[numero_dia] = os.path.join(carpeta_filipinas, archivo)
        print(f"Archivos de Filipinas encontrados: {len(archivos_filipinas)}\n")
    else:
        print("No se encontraron archivos de Filipinas. Continuando solo con datos de Chile.\n")
    
    # Realizar merge para cada día
    for numero_dia in archivos_chile.keys():
        print(f"Procesando día {numero_dia}...")
        
        if numero_dia in archivos_filipinas:
            # Si hay archivo de Filipinas, hacer merge normal
            df_merged = merge_archivos_dia(
                archivos_chile[numero_dia],
                archivos_filipinas[numero_dia],
                carpeta_salida
            )
        else:
            # Si no hay archivo de Filipinas, procesar solo Chile
            try:
                df_chile = pd.read_excel(archivos_chile[numero_dia])
                df_chile.columns = df_chile.columns.str.upper()
                
                # Agregar columna TRICYCLE con ceros
                df_chile['TRICYCLE'] = 0
                
                # Guardar archivo
                archivo_salida = os.path.join(carpeta_salida, f"{os.path.splitext(os.path.basename(archivos_chile[numero_dia]))[0]}_completo.xlsx")
                df_chile.to_excel(archivo_salida, index=False)
                print(f"Archivo procesado sin datos de Filipinas: {os.path.basename(archivo_salida)}")
                df_merged = df_chile
            except Exception as e:
                print(f"Error procesando archivo de Chile: {str(e)}")
                continue
        
        if df_merged is not None and not df_merged.empty:
            print(f"Procesamiento del día {numero_dia} completado exitosamente\n")
        else:
            print(f"Error en el procesamiento del día {numero_dia}\n")
    
    print("="*50)
    print("Proceso de merge finalizado")
    print("="*50)

if __name__ == "__main__":
    main()