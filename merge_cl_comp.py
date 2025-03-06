import pandas as pd
import os
from datetime import datetime, timedelta
import numpy as np
import glob
import re
import warnings
import unicodedata

warnings.simplefilter(action='ignore', category=Warning)

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
        'movement': 'movimiento'
    }
    
    # Normalizar texto (eliminar tildes y convertir a minúsculas)
    texto_normalizado = ''.join(
        c for c in unicodedata.normalize('NFD', str(texto))
        if unicodedata.category(c) != 'Mn'
    ).lower().strip()
    
    # Aplicar equivalencias si existe
    return equivalencias.get(texto_normalizado, texto_normalizado)

def normalizar_columnas(df):
    """
    Normaliza los nombres de las columnas del DataFrame
    """
    # Crear un diccionario de mapeo para las columnas
    columnas_normalizadas = {
        col: normalizar_texto(col)
        for col in df.columns
    }
    
    # Renombrar las columnas
    return df.rename(columns=columnas_normalizadas)

def merge_vehicle_data(archivo_base, archivos_complementarios):
    """
    Realiza el merge entre el archivo base y los complementarios
    """
    def log_mensaje(mensaje, critical=False):
        if critical:
            print(mensaje)

    log_mensaje(f"\nIniciando procesamiento de archivo base: {os.path.basename(archivo_base)}", critical=True)
    
    try:
        df_resultado = pd.read_excel(archivo_base, sheet_name=1, skiprows=1)
        log_mensaje(f"Archivo base cargado exitosamente. Filas: {len(df_resultado)}", critical=True)

        # Normalizar columnas
        df_resultado = normalizar_columnas(df_resultado)
        
        # Debug: Imprimir valores únicos de FUENTE DE DATOS antes de procesar
        print("\nValores únicos en FUENTE DE DATOS antes de procesar:")
        print(df_resultado['fuente de datos'].unique())

        # Extraer fecha y nombre del día del campo LOCALIZACIÓN
        fecha_str = df_resultado['localizacion'].iloc[0]
        
        # Intentar diferentes formatos de fecha
        fecha = None
        nombre_dia = None
        
        # Intento 1: Formato DD.MM.YYYY o DD.MM
        match_fecha = re.search(r'(\d{2}\.\d{2}(?:\.\d{4})?)', fecha_str)
        if match_fecha:
            fecha = match_fecha.group(1).replace('.', '-')[:5]  # Tomar solo DD-MM
            # Buscar el nombre del día después de la fecha
            match_dia = re.search(fr'{match_fecha.group(1)}\s*(.*?)(?:\s+|$)', fecha_str)
            nombre_dia = match_dia.group(1).strip() if match_dia else 'dia'
        
        # Intento 2: Formato "Dia X - Tipo"
        if fecha is None:
            match_dia = re.search(r'Dia (\d+)\s*-\s*(.*?)(?:\s+|$)', fecha_str)
            if match_dia:
                numero_dia = match_dia.group(1)
                nombre_dia = match_dia.group(2).strip()
                # Usar fecha actual para el formato DD-MM
                fecha_actual = datetime.now()
                fecha = fecha_actual.strftime("%d-%m")
        
        # Si no se encontró ningún formato válido
        if fecha is None:
            raise ValueError(f"No se pudo extraer la fecha de LOCALIZACIÓN: {fecha_str}")
        
        if nombre_dia is None:
            nombre_dia = 'dia'  # valor por defecto
            
        log_mensaje(f"Fecha extraída: {fecha}", critical=True)
        log_mensaje(f"Nombre del día: {nombre_dia}", critical=True)

        # Columnas identificadoras para el merge
        columnas_identificadoras = ['fuente de datos', 'intervalo', 'movimiento']
        
        # Obtener columnas de vehículos
        columnas_base = df_resultado.columns.tolist()
        columnas_vehiculos = [col for col in columnas_base 
                            if col not in columnas_identificadoras 
                            and col not in ['proyecto', 'localizacion', 'geolocalizacion']]
        
        # Normalizar valores pero mantener la información completa del PC
        df_resultado['fuente de datos'] = df_resultado['fuente de datos'].astype(str).str.strip()
        df_resultado['intervalo'] = df_resultado['intervalo'].astype(str).str.strip()
        df_resultado['movimiento'] = df_resultado['movimiento'].astype(str).str.strip()
        
        for archivo_comp in archivos_complementarios:
            df2 = pd.read_excel(archivo_comp, sheet_name=1, skiprows=1)
            df2 = normalizar_columnas(df2)
            df2['fuente de datos'] = df2['fuente de datos'].astype(str).str.strip()
            df2['intervalo'] = df2['intervalo'].astype(str).str.strip()
            df2['movimiento'] = df2['movimiento'].astype(str).str.strip()
            
            # Debug: Imprimir valores únicos de FUENTE DE DATOS en archivo complementario
            print(f"\nValores únicos en FUENTE DE DATOS del archivo complementario {os.path.basename(archivo_comp)}:")
            print(df2['fuente de datos'].unique())
            
            actualizaciones_totales = 0
            coincidencias_totales = 0
            try:
                for idx, fila in df_resultado.iterrows():
                    filas_coincidentes = df2[
                        (df2['fuente de datos'] == fila['fuente de datos']) &
                        (df2['intervalo'] == fila['intervalo']) &
                        (df2['movimiento'].astype(str) == str(fila['movimiento']))
                    ]
                    
                    if not filas_coincidentes.empty:
                        coincidencias_totales += 1
                        actualizaciones = 0
                        
                        for col in columnas_vehiculos:
                            if pd.isna(df_resultado.at[idx, col]) or df_resultado.at[idx, col] == 0:
                                col_correspondiente = col.strip()
                                
                                if col_correspondiente in df2.columns:
                                    valor_nuevo = filas_coincidentes.iloc[0][col_correspondiente]
                                    if not pd.isna(valor_nuevo) and valor_nuevo != 0:
                                        df_resultado.at[idx, col] = valor_nuevo
                                        actualizaciones += 1
                        
                        if actualizaciones > 0:
                            actualizaciones_totales += 1
                
                log_mensaje(f"\nArchivo complementario {os.path.basename(archivo_comp)}:", critical=True)
                log_mensaje(f"   - Coincidencias encontradas: {coincidencias_totales}", critical=True)
                log_mensaje(f"   - Filas actualizadas: {actualizaciones_totales}", critical=True)
                log_mensaje(f"   - Coincidencias sin actualización: {coincidencias_totales - actualizaciones_totales}", critical=True)
            
            except Exception as e:
                log_mensaje(f"Error procesando archivo {os.path.basename(archivo_comp)}: {str(e)}", critical=True)
                continue
        
        # Debug: Verificar valores finales para PC1
        print("\nVerificando valores finales para PC1:")
        pc1_data = df_resultado[df_resultado['fuente de datos'].str.contains('PC1', na=False)]
        print(f"Filas con PC1: {len(pc1_data)}")
        print("Movimientos únicos en PC1:")
        print(pc1_data['movimiento'].unique())
        
        return df_resultado, (nombre_dia, fecha)
        
    except Exception as e:
        log_mensaje(f"Error cargando archivo base: {str(e)}", critical=True)
        raise

def obtener_numero_dia(nombre_carpeta):
    """
    Extrae el número del día del nombre de la carpeta
    Solo considera el número inicial
    """
    match = re.match(r'(\d+)', nombre_carpeta)
    return int(match.group(1)) if match else None

def main():
    print("\n" + "="*50)
    print("Iniciando procesamiento de datos de Chile")
    print("="*50 + "\n")
    
    # Definir carpetas
    carpeta_base = 'Multisource Categoría Chile'
    carpeta_salida = 'data_chile'
    os.makedirs(carpeta_salida, exist_ok=True)
    
    # Obtener archivos complementarios
    carpeta_complementarios = os.path.join(carpeta_base, 'Complementarios')
    archivos_complementarios = []
    if os.path.exists(carpeta_complementarios):
        archivos_complementarios = glob.glob(os.path.join(carpeta_complementarios, '*.xlsx'))
        
    print(f"Se encontraron {len(archivos_complementarios)} archivos complementarios")
    for archivo in archivos_complementarios:
        print(f"Archivo complementario: {archivo}")
    
    # Procesar cada carpeta numerada
    carpetas_dias = []
    for item in os.listdir(carpeta_base):
        ruta_completa = os.path.join(carpeta_base, item)
        if os.path.isdir(ruta_completa) and item != 'Complementarios':
            if obtener_numero_dia(item) is not None:
                carpetas_dias.append((obtener_numero_dia(item), ruta_completa))
    
    # Ordenar carpetas por número
    carpetas_dias.sort(key=lambda x: x[0])
    
    # Procesar cada carpeta
    for numero_dia, carpeta_dia in carpetas_dias:
        archivos_principales = glob.glob(os.path.join(carpeta_dia, '*.xlsx'))
        
        for archivo_principal in archivos_principales:
            try:
                print(f"Procesando día {numero_dia}...")
                resultado = merge_vehicle_data(
                    archivo_principal,
                    archivos_complementarios
                )
                
                if isinstance(resultado, tuple):
                    df_merged, (nombre_dia, fecha) = resultado
                    
                    # Guardar resultado usando el mismo formato que interp_tricycles.py
                    nombre_salida = f'{numero_dia}.{nombre_dia}_{fecha}_chile.xlsx'
                    archivo_salida = os.path.join(carpeta_salida, nombre_salida)
                    
                    try:
                        if not df_merged.empty:
                            df_merged.to_excel(archivo_salida, index=False)
                            print(f"Archivo guardado: {os.path.basename(archivo_salida)}\n")
                        else:
                            print(f"Error: DataFrame vacío para {archivo_principal}")
                    except Exception as e:
                        print(f"Error guardando archivo {archivo_salida}: {str(e)}")
                
            except Exception as e:
                print(f"Error en día {numero_dia}: {str(e)}\n")
                continue

if __name__ == "__main__":
    main()