from flask import Flask, render_template, request, send_file, jsonify
import os
from werkzeug.utils import secure_filename
import shutil
import uuid
from datetime import datetime
import re
import pandas as pd
import json
from pathlib import Path

app = Flask(__name__, static_folder='static')

# Configuración básica
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max-limit

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def create_processing_structure(session_id):
    """Crea la estructura de carpetas necesaria para el procesamiento"""
    base_path = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
    
    folders = [
        'Multisource Categoría Filipinas',
        'Multisource Categoría Chile',
        'Multisource Categoría Chile/Complementarios',
        'Plantilla',
        'Multisource Categoría Peatones'
    ]
    
    for folder in folders:
        os.makedirs(os.path.join(base_path, folder), exist_ok=True)
    
    return base_path

def detect_file_type(file):
    """Detecta el tipo de archivo basado en su contenido"""
    try:
        df = pd.read_excel(file)
        
        # Normalizar nombres de columnas
        df.columns = [str(col).lower().strip() for col in df.columns]
        
        # Detectar plantilla
        if 'punto norun' in df.columns and 'nombre para cliente' in df.columns:
            return 'plantilla'
        
        # Detectar si es un archivo de datos principal
        if 'fuente de datos' in df.columns and 'intervalo' in df.columns:
            # Buscar en el contenido para diferenciar entre Chile y Filipinas
            localizacion = df['localizacion'].iloc[0].lower() if 'localizacion' in df.columns else ''
            
            if 'tricycle' in df.columns or 'tricycles' in df.columns:
                return 'filipinas'
            else:
                return 'chile'
                
        # Detectar complementarios
        if 'adi total' in str(df.sheet_names).lower():
            return 'complementario'
            
        return None
    except Exception as e:
        print(f"Error detectando tipo de archivo: {str(e)}")
        return None

def save_file_in_structure(file, file_type, session_id, filename):
    """Guarda el archivo en la ubicación correcta según su tipo"""
    base_path = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
    
    if file_type == 'plantilla':
        save_path = os.path.join(base_path, 'Plantilla', 'plantilla_peru.xlsx')
    elif file_type == 'filipinas':
        save_path = os.path.join(base_path, 'Multisource Categoría Filipinas', '1', filename)
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
    elif file_type == 'chile':
        save_path = os.path.join(base_path, 'Multisource Categoría Chile', '1', filename)
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
    elif file_type == 'complementario':
        save_path = os.path.join(base_path, 'Multisource Categoría Chile', 'Complementarios', filename)
    else:
        raise ValueError(f"Tipo de archivo no válido: {file_type}")
    
    file.save(save_path)
    return save_path

def process_files(session_id):
    """Ejecuta el proceso completo de procesamiento"""
    base_path = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
    
    # Importar los módulos de procesamiento
    import interp_tricycles
    import merge_cl_comp
    import merge_cl_ph
    import homologate
    
    # Cambiar al directorio de trabajo
    original_dir = os.getcwd()
    os.chdir(base_path)
    
    try:
        # Ejecutar cada paso del proceso
        interp_tricycles.main()
        merge_cl_comp.main()
        merge_cl_ph.main()
        homologate.main()
        
        # Verificar que se generó el archivo final
        result_file = os.path.join('data_peru_final', 'reporte_final_peru.xlsx')
        if not os.path.exists(result_file):
            raise Exception("No se generó el archivo final")
            
        return result_file
        
    finally:
        # Restaurar el directorio original
        os.chdir(original_dir)

def process_peatones_files(session_id):
    """Ejecuta el proceso de archivos peatonales"""
    base_path = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
    
    # Importar el módulo de procesamiento
    import homologate_pedestrians
    
    # Cambiar al directorio de trabajo
    original_dir = os.getcwd()
    os.chdir(base_path)
    
    try:
        # Ejecutar el procesamiento
        homologate_pedestrians.main()
        
        # Verificar que se generó el archivo final
        result_file = os.path.join('data_peatones_final', 'reporte_final_peatones.xlsx')
        if not os.path.exists(result_file):
            raise Exception("No se generó el archivo final")
            
        return result_file
        
    finally:
        # Restaurar el directorio original
        os.chdir(original_dir)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    session_id = str(uuid.uuid4())
    base_path = create_processing_structure(session_id)
    
    try:
        # Obtener configuración de muestreo
        sampling_type = request.form.get('samplingType', 'CADA_HORA')
        sampling_minutes = request.form.get('samplingMinutes', '5')
        
        # Guardar configuración en un archivo
        config_path = os.path.join(base_path, 'config.json')
        with open(config_path, 'w') as f:
            json.dump({
                'TIPO_MUESTREO': sampling_type,
                'MINUTOS_MUESTREO': int(sampling_minutes)
            }, f)
        
        # Procesar archivo de plantilla (obligatorio)
        if 'plantilla' not in request.files:
            return jsonify({'error': 'Falta el archivo de plantilla'}), 400
        
        plantilla = request.files['plantilla']
        if plantilla:
            save_path = os.path.join(base_path, 'Plantilla', 'plantilla_peru.xlsx')
            plantilla.save(save_path)
        
        # Procesar archivos de Chile (obligatorio)
        if 'chile[]' not in request.files:
            return jsonify({'error': 'Faltan archivos de Chile'}), 400
        
        chile_files = request.files.getlist('chile[]')
        if not any(file.filename for file in chile_files):
            return jsonify({'error': 'Faltan archivos de Chile'}), 400
            
        for i, file in enumerate(chile_files, 1):
            if file and allowed_file(file.filename):
                save_path = os.path.join(base_path, 'Multisource Categoría Chile', str(i), secure_filename(file.filename))
                os.makedirs(os.path.dirname(save_path), exist_ok=True)
                file.save(save_path)
        
        # Procesar archivos de Filipinas (opcional)
        if 'filipinas[]' in request.files:
            filipinas_files = request.files.getlist('filipinas[]')
            for i, file in enumerate(filipinas_files, 1):
                if file and file.filename and allowed_file(file.filename):
                    save_path = os.path.join(base_path, 'Multisource Categoría Filipinas', str(i), secure_filename(file.filename))
                    os.makedirs(os.path.dirname(save_path), exist_ok=True)
                    file.save(save_path)
        
        # Procesar archivos complementarios (opcional)
        if 'complementarios[]' in request.files:
            complementarios = request.files.getlist('complementarios[]')
            for file in complementarios:
                if file and file.filename and allowed_file(file.filename):
                    save_path = os.path.join(base_path, 'Multisource Categoría Chile', 'Complementarios', secure_filename(file.filename))
                    file.save(save_path)
        
        # Iniciar procesamiento
        try:
            result_file = process_files(session_id)
            return jsonify({
                'success': True,
                'session_id': session_id,
                'result_file': result_file
            })
        except Exception as e:
            return jsonify({'error': f'Error en el procesamiento: {str(e)}'}), 500
            
    except Exception as e:
        return jsonify({'error': f'Error subiendo archivos: {str(e)}'}), 500

@app.route('/download/<session_id>')
def download_result(session_id):
    # Lógica para enviar el archivo procesado
    result_path = os.path.join(app.config['UPLOAD_FOLDER'], session_id, 'data_peru_final', 'reporte_final_peru.xlsx')
    return send_file(result_path, as_attachment=True)

@app.route('/upload_peatones', methods=['POST'])
def upload_peatones():
    session_id = str(uuid.uuid4())
    base_path = create_processing_structure(session_id)
    
    try:
        # Validar archivos requeridos
        if 'peatones[]' not in request.files:
            return jsonify({'error': 'Faltan archivos de peatones'}), 400
            
        if 'plantilla' not in request.files:
            return jsonify({'error': 'Falta el archivo de plantilla'}), 400
        
        # Guardar archivos
        peatones_files = request.files.getlist('peatones[]')
        for i, file in enumerate(peatones_files, 1):
            if file and file.filename and allowed_file(file.filename):
                save_path = os.path.join(base_path, 'Multisource Categoría Peatones', secure_filename(file.filename))
                os.makedirs(os.path.dirname(save_path), exist_ok=True)
                file.save(save_path)
        
        plantilla = request.files['plantilla']
        if plantilla:
            save_path = os.path.join(base_path, 'Plantilla', 'plantilla_peru.xlsx')
            plantilla.save(save_path)
        
        # Procesar archivos
        result_file = process_peatones_files(session_id)
        return jsonify({
            'success': True,
            'session_id': session_id,
            'result_file': result_file
        })
        
    except Exception as e:
        return jsonify({'error': f'Error en el procesamiento: {str(e)}'}), 500

@app.route('/downloadPeatones/<session_id>')
def download_peatones_result(session_id):
    # Lógica para enviar el archivo procesado de peatones
    result_path = os.path.join(app.config['UPLOAD_FOLDER'], session_id, 'data_peatones_final', 'reporte_final_peatones.xlsx')
    return send_file(result_path, as_attachment=True)

@app.route('/download_docs/<filename>')
def download_docs(filename):
    """Descarga documentación (README o requirements.txt)"""
    if filename not in ['README.md', 'requirements.txt']:
        return jsonify({'error': 'Archivo no encontrado'}), 404
    return send_file(filename, as_attachment=True)

@app.route('/download_template/<category>')
def download_template(category):
    """Descarga plantillas de ejemplo"""
    # Mapeo de categorías a rutas de archivos
    templates = {
        'chile': 'plantillas/chile/DIA 1 - MIERCOLES 29.01.xlsx',
        'complementarios': 'plantillas/complementarios/Copia de TMC_Multi-traffic_report_PC1A_2025-01-09 (7).xlsx',
        'filipinas': 'plantillas/filipinas/DIA 1 FILIPINAS.xlsx',
        'plantilla': 'plantillas/plantilla/plantilla_peru.xlsx',
        'peatones': 'plantillas/peatones/Peaton Plantilla Peru Intersecciones.xlsx'
    }
    
    if category not in templates:
        return jsonify({'error': 'Categoría no válida'}), 404
        
    template_path = templates[category]
    if not Path(template_path).exists():
        return jsonify({'error': 'Archivo de plantilla no encontrado'}), 404
        
    return send_file(template_path, as_attachment=True)

if __name__ == '__main__':
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    print("\nIniciando servidor...")
    app.run(host='0.0.0.0', port=5000, debug=False) 