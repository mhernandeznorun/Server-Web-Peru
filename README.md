# Sistema Web de Procesamiento de Datos de Tráfico para Perú

Este sistema web procesa datos de tráfico vehicular y peatonal, combinando y homologando datos para generar reportes en formato Perú. El sistema maneja dos tipos de procesamiento independientes:
1. Procesamiento vehicular (Filipinas y Chile)
2. Procesamiento peatonal

## Requisitos Previos

### Software Necesario
- Python 3.8 o superior
- Navegador web moderno

### Librerías Python Requeridas
```bash
pip install flask pandas numpy scipy openpyxl werkzeug
```

## Estructura del Proyecto

```
proyecto/
│
├── app.py                        # Servidor web Flask
├── templates/            
│   └── index.html               # Interfaz web
├── uploads/                     # Carpeta para archivos subidos
├── interp_tricycles.py          # Procesamiento de datos Filipinas
├── merge_cl_comp.py             # Procesamiento de datos Chile
├── merge_cl_ph.py               # Fusión Chile-Filipinas
├── homologate.py                # Homologación final vehicular
└── homologate_pedestrians.py    # Procesamiento de datos peatonales
```

## Ejecución del Servidor

1. Instalar dependencias:
```bash
pip install -r requirements.txt
```

2. Ejecutar el servidor:
```bash
python app.py
```

3. Acceder a la aplicación:
   - Localmente: `http://localhost:5000`
   - Desde otra computadora en la red: `http://IP_DEL_SERVIDOR:5000`

## Uso de la Aplicación

### Procesamiento Vehicular

1. **Subida de Archivos**
   - Archivos de Filipinas (opcional)
   - Archivos de Chile (requerido)
   - Archivos Complementarios (opcional)
   - Archivo Plantilla (requerido)

2. **Formato de Archivos**
   - Todos los archivos deben estar en formato Excel (.xlsx)
   - Los archivos principales deben contener los datos en la segunda hoja
   - La plantilla debe contener las hojas de mapeo necesarias

3. **Configuración de Muestreo**
   - Muestreo cada hora (por defecto)
   - Muestreo cada 15 minutos
     - Configurable entre 1-15 minutos

### Procesamiento Peatonal

1. **Subida de Archivos**
   - Archivos de Peatones (requerido)
   - Archivo Plantilla (requerido)

2. **Formato de Archivos**
   - Archivos Excel (.xlsx)
   - Los datos deben estar en la segunda hoja
   - Soporta formatos de columnas en español e inglés

## Estructura de los Archivos de Entrada

### Archivo de Plantilla
El mismo archivo de plantilla se usa para ambos procesamientos y debe contener:
1. Mapeo de puntos de control (PC) a nombres de intersecciones
2. Configuraciones adicionales según el tipo de procesamiento

### Archivos de Datos
- **Vehicular**:
  - Filipinas: Datos de triciclos (opcional)
  - Chile: Datos principales
  - Complementarios: Datos adicionales (opcional)
- **Peatonal**:
  - Archivos con conteos peatonales
  - Soporta formato multilingüe (ESP/ENG)

## Procesos de Transformación

### Proceso Vehicular
1. Procesamiento de Filipinas (si existen)
   - Interpolación de datos
   - Ajuste de intervalos
2. Procesamiento de Chile
3. Fusión de datos
4. Homologación final

### Proceso Peatonal
1. Lectura y normalización de datos
2. Mapeo de puntos de control a intersecciones
3. Procesamiento de fechas y horarios
4. Generación de reporte final

## Solución de Problemas

### Errores Comunes

1. **Error al Subir Archivos**
   - Verificar formato .xlsx
   - Comprobar archivos requeridos según el tipo de proceso

2. **Error en el Procesamiento**
   - Verificar que los datos estén en la segunda hoja
   - Comprobar formato de columnas (soporta ESP/ENG)
   - Verificar que la plantilla contenga los mapeos necesarios

3. **Error de Acceso**
   - Verificar servidor activo
   - Comprobar conectividad

## Notas de Seguridad y Mantenimiento

- Aplicación diseñada para red local
- Procesamiento en sesiones independientes
- Limpieza periódica recomendada de la carpeta `uploads`
- Cada sesión tiene su propio directorio UUID

## Soporte

Para reportar problemas:
1. Verificar logs del servidor
2. Comprobar mensajes de error en navegador
3. Contactar al equipo de soporte con detalles del error

---

