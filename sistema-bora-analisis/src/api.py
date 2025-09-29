import os
import json
import ftplib
from pathlib import Path
from flask import Flask, jsonify, request
from flask_cors import CORS
import google.generativeai as genai
import tempfile

# --- Inicialización y Configuración ---
app = Flask(__name__)
CORS(app)  # Permite que el dashboard web se conecte a esta API

# Configurar Gemini (necesario para el endpoint de patrones)
try:
    genai.configure(api_key=os.getenv('GEMINI_API_KEY'))
    model = genai.GenerativeModel('gemini-1.5-flash')
    print("✓ API: Conexión con Gemini configurada.")
except Exception as e:
    print(f"ADVERTENCIA API: No se pudo configurar Gemini. El endpoint de patrones fallará. Error: {e}")

# Configuración FTP (la leemos una vez)
FTP_HOST = "ftp.agoraenlared.com"
FTP_USER = "u112219758.boria"
FTP_PASS = os.getenv('HOSTINGER_FTP_PASSWORD', "Marta1664?")


# --- Funciones de Ayuda para FTP ---

def list_hostinger_files(remote_dir):
    """Lista archivos en un directorio de Hostinger."""
    try:
        ftp = ftplib.FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(f'data/{remote_dir}')
        files = ftp.nlst()
        ftp.quit()
        return [f for f in files if f not in ('.', '..')]
    except Exception as e:
        print(f"API Error (list_hostinger_files): {e}")
        return []

def download_from_hostinger(remote_path):
    """Descarga un archivo desde Hostinger a una carpeta temporal."""
    try:
        temp_dir = tempfile.gettempdir()
        local_filepath = os.path.join(temp_dir, Path(remote_path).name)
        
        ftp = ftplib.FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)
        
        full_remote_path = f'data/{remote_path}'
        with open(local_filepath, 'wb') as f:
            ftp.retrbinary(f'RETR {full_remote_path}', f.write)
        
        ftp.quit()
        return local_filepath
    except Exception as e:
        print(f"API Error (download_from_hostinger): {e}")
        return None

# --- Endpoints de la API ---

@app.route('/', methods=['GET'])
def home():
    """Endpoint de verificación para saber si la API está funcionando."""
    return jsonify({
        'status': 'ok',
        'service': 'BORA Analysis API',
        'endpoints_disponibles': ['/api/stats', '/api/medidas', '/api/patrones']
    })

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Devuelve estadísticas básicas del almacenamiento."""
    try:
        raw_files = list_hostinger_files('raw')
        analyzed_files = list_hostinger_files('analyzed')
        
        return jsonify({
            'total_medidas_raw': len(raw_files),
            'total_medidas_analizadas': len(analyzed_files),
            'medidas_pendientes_analisis': len(raw_files) - len(analyzed_files)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/medidas', methods=['GET'])
def get_medidas():
    """Devuelve una lista de las últimas medidas analizadas."""
    try:
        # El parámetro 'limit' permite que el dashboard pida, por ej., solo las 50 más nuevas
        limit = int(request.args.get('limit', 50))
        
        files = list_hostinger_files('analyzed')
        # Ordenamos para devolver las más recientes primero (asumiendo nombres de archivo secuenciales)
        files.sort(reverse=True)
        files_to_fetch = files[:limit]
        
        medidas = []
        for filename in files_to_fetch:
            if filename.endswith(".json"): # Ignorar otros posibles archivos
                local_path = download_from_hostinger(f'analyzed/{filename}')
                if local_path:
                    with open(local_path, 'r', encoding='utf-8') as f:
                        medidas.append(json.load(f))
                    os.remove(local_path) # Limpiar el temporal

        return jsonify({'total_encontrado': len(medidas), 'medidas': medidas})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Este endpoint es más avanzado y lo usaremos en el futuro.
# Inicia un análisis de patrones a demanda.
@app.route('/api/patrones', methods=['POST'])
def detectar_patrones():
    # Por ahora, devolvemos una respuesta de ejemplo
    # En el futuro, este endpoint ejecutará la lógica de IA para detectar patrones
    return jsonify({
        "status": "endpoint_en_desarrollo",
        "descripcion": "Este endpoint activará el análisis de patrones con IA en el futuro."
    })

# --- Ejecución del Servidor ---
if __name__ == '__main__':
    # Render usa la variable de entorno PORT para saber en qué puerto ejecutar la app
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)