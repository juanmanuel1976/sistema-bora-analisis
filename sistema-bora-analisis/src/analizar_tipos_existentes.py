import ftplib
import json
import pandas as pd
from collections import defaultdict
from datetime import datetime
import time
from bs4 import BeautifulSoup
import re
import os
import shutil

# --- CONFIGURACIÓN ---
FTP_HOST = 'ftp.agoraenlared.com'
FTP_USER = 'u112219758.boria'
FTP_PASS = 'Marta1664?'
REMOTE_DATA_DIR = 'data/raw'
BATCH_SIZE = 1000  # Procesar archivos en lotes de 1000

# --- NOMBRES DE ARCHIVOS DE CONTROL Y RESULTADOS ---
CHECKPOINT_FILE = 'checkpoint.json'
FINAL_CSV_RESULT = 'tipos_desde_h2.csv'
FINAL_JSON_EXAMPLES = 'ejemplos_h2_completos.json'
TEMP_BATCH_DIR = 'temp_json_batch'

def conectar_ftp():
    """Conecta al FTP y retorna el objeto de conexión."""
    print("Conectando a FTP...")
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, 21, timeout=60)
        ftp.login(FTP_USER, FTP_PASS)
        print("✓ Conexión FTP exitosa.")
        return ftp
    except Exception as e:
        print(f"✗ FATAL: No se pudo conectar al FTP. Error: {e}")
        return None

def extraer_tipo_desde_h2(html_titulo):
    """Extrae el tipo de medida desde una etiqueta <h2>."""
    if not html_titulo: return "SIN_TIPO"
    try:
        soup = BeautifulSoup(html_titulo, 'html.parser')
        h2 = soup.find('h2')
        if h2:
            texto = h2.get_text(strip=True)
            texto = re.sub(r'\d+/\d{4}', '', texto).strip()
            texto = re.sub(r'N°\s*\d+', '', texto).strip()
            return texto or "SIN_TIPO"
    except Exception:
        pass
    return "SIN_TIPO"

def main():
    """
    Proceso principal optimizado con lotes y puntos de control para garantizar fiabilidad.
    """
    start_time = time.time()
    print("="*70)
    print(f"INICIO DEL PROCESO OPTIMIZADO: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    # --- FASE 1: CARGAR ESTADO ANTERIOR (SI EXISTE) ---
    if os.path.exists(CHECKPOINT_FILE):
        print(f"🔄 Se encontró un punto de control. Reanudando proceso...")
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoint = json.load(f)
        processed_files = set(checkpoint['processed_files'])
        tipos = defaultdict(int, checkpoint['tipos'])
        con_pdf = defaultdict(int, checkpoint['con_pdf'])
        ejemplos = defaultdict(list, checkpoint['ejemplos'])
    else:
        print("✨ Iniciando un nuevo análisis desde cero.")
        processed_files = set()
        tipos = defaultdict(int)
        con_pdf = defaultdict(int)
        ejemplos = defaultdict(list)

    # --- FASE 2: OBTENER LISTA COMPLETA DE ARCHIVOS DEL SERVIDOR ---
    ftp = conectar_ftp()
    if not ftp: return

    try:
        print(f"Navegando al directorio remoto: {REMOTE_DATA_DIR}")
        ftp.cwd(REMOTE_DATA_DIR)
        print("✓ Directorio encontrado. Obteniendo lista de archivos...")
        all_files = [f for f in ftp.nlst() if f.endswith('.json')]
        ftp.quit() # Nos desconectamos. Solo nos conectaremos para descargar lotes.
        print(f"✓ Se encontraron {len(all_files)} archivos en total.")
    except Exception as e:
        print(f"✗ FATAL: No se pudo obtener la lista de archivos. Error: {e}")
        return

    # --- FASE 3: PROCESAR ARCHIVOS EN LOTES ---
    files_to_process = [f for f in all_files if f not in processed_files]
    
    if not files_to_process:
        print("🎉 ¡No hay archivos nuevos que procesar! El análisis ya estaba completo.")
    else:
        print(f"Quedan {len(files_to_process)} archivos por procesar.")

    for i in range(0, len(files_to_process), BATCH_SIZE):
        batch_files = files_to_process[i:i + BATCH_SIZE]
        batch_number = (i // BATCH_SIZE) + 1
        total_batches = -(-len(files_to_process) // BATCH_SIZE)

        print("\n" + "-"*70)
        print(f"PROCESANDO LOTE {batch_number}/{total_batches}")
        print("-" * 70)

        # Crear directorio temporal para el lote
        os.makedirs(TEMP_BATCH_DIR, exist_ok=True)

        # Descargar el lote completo
        print(f"Descargando {len(batch_files)} archivos del lote...")
        ftp_batch = conectar_ftp()
        if not ftp_batch: continue
        ftp_batch.cwd(REMOTE_DATA_DIR)
        for filename in batch_files:
            local_filepath = os.path.join(TEMP_BATCH_DIR, filename)
            with open(local_filepath, 'wb') as f:
                ftp_batch.retrbinary(f'RETR {filename}', f.write)
        ftp_batch.quit()
        print("✓ Lote descargado.")

        # Procesar el lote localmente (mucho más rápido)
        print("Analizando archivos del lote localmente...")
        for filename in batch_files:
            try:
                local_filepath = os.path.join(TEMP_BATCH_DIR, filename)
                with open(local_filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                html_titulo = data.get('contenido_html_completo', {}).get('titulo', '')
                tipo = extraer_tipo_desde_h2(html_titulo)
                
                tipos[tipo] += 1
                if data.get('tiene_pdf', False):
                    con_pdf[tipo] += 1
                if len(ejemplos.get(tipo, [])) < 5:
                    ejemplos[tipo].append({
                        'numero': data.get('numero_medida'),
                        'fecha': data.get('fecha_boletin'),
                        'titulo': data.get('titulo_raw', '')[:100]
                    })
                
                processed_files.add(filename)

            except Exception as e:
                print(f"  - Advertencia: No se pudo procesar {filename}. Error: {e}")
        
        # Limpiar directorio temporal
        shutil.rmtree(TEMP_BATCH_DIR)

        # Guardar punto de control
        print("💾 Guardando punto de control...")
        checkpoint_data = {
            'processed_files': list(processed_files),
            'tipos': tipos,
            'con_pdf': con_pdf,
            'ejemplos': ejemplos,
            'last_update': datetime.now().isoformat()
        }
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
        print(f"✓ Progreso guardado. {len(processed_files)}/{len(all_files)} archivos completados.")

    # --- FASE 4: GENERAR Y SUBIR RESULTADOS FINALES ---
    print("\n" + "="*70)
    print("PROCESO COMPLETADO. GENERANDO ARCHIVOS FINALES...")
    print("=" * 70)
    
    total_final = sum(tipos.values())
    df = pd.DataFrame([
        {
            'Tipo': tipo,
            'Cantidad': count,
            'Porcentaje': f"{(count/total_final)*100:.2f}%",
            'Con_PDF': con_pdf.get(tipo, 0)
        }
        for tipo, count in sorted(tipos.items(), key=lambda x: x[1], reverse=True)
    ])
    df.to_csv(FINAL_CSV_RESULT, index=False, encoding='utf-8')
    print(f"✓ Archivo CSV final generado: {FINAL_CSV_RESULT}")

    with open(FINAL_JSON_EXAMPLES, 'w', encoding='utf-8') as f:
        json.dump(ejemplos, f, ensure_ascii=False, indent=2)
    print(f"✓ Archivo JSON de ejemplos generado: {FINAL_JSON_EXAMPLES}")

    # Subir resultados a Hostinger
    print("\nSubiendo resultados finales a Hostinger...")
    ftp_final = conectar_ftp()
    if ftp_final:
        try:
            with open(FINAL_CSV_RESULT, 'rb') as f:
                ftp_final.storbinary(f'STOR {FINAL_CSV_RESULT}', f)
            print(f"✓ {FINAL_CSV_RESULT} subido exitosamente.")
            
            with open(FINAL_JSON_EXAMPLES, 'rb') as f:
                ftp_final.storbinary(f'STOR {FINAL_JSON_EXAMPLES}', f)
            print(f"✓ {FINAL_JSON_EXAMPLES} subido exitosamente.")
            
            ftp_final.quit()
        except Exception as e:
            print(f"✗ ERROR al subir los archivos finales: {e}")

    # Limpiar archivo de control
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)

    end_time = time.time()
    print("\n" + "="*70)
    print(f"🏁 FIN DEL PROCESO. Tiempo total: {int((end_time - start_time)/60)} minutos.")
    print("="*70)
    print("\nResultados disponibles en:")
    print(f"  - https://boria.agoraenlared.com/{FINAL_CSV_RESULT}")
    print(f"  - https://boria.agoraenlared.com/{FINAL_JSON_EXAMPLES}")

if __name__ == "__main__":
    main()
