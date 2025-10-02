import ftplib
import json
import pandas as pd
from collections import defaultdict
from datetime import datetime
import time
from bs4 import BeautifulSoup
import re
import os

FTP_HOST = 'ftp.agoraenlared.com'
FTP_USER = 'u112219758.boria'
FTP_PASS = 'Marta1664?'

def conectar_ftp():
    """Conecta al FTP y retorna el objeto"""
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, 21, timeout=60)
    ftp.login(FTP_USER, FTP_PASS)
    return ftp

def extraer_tipo_desde_h2(html_titulo):
    if not html_titulo:
        return "SIN_TIPO"
    try:
        soup = BeautifulSoup(html_titulo, 'html.parser')
        h2 = soup.find('h2')
        if h2:
            texto = h2.get_text(strip=True)
            texto = re.sub(r'\d+/\d{4}', '', texto)
            texto = re.sub(r'N°\s*\d+', '', texto)
            return texto.strip() or "SIN_TIPO"
    except:
        pass
    return "SIN_TIPO"

def main():
    print("="*70)
    print(f"INICIO: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Conexión inicial
    print("\nConectando FTP...")
    ftp = conectar_ftp()
    ftp.cwd('/public_html/boria/data/raw/')
    archivos = [f for f in ftp.nlst() if f.endswith('.json')]
    total = len(archivos)
    print(f"Total: {total} archivos")
    
    # Variables de análisis
    tipos = defaultdict(int)
    con_pdf = defaultdict(int)
    ejemplos = defaultdict(list)
    
    start = time.time()
    RECONEXION_CADA = 5000
    
    print(f"\nProcesando (reconexión cada {RECONEXION_CADA} archivos)...\n")
    
    for idx, archivo in enumerate(archivos, 1):
        
        # RECONECTAR cada 5000 archivos
        if idx % RECONEXION_CADA == 0:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Reconectando FTP...")
            try:
                ftp.quit()
            except:
                pass
            ftp = conectar_ftp()
            ftp.cwd('/public_html/boria/data/raw/')
            print(f"OK - Reconexión exitosa en archivo {idx}")
        
        try:
            # Descargar y procesar
            with open('temp.json', 'wb') as f:
                ftp.retrbinary(f'RETR {archivo}', f.write)
            
            with open('temp.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            html_titulo = data.get('contenido_html_completo', {}).get('titulo', '')
            tipo = extraer_tipo_desde_h2(html_titulo)
            
            tipos[tipo] += 1
            if data.get('tiene_pdf', False):
                con_pdf[tipo] += 1
            
            if len(ejemplos[tipo]) < 5:
                ejemplos[tipo].append({
                    'numero': data.get('numero_medida'),
                    'fecha': data.get('fecha_boletin'),
                    'titulo': data.get('titulo_raw', '')[:80]
                })
        
        except Exception as e:
            print(f"Error {archivo}: {str(e)[:50]}")
        
        # Progreso cada 1000
        if idx % 1000 == 0:
            elapsed = time.time() - start
            rate = idx / elapsed
            eta_min = int((total - idx) / rate / 60)
            pct = (idx / total) * 100
            print(f"{idx}/{total} ({pct:.1f}%) - ETA: {eta_min} min")
    
    print(f"\nProcesamiento: {int((time.time()-start)/60)} minutos")
    
    # Generar archivos
    print("\nGenerando resultados...")
    df = pd.DataFrame([
        {
            'Tipo': tipo,
            'Cantidad': count,
            'Porcentaje': f"{(count/total)*100:.2f}%",
            'Con_PDF': con_pdf[tipo]
        }
        for tipo, count in sorted(tipos.items(), key=lambda x: x[1], reverse=True)
    ])
    df.to_csv('tipos_desde_h2.csv', index=False, encoding='utf-8')
    
    with open('ejemplos_h2_completos.json', 'w', encoding='utf-8') as f:
        json.dump(ejemplos, f, ensure_ascii=False, indent=2)
    
    print("Archivos locales: OK")
    
    # Subir (con NUEVA conexión)
    print("\nSubiendo a Hostinger...")
    try:
        ftp.quit()
    except:
        pass
    
    ftp = conectar_ftp()
    ftp.cwd('/public_html/boria/')
    
    with open('tipos_desde_h2.csv', 'rb') as f:
        ftp.storbinary('STOR tipos_desde_h2.csv', f)
    print("CSV: OK")
    
    with open('ejemplos_h2_completos.json', 'rb') as f:
        ftp.storbinary('STOR ejemplos_h2_completos.json', f)
    print("JSON: OK")
    
    ftp.quit()
    
    # Cleanup
    try:
        os.remove('temp.json')
    except:
        pass
    
    print("\n" + "="*70)
    print(f"FIN: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    print("\nhttps://boria.agoraenlared.com/tipos_desde_h2.csv")
    print("https://boria.agoraenlared.com/ejemplos_h2_completos.json")

if __name__ == "__main__":
    main()
