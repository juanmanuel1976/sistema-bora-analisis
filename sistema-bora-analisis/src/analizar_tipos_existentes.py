import ftplib
import json
import pandas as pd
from collections import defaultdict
from datetime import datetime
import time
from bs4 import BeautifulSoup
import re
import os
import sys

FTP_HOST = 'ftp.agoraenlared.com'
FTP_USER = 'u112219758.boria'
FTP_PASS = 'Marta1664?'

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
    sys.stdout.flush()

def conectar_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, 21, timeout=120)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.set_pasv(True)
    return ftp

def extraer_tipo_desde_h2(html_titulo):
    if not html_titulo: return "SIN_TIPO"
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
    log("="*70)
    log("INICIO")
    log("="*70)
    
    ftp = conectar_ftp()
    log("FTP conectado")
    
    ftp.cwd('data/raw')
    log(f"Directorio: {ftp.pwd()}")
    
    archivos = [f for f in ftp.nlst() if f.endswith('.json')]
    total = len(archivos)
    log(f"Total: {total}")
    
    tipos = defaultdict(int)
    con_pdf = defaultdict(int)
    ejemplos = defaultdict(list)
    start = time.time()
    errores = 0
    
    for idx, archivo in enumerate(archivos, 1):
        
        if idx % 5000 == 0:
            log("Reconectando FTP...")
            ftp.quit()
            ftp = conectar_ftp()
            ftp.cwd('data/raw')
            log("Reconectado")
        
        try:
            with open('temp.json', 'wb') as f:
                ftp.retrbinary(f'RETR {archivo}', f.write)
            
            with open('temp.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            tipo = extraer_tipo_desde_h2(data.get('contenido_html_completo', {}).get('titulo', ''))
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
            errores += 1
            if errores <= 10:
                log(f"Error {archivo}: {str(e)[:40]}")
            if errores > 100:
                log("FATAL: Demasiados errores")
                ftp.quit()
                return
        
        if idx % 1000 == 0:
            elapsed = time.time() - start
            eta = int(((total - idx) / (idx / elapsed)) / 60)
            log(f"{idx}/{total} ({100*idx/total:.1f}%) ETA:{eta}min Err:{errores}")
    
    log(f"Completado: {int((time.time()-start)/60)}min Total errores: {errores}")
    
    df = pd.DataFrame([
        {'Tipo': t, 'Cantidad': c, 'Porcentaje': f"{100*c/total:.2f}%", 'Con_PDF': con_pdf[t]}
        for t, c in sorted(tipos.items(), key=lambda x: x[1], reverse=True)
    ])
    df.to_csv('tipos_desde_h2.csv', index=False, encoding='utf-8')
    log("CSV generado")
    
    with open('ejemplos_h2_completos.json', 'w', encoding='utf-8') as f:
        json.dump(ejemplos, f, ensure_ascii=False, indent=2)
    log("JSON generado")
    
    log("Subiendo a Hostinger...")
    ftp.quit()
    ftp = conectar_ftp()
    
    with open('tipos_desde_h2.csv', 'rb') as f:
        ftp.storbinary('STOR tipos_desde_h2.csv', f)
    log("CSV subido")
    
    with open('ejemplos_h2_completos.json', 'rb') as f:
        ftp.storbinary('STOR ejemplos_h2_completos.json', f)
    log("JSON subido")
    
    ftp.quit()
    os.remove('temp.json')
    
    log("="*70)
    log("FINALIZADO")
    log("="*70)

if __name__ == "__main__":
    main()
