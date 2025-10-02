from ftplib import FTP
import json
from collections import Counter
import pandas as pd
import io
from bs4 import BeautifulSoup
import re

def extraer_tipo_desde_h2(medida):
    try:
        html_titulo = medida.get('contenido_html_completo', {}).get('titulo', '')
        if not html_titulo:
            return 'SIN_H2'
        
        soup = BeautifulSoup(html_titulo, 'html.parser')
        h2 = soup.find('h2')
        if not h2:
            return 'SIN_H2'
        
        texto_h2 = h2.get_text(strip=True)
        match = re.match(r'^([A-Za-zÁÉÍÓÚáéíóúñÑ\s]+?)(?:\s+N°?\s*\d|\s+\d)', texto_h2)
        
        if match:
            return match.group(1).strip()
        
        partes = re.split(r'\d', texto_h2, 1)
        return partes[0].strip() if partes else texto_h2
        
    except:
        return 'ERROR'

def main():
    print("Conectando a FTP...")
    ftp = FTP("ftp.agoraenlared.com", timeout=60)
    ftp.set_pasv(True)
    ftp.login("u112219758.boria", "Marta1664?")
    ftp.cwd("data/raw")
    ftp.sendcmd("TYPE I")
    
    archivos = [f for f in ftp.nlst() if f.endswith('.json')]
    total = len(archivos)
    print(f"Total: {total}\n")
    
    tipos = Counter()
    ejemplos_h2 = {}
    tiene_pdf_por_tipo = Counter()
    errores = 0
    
    for i, archivo in enumerate(archivos):
        if i % 1000 == 0:
            print(f"{i}/{total} ({i/total*100:.1f}%)")
        
        try:
            buffer = io.BytesIO()
            ftp.retrbinary(f'RETR {archivo}', buffer.write)
            buffer.seek(0)
            medida = json.load(buffer)
            
            tipo = extraer_tipo_desde_h2(medida)
            tipos[tipo] += 1
            
            if tipo not in ejemplos_h2:
                ejemplos_h2[tipo] = []
            
            if len(ejemplos_h2[tipo]) < 5:
                html_titulo = medida.get('contenido_html_completo', {}).get('titulo', '')
                soup = BeautifulSoup(html_titulo, 'html.parser')
                h2_text = soup.find('h2').get_text(strip=True) if soup.find('h2') else 'N/A'
                
                ejemplos_h2[tipo].append({
                    'h2_completo': h2_text,
                    'fecha': medida.get('fecha_boletin', ''),
                    'tiene_pdf': medida.get('tiene_pdf', False)
                })
            
            if medida.get('tiene_pdf', False):
                tiene_pdf_por_tipo[tipo] += 1
        
        except Exception as e:
            errores += 1
            if errores <= 5:
                print(f"Error: {str(e)[:50]}")
            continue
    
    ftp.quit()
    
    print(f"\nProcesadas: {total - errores}, Errores: {errores}\n")
    
    resultados = []
    for tipo, cantidad in tipos.most_common():
        pct_pdf = (tiene_pdf_por_tipo.get(tipo, 0) / cantidad * 100) if cantidad > 0 else 0
        ejemplo = ejemplos_h2[tipo][0]['h2_completo'] if tipo in ejemplos_h2 else ''
        
        resultados.append({
            'Tipo': tipo,
            'Cantidad': cantidad,
            'Porcentaje': f"{cantidad/total*100:.2f}%",
            'Con_PDF': tiene_pdf_por_tipo.get(tipo, 0),
            'Pct_PDF': f"{pct_pdf:.1f}%",
            'Ejemplo_H2': ejemplo[:80]
        })
    
    df = pd.DataFrame(resultados)
    df.to_csv('tipos_desde_h2.csv', index=False, encoding='utf-8')
    
    print("="*120)
    print("TODOS LOS TIPOS")
    print("="*120)
    print(df.to_string(index=False, max_rows=None))
    
    with open('ejemplos_h2_completos.json', 'w', encoding='utf-8') as f:
        json.dump(ejemplos_h2, f, ensure_ascii=False, indent=2)
    
    # Subir resultados al FTP
    print("\nSubiendo resultados...")
    ftp_up = FTP("ftp.agoraenlared.com", timeout=60)
    ftp_up.set_pasv(True)
    ftp_up.login("u112219758.boria", "Marta1664?")
    
    with open('tipos_desde_h2.csv', 'rb') as f:
        ftp_up.storbinary('STOR tipos_desde_h2.csv', f)
    
    with open('ejemplos_h2_completos.json', 'rb') as f:
        ftp_up.storbinary('STOR ejemplos_h2_completos.json', f)
    
    ftp_up.quit()
    print("Resultados en Hostinger raíz")

if __name__ == "__main__":
    main()
