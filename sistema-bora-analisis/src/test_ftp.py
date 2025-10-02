import ftplib
import json
from datetime import datetime

FTP_HOST = 'ftp.agoraenlared.com'
FTP_USER = 'u112219758.boria'
FTP_PASS = 'Marta1664?'

print(f"[{datetime.now().strftime('%H:%M:%S')}] Conectando FTP...")
ftp = ftplib.FTP()
ftp.connect(FTP_HOST, 21, timeout=120)
ftp.login(FTP_USER, FTP_PASS)
ftp.set_pasv(True)
print("OK - Conectado")

print("Navegando a data/raw...")
ftp.cwd('data/raw')
print(f"OK - {ftp.pwd()}")

print("Descargando 10 archivos de prueba...")
archivos = [f for f in ftp.nlst() if f.endswith('.json')][:10]

for i, archivo in enumerate(archivos, 1):
    with open('temp.json', 'wb') as f:
        ftp.retrbinary(f'RETR {archivo}', f.write)
    with open('temp.json', 'r') as f:
        data = json.load(f)
    print(f"  {i}/10 - {data.get('numero_medida')}")

print("\nSubiendo archivo de prueba...")
with open('test_ok.txt', 'w') as f:
    f.write(f"Prueba exitosa - {datetime.now()}")

ftp.cwd('..')
with open('test_ok.txt', 'rb') as f:
    ftp.storbinary('STOR test_ok.txt', f)

ftp.quit()
print("\nTODO OK - Revisar: https://boria.agoraenlared.com/test_ok.txt")
