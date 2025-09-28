import os
import sys
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import time
from datetime import datetime, date, timedelta
import re
from pathlib import Path

class BoraScraperCore:
    def __init__(self):
        self.session = self.setup_session()
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
    def setup_session(self):
        """Configurar sesión HTTP con retry"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def get_text_from_measure_page(self, numero_medida, fecha_str):
        """Extraer contenido de una medida específica - ENFOQUE AGNÓSTICO"""
        url = f"https://www.boletinoficial.gob.ar/detalleAviso/primera/{numero_medida}/{fecha_str.replace('-', '')}"
        
        try:
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 404:
                return None
                
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Estructura agnóstica - guardamos todo raw para análisis posterior
            data = {
                'numero_medida': numero_medida,
                'fecha_boletin': fecha_str,
                'url': url,
                'titulo_raw': '',
                'contenido_html_completo': str(soup),
                'texto_completo_limpio': '',
                'estructura_detectada': {},
                'metadatos_extraidos': {},
                'elementos_detectados': [],
                'pdf_urls': [],
                'tiene_pdf': False,
                'timestamp_scraping': datetime.now().isoformat()
            }
            
            # Extraer título sin categorizar
            titulo_elem = soup.find('h1') or soup.find('h2', class_='titulo') or soup.find('h2')
            if titulo_elem:
                data['titulo_raw'] = titulo_elem.get_text(strip=True)
            
            # Extraer TODOS los elementos de navegación/breadcrumb sin asumir estructura
            navegacion_elementos = []
            breadcrumb = soup.find('nav', {'aria-label': 'breadcrumb'})
            if breadcrumb:
                links = breadcrumb.find_all('a')
                for link in links:
                    navegacion_elementos.append({
                        'texto': link.get_text(strip=True),
                        'href': link.get('href', ''),
                        'posicion': len(navegacion_elementos)
                    })
            data['metadatos_extraidos']['navegacion'] = navegacion_elementos
            
            # Extraer contenido principal sin estructura predefinida
            content_div = soup.find('div', id='page-content') or soup.find('div', class_='contenido')
            if content_div:
                # Texto completo limpio
                data['texto_completo_limpio'] = content_div.get_text(separator='\n', strip=True)
                
                # Detectar estructura de manera flexible
                data['estructura_detectada'] = self.detect_document_structure_flexible(content_div)
            
            # Extraer TODOS los elementos que parezcan firmantes/autoridades
            data['elementos_detectados'] = self.extract_all_potential_signers(soup)
            
            # Extraer PDFs
            pdf_links = soup.find_all('a', href=re.compile(r'\.pdf', re.IGNORECASE))
            for link in pdf_links:
                pdf_url = link.get('href')
                if pdf_url:
                    if not pdf_url.startswith('http'):
                        pdf_url = 'https://www.boletinoficial.gob.ar' + pdf_url
                    data['pdf_urls'].append({
                        'url': pdf_url,
                        'texto_enlace': link.get_text(strip=True),
                        'contexto': self.get_link_context(link)
                    })
            
            data['tiene_pdf'] = len(data['pdf_urls']) > 0
            
            # Extraer metadatos adicionales sin categorizar
            data['metadatos_extraidos'].update(self.extract_all_metadata(soup))
            
            return data
            
        except Exception as e:
            print(f"Error procesando medida {numero_medida}: {str(e)}")
            return None

    def detect_document_structure_flexible(self, content_div):
        """Detectar estructura del documento de manera flexible"""
        texto_completo = content_div.get_text()
        estructura = {}
        
        # Buscar secciones usando patrones flexibles (no predefinidos)
        patrones_secciones = [
            (r'VISTO:?\s*(.*?)(?=\n[A-Z]{4,}:|$)', 'visto'),
            (r'CONSIDERANDO:?\s*(.*?)(?=\n[A-Z]{4,}:|$)', 'considerandos'),
            (r'(?:RESUELVE|DECRETA|DISPONE):?\s*(.*)', 'dispositivo'),
            (r'POR\s+TANTO:?\s*(.*?)(?=\n[A-Z]{4,}:|$)', 'por_tanto'),
            (r'ARTÍCULO\s+.*?:?\s*(.*?)(?=\nARTÍCULO|\nCOMUNÍQUESE|$)', 'articulos'),
            (r'FUNDAMENTOS:?\s*(.*?)(?=\n[A-Z]{4,}:|$)', 'fundamentos')
        ]
        
        for patron, nombre_seccion in patrones_secciones:
            match = re.search(patron, texto_completo, re.DOTALL | re.IGNORECASE)
            if match:
                seccion_data = {
                    'contenido': match.group(1).strip(),
                    'posicion_inicio': match.start(),
                    'posicion_fin': match.end(),
                    'longitud_caracteres': len(match.group(1).strip())
                }
                
                # MARCADO ESPECIAL: Los considerandos siempre requieren análisis crítico
                if nombre_seccion == 'considerandos':
                    seccion_data['requiere_analisis_critico'] = True
                    seccion_data['nota_critica'] = 'Justificación oficial - analizar gaps entre retórica y realidad'
                
                estructura[nombre_seccion] = seccion_data
        
        # Detectar cualquier sección en mayúsculas que no hayamos capturado
        secciones_adicionales = re.findall(r'\n([A-ZÁÉÍÓÚÑÜ]{4,}(?:\s+[A-ZÁÉÍÓÚÑÜ]+)*):?\s*([^\n]+)', texto_completo)
        estructura['secciones_adicionales'] = [
            {'nombre': seccion[0], 'contenido_primera_linea': seccion[1]} 
            for seccion in secciones_adicionales
        ]
        
        return estructura

    def extract_all_potential_signers(self, soup):
        """Extraer TODOS los elementos que podrían ser firmantes/autoridades"""
        elementos_detectados = []
        
        # Buscar elementos en mayúsculas (posibles firmantes)
        texto_completo = soup.get_text()
        nombres_mayusculas = re.findall(r'\b[A-ZÁÉÍÓÚÑÜ][a-záéíóúñü]+(?:\s+[A-ZÁÉÍÓÚÑÜ][a-záéíóúñü]+)*\s+[A-ZÁÉÍÓÚÑÜ]{2,}(?:\s+[A-ZÁÉÍÓÚÑÜ]{2,})*\b', texto_completo)
        
        for nombre in nombres_mayusculas:
            elementos_detectados.append({
                'tipo_detectado': 'posible_firmante',
                'contenido': nombre,
                'patron_usado': 'nombre_apellido_mayusculas'
            })
        
        # Buscar elementos con palabras clave relacionadas a autoridad
        palabras_autoridad = ['firmado', 'firma', 'suscribe', 'autoriza', 'designa', 'nombra']
        for palabra in palabras_autoridad:
            elementos_con_palabra = soup.find_all(text=re.compile(palabra, re.IGNORECASE))
            for elemento in elementos_con_palabra:
                parent = elemento.parent if elemento.parent else elemento
                elementos_detectados.append({
                    'tipo_detectado': 'contexto_autoridad',
                    'contenido': parent.get_text(strip=True) if hasattr(parent, 'get_text') else str(elemento),
                    'palabra_clave': palabra,
                    'patron_usado': 'contexto_semantico'
                })
        
        return elementos_detectados

    def get_link_context(self, link):
        """Obtener contexto alrededor de un enlace"""
        parent = link.parent
        if parent:
            return parent.get_text(strip=True)
        return ""

    def extract_all_metadata(self, soup):
        """Extraer TODOS los metadatos sin predefinir categorías"""
        metadatos = {}
        
        # Extraer todos los elementos meta
        meta_tags = soup.find_all('meta')
        metadatos['meta_tags'] = []
        for meta in meta_tags:
            meta_data = {}
            for attr in ['name', 'content', 'property', 'http-equiv']:
                if meta.has_attr(attr):
                    meta_data[attr] = meta[attr]
            if meta_data:
                metadatos['meta_tags'].append(meta_data)
        
        # Extraer elementos con clases que podrían indicar tipo
        elementos_con_clase = soup.find_all(attrs={'class': True})
        clases_relevantes = []
        for elem in elementos_con_clase:
            clases = elem.get('class', [])
            for clase in clases:
                if any(palabra in clase.lower() for palabra in ['tipo', 'category', 'section', 'document', 'official']):
                    clases_relevantes.append({
                        'clase': clase,
                        'texto': elem.get_text(strip=True)[:100],  # Primeros 100 chars
                        'tag': elem.name
                    })
        metadatos['clases_relevantes'] = clases_relevantes
        
        # Buscar fechas adicionales en el texto
        fechas_detectadas = re.findall(r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2}', soup.get_text())
        metadatos['fechas_detectadas'] = list(set(fechas_detectadas))
        
        # Buscar números que podrían ser referencias
        numeros_referencia = re.findall(r'N[°º]?\s*\d+[/-]?\d*', soup.get_text())
        metadatos['numeros_referencia'] = list(set(numeros_referencia))
        
        return metadatos

    def scrape_fecha_especifica(self, fecha_str, limit=None):
        """Scraper medidas de una fecha específica - AGNÓSTICO"""
        print(f"Scraping fecha: {fecha_str} (modo agnóstico)")
        
        # Empezar desde número base más conservador
        numero_actual = 300720  # Número más bajo para asegurar completitud
        medidas_encontradas = 0
        medidas_consecutivas_404 = 0
        max_404_consecutivos = 50  # Más tolerante para no perder medidas
        
        while True:
            if limit and medidas_encontradas >= limit:
                break
                
            if medidas_consecutivas_404 >= max_404_consecutivos:
                print(f"Se encontraron {max_404_consecutivos} medidas consecutivas inexistentes. Terminando scraping.")
                break
            
            print(f"Probando medida {numero_actual}...")
            
            medida_data = self.get_text_from_measure_page(numero_actual, fecha_str)
            
            if medida_data is None:
                medidas_consecutivas_404 += 1
                numero_actual += 1
                continue
            
            # Reset contador de 404s cuando encontramos una medida
            medidas_consecutivas_404 = 0
            
            # Guardar medida
            self.save_medida(medida_data)
            medidas_encontradas += 1
            print(f"✓ Medida {numero_actual} guardada (total: {medidas_encontradas})")
            
            numero_actual += 1
            time.sleep(0.5)  # Pausa más corta para eficiencia
        
        print(f"Scraping completado. {medidas_encontradas} medidas encontradas.")
        return medidas_encontradas

    def scrape_sistematico(self, fecha_inicio, fecha_fin):
        """Scraping sistemático completo - AGNÓSTICO"""
        print(f"=== SCRAPING SISTEMÁTICO AGNÓSTICO ===")
        print(f"Desde: {fecha_inicio} hasta: {fecha_fin}")
        
        fecha_actual = datetime.strptime(fecha_inicio, "%Y-%m-%d").date()
        fecha_limite = datetime.strptime(fecha_fin, "%Y-%m-%d").date()
        
        total_medidas = 0
        
        while fecha_actual <= fecha_limite:
            fecha_str = fecha_actual.strftime("%Y-%m-%d")
            print(f"\n--- Procesando {fecha_str} ---")
            
            medidas_del_dia = self.scrape_fecha_especifica(fecha_str)
            total_medidas += medidas_del_dia
            
            print(f"Día {fecha_str}: {medidas_del_dia} medidas")
            print(f"Total acumulado: {total_medidas} medidas")
            
            fecha_actual += timedelta(days=1)
            time.sleep(2)  # Pausa entre días
        
        print(f"\n=== SCRAPING COMPLETADO ===")
        print(f"Total de medidas scrapeadas: {total_medidas}")
        return total_medidas

    def save_medida(self, medida_data):
        """Guardar medida en archivo JSON"""
        filename = f"medida_{medida_data['numero_medida']}_{medida_data['fecha_boletin'].replace('-', '')}.json"
        filepath = self.data_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(medida_data, f, ensure_ascii=False, indent=2)

    def get_scraping_stats(self):
        """Obtener estadísticas del scraping realizado"""
        json_files = list(self.data_dir.glob("medida_*.json"))
        
        stats = {
            'total_medidas': len(json_files),
            'fechas_cubiertas': set(),
            'organismos_detectados': set(),
            'tipos_documento_detectados': set()
        }
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    stats['fechas_cubiertas'].add(data['fecha_boletin'])
                    
                    # Extraer organismos de navegación
                    nav = data.get('metadatos_extraidos', {}).get('navegacion', [])
                    for elem in nav:
                        if elem['texto']:
                            stats['organismos_detectados'].add(elem['texto'])
            except Exception as e:
                print(f"Error leyendo {json_file}: {e}")
        
        # Convertir sets a listas para JSON
        stats['fechas_cubiertas'] = sorted(list(stats['fechas_cubiertas']))
        stats['organismos_detectados'] = sorted(list(stats['organismos_detectados']))
        stats['tipos_documento_detectados'] = sorted(list(stats['tipos_documento_detectados']))
        
        return stats

def main():
    print("=== SISTEMA DE SCRAPING BOLETÍN OFICIAL AGNÓSTICO ===")
    print("Modo TEST: Scraping limitado sin categorías predefinidas")
    
    scraper = BoraScraperCore()
    
    # Test con fecha específica y límite
    fecha_test = "2023-12-11"  # Primera fecha del gobierno
    limite_test = 5
    
    print(f"Iniciando test agnóstico: {limite_test} medidas del {fecha_test}")
    medidas_encontradas = scraper.scrape_fecha_especifica(fecha_test, limit=limite_test)
    
    print(f"\nTest completado. {medidas_encontradas} medidas guardadas.")
    print("Revisá la carpeta 'data' para ver los resultados.")
    
    # Mostrar estadísticas
    stats = scraper.get_scraping_stats()
    print(f"\nEstadísticas del scraping:")
    print(f"- Total medidas: {stats['total_medidas']}")
    print(f"- Fechas cubiertas: {stats['fechas_cubiertas']}")
    print(f"- Organismos detectados: {len(stats['organismos_detectados'])}")

if __name__ == "__main__":

    main()
