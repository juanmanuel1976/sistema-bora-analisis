import os
import json
import re
import google.generativeai as genai
from sentence_transformers import SentenceTransformer
from pathlib import Path
from datetime import datetime

class TripleAnalyzerAgnostic:
    def __init__(self):
        # Configurar Gemini
        api_key = os.getenv('GEMINI_API_KEY')
        if not api_key:
            print("ERROR: GEMINI_API_KEY no encontrada en variables de entorno")
            print("Configurala como variable de entorno o en archivo .env")
            return
        
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash')
        
        # Configurar encoder para embeddings
        try:
            self.encoder = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
            print("✓ Analyzer agnóstico inicializado correctamente")
        except Exception as e:
            print(f"Error inicializando encoder: {e}")
    
    def analyze_medida(self, medida_data):
        """Análisis triple completo de una medida - ENFOQUE AGNÓSTICO"""
        print(f"Analizando medida {medida_data['numero_medida']} con enfoque agnóstico...")
        
        resultado = {
            'numero_medida': medida_data['numero_medida'],
            'fecha_boletin': medida_data['fecha_boletin'],
            'fecha_analisis': datetime.now().isoformat(),
            'analisis_literal': self.extract_literal_data_agnostic(medida_data),
            'analisis_critico': self.analyze_critical_agnostic(medida_data),
            'analisis_abogado_diablo': self.devil_advocate_agnostic(medida_data),
            'analisis_semantico': self.semantic_analysis(medida_data),
            'embeddings': self.generate_embeddings_agnostic(medida_data),
            'metadatos_analisis': {
                'enfoque': 'agnostico_sin_presupuestos',
                'version_analyzer': '2.0_agnostic',
                'confianza_general': 'pendiente_validacion'
            }
        }
        
        return resultado
    
    def extract_literal_data_agnostic(self, medida_data):
        """Análisis literal agnóstico - sin categorías predefinidas"""
        
        # Datos directos del scraping (objetivos)
        literal_data = {
            'numero_medida': medida_data['numero_medida'],
            'fecha_boletin': medida_data['fecha_boletin'],
            'url_fuente': medida_data['url'],
            'titulo_raw': medida_data.get('titulo_raw', ''),
            'tiene_pdfs': medida_data.get('tiene_pdf', False),
            'cantidad_pdfs': len(medida_data.get('pdf_urls', [])),
            'timestamp_scraping': medida_data.get('timestamp_scraping', ''),
            
            # Estructura detectada por el scraper
            'estructura_detectada': medida_data.get('estructura_detectada', {}),
            'metadatos_scraping': medida_data.get('metadatos_extraidos', {}),
            'elementos_detectados_scraper': medida_data.get('elementos_detectados', []),
            
            # Análisis IA de elementos sin predefinir categorías
            'elementos_detectados_ia': self.detect_elements_with_ai(medida_data),
            'estadisticas_texto': self.calculate_text_stats(medida_data)
        }
        
        return literal_data
    
    def detect_elements_with_ai(self, medida_data):
        """Usar IA para detectar elementos sin categorías predefinidas"""
        texto_completo = medida_data.get('texto_completo_limpio', '')
        
        if not texto_completo or len(texto_completo) < 50:
            return {"error": "Texto insuficiente para análisis IA"}
        
        prompt = f"""
        Analiza este texto oficial argentino SIN usar categorías predefinidas.
        Detecta SOLO lo que esté EXPLÍCITAMENTE presente:

        {texto_completo[:2000]}

        Extrae en JSON:
        {{
          "entidades_mencionadas": ["organismos, personas, instituciones mencionadas"],
          "referencias_normativas": ["decretos, leyes, resoluciones citadas"], 
          "elementos_temporales": ["fechas, plazos, períodos mencionados"],
          "elementos_financieros": ["montos, presupuestos, costos mencionados"],
          "acciones_principales": ["qué acciones ordena este documento"],
          "autoridades_involucradas": ["quién firma, autoriza, designa"],
          "ambitos_afectados": ["qué áreas/sectores menciona"],
          "palabras_clave_destacadas": ["términos técnicos o significativos repetidos"]
        }}

        NO inventes. Solo extrae lo que esté literalmente presente.
        """
        
        try:
            response = self.model.generate_content(prompt)
            result = json.loads(response.text.strip())
            return result
        except Exception as e:
            return {"error": f"Error detección IA: {str(e)}"}
    
    def calculate_text_stats(self, medida_data):
        """Estadísticas objetivas del texto"""
        texto = medida_data.get('texto_completo_limpio', '')
        estructura = medida_data.get('estructura_detectada', {})
        
        stats = {
            'longitud_total_caracteres': len(texto),
            'longitud_total_palabras': len(texto.split()),
            'secciones_detectadas': len(estructura),
            'tiene_considerandos': 'considerandos' in estructura,
            'longitud_considerandos': 0,
            'longitud_dispositivo': 0
        }
        
        # Longitudes de secciones específicas
        if 'considerandos' in estructura:
            stats['longitud_considerandos'] = estructura['considerandos'].get('longitud_caracteres', 0)
        
        if 'dispositivo' in estructura:
            stats['longitud_dispositivo'] = estructura['dispositivo'].get('longitud_caracteres', 0)
        elif 'articulos' in estructura:
            stats['longitud_dispositivo'] = estructura['articulos'].get('longitud_caracteres', 0)
        
        # Ratio considerandos vs dispositivo
        if stats['longitud_dispositivo'] > 0:
            stats['ratio_justificacion_accion'] = stats['longitud_considerandos'] / stats['longitud_dispositivo']
        
        return stats
    
    def analyze_critical_agnostic(self, medida_data):
        """Análisis crítico agnóstico - especial atención a considerandos"""
        
        estructura = medida_data.get('estructura_detectada', {})
        
        # Verificar si hay considerandos marcados para análisis crítico
        considerandos_data = estructura.get('considerandos', {})
        requiere_critico = considerandos_data.get('requiere_analisis_critico', False)
        
        # Construir texto para análisis
        texto_considerandos = considerandos_data.get('contenido', '')
        texto_dispositivo = ''
        
        # Obtener dispositivo (artículos, resolución, etc.)
        for seccion in ['dispositivo', 'articulos']:
            if seccion in estructura:
                texto_dispositivo = estructura[seccion].get('contenido', '')
                break
        
        prompt = f"""
        Análisis crítico de medida oficial argentina.
        
        {"CONSIDERANDOS (JUSTIFICACIÓN OFICIAL - ANALIZAR CRÍTICAMENTE):" if requiere_critico else "JUSTIFICACIÓN:"}
        {texto_considerandos}
        
        DISPOSITIVO/ARTÍCULOS (ACCIÓN REAL):
        {texto_dispositivo}
        
        Detecta gaps y problemas sin asumir categorías predefinidas:
        
        1. ¿La justificación es PROPORCIONAL a las facultades otorgadas?
        2. ¿El dispositivo va MÁS ALLÁ de lo justificado?
        3. ¿Hay AMBIGÜEDADES que permiten interpretaciones amplias?
        4. ¿Qué CAMBIOS en distribución de facultades detectas?
        5. ¿Hay REDISTRIBUCIÓN de responsabilidades entre organismos?
        6. ¿Detectas TRANSFORMACIONES en roles institucionales?
        7. ¿Qué MOVIMIENTOS de poder o autoridad identificas?
        8. ¿Hay MODIFICACIONES en la estructura de toma de decisiones?
        9. ¿Qué OMISIONES significativas detectas?
        10. ¿Hay EUFEMISMOS o lenguaje que oculta la verdadera función?
        
        Responde SOLO en JSON:
        {{
          "proporcionalidad_justificacion_accion": "alta/media/baja/sin_justificacion",
          "excesos_detectados": "descripción específica si el dispositivo excede justificación",
          "ambiguedades_detectadas": ["ambiguedades específicas que permiten interpretación expansiva"],
          "concentracion_poder": "descripción específica de poder concentrado y en qué organismo/persona",
          "omisiones_significativas": ["qué información importante se omite deliberadamente"],
          "eufemismos_detectados": ["lenguaje que oculta verdadera función"],
          "nivel_transparencia": "alto/medio/bajo",
          "señales_alerta": ["aspectos específicos que requieren escrutinio adicional"]
        }}
        """
        
        try:
            response = self.model.generate_content(prompt)
            result = json.loads(response.text.strip())
            
            # Agregar metadatos del análisis
            result['considerandos_analizados_criticamente'] = requiere_critico
            result['ratio_justificacion_accion'] = self.calculate_justification_ratio(medida_data)
            
            return result
        except Exception as e:
            return {"error": f"Error análisis crítico: {str(e)}"}
    
    def devil_advocate_agnostic(self, medida_data):
        """Abogado del diablo agnóstico - cuestionamiento sistemático"""
        
        texto_completo = medida_data.get('texto_completo_limpio', '')
        titulo = medida_data.get('titulo_raw', '')
        
        prompt = f"""
        Actúa como abogado del diablo. Cuestiona MALICIOSAMENTE esta medida oficial:
        
        TÍTULO: {titulo}
        
        CONTENIDO: {texto_completo[:2000]}
        
        Pregúntate con máxima suspicacia, SIN categorías predefinidas:
        
        1. ¿Qué usos NO DECLARADOS permite esta medida?
        2. ¿A quién puede perjudicar que NO está mencionado?
        3. ¿Qué interpretaciones ABUSIVAS permite?
        4. ¿Qué precedentes PELIGROSOS establece?
        5. ¿Cómo se puede usar de manera NO PREVISTA?
        6. ¿Qué información se oculta deliberadamente?
        7. ¿Qué grupos de poder se benefician sin aparecer mencionados?
        
        Responde SOLO en JSON:
        {{
          "usos_no_declarados": ["usos ocultos específicos que permite"],
          "perjudicados_omitidos": ["sectores que se verán afectados sin ser mencionados"],
          "interpretaciones_abusivas": ["cómo se puede abusar de la redacción"],
          "precedentes_peligrosos": ["qué precedentes problemáticos establece"],
          "usos_no_previstos": ["aplicaciones alternativas no contempladas"],
          "informacion_oculta": ["qué información importante se omite"],
          "beneficiarios_ocultos": ["quién se beneficia sin aparecer mencionado"],
          "nivel_riesgo_democratico": "bajo/medio/alto/critico",
          "red_flags_principales": ["señales de alerta más importantes"]
        }}
        """
        
        try:
            response = self.model.generate_content(prompt)
            result = json.loads(response.text.strip())
            return result
        except Exception as e:
            return {"error": f"Error abogado diablo: {str(e)}"}
    
    def semantic_analysis(self, medida_data):
        """Análisis semántico agnóstico para detectar patrones emergentes"""
        
        texto_completo = medida_data.get('texto_completo_limpio', '')
        
        prompt = f"""
        Análisis semántico agnóstico de medida oficial.
        NO uses categorías predefinidas. Detecta patrones emergentes:
        
        {texto_completo[:1500]}
        
        Analiza semánticamente SIN presupuestos:
        
        1. ¿Qué TEMA central aborda realmente esta medida?
        2. ¿Qué PATRONES de lenguaje oficial detectas?
        3. ¿Hay CORRELACIONES entre diferentes partes del texto?
        4. ¿Qué FUNCIONES múltiples podría tener esta medida?
        5. ¿Detectas INTENCIONES no explícitas?
        
        Responde SOLO en JSON:
        {{
          "tema_central_real": "descripción del tema real más allá del título oficial",
          "subtemas_detectados": ["temas secundarios identificados"],
          "patrones_linguisticos": ["patrones de lenguaje oficial/burocrático detectados"],
          "correlaciones_internas": ["conexiones entre diferentes partes del texto"],
          "funciones_multiples": ["diferentes funciones que podría cumplir esta medida"],
          "intenciones_implicitas": ["intenciones no declaradas que se pueden inferir"],
          "complejidad_semantica": "baja/media/alta",
          "categoria_emergente": "categoría que emerge del análisis semántico"
        }}
        """
        
        try:
            response = self.model.generate_content(prompt)
            result = json.loads(response.text.strip())
            return result
        except Exception as e:
            return {"error": f"Error análisis semántico: {str(e)}"}
    
    def calculate_justification_ratio(self, medida_data):
        """Calcular ratio entre justificación y acción"""
        estructura = medida_data.get('estructura_detectada', {})
        
        len_considerandos = 0
        len_dispositivo = 0
        
        if 'considerandos' in estructura:
            len_considerandos = estructura['considerandos'].get('longitud_caracteres', 0)
        
        for seccion in ['dispositivo', 'articulos']:
            if seccion in estructura:
                len_dispositivo = estructura[seccion].get('longitud_caracteres', 0)
                break
        
        if len_dispositivo > 0:
            return {
                'ratio_numerico': len_considerandos / len_dispositivo,
                'interpretacion': 'justificacion_extensa' if len_considerandos / len_dispositivo > 2 else 'justificacion_proporcionada'
            }
        
        return {'ratio_numerico': 0, 'interpretacion': 'sin_dispositivo_claro'}
    
    def generate_embeddings_agnostic(self, medida_data):
        """Generar embeddings semánticos agnósticos"""
        try:
            # Texto completo limpio
            texto_completo = medida_data.get('texto_completo_limpio', '')
            if not texto_completo:
                return {"error": "Sin texto para generar embeddings"}
            
            embedding_completo = self.encoder.encode(texto_completo).tolist()
            
            # Embeddings por secciones detectadas
            estructura = medida_data.get('estructura_detectada', {})
            embeddings_secciones = {}
            
            for seccion, datos in estructura.items():
                contenido = datos.get('contenido', '')
                if contenido and len(contenido) > 20:
                    embeddings_secciones[f'embedding_{seccion}'] = self.encoder.encode(contenido).tolist()
            
            # Embedding del título
            titulo = medida_data.get('titulo_raw', '')
            embedding_titulo = self.encoder.encode(titulo).tolist() if titulo else []
            
            return {
                'embedding_completo': embedding_completo,
                'embedding_titulo': embedding_titulo,
                'embeddings_por_seccion': embeddings_secciones,
                'dimension': len(embedding_completo),
                'metadatos_embedding': {
                    'modelo_usado': 'paraphrase-multilingual-MiniLM-L12-v2',
                    'secciones_vectorizadas': list(embeddings_secciones.keys()),
                    'fecha_generacion': datetime.now().isoformat()
                }
            }
        except Exception as e:
            return {"error": f"Error generando embeddings: {str(e)}"}

class BatchAnalyzer:
    """Análisis batch de todas las medidas"""
    
    def __init__(self):
        self.analyzer = TripleAnalyzerAgnostic()
        
    def analyze_all_measures_in_directory(self):
        """Descarga, analiza y vuelve a subir las medidas desde Hostinger."""
        print("Iniciando análisis batch desde Hostinger...")
        
        raw_files = self.list_hostinger_files('raw')
        analyzed_files = self.list_hostinger_files('analyzed')
        
        analyzed_stems = {Path(f).stem.replace('_analysis_agnostic', '') for f in analyzed_files}
        files_to_analyze = [f for f in raw_files if Path(f).stem not in analyzed_stems]
        
        if not files_to_analyze:
            print("No hay nuevas medidas para analizar. Todo está al día.")
            return {'total_analizadas': 0, 'total_errores': 0}
            
        print(f"Se encontraron {len(files_to_analyze)} nuevas medidas para analizar.")
        
        resultados = []
        errores = []
        
        for i, filename in enumerate(files_to_analyze, 1):
            local_raw_path = None
            try:
                print(f"Procesando {i}/{len(files_to_analyze)}: {filename}")
                
                local_raw_path = self.download_from_hostinger(f'raw/{filename}')
                
                with open(local_raw_path, 'r', encoding='utf-8') as f:
                    medida_data = json.load(f)
                
                analysis = self.analyzer.analyze_medida(medida_data)
                resultados.append(analysis)
                
                analysis_filename = Path(filename).stem + "_analysis_agnostic.json"
                import tempfile
                temp_dir = tempfile.gettempdir()
                local_analysis_path = os.path.join(temp_dir, analysis_filename)
                
                with open(local_analysis_path, 'w', encoding='utf-8') as f:
                    json.dump(analysis, f, ensure_ascii=False, indent=2)
                
                self.upload_analysis_to_hostinger(local_analysis_path)
                
            except Exception as e:
                error_info = f"Error procesando {filename}: {e}"
                errores.append(error_info)
                print(f"✗ {error_info}")
            finally:
                if local_raw_path and os.path.exists(local_raw_path):
                    os.remove(local_raw_path)
        
        self.save_batch_summary(resultados, errores)
        
        return {
            'total_analizadas': len(resultados),
            'total_errores': len(errores)
        }

    def list_hostinger_files(self, remote_dir):
        """Lista archivos en un directorio de Hostinger."""
        import ftplib
        FTP_HOST = "ftp.agoraenlared.com"
        FTP_USER = "u112219758.boria"
        FTP_PASS = os.getenv('HOSTINGER_FTP_PASSWORD', "Marta1664?")
        
        try:
            ftp = ftplib.FTP(FTP_HOST)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(f'data/{remote_dir}')
            files = ftp.nlst()
            ftp.quit()
            return [f for f in files if f not in ('.', '..')]
        except Exception as e:
            print(f"Error listando archivos en Hostinger: {e}")
            return []

    def download_from_hostinger(self, remote_path):
        """Descarga un archivo desde Hostinger a una carpeta temporal."""
        import ftplib
        import tempfile
        FTP_HOST = "ftp.agoraenlared.com"
        FTP_USER = "u112219758.boria"
        FTP_PASS = os.getenv('HOSTINGER_FTP_PASSWORD', "Marta1664?")
        
        temp_dir = tempfile.gettempdir()
        local_filepath = os.path.join(temp_dir, Path(remote_path).name)
        
        ftp = ftplib.FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)
        
        full_remote_path = f'data/{remote_path}'
        with open(local_filepath, 'wb') as f:
            ftp.retrbinary(f'RETR {full_remote_path}', f.write)
        
        ftp.quit()
        print(f"✓ Archivo '{Path(remote_path).name}' descargado a temporal.")
        return local_filepath

    def upload_analysis_to_hostinger(self, local_filepath):
        """Sube un archivo de análisis a la carpeta 'analyzed' en Hostinger."""
        import ftplib
        FTP_HOST = "ftp.agoraenlared.com"
        FTP_USER = "u112219758.boria"
        FTP_PASS = os.getenv('HOSTINGER_FTP_PASSWORD', "Marta1664?")
        
        ftp = ftplib.FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)
        
        target_dir = 'data/analyzed'
        ftp.cwd(target_dir)
        
        filename = Path(local_filepath).name
        with open(local_filepath, 'rb') as f:
            ftp.storbinary(f'STOR {filename}', f)
        
        ftp.quit()
        print(f"✓ Análisis '{filename}' subido a Hostinger.")
    
    def save_batch_summary(self, resultados, errores):
    """Crea el resumen del análisis y lo sube a Hostinger."""
    import tempfile
    import ftplib

    if not resultados:
        return # No hay nada que resumir

    summary = {
        'fecha_analisis_batch': datetime.now().isoformat(),
        'enfoque': 'agnostico_sin_presupuestos',
        'total_medidas_analizadas': len(resultados),
        'total_errores': len(errores),
        'estadisticas_generales': self.calculate_batch_stats(resultados),
        'errores_detalle': errores
    }

    # Guardar el resumen en un archivo temporal local
    temp_dir = tempfile.gettempdir()
    summary_path = os.path.join(temp_dir, "batch_analysis_summary_agnostic.json")

    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # Subir el archivo de resumen a Hostinger
    print("Subiendo resumen del análisis a Hostinger...")
    try:
        FTP_HOST = "ftp.agoraenlared.com"
        FTP_USER = "u112219758.boria"
        FTP_PASS = os.getenv('HOSTINGER_FTP_PASSWORD', "Marta1664?")

        ftp = ftplib.FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)

        # El resumen va en la carpeta 'analyzed' junto con los otros análisis
        ftp.cwd('data/analyzed') 

        with open(summary_path, 'rb') as f:
            ftp.storbinary(f'STOR {Path(summary_path).name}', f)

        ftp.quit()
        print(f"✓ Resumen batch subido a Hostinger.")
    except Exception as e:
        print(f"✗ Error al subir el resumen batch: {e}")
    
    def calculate_batch_stats(self, resultados):
        """Estadísticas generales del batch"""
        if not resultados:
            return {}
        
        stats = {
            'medidas_con_considerandos': 0,
            'medidas_con_pdfs': 0,
            'organismos_detectados': set(),
            'temas_emergentes': {},
            'niveles_riesgo': {'bajo': 0, 'medio': 0, 'alto': 0, 'critico': 0}
        }
        
        for resultado in resultados:
            # Considerandos
            literal = resultado.get('analisis_literal', {})
            if literal.get('estadisticas_texto', {}).get('tiene_considerandos'):
                stats['medidas_con_considerandos'] += 1
            
            # PDFs
            if literal.get('tiene_pdfs'):
                stats['medidas_con_pdfs'] += 1
            
            # Organismos (desde IA)
            elementos_ia = literal.get('elementos_detectados_ia', {})
            entidades = elementos_ia.get('entidades_mencionadas', [])
            stats['organismos_detectados'].update(entidades)
            
            # Temas emergentes
            semantico = resultado.get('analisis_semantico', {})
            categoria = semantico.get('categoria_emergente', 'sin_clasificar')
            stats['temas_emergentes'][categoria] = stats['temas_emergentes'].get(categoria, 0) + 1
            
            # Niveles de riesgo
            diablo = resultado.get('analisis_abogado_diablo', {})
            riesgo = diablo.get('nivel_riesgo_democratico', 'bajo')
            if riesgo in stats['niveles_riesgo']:
                stats['niveles_riesgo'][riesgo] += 1
        
        # Convertir set a lista para JSON
        stats['organismos_detectados'] = sorted(list(stats['organismos_detectados']))
        
        return stats

def main():
    print("=== ANÁLISIS TRIPLE AGNÓSTICO DE MEDIDAS ===")
    print("Enfoque: Sin categorías predefinidas, detección semántica con IA")
    
    batch_analyzer = BatchAnalyzer()
    resultado = batch_analyzer.analyze_all_measures_in_directory()
    
    print(f"\n=== ANÁLISIS COMPLETADO ===")
    print(f"Medidas analizadas: {resultado['total_analizadas']}")
    print(f"Errores: {resultado['total_errores']}")
    print("Revisá los archivos *_analysis_agnostic.json para ver resultados detallados")

if __name__ == "__main__":

    main()
