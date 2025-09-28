#!/usr/bin/env python3
import os
import sys
from pathlib import Path

# Agregar src al path
sys.path.append(str(Path(__file__).parent / "src"))

def test_complete_system():
    """Test completo del sistema"""
    print("=== TEST SISTEMA COMPLETO BORA ===")
    
    # Test 1: Scraping básico
    print("\n1. Testing scraping...")
    try:
        from scraper import BoraScraperCore
        scraper = BoraScraperCore()
        
        # Test con 2 medidas del primer día
        resultado = scraper.scrape_fecha_especifica("2023-12-11", limit=2)
        print(f"✓ Scraping test: {resultado} medidas guardadas")
        
    except Exception as e:
        print(f"✗ Error en scraping: {e}")
        return False
    
    # Test 2: Análisis agnóstico
    print("\n2. Testing análisis agnóstico...")
    try:
        from analyzer import BatchAnalyzer
        analyzer = BatchAnalyzer()
        
        # Analizar las medidas scrapeadas
        resultado = analyzer.analyze_all_measures_in_directory()
        print(f"✓ Análisis test: {resultado['total_analizadas']} medidas analizadas")
        print(f"Errores: {resultado['total_errores']}")
        
    except Exception as e:
        print(f"✗ Error en análisis: {e}")
        return False
    
    # Test 3: Verificar archivos generados
    print("\n3. Verificando archivos generados...")
    data_dir = Path("data")
    
    medidas_raw = list(data_dir.glob("medida_*.json"))
    analisis_files = list(data_dir.glob("*_analysis_agnostic.json"))
    
    print(f"✓ Archivos de medidas: {len(medidas_raw)}")
    print(f"✓ Archivos de análisis: {len(analisis_files)}")
    
    if len(medidas_raw) > 0 and len(analisis_files) > 0:
        print("\n✓ SISTEMA FUNCIONANDO CORRECTAMENTE")
        print("Podés proceder con scraping completo")
        return True
    else:
        print("\n✗ Sistema no generó archivos esperados")
        return False

if __name__ == "__main__":
    # Configurar API key temporal para test
    if not os.getenv('GEMINI_API_KEY'):
        api_key = input("Ingresá tu GEMINI_API_KEY para el test: ")
        os.environ['GEMINI_API_KEY'] = api_key
    
    test_complete_system()