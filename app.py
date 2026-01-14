#!/usr/bin/env python3
"""
Base44 API Server - Rent Roll Extraction REST API
==================================================

KORRIGIERTE VERSION - Fixes:
1. Import-Pfad korrigiert (rent_roll_extraktor_v2_1_FINAL statt _updated)
2. Verbesserte CORS-Konfiguration für Base44
3. Bessere Fehlerbehandlung

Autor: Christian Kaun @ Cognaize
Version: 1.0.1
Datum: 14. Januar 2026

Endpoints:
- POST /api/extract/excel - Excel-Datei extrahieren
- POST /api/extract/pdf - PDF-Datei extrahieren  
- GET /api/health - Health Check
- GET /api/schema - Verfügbare Felder abrufen

Verwendung:
    python base44_api_server.py --port 8080
"""

import argparse
import json
import os
import tempfile
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Flask für REST API
try:
    from flask import Flask, request, jsonify, send_file, make_response
    from flask_cors import CORS
except ImportError:
    print("Flask nicht installiert. Installiere mit:")
    print("  pip install flask flask-cors")
    exit(1)

# KORRIGIERT: Import des Extraktors mit richtigem Dateinamen
try:
    from rent_roll_extraktor_v2_1_FINAL import (
        RentRollExcelReader,
        NumberUnitParser,
        DataValidator,
        ExtractionResult
    )
except ImportError as e:
    print(f"FEHLER: Kann rent_roll_extraktor_v2_1_FINAL.py nicht importieren: {e}")
    print("Stelle sicher, dass die Datei im gleichen Verzeichnis liegt!")
    exit(1)

# Optional: PDF-Extraktion
try:
    import pdfplumber
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
    print("⚠️ pdfplumber nicht installiert - PDF-Extraktion deaktiviert")

# ============================================================================
# FLASK APP MIT KORRIGIERTER CORS-KONFIGURATION
# ============================================================================

app = Flask(__name__)

# ERWEITERTE CORS-KONFIGURATION für Base44
CORS(app, resources={
    r"/api/*": {
        "origins": ["*"],  # Erlaubt alle Origins (für Entwicklung)
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization", "X-Requested-With"],
        "expose_headers": ["Content-Type", "X-Total-Count"],
        "supports_credentials": False,
        "max_age": 3600
    }
})

# Global instances
excel_reader = RentRollExcelReader()
validator = DataValidator()

# ============================================================================
# CORS PREFLIGHT HANDLER
# ============================================================================

@app.after_request
def after_request(response):
    """Füge CORS-Header zu jeder Response hinzu."""
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Requested-With')
    response.headers.add('Access-Control-Allow-Methods', 'GET,POST,OPTIONS')
    response.headers.add('Access-Control-Max-Age', '3600')
    return response

@app.route('/api/<path:path>', methods=['OPTIONS'])
def handle_options(path):
    """Handle CORS preflight requests."""
    response = make_response()
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Requested-With')
    response.headers.add('Access-Control-Allow-Methods', 'GET,POST,OPTIONS')
    response.headers.add('Access-Control-Max-Age', '3600')
    return response

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def allowed_file(filename: str, allowed_extensions: set) -> bool:
    """Check if file has allowed extension."""
    if not filename:
        return False
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in allowed_extensions

def extract_pdf_text(filepath: Path) -> str:
    """Extract text from PDF using pdfplumber."""
    if not PDF_SUPPORT:
        raise RuntimeError("PDF support not available - install pdfplumber")
    
    text_parts = []
    with pdfplumber.open(filepath) as pdf:
        for page in pdf.pages:
            # Text extrahieren
            text = page.extract_text()
            if text:
                text_parts.append(text)
            
            # Tabellen extrahieren
            tables = page.extract_tables()
            for table in tables:
                if table:
                    for row in table:
                        if row:
                            text_parts.append('\t'.join(str(cell or '') for cell in row))
    
    return '\n'.join(text_parts)

def format_response(result: ExtractionResult) -> Dict[str, Any]:
    """Format ExtractionResult for JSON response."""
    return {
        'success': result.success,
        'message': result.message,
        'data': result.data,
        'metadata': {
            **result.metadata,
            'sheets_processed': result.sheets_processed,
            'total_rows_extracted': result.total_rows_extracted,
            'extraction_timestamp': datetime.now().isoformat()
        },
        'warnings': result.warnings,
        'validation_errors': []
    }

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/', methods=['GET'])
def root():
    """Root endpoint - redirect to health."""
    return jsonify({
        'message': 'Rent Roll Extraction API Server',
        'version': '2.1.0',
        'endpoints': {
            'health': '/api/health',
            'schema': '/api/schema',
            'extract_excel': '/api/extract/excel (POST)',
            'extract_pdf': '/api/extract/pdf (POST)',
            'validate': '/api/validate (POST)'
        }
    })

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'version': '2.1.0',
        'timestamp': datetime.now().isoformat(),
        'capabilities': {
            'excel': True,
            'pdf': PDF_SUPPORT,
            'multi_sheet': True,
            'unit_separation': True,
            'locale_aware': True
        }
    })

@app.route('/api/schema', methods=['GET'])
def get_schema():
    """Return available canonical fields and their descriptions."""
    schema = {
        'core_fields': {
            'unit_id': 'Eindeutige Einheits-ID',
            'tenant_name': 'Name des Mieters',
            'tenant_id': 'Numerische Mieter-ID (falls vorhanden)',
            'contractual_partner': 'Vertragspartner (Fallback für Mieter)',
            'contract_id': 'Vertragsnummer',
            'asset_id': 'SAP-Objektnummer / Asset-ID'
        },
        'area_fields': {
            'area_sqm_value': 'Fläche in m² (Zahlenwert)',
            'area_sqm_unit': 'Flächen-Einheit (m²)',
            'area_sqft_value': 'Fläche in sqft (Zahlenwert)',
            'area_sqft_unit': 'Flächen-Einheit (sqft)'
        },
        'rent_fields': {
            'monthly_rent_value': 'Monatliche Miete (Zahlenwert)',
            'monthly_rent_unit': 'Währung (EUR, USD, etc.)',
            'annual_rent_value': 'Jahresmiete (Zahlenwert)',
            'annual_rent_unit': 'Währung'
        },
        'date_fields': {
            'lease_start': 'Mietbeginn',
            'lease_end': 'Mietende',
            'break_date': 'Kündigungsoption'
        },
        'status_fields': {
            'status': 'Belegungsstatus (Vermietet, Leer, etc.)',
            'usage_type': 'Nutzungsart (Büro, Retail, etc.)',
            'lease_type': 'Vertragsart'
        },
        'meta_fields': {
            '_source_file': 'Quelldatei',
            '_source_sheet': 'Quell-Sheet',
            '_source_row': 'Quell-Zeile (1-indexed)',
            '_extraction_timestamp': 'Extraktionszeitpunkt'
        },
        'format_note': 'Felder mit Units werden in 3 Teile gesplittet: _value, _unit, _original'
    }
    return jsonify(schema)

@app.route('/api/extract/excel', methods=['POST', 'OPTIONS'])
def extract_excel():
    """
    Extract rent roll data from Excel file.
    
    Request:
        POST /api/extract/excel
        Content-Type: multipart/form-data
        Body: file=<excel_file>
        
    Optional params:
        - process_all_sheets: bool (default: true)
        - validate: bool (default: true)
    """
    if request.method == 'OPTIONS':
        return handle_options('extract/excel')
    
    # Check file
    if 'file' not in request.files:
        return jsonify({
            'success': False, 
            'error': 'No file provided',
            'hint': 'Send file as multipart/form-data with key "file"'
        }), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'}), 400
    
    # KORRIGIERT: Erweiterte Dateiformat-Prüfung
    filename = file.filename.lower()
    allowed_extensions = {'xlsx', 'xls', 'xlsm', 'xlsb'}
    
    if not allowed_file(filename, allowed_extensions):
        return jsonify({
            'success': False, 
            'error': f'Invalid file type: {filename}',
            'allowed': list(allowed_extensions),
            'hint': 'Please upload an Excel file (.xlsx, .xls, .xlsm)'
        }), 400
    
    # Parse options
    process_all_sheets = request.form.get('process_all_sheets', 'true').lower() == 'true'
    do_validate = request.form.get('validate', 'true').lower() == 'true'
    
    # Determine suffix from filename
    suffix = '.' + filename.rsplit('.', 1)[1] if '.' in filename else '.xlsx'
    
    # Save to temp file
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        file.save(tmp.name)
        tmp_path = Path(tmp.name)
    
    try:
        # Extract
        result = excel_reader.read_excel(tmp_path, process_all_sheets=process_all_sheets)
        response = format_response(result)
        
        # Validate if requested
        if do_validate and result.success:
            errors = validator.validate(result.data)
            response['validation_errors'] = [
                {
                    'severity': e.severity,
                    'row_index': e.row_index,
                    'field': e.field,
                    'message': e.message,
                    'value': e.value
                }
                for e in errors
            ]
        
        return jsonify(response)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'error_type': type(e).__name__,
            'traceback': traceback.format_exc()
        }), 500
        
    finally:
        # Cleanup
        if tmp_path.exists():
            tmp_path.unlink()

@app.route('/api/extract/pdf', methods=['POST', 'OPTIONS'])
def extract_pdf():
    """
    Extract rent roll data from PDF file.
    """
    if request.method == 'OPTIONS':
        return handle_options('extract/pdf')
    
    if not PDF_SUPPORT:
        return jsonify({
            'success': False, 
            'error': 'PDF support not available',
            'hint': 'Install pdfplumber: pip install pdfplumber'
        }), 501
    
    # Check file
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'}), 400
    
    if not allowed_file(file.filename, {'pdf'}):
        return jsonify({
            'success': False, 
            'error': 'Invalid file type. Only PDF allowed',
            'filename': file.filename
        }), 400
    
    # Save to temp file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp:
        file.save(tmp.name)
        tmp_path = Path(tmp.name)
    
    try:
        # Extract text
        text = extract_pdf_text(tmp_path)
        
        # Count pages and tables
        page_count = 0
        tables_found = 0
        with pdfplumber.open(tmp_path) as pdf:
            page_count = len(pdf.pages)
            for page in pdf.pages:
                tables = page.extract_tables()
                tables_found += len([t for t in tables if t])
        
        return jsonify({
            'success': True,
            'extracted_text': text,
            'text_length': len(text),
            'page_count': page_count,
            'tables_found': tables_found,
            'extraction_timestamp': datetime.now().isoformat(),
            'note': 'PDF text extracted. Use LLM for structured extraction.'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500
        
    finally:
        # Cleanup
        if tmp_path.exists():
            tmp_path.unlink()

@app.route('/api/parse/number', methods=['POST', 'OPTIONS'])
def parse_number():
    """Parse a number string with unit separation."""
    if request.method == 'OPTIONS':
        return handle_options('parse/number')
    
    data = request.get_json()
    if not data or 'value' not in data:
        return jsonify({'success': False, 'error': 'No value provided'}), 400
    
    parser = NumberUnitParser()
    result = parser.parse(data['value'], data.get('context'))
    
    return jsonify({
        'success': True,
        'value': result.value,
        'unit': result.unit,
        'original': result.original_text
    })

@app.route('/api/validate', methods=['POST', 'OPTIONS'])
def validate_data():
    """Validate extracted rent roll data."""
    if request.method == 'OPTIONS':
        return handle_options('validate')
    
    data = request.get_json()
    if not data or 'data' not in data:
        return jsonify({'success': False, 'error': 'No data provided'}), 400
    
    records = data['data']
    errors = validator.validate(records)
    
    error_list = [e for e in errors if e.severity == 'error']
    warning_list = [e for e in errors if e.severity == 'warning']
    
    return jsonify({
        'valid': len(error_list) == 0,
        'errors': [
            {'row': e.row_index, 'field': e.field, 'message': e.message, 'value': e.value}
            for e in error_list
        ],
        'warnings': [
            {'row': e.row_index, 'field': e.field, 'message': e.message, 'value': e.value}
            for e in warning_list
        ],
        'summary': {
            'total_records': len(records),
            'error_count': len(error_list),
            'warning_count': len(warning_list)
        }
    })

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Base44 API Server for Rent Roll Extraction')
    parser.add_argument('--port', type=int, default=8080, help='Port to run server on')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to bind to (0.0.0.0 for all interfaces)')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    
    args = parser.parse_args()
    
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║          BASE44 RENT ROLL EXTRACTION API SERVER              ║
╠══════════════════════════════════════════════════════════════╣
║  Version: 2.1.1 (CORS-Fix + Import-Fix)                      ║
║  Host:    http://{args.host}:{args.port}                            ║
╠══════════════════════════════════════════════════════════════╣
║  Endpoints:                                                  ║
║    GET  /api/health        - Health check                    ║
║    GET  /api/schema        - Available fields                ║
║    POST /api/extract/excel - Extract from Excel              ║
║    POST /api/extract/pdf   - Extract from PDF                ║
║    POST /api/parse/number  - Parse number with unit          ║
║    POST /api/validate      - Validate extracted data         ║
╠══════════════════════════════════════════════════════════════╣
║  PDF Support: {'✅ Enabled' if PDF_SUPPORT else '❌ Disabled (install pdfplumber)'}                           ║
║  CORS:        ✅ Enabled for all origins                     ║
╚══════════════════════════════════════════════════════════════╝
""")
    
    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)

if __name__ == '__main__':
    main()
