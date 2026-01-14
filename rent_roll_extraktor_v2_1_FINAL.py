#!/usr/bin/env python3
"""
Rent Roll Extraktor v2.1 FINAL - Production-Ready für 98%+ Genauigkeit
======================================================================

Autor: Christian Kaun @ Cognaize
Version: 2.1.0 FINAL
Datum: 13. Januar 2026

KRITISCHE FEATURES (gegenüber v2.0):
1. ✅ NumberUnitParser - KEINE automatische Unit-Konvertierung
2. ✅ Unit-Separation - value/unit/original in separate Felder
3. ✅ Multi-Sheet Processing - ALLE Sheets verarbeiten
4. ✅ Locale-aware Number Parsing - European vs US Format
5. ✅ Negative Zahlen in Klammern - Accounting-Format
6. ✅ Prozent-Handling mit Unit
7. ✅ Asset-Identifikation (_source_file, _source_sheet, _source_row)

Basiert auf:
- Google Drive: "Refined Data Extraction Prompt – Rent Roll Consolidation"
- Master_Header_Mapping_MultiLanguage_3_sheets_updated.xlsx
- CRITICAL_UPDATES_V2_1.md
- number_parsing_rules.json
"""

import json
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class NumberWithUnit:
    """
    Represents a number separated from its unit.
    CRITICAL: This ensures no automatic conversion occurs.
    """
    value: Optional[float]
    unit: Optional[str]
    original_text: str
    
    def to_dict(self, field_name: str) -> Dict[str, Any]:
        """Convert to dict with proper field naming for output."""
        if self.unit:
            return {
                f'{field_name}_value': self.value,
                f'{field_name}_unit': self.unit,
                f'{field_name}_original': self.original_text
            }
        else:
            return {field_name: self.value}


@dataclass
class ExtractionResult:
    """Result of extraction operation."""
    success: bool
    data: List[Dict[str, Any]]
    message: str
    warnings: List[str]
    metadata: Dict[str, Any]
    sheets_processed: int = 0
    total_rows_extracted: int = 0


@dataclass
class ValidationError:
    """Validation error details."""
    severity: str  # 'error' or 'warning'
    row_index: int
    field: str
    message: str
    value: Any


# ============================================================================
# NUMBER UNIT PARSER - CRITICAL COMPONENT
# ============================================================================

class NumberUnitParser:
    """
    CRITICAL: Parses numbers with units WITHOUT conversion.
    
    Based on Google Drive requirements:
    - "Do not perform any unit or currency conversions – simply take the data as-is."
    - "If a cell contains a number with a currency symbol or unit, split it into two fields"
    
    Implements:
    - European number format: 1.234,56
    - US number format: 1,234.56
    - Swiss number format: 1'234.56
    - Negative numbers in parentheses: (500) → -500
    - Currency symbols: €, $, £, CHF, zł, kr
    - Area units: m², sqft, sqm, sf, ft²
    - Percentage: 95%
    """
    
    # Currency symbols → ISO code mapping
    CURRENCY_SYMBOLS = {
        '€': 'EUR', '$': 'USD', '£': 'GBP',
        'zł': 'PLN', 'kr': 'SEK', '₹': 'INR', '¥': 'JPY',
        '元': 'CNY'
    }
    
    # Special currency patterns (need word boundaries)
    CURRENCY_CODES_PATTERN = ['CHF', 'Fr.']
    
    # ISO currency codes (direct recognition)
    ISO_CURRENCIES = {'EUR', 'USD', 'GBP', 'CHF', 'PLN', 'SEK', 'DKK', 'NOK', 
                      'CZK', 'HUF', 'INR', 'JPY', 'CNY', 'CAD', 'AUD', 'NZD'}
    
    # Area units → normalized form
    AREA_UNITS = {
        'm²': 'm²', 'm2': 'm²', 'sqm': 'm²', 'qm': 'm²',
        'sqft': 'sqft', 'sf': 'sqft', 'ft²': 'sqft', 'ft2': 'sqft',
        'square feet': 'sqft', 'square metres': 'm²', 'square meters': 'm²'
    }
    
    # Approximate value prefixes
    APPROX_PREFIXES = ['approx.', 'ca.', '~', 'circa', 'ungefähr', 'environ', 'about', 'roughly']
    
    # Operators to remove
    OPERATORS = ['>=', '<=', '>', '<', '=', '≥', '≤']
    
    def __init__(self):
        # Compile regex patterns
        # Currency symbols only - Fr. pattern removed to avoid false positives
        self.currency_symbol_pattern = re.compile(r'([€$£¥₹元])')
        self.currency_code_pattern = re.compile(r'\b(EUR|USD|GBP|CHF|PLN|SEK|DKK|NOK|CZK|HUF|CAD|AUD|INR|JPY|CNY|NZD)\b', re.IGNORECASE)
        self.percent_pattern = re.compile(r'(\d+(?:[.,]\d+)?)\s*%')
        self.area_pattern = re.compile(r'(m²|m2|sqm|qm|sqft|sf|ft²|ft2|square\s*(?:feet|metres?|meters?))', re.IGNORECASE)
        self.parentheses_negative = re.compile(r'^\s*\(([^)]+)\)\s*$')
        self.number_pattern = re.compile(r"-?[\d.,']+")
    def parse(self, value: Any, context_hint: Optional[str] = None) -> NumberWithUnit:
        """
        Parse a value into number and unit WITHOUT conversion.
        
        Args:
            value: The value to parse (string, int, float, etc.)
            context_hint: Optional hint about the field type ('currency', 'area', 'percentage')
        
        Returns:
            NumberWithUnit object with separated value and unit
        
        CRITICAL: This method NEVER converts units or currencies.
        """
        # Handle None/NaN/empty
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return NumberWithUnit(None, None, '')
        
        # Handle pure numbers (already numeric)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return NumberWithUnit(float(value), None, str(value))
        
        # Convert to string and strip
        original = str(value).strip()
        
        # Handle empty strings
        if not original or original.lower() in ('', '-', '–', 'n/a', 'na', 'null', 'none', '00:00:00'):
            return NumberWithUnit(None, None, original)
        
        # Clean approximate prefixes and operators FIRST
        cleaned = self._clean_prefixes(original)
        
        # Check for percentage first
        if '%' in cleaned:
            return self._parse_percentage(original)
        
        # Check for currency
        if self.currency_symbol_pattern.search(cleaned) or self.currency_code_pattern.search(cleaned):
            return self._parse_currency(original)
        
        # Check for area units
        if self.area_pattern.search(cleaned):
            return self._parse_area(original)
        
        # Check for other units (Jahre, months, mio, etc.)
        other_unit = self._extract_other_unit(cleaned)
        if other_unit:
            return self._parse_with_unit(original, other_unit)
        
        # Pure number parsing - use cleaned string!
        number = self._parse_numeric_string(cleaned)
        return NumberWithUnit(number, None, original)
    
    def _parse_percentage(self, text: str) -> NumberWithUnit:
        """Parse percentage value: 95% → value: 95, unit: '%'"""
        # Remove approximate prefixes and operators
        cleaned = self._clean_prefixes(text)
        
        match = self.percent_pattern.search(cleaned)
        if match:
            num_str = match.group(1)
            number = self._parse_numeric_string(num_str)
            return NumberWithUnit(number, '%', text)
        
        # Fallback: try to extract any number
        number = self._parse_numeric_string(cleaned.replace('%', ''))
        return NumberWithUnit(number, '%', text)
    
    def _parse_currency(self, text: str) -> NumberWithUnit:
        """
        Parse currency value: 1.000 € → value: 1000, unit: EUR
        CRITICAL: NEVER convert currencies.
        """
        cleaned = self._clean_prefixes(text)
        
        # Find currency symbol or code
        currency = None
        
        # Check for symbol first
        symbol_match = self.currency_symbol_pattern.search(cleaned)
        if symbol_match:
            symbol = symbol_match.group(1)
            currency = self.CURRENCY_SYMBOLS.get(symbol, symbol)
            cleaned = self.currency_symbol_pattern.sub('', cleaned)
        
        # Check for ISO code
        code_match = self.currency_code_pattern.search(cleaned)
        if code_match:
            currency = code_match.group(1).upper()
            cleaned = self.currency_code_pattern.sub('', cleaned)
        
        # Parse the numeric part
        number = self._parse_numeric_string(cleaned)
        
        return NumberWithUnit(number, currency, text)
    
    def _parse_area(self, text: str) -> NumberWithUnit:
        """
        Parse area value: 500 m² → value: 500, unit: m²
        CRITICAL: NEVER convert sqft to m² or vice versa.
        """
        cleaned = self._clean_prefixes(text)
        
        # Find area unit
        match = self.area_pattern.search(cleaned)
        if match:
            unit_raw = match.group(1).lower().strip()
            unit = self.AREA_UNITS.get(unit_raw, unit_raw)
            # Remove the unit from string
            cleaned = self.area_pattern.sub('', cleaned)
        else:
            unit = None
        
        # Parse numeric part
        number = self._parse_numeric_string(cleaned)
        
        return NumberWithUnit(number, unit, text)
    
    def _extract_other_unit(self, text: str) -> Optional[str]:
        """Extract other units like Jahre, months, mio, etc."""
        text_lower = text.lower()
        
        # Common time units
        time_units = {
            'jahre': 'Jahre', 'year': 'years', 'years': 'years', 'yr': 'years',
            'monate': 'Monate', 'month': 'months', 'months': 'months', 'mo': 'months',
            'tage': 'Tage', 'day': 'days', 'days': 'days',
            'wochen': 'Wochen', 'week': 'weeks', 'weeks': 'weeks'
        }
        
        # Magnitude units
        magnitude_units = {
            'mio': 'mio', 'million': 'mio', 'mrd': 'mrd', 'billion': 'billion',
            'tsd': 'tsd', 'thousand': 'thousand', 'k': 'k'
        }
        
        # Check for matches
        for key, val in {**time_units, **magnitude_units}.items():
            pattern = r'\b' + re.escape(key) + r'\b'
            if re.search(pattern, text_lower):
                return val
        
        return None
    
    def _parse_with_unit(self, text: str, unit: str) -> NumberWithUnit:
        """Parse a value with a detected unit."""
        cleaned = self._clean_prefixes(text)
        
        # Remove the unit
        for key in [unit.lower(), unit]:
            cleaned = re.sub(r'\b' + re.escape(key) + r'\b', '', cleaned, flags=re.IGNORECASE)
        
        number = self._parse_numeric_string(cleaned)
        return NumberWithUnit(number, unit, text)
    
    def _clean_prefixes(self, text: str) -> str:
        """Remove approximate prefixes and operators."""
        cleaned = text.strip()
        
        # Remove approximate prefixes (case-insensitive)
        for prefix in self.APPROX_PREFIXES:
            if cleaned.lower().startswith(prefix.lower()):
                cleaned = cleaned[len(prefix):].strip()
                break
        
        # Remove operators
        for op in self.OPERATORS:
            if cleaned.startswith(op):
                cleaned = cleaned[len(op):].strip()
                break
        
        return cleaned
    
    def _parse_numeric_string(self, text: str) -> Optional[float]:
        """
        Parse a numeric string with locale awareness.
        
        Handles:
        - European format: 1.234,56 → 1234.56
        - US format: 1,234.56 → 1234.56
        - Swiss format: 1'234.56 → 1234.56
        - Negative in parentheses: (500) → -500
        - Negative with minus: -500 → -500
        """
        if not text:
            return None
        
        cleaned = text.strip()
        
        # Handle negative numbers in parentheses (accounting format)
        is_negative = False
        paren_match = self.parentheses_negative.match(cleaned)
        if paren_match:
            cleaned = paren_match.group(1)
            is_negative = True
        elif cleaned.startswith('-'):
            is_negative = True
            cleaned = cleaned[1:]
        elif cleaned.startswith('−'):  # Unicode minus
            is_negative = True
            cleaned = cleaned[1:]
        
        # Extract just the numeric part
        numeric_chars = re.findall(r"[\d.,']", cleaned)
        if not numeric_chars:
            return None
        
        num_str = ''.join(numeric_chars)
        
        # Determine locale format
        has_comma = ',' in num_str
        has_dot = '.' in num_str
        has_apostrophe = "'" in num_str
        
        # Swiss format: 1'234.56
        if has_apostrophe:
            num_str = num_str.replace("'", "")
            # After removing apostrophes, treat as US format
            has_comma = ',' in num_str
            has_dot = '.' in num_str
        
        try:
            if has_comma and has_dot:
                # Both present - last separator is decimal
                last_comma = num_str.rfind(',')
                last_dot = num_str.rfind('.')
                
                if last_comma > last_dot:
                    # European: 1.234,56
                    num_str = num_str.replace('.', '').replace(',', '.')
                else:
                    # US: 1,234.56
                    num_str = num_str.replace(',', '')
            
            elif has_comma and not has_dot:
                # Only comma - check position
                parts = num_str.split(',')
                if len(parts) == 2 and len(parts[1]) == 3:
                    # Likely thousand separator: 1,234
                    num_str = num_str.replace(',', '')
                elif len(parts) == 2 and len(parts[1]) <= 2:
                    # Likely decimal: 1234,56
                    num_str = num_str.replace(',', '.')
                else:
                    # Multiple commas - thousand separators
                    num_str = num_str.replace(',', '')
            
            elif has_dot and not has_comma:
                # Only dot - check position
                parts = num_str.split('.')
                if len(parts) == 2 and len(parts[1]) == 3:
                    # Likely thousand separator: 1.234
                    num_str = num_str.replace('.', '')
                # else: treat as decimal
            
            result = float(num_str)
            return -abs(result) if is_negative else result
            
        except (ValueError, TypeError):
            return None


# ============================================================================
# HEADER MAPPER
# ============================================================================

class HeaderMapper:
    """Maps raw headers to canonical field names using synonyms."""
    
    # Default synonyms (subset - full mapping loaded from JSON)
    DEFAULT_SYNONYMS = {
        'unit_id': ['unit id', 'unit-id', 'unit number', 'einheit', 'einheit nr', 'unit no', 
                    'mieteinheit', 'numéro d\'unité', 'numero unità', 'eenheidnummer'],
        'tenant_name': ['tenant', 'tenant name', 'mieter', 'mietername', 'locataire', 
                        'nom du locataire', 'inquilino', 'huurder', 'najemca', 'hyresgäst',
                        'customer', 'kunde', 'client'],
        'area_sqm': ['area sqm', 'area (sqm)', 'fläche m²', 'fläche (m²)', 'sqm', 'm²', 'm2',
                     'superficie (m²)', 'oppervlakte (m²)', 'powierzchnia (m²)'],
        'area_sqft': ['area sqft', 'area (sqft)', 'sqft', 'sq ft', 'square feet', 'sf'],
        'monthly_rent': ['monthly rent', 'rent', 'miete', 'monatliche miete', 'monatsmiete',
                         'loyer mensuel', 'affitto mensile', 'maandelijkse huur', 'czynsz miesięczny',
                         'nkm', 'nkm ist-miete', 'netto kaltmiete', 'cold rent'],
        'annual_rent': ['annual rent', 'yearly rent', 'jahresmiete', 'loyer annuel', 
                        'affitto annuale', 'jaarlijkse huur', 'roczny czynsz'],
        'lease_start': ['lease start', 'lease start date', 'start date', 'mietbeginn',
                        'vertragsbeginn', 'début du bail', 'inizio locazione', 'startdatum'],
        'lease_end': ['lease end', 'lease end date', 'end date', 'mietende', 'vertragsende',
                      'laufzeitende', 'fin du bail', 'fine locazione', 'einddatum'],
        'status': ['status', 'occupancy', 'belegung', 'état', 'stato', 'bezettingsstatus'],
        'currency': ['currency', 'währung', 'devise', 'valuta', 'moneda'],
        'lease_type': ['lease type', 'vertragsart', 'mietvertragsart', 'type de bail'],
        'usage_type': ['usage type', 'nutzungsart', 'type d\'utilisation', 'uso'],
        'occupancy_rate': ['occupancy rate', 'belegungsquote', 'taux d\'occupation'],
        'service_charge': ['service charge', 'nebenkosten', 'betriebskosten', 'nk', 
                           'charges', 'oneri accessori', 'servicekosten'],
        'parking_spaces': ['parking', 'parking spaces', 'parkplätze', 'stellplätze',
                           'places de parking', 'posti auto', 'parkeerplaatsen']
        ,
        #
        # Additional synonyms to support extended tenant and unit identification
        #
        # SAP object number (often used as a unique asset identifier)
        'sap_object_number': [
            'sap objektnummer', 'sap-objektnummer', 'sap object number', 'sap object',
            'sap-objektnr', 'sap objektnr', 'sap id', 'sap-object', 'sap objekt id',
            'sap-objekt'  # German variations
        ],

        # Composite unit identifier (e.g., MO/BK/Wirtschaftseinheit/MO-Nr)
        'composite_unit_id': [
            'mo/bk/wirtschaftseinheit/mo-nr', 'mo/bk/wirtschaftseinheit/mo',
            'mo/bk/wi/mo-nr', 'mo-bk-wirtschaftseinheit-mo-nr',
            'mo / bk / wirtschaftseinheit / mo-nr', 'composite unit id',
            'composite unit', 'mo bk wirtschaftseinheit mo nr'
        ],

        # Individual MO number (Mietobjekt-Nummer)
        'mo_number': [
            'mo nummer', 'mo-nummer', 'mo nr', 'mo-nr', 'mo number', 'mo no',
            'mietobjekt nummer', 'mietobjekt-nr', 'mietobjekt-nr.'
        ],

        # Business unit (Wirtschaftseinheit)
        'business_unit': [
            'wirtschaftseinheit', 'business unit', 'wirtschafts einheit',
            'wi einheit', 'we', 'we einheit'
        ],

        # Business unit code / abbreviation (Kürzel der Wirtschaftseinheit)
        'business_unit_code': [
            'kürzel der wirtschaftseinheit', 'wirtschaftseinheit kürzel', 'we kürzel',
            'unit code', 'einheitskürzel', 'c', 'we code'
        ],

        # Bookkeeping area (Buchungskreis)
        'bookkeeping_area': [
            'bk', 'buchungskreis', 'booking area', 'buchungsbereich'
        ],

        # Contractual partner (Vertragspartner) – often used as fallback tenant name
        'contractual_partner': [
            'vertragspartner', 'vrtragspartner', 'contractual partner', 'contract partner'
        ],

        # Contract ID (Vertragsnummer) – extend synonyms beyond default definitions
        'contract_id': [
            'contract id', 'stammvertrag-id', 'vertragsnummer', 'vertrags-nr',
            'vertragsnr', 'vertrags nr', 'contract number', 'contract no.', 'u'
        ]
    }
    
    # Field type hints for parsing
    FIELD_TYPE_HINTS = {
        'monthly_rent': 'currency',
        'annual_rent': 'currency',
        'market_rent': 'currency',
        'rent_reduction': 'currency',
        'rent_parking_monthly': 'currency',
        'rent_other_income': 'currency',
        'service_charge': 'currency',
        'deposit': 'currency',
        'area_sqm': 'area',
        'area_sqft': 'area',
        'occupancy_rate': 'percentage',
        'escalation_rate': 'percentage',
        'indexation_rate': 'percentage',
        'vacancy_rate': 'percentage'
    }
    
    def __init__(self, synonyms_file: Optional[Path] = None):
        self.synonyms = self.DEFAULT_SYNONYMS.copy()
        self.parser = NumberUnitParser()
        
        # Load additional synonyms from file if provided
        if synonyms_file and synonyms_file.exists():
            self._load_synonyms_file(synonyms_file)
    
    def _load_synonyms_file(self, filepath: Path):
        """Load synonyms from JSON file."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for field, syns in data.items():
                    if not field.startswith('_'):  # Skip metadata
                        if field in self.synonyms:
                            self.synonyms[field].extend(syns)
                        else:
                            self.synonyms[field] = syns
        except Exception as e:
            print(f"Warning: Could not load synonyms file: {e}")
    
    def normalize_header(self, header: str) -> str:
        """Normalize header string for matching."""
        if pd.isna(header):
            return ''
        h = str(header).lower().strip()
        # Remove special characters but keep spaces
        h = re.sub(r'[^\w\s]', ' ', h)
        # Collapse multiple spaces
        h = re.sub(r'\s+', ' ', h)
        return h.strip()
    
    def map_header(self, raw_header: str) -> Optional[str]:
        """Map a raw header to its canonical field name."""
        normalized = self.normalize_header(raw_header)
        if not normalized:
            return None
        
        for canonical, synonyms in self.synonyms.items():
            for syn in synonyms:
                if self.normalize_header(syn) == normalized:
                    return canonical
                # Also check if normalized contains the synonym
                if len(syn) > 3 and self.normalize_header(syn) in normalized:
                    return canonical
        
        return None
    
    def get_field_type_hint(self, canonical_field: str) -> Optional[str]:
        """Get the type hint for a canonical field."""
        return self.FIELD_TYPE_HINTS.get(canonical_field)
    
    def detect_language(self, headers: List[str]) -> str:
        """Detect primary language from headers."""
        lang_scores = {'en': 0, 'de': 0, 'fr': 0, 'it': 0, 'nl': 0, 'pl': 0, 'sv': 0}
        
        de_words = ['mieter', 'fläche', 'miete', 'vertrag', 'einheit', 'nutzung']
        fr_words = ['locataire', 'loyer', 'bail', 'surface', 'unité']
        it_words = ['inquilino', 'affitto', 'locazione', 'superficie']
        nl_words = ['huurder', 'huur', 'oppervlakte', 'eenheid']
        pl_words = ['najemca', 'czynsz', 'powierzchnia', 'umowa']
        
        for header in headers:
            h_lower = str(header).lower()
            for word in de_words:
                if word in h_lower:
                    lang_scores['de'] += 1
            for word in fr_words:
                if word in h_lower:
                    lang_scores['fr'] += 1
            for word in it_words:
                if word in h_lower:
                    lang_scores['it'] += 1
            for word in nl_words:
                if word in h_lower:
                    lang_scores['nl'] += 1
            for word in pl_words:
                if word in h_lower:
                    lang_scores['pl'] += 1
        
        # Default to English if no other language detected
        if all(v == 0 for v in lang_scores.values()):
            return 'en'
        
        return max(lang_scores, key=lang_scores.get)


# ============================================================================
# SHEET SELECTOR
# ============================================================================

class SheetSelector:
    """Selects the appropriate worksheet(s) from an Excel file."""
    
    # Keywords indicating rent roll sheet
    SHEET_KEYWORDS = [
        'rent roll', 'rentroll', 'rent_roll',
        'mieterliste', 'mieter', 'mieterübersicht',
        'tenancy', 'tenant', 'tenants',
        'état locatif', 'locataire',
        'elenco locatari', 'inquilini',
        'huurderslijst', 'huurders',
        'lista najemców', 'najemcy',
        'hyresgäster', 'hyresförteckning',
        'stacking plan', 'schedule'
    ]
    
    # Keywords indicating sheets to skip
    SKIP_KEYWORDS = [
        'summary', 'total', 'totals', 'zusammenfassung', 'übersicht',
        'notes', 'notizen', 'hinweise', 'instructions', 'anleitung',
        'index', 'inhaltsverzeichnis', 'cover', 'deckblatt',
        'template', 'vorlage', 'example', 'beispiel'
    ]
    
    def __init__(self, mapper: HeaderMapper):
        self.mapper = mapper
    
    def should_skip_sheet(self, sheet_name: str) -> bool:
        """Check if a sheet should be skipped based on its name."""
        name_lower = sheet_name.lower().strip()
        return any(kw in name_lower for kw in self.SKIP_KEYWORDS)
    
    def is_likely_rent_roll(self, sheet_name: str) -> bool:
        """Check if sheet name indicates a rent roll."""
        name_lower = sheet_name.lower().strip()
        return any(kw in name_lower for kw in self.SHEET_KEYWORDS)
    
    def select_best_sheet(self, excel_file: pd.ExcelFile) -> Optional[str]:
        """
        Select the single best sheet for extraction.
        Used when ONLY processing one sheet is desired.
        """
        sheet_names = excel_file.sheet_names
        
        if len(sheet_names) == 1:
            return sheet_names[0]
        
        # Score sheets
        best_sheet = None
        best_score = -1
        
        for sheet_name in sheet_names:
            if self.should_skip_sheet(sheet_name):
                continue
            
            score = 0
            
            # Keyword match
            if self.is_likely_rent_roll(sheet_name):
                score += 10
            
            # Check header matches in sheet
            try:
                df = pd.read_excel(excel_file, sheet_name=sheet_name, nrows=30, header=None)
                header_matches = self._count_header_matches(df)
                score += header_matches
            except:
                pass
            
            if score > best_score:
                best_score = score
                best_sheet = sheet_name
        
        return best_sheet or sheet_names[0]
    
    def get_all_data_sheets(self, excel_file: pd.ExcelFile) -> List[str]:
        """
        Get all sheets that should be processed.
        CRITICAL: For multi-sheet processing, we want ALL data sheets.
        """
        data_sheets = []
        
        for sheet_name in excel_file.sheet_names:
            if self.should_skip_sheet(sheet_name):
                continue
            
            # Quick check if sheet has any recognizable headers
            try:
                df = pd.read_excel(excel_file, sheet_name=sheet_name, nrows=30, header=None)
                if self._count_header_matches(df) > 0:
                    data_sheets.append(sheet_name)
            except:
                pass
        
        # If no sheets passed the filter, return all non-skipped sheets
        if not data_sheets:
            data_sheets = [s for s in excel_file.sheet_names if not self.should_skip_sheet(s)]
        
        return data_sheets if data_sheets else excel_file.sheet_names[:1]
    
    def _count_header_matches(self, df: pd.DataFrame) -> int:
        """Count how many recognizable headers are in the sheet."""
        count = 0
        for row_idx in range(min(30, len(df))):
            row = df.iloc[row_idx]
            for cell in row:
                if self.mapper.map_header(str(cell)):
                    count += 1
        return count


# ============================================================================
# HEADER DETECTOR
# ============================================================================

class HeaderDetector:
    """Detects header row in Excel sheets."""
    
    def __init__(self, mapper: HeaderMapper):
        self.mapper = mapper
    
    def find_header_row(self, df: pd.DataFrame, max_rows: int = 30) -> Optional[Tuple[int, List[str]]]:
        """
        Find the header row by looking for the row with most recognized headers.
        
        Returns:
            Tuple of (row_index, list_of_headers) or None
        """
        best_row = None
        best_score = 0
        best_headers = []
        
        for row_idx in range(min(max_rows, len(df))):
            row = df.iloc[row_idx]
            headers = [str(cell) if not pd.isna(cell) else '' for cell in row]
            
            # Count recognized headers
            score = sum(1 for h in headers if self.mapper.map_header(h))
            
            if score > best_score:
                best_score = score
                best_row = row_idx
                best_headers = headers
        
        # Require at least 2 recognized headers
        if best_score >= 2:
            return (best_row, best_headers)
        
        return None
    
    def handle_multi_level_headers(self, df: pd.DataFrame, header_row: int) -> List[str]:
        """
        Handle multi-level headers (e.g., two rows merged as header).
        """
        if header_row == 0:
            return [str(c) if not pd.isna(c) else '' for c in df.iloc[0]]
        
        # Check if previous row is also part of header
        prev_row = df.iloc[header_row - 1]
        curr_row = df.iloc[header_row]
        
        combined = []
        for i in range(len(curr_row)):
            prev_val = str(prev_row.iloc[i]) if not pd.isna(prev_row.iloc[i]) else ''
            curr_val = str(curr_row.iloc[i]) if not pd.isna(curr_row.iloc[i]) else ''
            
            if prev_val and curr_val:
                combined.append(f"{prev_val} {curr_val}")
            else:
                combined.append(curr_val or prev_val)
        
        return combined


# ============================================================================
# DATA EXTRACTOR
# ============================================================================

class DataExtractor:
    """Extracts data from Excel sheets with proper unit separation."""
    
    # Summary keywords indicating end of data
    SUMMARY_KEYWORDS = [
        'total', 'totals', 'gesamt', 'summe', 'ergebnis',
        'total général', 'totale', 'totaal', 'razem', 'totalt',
        'grand total', 'subtotal', 'zwischensumme',
        'vacant', 'vacancy', 'leerstand'
    ]
    
    def __init__(self, mapper: HeaderMapper):
        self.mapper = mapper
        self.parser = NumberUnitParser()
    
    def extract_data(self, df: pd.DataFrame, header_row: int, 
                     raw_headers: List[str]) -> List[Dict[str, Any]]:
        """
        Extract data from DataFrame starting after header row.
        
        CRITICAL: Implements unit separation - value/unit/original in separate fields.
        """
        records = []
        
        # Map headers to canonical names
        mapped_headers = [self.mapper.map_header(h) for h in raw_headers]
        
        # Process each data row
        for row_idx in range(header_row + 1, len(df)):
            row = df.iloc[row_idx]
            
            # Check for summary/end row
            first_cell = str(row.iloc[0]) if not pd.isna(row.iloc[0]) else ''
            if self._is_summary_row(first_cell):
                break
            
            # Check for empty row
            if row.isna().all() or all(str(cell).strip() == '' for cell in row if not pd.isna(cell)):
                continue
            
            # Extract record
            record = self._process_row(row, mapped_headers, row_idx)
            
            # Only add if record has meaningful data
            if self._has_meaningful_data(record):
                record['_source_row'] = row_idx + 1  # Excel row (1-indexed)
                records.append(record)
        
        return records
    
    def _process_row(self, row: pd.Series, mapped_headers: List[str], 
                     row_idx: int) -> Dict[str, Any]:
        """
        Process a single row, separating values from units.
        
        CRITICAL: This implements the value/unit/original separation required for 98%+ accuracy.
        """
        record = {}
        
        for col_idx, canonical in enumerate(mapped_headers):
            if canonical is None or col_idx >= len(row):
                continue
            
            value = row.iloc[col_idx]

            # Get type hint for better parsing
            type_hint = self.mapper.get_field_type_hint(canonical)

            # Parse value with unit separation
            parsed = self.parser.parse(value, type_hint)

            # Define canonical fields that should always be treated as text (no numeric conversion)
            text_fields = [
                'tenant_name', 'unit_id', 'status', 'usage_type', 'lease_type',
                # Extended text fields
                'contract_id', 'contractual_partner', 'sap_object_number', 'composite_unit_id',
                'mo_number', 'business_unit', 'business_unit_code', 'bookkeeping_area'
            ]

            # Store with proper field naming
            # CRITICAL: Separate fields for value, unit, and original
            if parsed.unit:
                record[f'{canonical}_value'] = parsed.value
                record[f'{canonical}_unit'] = parsed.unit
                record[f'{canonical}_original'] = parsed.original_text
            else:
                # If this canonical field is designated as a text field, preserve original text
                if canonical in text_fields:
                    # Use the original string representation (if None, fallback to parsed.value)
                    orig = parsed.original_text if parsed.original_text is not None else (
                        str(parsed.value) if parsed.value is not None else ''
                    )
                    # Strip whitespace
                    record[canonical] = orig.strip() if isinstance(orig, str) else orig
                else:
                    # Numeric field: use parsed numeric value if available, otherwise use original text
                    if parsed.value is not None:
                        record[canonical] = parsed.value
                    elif parsed.original_text:
                        record[canonical] = parsed.original_text
        
        return record
    
    def _is_summary_row(self, first_cell: str) -> bool:
        """Check if this row is a summary/total row."""
        cell_lower = first_cell.lower().strip()
        return any(kw in cell_lower for kw in self.SUMMARY_KEYWORDS)
    
    def _has_meaningful_data(self, record: Dict[str, Any]) -> bool:
        """Check if record has at least some meaningful data."""
        # A record is considered meaningful if at least one of these fields is present.
        # In addition to tenant_name and unit_id, include fallback identifiers and
        # other IDs such as contractual_partner, contract_id, sap_object_number,
        # mo_number, and bookkeeping_area. Also include any rent or area values.
        meaningful_fields = [
            'tenant_name', 'unit_id', 'monthly_rent_value', 'annual_rent_value',
            'area_sqm_value', 'area_sqft_value',
            'contractual_partner', 'contract_id', 'sap_object_number',
            'mo_number', 'bookkeeping_area'
        ]
        return any(record.get(f) not in [None, '', 0] for f in meaningful_fields)


# ============================================================================
# MAIN EXCEL READER - MULTI-SHEET PROCESSING
# ============================================================================

class RentRollExcelReader:
    """
    Main class for reading rent roll Excel files.
    
    CRITICAL: Implements multi-sheet processing as required for 98%+ accuracy.
    """
    
    def __init__(self, synonyms_file: Optional[Path] = None):
        self.mapper = HeaderMapper(synonyms_file)
        self.sheet_selector = SheetSelector(self.mapper)
        self.header_detector = HeaderDetector(self.mapper)
        self.data_extractor = DataExtractor(self.mapper)

    def _is_phone_number(self, text: str) -> bool:
        """
        Heuristically determine if the provided numeric string looks like a phone number.

        A tenant ID is usually a shorter purely numeric identifier (e.g., 62210),
        whereas phone numbers often start with country/area codes (e.g., +49, 01, 015, 017)
        and contain 9 or more digits. This function returns True if the string
        appears to be a phone number, otherwise False.
        """
        if not text:
            return False
        # Remove whitespace and typical separators
        cleaned = re.sub(r'[\s\-]', '', str(text))
        # Patterns that indicate a phone number
        # Starts with + followed by 2–3 digit country code
        if cleaned.startswith('+'):
            digits = re.sub(r'\D', '', cleaned)
            return len(digits) >= 9  # phone numbers typically longer
        # German local numbers often start with 0 or 01/015/017 etc.
        if cleaned.startswith(('01', '02', '03', '04', '05', '06', '07', '08', '09')):
            digits = re.sub(r'\D', '', cleaned)
            return len(digits) >= 9
        return False

    def _resolve_tenant_and_unit(self, records: List[Dict[str, Any]]) -> None:
        """
        Resolve tenant and unit identifiers based on extended logic.

        This method iterates through all extracted records and performs the following:
        - Determines whether values in the tenant_name field represent a numeric
          tenant ID (and not a phone number). If so, the value is moved to
          tenant_id and tenant_name is cleared.
        - Applies a fallback to contractual_partner if tenant_name is missing.
        - Resolves unit identifiers from composite_unit_id, sap_object_number,
          contract_id, or combinations of bookkeeping_area, business_unit and
          mo_number.
        - Assigns asset_id from sap_object_number when available.
        """
        for record in records:
            # -----------------------------------------------------------------
            # Tenant resolution
            tenant_value = record.get('tenant_name')
            # If a numeric tenant name exists, check if it's actually a tenant ID
            if tenant_value is not None and tenant_value != '':
                # Only consider if it's purely numeric (possibly with .0 from Excel)
                try:
                    # Convert floats like 62210.0 to integer-like string
                    if isinstance(tenant_value, (int, float)):
                        num_str = str(int(float(tenant_value)))
                    else:
                        num_str = str(tenant_value).strip()
                    # Remove trailing .0 if present
                    num_str = re.sub(r'\.0+$', '', num_str)
                    # Check if string consists of digits only
                    if re.fullmatch(r'\d+', num_str):
                        # Distinguish between tenant ID and phone number
                        if not self._is_phone_number(num_str):
                            # Assign as tenant_id and clear tenant_name
                            record['tenant_id'] = num_str
                            # Remove tenant_name to avoid duplication
                            del record['tenant_name']
                except Exception:
                    pass

            # If tenant_name still missing, fallback to contractual_partner
            if not record.get('tenant_name') and not record.get('tenant_id'):
                partner = record.get('contractual_partner')
                if partner:
                    record['tenant_name'] = partner

            # -----------------------------------------------------------------
            # Unit ID resolution
            # Use existing unit_id if present
            if not record.get('unit_id'):
                # Primary: composite_unit_id (e.g., MO/BK/Wirtschaftseinheit/MO-Nr)
                composite = record.get('composite_unit_id')
                if composite:
                    record['unit_id'] = composite
                else:
                    # Secondary: sap_object_number → assign both unit_id and asset_id
                    sap_obj = record.get('sap_object_number')
                    if sap_obj:
                        record['unit_id'] = sap_obj
                    else:
                        # Tertiary: contract_id
                        contract = record.get('contract_id')
                        if contract:
                            record['unit_id'] = contract
                        else:
                            # Final: Combine bookkeeping_area, business_unit, and mo_number
                            bk = record.get('bookkeeping_area')
                            bu = record.get('business_unit') or record.get('business_unit_code')
                            mo = record.get('mo_number')
                            parts = []
                            for part in [bk, bu, mo]:
                                if part:
                                    # Convert floats like 3.0 to int strings
                                    if isinstance(part, (int, float)):
                                        part_str = str(int(float(part)))
                                    else:
                                        part_str = str(part).strip()
                                    parts.append(part_str)
                            if parts:
                                record['unit_id'] = '-'.join(parts)

            # -----------------------------------------------------------------
            # Asset ID resolution: use sap_object_number when available
            if record.get('sap_object_number') and not record.get('asset_id'):
                record['asset_id'] = record['sap_object_number']
    
    def read_excel(self, filepath: Path, process_all_sheets: bool = True) -> ExtractionResult:
        """
        Read and extract rent roll from Excel file.
        
        CRITICAL: When process_all_sheets=True, processes ALL sheets and adds
        _source_file and _source_sheet to each record.
        """
        warnings = []
        all_records = []
        sheets_processed = 0
        
        # Open file
        try:
            excel_file = pd.ExcelFile(filepath)
        except Exception as e:
            return ExtractionResult(
                success=False,
                data=[],
                message=f"Failed to open Excel file: {e}",
                warnings=[str(e)],
                metadata={'filepath': str(filepath)}
            )
        
        # Determine which sheets to process
        if process_all_sheets:
            sheets_to_process = self.sheet_selector.get_all_data_sheets(excel_file)
        else:
            best_sheet = self.sheet_selector.select_best_sheet(excel_file)
            sheets_to_process = [best_sheet] if best_sheet else []
        
        if not sheets_to_process:
            return ExtractionResult(
                success=False,
                data=[],
                message="No suitable sheets found in Excel file",
                warnings=["Could not identify any rent roll sheets"],
                metadata={'filepath': str(filepath), 'available_sheets': excel_file.sheet_names}
            )
        
        # Process each sheet
        for sheet_name in sheets_to_process:
            try:
                # Read sheet
                df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
                
                # Find header
                header_result = self.header_detector.find_header_row(df)
                if not header_result:
                    warnings.append(f"Sheet '{sheet_name}': Could not detect header row")
                    continue
                
                header_row, raw_headers = header_result
                
                # Handle multi-level headers
                if header_row > 0:
                    raw_headers = self.header_detector.handle_multi_level_headers(df, header_row)
                
                # Extract data
                records = self.data_extractor.extract_data(df, header_row, raw_headers)
                
                # CRITICAL: Add source identification to each record
                extraction_timestamp = datetime.now().isoformat()
                for record in records:
                    record['_source_file'] = filepath.name
                    record['_source_sheet'] = sheet_name
                    record['_extraction_timestamp'] = extraction_timestamp
                
                all_records.extend(records)
                sheets_processed += 1
                
            except Exception as e:
                warnings.append(f"Sheet '{sheet_name}': Error - {str(e)}")
        
        # Detect language
        all_headers = []
        for sheet in sheets_to_process[:3]:
            try:
                df = pd.read_excel(excel_file, sheet_name=sheet, nrows=30, header=None)
                header_result = self.header_detector.find_header_row(df)
                if header_result:
                    all_headers.extend(header_result[1])
            except:
                pass
        
        detected_lang = self.mapper.detect_language(all_headers)

        # After extracting all records, apply tenant and unit resolution.
        # This step harmonizes tenant_name/tenant_id and unit_id/asset_id fields
        # based on extended logic (numeric IDs, fallbacks, and compound fields).
        self._resolve_tenant_and_unit(all_records)
        
        return ExtractionResult(
            success=len(all_records) > 0,
            data=all_records,
            message=f"Extracted {len(all_records)} records from {sheets_processed} sheet(s)",
            warnings=warnings,
            metadata={
                'filepath': str(filepath),
                'sheets_processed': sheets_processed,
                'sheets_available': excel_file.sheet_names,
                'detected_language': detected_lang,
                'total_rows': len(all_records)
            },
            sheets_processed=sheets_processed,
            total_rows_extracted=len(all_records)
        )


# ============================================================================
# VALIDATION
# ============================================================================

class DataValidator:
    """Validates extracted data for consistency and completeness."""
    
    def validate(self, records: List[Dict[str, Any]]) -> List[ValidationError]:
        """Validate extracted records."""
        errors = []
        
        for idx, record in enumerate(records):
            # Check for required identifiers. A row is considered incomplete only
            # if it lacks tenant_name, tenant_id, unit_id, contract_id and asset_id.
            if (not record.get('tenant_name') and not record.get('tenant_id')) \
               and not record.get('unit_id') \
               and not record.get('contract_id') \
               and not record.get('asset_id'):
                errors.append(ValidationError(
                    severity='warning',
                    row_index=idx,
                    field='tenant/unit identifiers',
                    message='Missing tenant and unit identifiers (tenant_name, tenant_id, unit_id, contract_id, asset_id)',
                    value=None
                ))
            
            # Check rent value is positive (unless it's a credit)
            rent_value = record.get('monthly_rent_value')
            if rent_value is not None and rent_value < 0:
                errors.append(ValidationError(
                    severity='warning',
                    row_index=idx,
                    field='monthly_rent_value',
                    message='Negative rent value detected',
                    value=rent_value
                ))
            
            # Check for very large values that might indicate parsing errors
            for field in ['monthly_rent_value', 'annual_rent_value', 'area_sqm_value', 'area_sqft_value']:
                val = record.get(field)
                if val and val > 1e9:  # 1 billion
                    errors.append(ValidationError(
                        severity='warning',
                        row_index=idx,
                        field=field,
                        message=f'Unusually large value detected: {val}',
                        value=val
                    ))
        
        return errors


# ============================================================================
# TEST SUITE
# ============================================================================

def run_parser_tests():
    """
    Run test suite for NumberUnitParser.
    CRITICAL: These tests must all pass for 98%+ accuracy.
    """
    parser = NumberUnitParser()
    
    test_cases = [
        # European format
        ('1.234,56 €', 1234.56, 'EUR'),
        ('€ 1.234,56', 1234.56, 'EUR'),
        ('1.234.567,89 EUR', 1234567.89, 'EUR'),
        
        # US format
        ('1,234.56 USD', 1234.56, 'USD'),
        ('$1,234.56', 1234.56, 'USD'),
        ('$1,234,567.89', 1234567.89, 'USD'),
        
        # Swiss format
        ("1'234.56 CHF", 1234.56, 'CHF'),
        ("CHF 1'234'567.89", 1234567.89, 'CHF'),
        
        # Negative numbers
        ('(500)', -500, None),
        ('(1,234.56)', -1234.56, None),
        ('-500 €', -500, 'EUR'),
        
        # Percentages
        ('95%', 95, '%'),
        ('95 %', 95, '%'),
        ('99.5%', 99.5, '%'),
        ('99,5%', 99.5, '%'),
        
        # Area units - CRITICAL: NO CONVERSION
        ('7,200 sqft', 7200, 'sqft'),
        ('500 m²', 500, 'm²'),
        ('1.234 m²', 1234, 'm²'),
        ('1,234 sqm', 1234, 'm²'),
        
        # Edge cases
        ('1000€', 1000, 'EUR'),
        ('€1000', 1000, 'EUR'),
        ('approx. 1000', 1000, None),
        ('ca. 500 €', 500, 'EUR'),
        ('>= 90%', 90, '%'),
        
        # Pure numbers
        ('1234', 1234, None),
        ('1,234', 1234, None),
        ('1.234', 1234, None),
        
        # Time units
        ('3 Jahre', 3, 'Jahre'),
        ('12 months', 12, 'months'),
        
        # Empty/null
        ('', None, None),
        ('-', None, None),
        ('n/a', None, None),
        ('00:00:00', None, None),
    ]
    
    print("\n" + "="*80)
    print("NUMBERUNITPARSER TEST SUITE")
    print("="*80)
    
    passed = 0
    failed = 0
    
    for text, expected_value, expected_unit in test_cases:
        result = parser.parse(text)
        
        # Check value
        value_match = False
        if expected_value is None:
            value_match = result.value is None
        elif result.value is not None:
            value_match = abs(result.value - expected_value) < 0.01
        
        # Check unit
        unit_match = result.unit == expected_unit
        
        if value_match and unit_match:
            print(f"✅ PASS: '{text}' → value: {result.value}, unit: {result.unit}")
            passed += 1
        else:
            print(f"❌ FAIL: '{text}'")
            print(f"   Expected: value={expected_value}, unit={expected_unit}")
            print(f"   Got:      value={result.value}, unit={result.unit}")
            failed += 1
    
    print("\n" + "-"*80)
    print(f"Results: {passed} passed, {failed} failed")
    print(f"Pass rate: {passed/(passed+failed)*100:.1f}%")
    print("="*80)
    
    return failed == 0


# ============================================================================
# CLI
# ============================================================================

def main():
    """Main entry point for CLI."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Rent Roll Extraktor v2.1 FINAL - 98%+ Extraction Accuracy'
    )
    parser.add_argument('file', nargs='?', help='Excel file to process')
    parser.add_argument('--test', action='store_true', help='Run parser tests')
    parser.add_argument('--single-sheet', action='store_true', 
                        help='Only process best sheet (default: all sheets)')
    parser.add_argument('--output', '-o', help='Output JSON file')
    parser.add_argument('--synonyms', help='Path to synonyms JSON file')
    
    args = parser.parse_args()
    
    # Run tests
    if args.test:
        success = run_parser_tests()
        sys.exit(0 if success else 1)
    
    # Require file for extraction
    if not args.file:
        parser.print_help()
        print("\n❌ Error: Please provide an Excel file or use --test")
        sys.exit(1)
    
    filepath = Path(args.file)
    if not filepath.exists():
        print(f"❌ Error: File not found: {filepath}")
        sys.exit(1)
    
    # Create reader
    synonyms_file = Path(args.synonyms) if args.synonyms else None
    reader = RentRollExcelReader(synonyms_file)
    
    # Extract
    print(f"\n📄 Processing: {filepath.name}")
    print(f"   Mode: {'Single sheet' if args.single_sheet else 'All sheets'}")
    
    result = reader.read_excel(filepath, process_all_sheets=not args.single_sheet)
    
    # Print results
    print(f"\n{'✅' if result.success else '❌'} {result.message}")
    print(f"   Sheets processed: {result.sheets_processed}")
    print(f"   Records extracted: {result.total_rows_extracted}")
    
    if result.warnings:
        print("\n⚠️ Warnings:")
        for w in result.warnings:
            print(f"   - {w}")
    
    # Output
    if args.output:
        output_path = Path(args.output)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump({
                'success': result.success,
                'message': result.message,
                'metadata': result.metadata,
                'warnings': result.warnings,
                'data': result.data
            }, f, indent=2, ensure_ascii=False, default=str)
        print(f"\n💾 Output saved to: {output_path}")
    else:
        # Print first 3 records as preview
        print("\n📊 Preview (first 3 records):")
        for i, record in enumerate(result.data[:3]):
            print(f"\n   Record {i+1}:")
            for key, value in record.items():
                if not key.startswith('_'):
                    print(f"      {key}: {value}")
    
    # Validate
    validator = DataValidator()
    errors = validator.validate(result.data)
    if errors:
        print(f"\n⚠️ Validation issues: {len(errors)}")
        for e in errors[:5]:
            print(f"   [{e.severity}] Row {e.row_index}: {e.message}")


if __name__ == '__main__':
    main()
