import pandas as pd
import json
import re

def try_fix_mojibake(s):
    # ... (código exatamente como o seu) ...
    if pd.isna(s):
        return s
    if not isinstance(s, str):
        s = str(s)
    s = s.strip()
    if "Ã" in s or "â" in s or "�" in s:
        try:
            fixed = s.encode('latin-1').decode('utf-8')
            if sum(1 for ch in fixed if ord(ch) > 127) >= sum(1 for ch in s if ord(ch) > 127):
                return fixed
        except Exception:
            pass
    return s

def normalize_competitor(row, domain_col, sold_by_col, sellers_col):
    # ... (lógica adaptada para receber nomes das colunas) ...
    candidate = None
    for col in (domain_col, sold_by_col):
        val = row.get(col)
        if val and not pd.isna(val) and str(val).strip() != '':
            candidate = val
            break
            
    if (candidate is None or str(candidate).strip() == "") and sellers_col:
        sellers_field = row.get(sellers_col)
        # ... (resto da sua lógica de sellers) ...

    if candidate is None:
        return None
    
    candidate = try_fix_mojibake(candidate)
    s = candidate.strip().lower()
    
    if 'pague' in s and 'menos' in s: return "Pague Menos"
    if 'venancio' in s: return "Venancio"
    
    if s.startswith('www.'): s = s[4:]
    if '.' in s: s = s.split('.')[0]
    
    s = re.sub(r'[^a-zA-Z0-9\s]', '', s).strip()
    
    return s.title() if s else None