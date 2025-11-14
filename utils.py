import pandas as pd
import numpy as np
import re
from datetime import datetime

# -------------------------
# Emails
# -------------------------
def standardize_email(email):
    """Nettoie et normalise les emails."""
    if not isinstance(email, str) or pd.isna(email):
        return None
    email = email.strip().lower()
    email = re.sub(r'\s+', '', email)
    if not re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', email):
        return None
    return email

def is_valid_email(email):
    """Vérifie si un email est valide."""
    if not isinstance(email, str) or pd.isna(email):
        return False
    return bool(re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', email.strip()))

# -------------------------
# Pays
# -------------------------
def standardize_country(country):
    """Standardise les noms de pays."""
    if not isinstance(country, str) or pd.isna(country):
        return None
    country = country.strip().lower()
    mapping = {
        # France
        "fr": "France", "fra": "France", "france": "France",
        # Allemagne
        "de": "Allemagne", "deu": "Allemagne", "germany": "Allemagne", "allemagne": "Allemagne",
        # États-Unis
        "us": "États-Unis", "usa": "États-Unis", "united states": "États-Unis", "etats-unis": "États-Unis", "états-unis": "États-Unis",
        # Royaume-Uni
        "uk": "Royaume-Uni", "gb": "Royaume-Uni", "united kingdom": "Royaume-Uni", "royaume-uni": "Royaume-Uni",
        # Espagne
        "es": "Espagne", "esp": "Espagne", "espagne": "Espagne", "spain": "Espagne",
        # Italie
        "it": "Italie", "ita": "Italie", "italie": "Italie", "italy": "Italie"
    }
    return mapping.get(country, country.title())

# -------------------------
# Téléphones
# -------------------------
def clean_phone(phone):
    """Nettoie et formate les numéros de téléphone français."""
    if pd.isna(phone):
        return None
    phone = str(phone).strip()
    if not phone:
        return None
    phone = re.sub(r'\D', '', phone)
    if len(phone) == 10 and phone.startswith('0'):
        phone = '33' + phone[1:]
    elif len(phone) == 9:
        phone = '33' + phone
    elif len(phone) in [11, 12] and phone.startswith('33'):
        phone = phone[:11]
    else:
        if not (9 <= len(phone) <= 12):
            return None
    if len(phone) != 11:
        return None
    return f"+{phone}"

# -------------------------
# Dates
# -------------------------
def parse_date(date_value, dayfirst=True):
    """Convertit une chaîne ou date en datetime."""
    if pd.isna(date_value):
        return None
    try:
        return pd.to_datetime(date_value, errors='coerce', dayfirst=dayfirst)
    except:
        return None

def is_future_date(date_value):
    """Vérifie si une date est dans le futur."""
    if pd.isna(date_value):
        return False
    try:
        date = pd.to_datetime(date_value, errors='coerce')
        return date > datetime.now()
    except:
        return False

def is_valid_birthdate(date_value, min_age=0, max_age=120):
    """Vérifie la validité d'une date de naissance."""
    if pd.isna(date_value):
        return False
    try:
        date = pd.to_datetime(date_value, errors='coerce')
        if pd.isna(date):
            return False
        age = (datetime.now() - date).days / 365.25
        return min_age <= age <= max_age and date <= datetime.now()
    except:
        return False

# -------------------------
# Catalogue
# -------------------------
def convert_weight_kg(weight, unit):
    """Convertit un poids en kg selon l'unité."""
    if pd.isna(weight):
        return None
    try:
        weight = float(weight)
    except:
        return None
    unit = str(unit).lower()
    if unit in ['g', 'gramme', 'grams']:
        return round(weight / 1000, 3)
    elif unit in ['kg']:
        return weight
    elif unit in ['lb', 'lbs']:
        return round(weight * 0.453592, 3)
    elif unit in ['oz']:
        return round(weight * 0.0283495, 3)
    else:
        return weight

def convert_price_eur(price, currency):
    """Convertit un prix en EUR selon la devise."""
    if pd.isna(price):
        return None
    try:
        price = float(price)
    except:
        return None
    currency = str(currency).upper()
    if currency in ['$', 'USD']:
        return round(price * 0.92, 2)
    elif currency in ['EUR', '€']:
        return price
    else:
        return price

# -------------------------
# Gestion des doublons
# -------------------------
def remove_duplicates(df, subset_cols):
    return df.drop_duplicates(subset=subset_cols, keep='first')

def calculate_completeness_score(df, columns=None):
    if columns is None:
        columns = df.columns
    return df[columns].notna().sum(axis=1)

def merge_duplicates(df, key_columns, keep='most_complete'):
    if keep == 'most_complete':
        df['_completeness_score'] = calculate_completeness_score(df)
        df = df.sort_values('_completeness_score', ascending=False)
        df = df.drop_duplicates(subset=key_columns, keep='first')
        df = df.drop(columns=['_completeness_score'])
    else:
        df = df.drop_duplicates(subset=key_columns, keep=keep)
    return df

# -------------------------
# KPI / Qualité
# -------------------------
def calculate_kpi(df, column_list):
    rows = []
    for col in column_list:
        rows.append({
            "column": col,
            "missing": df[col].isna().sum(),
            "unique": df[col].nunique()
        })
    return pd.DataFrame(rows)

def kpi_quality(df, name="Dataset"):
    metrics = {
        'dataset_name': name,
        'total_rows': len(df),
        'total_columns': len(df.columns)
    }
    completeness_by_column = ((1 - df.isnull().sum() / len(df)) * 100).round(2)
    metrics['completeness_per_column'] = completeness_by_column.to_dict()
    total_missing = df.isnull().sum().sum()
    total_cells = df.size
    metrics['global_completeness_rate'] = round((1 - total_missing / total_cells) * 100, 2)
    metrics['num_duplicates'] = df.duplicated().sum()
    metrics['duplicate_rate'] = round((metrics['num_duplicates'] / len(df) * 100) if len(df) > 0 else 0, 2)
    metrics['missing_rate'] = round((total_missing / total_cells * 100) if total_cells > 0 else 0, 2)
    return metrics

def print_quality_report(metrics):
    print(f"\n{'='*60}")
    print(f"RAPPORT DE QUALITÉ: {metrics['dataset_name']}")
    print(f"{'='*60}")
    print(f" Lignes totales: {metrics['total_rows']}")
    print(f" Colonnes totales: {metrics['total_columns']}")
    print(f" Taux de complétude global: {metrics['global_completeness_rate']}%")
    print(f" Taux de valeurs manquantes: {metrics['missing_rate']}%")
    print(f" Nombre de doublons: {metrics['num_duplicates']} ({metrics['duplicate_rate']}%)")
    print(f"\n Complétude par colonne:")
    for col, rate in metrics['completeness_per_column'].items():
        status = "good" if rate == 100 else "warning" if rate >= 80 else "bad"
        print(f"  {status} {col}: {rate}%")
    print(f"{'='*60}\n")
