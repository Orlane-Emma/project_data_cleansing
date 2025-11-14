import re
import pandas as pd
import numpy as np
from datetime import datetime


# NETTOYAGE DES EMAILS
# =====================

def normalize_email(email):
   
    if not isinstance(email, str) or pd.isna(email):
        return None
    
    # Supprimer les espaces et mettre en minuscules
    email = email.strip().lower()
    
    # Supprimer les espaces internes
    email = re.sub(r'\s+', '', email)
    
    # Vérification format basique: caractères@domaine.extension
    if not re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', email):
        return None
    
    return email


def is_valid_email(email):
   
    if not isinstance(email, str) or pd.isna(email):
        return False
    return bool(re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', email.strip()))



# NETTOYAGE DES PAYS
# ===================

def normalize_country(country_name):
    
    if not isinstance(country_name, str) or pd.isna(country_name):
        return None
    
    country_name = country_name.strip().lower()
    
    # Dictionnaire de correspondance pour les variations courantes
    country_map = {
        # France
        "france": "France",
        "fr": "France",
        "fra": "France",
        
        # Allemagne
        "allemagne": "Allemagne",
        "germany": "Allemagne",
        "de": "Allemagne",
        "deu": "Allemagne",
        
        # États-Unis
        "etats-unis": "États-Unis",
        "états-unis": "États-Unis",
        "usa": "États-Unis",
        "us": "États-Unis",
        "united states": "États-Unis",
        "etats unis": "États-Unis",
        
        # Royaume-Uni
        "royaume-uni": "Royaume-Uni",
        "uk": "Royaume-Uni",
        "gb": "Royaume-Uni",
        "united kingdom": "Royaume-Uni",
        
        # Espagne
        "espagne": "Espagne",
        "spain": "Espagne",
        "es": "Espagne",
        "esp": "Espagne",
        
        # Italie
        "italie": "Italie",
        "italy": "Italie",
        "it": "Italie",
        "ita": "Italie",
    }
    
    return country_map.get(country_name, country_name.capitalize())



# NETTOYAGE DES TÉLÉPHONES
# =========================

def normalize_phone_fr(phone_number):

    # Gérer les valeurs manquantes ou non-string
    if pd.isna(phone_number):
        return None
    
    # Convertir en string si ce n'est pas déjà le cas
    phone_number = str(phone_number).strip()
    
    if not phone_number:
        return None
    
    # Supprime tous les caractères non numériques
    clean_number = re.sub(r'\D', '', phone_number)
    
    # Si vide après nettoyage, retourner None
    if not clean_number:
        return None
    
    # Gestion des différents formats
    if len(clean_number) == 10 and clean_number.startswith('0'):
        # Format français classique: 06 12 34 56 78
        clean_number = '33' + clean_number[1:]
    elif len(clean_number) == 11 and clean_number.startswith('33'):
        # Déjà au format international sans le +
        pass
    elif len(clean_number) == 12 and clean_number.startswith('33'):
        # Format avec le + déjà présent (33612345678)
        clean_number = clean_number[0:11]
    elif len(clean_number) == 9:
        # Format sans le 0 initial (612345678)
        clean_number = '33' + clean_number
    else:
        # Format non reconnu - on retourne le numéro si longueur raisonnable
        if 9 <= len(clean_number) <= 12:
            # Essayer de forcer le format français
            if len(clean_number) == 10:
                clean_number = '33' + clean_number[1:]
            elif not clean_number.startswith('33'):
                return None
        else:
            return None
    
    # Vérification finale de la longueur (doit être 11 chiffres: 33 + 9)
    if len(clean_number) != 11:
        return None
    
    return f"+{clean_number}"



# NETTOYAGE DES DATES
# ====================

def normalize_date(date_value, dayfirst=True):
  
    if pd.isna(date_value):
        return None
    
    try:
        return pd.to_datetime(date_value, errors='coerce', dayfirst=dayfirst)
    except:
        return None


def is_future_date(date_value):
   
    if pd.isna(date_value):
        return False
    
    try:
        date = pd.to_datetime(date_value, errors='coerce')
        return date > datetime.now()
    except:
        return False


def is_valid_birthdate(date_value, min_age=0, max_age=120):
   
    if pd.isna(date_value):
        return False
    
    try:
        date = pd.to_datetime(date_value, errors='coerce')
        if pd.isna(date):
            return False
        
        today = datetime.now()
        age = (today - date).days / 365.25
        
        return min_age <= age <= max_age and date <= today
    except:
        return False



# KPI DE QUALITÉ
# ===============

def kpi_quality(df, name="Dataset"):
   
    quality_metrics = {
        'dataset_name': name,
        'total_rows': len(df),
        'total_columns': len(df.columns)
    }
    
    # Calcul du taux de complétude par colonne
    completeness_by_column = ((1 - df.isnull().sum() / len(df)) * 100).round(2)
    quality_metrics['completeness_per_column'] = completeness_by_column.to_dict()
    
    # Calcul du taux de complétude global
    total_missing = df.isnull().sum().sum()
    total_cells = df.size
    global_completeness_rate = (1 - (total_missing / total_cells)) * 100
    quality_metrics['global_completeness_rate'] = round(global_completeness_rate, 2)
    
    # Calcul du taux de doublons
    num_duplicates = df.duplicated().sum()
    duplicate_rate = (num_duplicates / len(df) * 100).round(2) if len(df) > 0 else 0
    quality_metrics['num_duplicates'] = num_duplicates
    quality_metrics['duplicate_rate'] = duplicate_rate
    
    # Taux de valeurs manquantes
    missing_rate = (total_missing / total_cells * 100).round(2) if total_cells > 0 else 0
    quality_metrics['missing_rate'] = missing_rate
    
    return quality_metrics


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



# GESTION DES DOUBLONS
# =====================

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