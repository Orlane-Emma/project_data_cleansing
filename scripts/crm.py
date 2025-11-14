import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime

# Ajouter le répertoire scripts au path pour importer utils
sys.path.append(os.path.dirname(__file__))
from utils import (
    normalize_email,
    is_valid_email,
    normalize_country,
    normalize_phone_fr,
    normalize_date,
    is_valid_birthdate,
    kpi_quality,
    print_quality_report,
    merge_duplicates
)


# CONFIGURATION
# ==============

# Chemins des fichiers
RAW_DATA_PATH = "../data/raw/clients.csv"
CLEAN_DATA_PATH = "../data/clean/clients_clean.csv"
REPORT_PATH = "../data/reports/kpi_qualite_crm.csv"
LOG_PATH = "../data/reports/crm_cleaning_log.txt"


# FONCTIONS PRINCIPALES
# ======================

def load_data():
  
    print(" Chargement des données clients...")
    
    try:
        df = pd.read_csv(RAW_DATA_PATH)
        print(f"{len(df)} lignes chargées avec succès")
        print(f" Colonnes trouvées: {list(df.columns)}")
        return df
    except FileNotFoundError:
        print(f" Erreur: Le fichier {RAW_DATA_PATH} n'existe pas!")
        return None
    except Exception as e:
        print(f" Erreur lors du chargement: {e}")
        return None


def clean_emails(df):
  
    print("\n Nettoyage des emails...")
    
    # Détecter le nom de la colonne email
    email_col = None
    for col in df.columns:
        if 'email' in col.lower() or 'courriel' in col.lower() or 'mail' in col.lower():
            email_col = col
            break
    
    if email_col is None:
        print(" Aucune colonne email trouvée")
        return df
    
    # Statistiques avant nettoyage
    total = len(df)
    valid_before = df[email_col].apply(is_valid_email).sum()
    
    # Nettoyage
    df[f'{email_col}_original'] = df[email_col].copy()
    df[email_col] = df[email_col].apply(normalize_email)
    
    # Statistiques après nettoyage
    valid_after = df[email_col].notna().sum()
    invalid_count = total - valid_after
    
    print(f"   Emails valides avant: {valid_before}/{total} ({valid_before/total*100:.1f}%)")
    print(f"   Emails valides après: {valid_after}/{total} ({valid_after/total*100:.1f}%)")
    print(f"   Emails invalides supprimés: {invalid_count}")
    
    return df


def clean_countries(df):
    
    print("\n Standardisation des pays...")
    
    # Détecter le nom de la colonne pays
    country_col = None
    for col in df.columns:
        if 'pays' in col.lower() or 'country' in col.lower():
            country_col = col
            break
    
    if country_col is None:
        print(" Aucune colonne pays trouvée")
        return df
    
    # Statistiques avant
    unique_before = df[country_col].nunique()
    
    # Nettoyage
    df[f'{country_col}_original'] = df[country_col].copy()
    df[country_col] = df[country_col].apply(normalize_country)
    
    # Statistiques après
    unique_after = df[country_col].nunique()
    
    print(f"   Variantes uniques avant: {unique_before}")
    print(f"   Variantes uniques après: {unique_after}")
    print(f"   Réduction de {unique_before - unique_after} variantes")
    
    return df


def clean_phones(df):
    
    print("\n Nettoyage des téléphones...")
    
    # Détecter le nom de la colonne téléphone
    phone_col = None
    for col in df.columns:
        if 'tel' in col.lower() or 'phone' in col.lower():
            phone_col = col
            break
    
    if phone_col is None:
        print(" Aucune colonne téléphone trouvée")
        return df
    
    # Statistiques avant
    valid_before = df[phone_col].notna().sum()
    
    # Nettoyage
    df[f'{phone_col}_original'] = df[phone_col].copy()
    df[f'{phone_col}_normalise'] = df[phone_col].apply(normalize_phone_fr)
    
    # Statistiques après
    valid_after = df[f'{phone_col}_normalise'].notna().sum()
    
    print(f"   Téléphones valides avant: {valid_before}/{len(df)}")
    print(f"   Téléphones valides après: {valid_after}/{len(df)}")
    print(f"   Téléphones invalides: {len(df) - valid_after}")
    
    return df


def clean_birthdates(df):
   
    print("\n Validation des dates de naissance...")
    
    # Détecter la colonne date de naissance
    birth_col = None
    for col in df.columns:
        if 'naissance' in col.lower() or 'birth' in col.lower() or 'dob' in col.lower():
            birth_col = col
            break
    
    if birth_col is None:
        print(" Aucune colonne date de naissance trouvée")
        return df
    
    # Conversion et validation
    df[f'{birth_col}_original'] = df[birth_col].copy()
    df[birth_col] = df[birth_col].apply(normalize_date)
    
    # Marquer les dates invalides
    df['date_naissance_valide'] = df[birth_col].apply(is_valid_birthdate)
    
    valid_count = df['date_naissance_valide'].sum()
    invalid_count = len(df) - valid_count
    
    print(f"   Dates valides: {valid_count}/{len(df)}")
    print(f"   Dates invalides (futures ou âge > 120 ans): {invalid_count}")
    
    # Mettre à None les dates invalides
    df.loc[~df['date_naissance_valide'], birth_col] = None
    
    return df


def remove_duplicates(df):
   
    print("\n Suppression des doublons...")
    
    rows_before = len(df)
    
    # Identifier les colonnes clés pour la déduplication
    key_columns = []
    
    # Chercher nom, prénom, email
    for col in df.columns:
        if any(keyword in col.lower() for keyword in ['nom', 'name', 'prenom', 'firstname', 'lastname']):
            if 'original' not in col.lower():
                key_columns.append(col)
        elif 'email' in col.lower() or 'courriel' in col.lower():
            if 'original' not in col.lower():
                key_columns.append(col)
    
    if not key_columns:
        print(" Impossible de détecter les colonnes clés pour déduplication")
        return df
    
    print(f"   Colonnes clés utilisées: {key_columns}")
    
    # Supprimer les doublons en gardant la ligne la plus complète
    df = merge_duplicates(df, key_columns, keep='most_complete')
    
    rows_after = len(df)
    duplicates_removed = rows_before - rows_after
    
    print(f"   Lignes avant: {rows_before}")
    print(f"   Lignes après: {rows_after}")
    print(f"   Doublons supprimés: {duplicates_removed} ({duplicates_removed/rows_before*100:.1f}%)")
    
    return df


def save_results(df_clean, kpi_before, kpi_after):
   
    print("\n Sauvegarde des résultats...")
    
    # Créer les dossiers si nécessaire
    os.makedirs(os.path.dirname(CLEAN_DATA_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    
    # Sauvegarder les données nettoyées
    df_clean.to_csv(CLEAN_DATA_PATH, index=False)
    print(f"   Données nettoyées sauvegardées: {CLEAN_DATA_PATH}")
    
    # Créer un rapport comparatif
    kpi_comparison = pd.DataFrame({
        'Métrique': [
            'Nombre de lignes',
            'Taux de complétude global (%)',
            'Taux de doublons (%)',
            'Taux de valeurs manquantes (%)'
        ],
        'Avant': [
            kpi_before['total_rows'],
            kpi_before['global_completeness_rate'],
            kpi_before['duplicate_rate'],
            kpi_before['missing_rate']
        ],
        'Après': [
            kpi_after['total_rows'],
            kpi_after['global_completeness_rate'],
            kpi_after['duplicate_rate'],
            kpi_after['missing_rate']
        ]
    })
    
    # Calculer l'amélioration
    kpi_comparison['Amélioration'] = kpi_comparison['Après'] - kpi_comparison['Avant']
    
    # Sauvegarder le rapport
    kpi_comparison.to_csv(REPORT_PATH, index=False)
    print(f"   Rapport KPI sauvegardé: {REPORT_PATH}")
    
    # Afficher le tableau comparatif
    print("\n COMPARAISON AVANT/APRÈS:")
    print(kpi_comparison.to_string(index=False))



# PIPELINE PRINCIPAL
# ===================


def main():
   
    print("\n" + "="*70)
    print(" PROJET 1: CRM DE QUALITÉ OPTIMALE")
    print("="*70)
    
    # 1. Charger les données
    df = load_data()
    if df is None:
        return
    
    # 2. KPI avant nettoyage
    print("\n" + "="*70)
    print(" ÉTAT INITIAL DES DONNÉES")
    print("="*70)
    kpi_before = kpi_quality(df, "Clients (AVANT)")
    print_quality_report(kpi_before)
    
    # 3. Nettoyage étape par étape
    print("\n" + "="*70)
    print(" NETTOYAGE DES DONNÉES")
    print("="*70)
    
    df_clean = df.copy()
    df_clean = clean_emails(df_clean)
    df_clean = clean_countries(df_clean)
    df_clean = clean_phones(df_clean)
    df_clean = clean_birthdates(df_clean)
    df_clean = remove_duplicates(df_clean)
    
    # 4. KPI après nettoyage
    print("\n" + "="*70)
    print(" ÉTAT FINAL DES DONNÉES")
    print("="*70)
    kpi_after = kpi_quality(df_clean, "Clients (APRÈS)")
    print_quality_report(kpi_after)
    
    # 5. Sauvegarder les résultats
    save_results(df_clean, kpi_before, kpi_after)
    
    print("\n" + "="*70)
    print(" NETTOYAGE TERMINÉ AVEC SUCCÈS!")
    print("="*70)
    print(f" Fichier nettoyé: {CLEAN_DATA_PATH}")
    print(f" Rapport KPI: {REPORT_PATH}")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()