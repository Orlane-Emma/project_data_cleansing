import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime

# Ajouter le répertoire scripts au path pour importer utils
sys.path.append(os.path.dirname(__file__))
from utils import (
    standardize_email as normalize_email,
    is_valid_email,
    standardize_country as normalize_country,
    clean_phone as normalize_phone_fr,
    parse_date as normalize_date,
    is_valid_birthdate,
    kpi_quality,
    print_quality_report,
    merge_duplicates
)

# -------------------------
# CONFIGURATION
# -------------------------
RAW_DATA_PATH = "../data/raw/clients.csv"
CLEAN_DATA_PATH = "../data/clean/clients_clean.csv"
REPORT_PATH = "../data/reports/kpi_qualite_crm.csv"

# -------------------------
# FONCTIONS DE NETTOYAGE
# -------------------------
def load_data():
    print("Chargement des données clients...")
    try:
        df = pd.read_csv(RAW_DATA_PATH)
        print(f"{len(df)} lignes chargées avec succès")
        print(f"Colonnes trouvées: {list(df.columns)}")
        return df
    except FileNotFoundError:
        print(f"Erreur: Le fichier {RAW_DATA_PATH} n'existe pas!")
        return None
    except Exception as e:
        print(f"Erreur lors du chargement: {e}")
        return None

def clean_emails(df):
    print("\nNettoyage des emails...")
    email_col = next((col for col in df.columns if any(k in col.lower() for k in ['email','courriel','mail'])), None)
    if email_col is None:
        print("Aucune colonne email trouvée")
        return df
    total = len(df)
    valid_before = df[email_col].apply(is_valid_email).sum()
    df[f'{email_col}_original'] = df[email_col].copy()
    df[email_col] = df[email_col].apply(normalize_email)
    valid_after = df[email_col].notna().sum()
    invalid_count = total - valid_after
    print(f"  Emails valides avant: {valid_before}/{total} ({valid_before/total*100:.1f}%)")
    print(f"  Emails valides après: {valid_after}/{total} ({valid_after/total*100:.1f}%)")
    print(f"  Emails invalides supprimés: {invalid_count}")
    return df

def clean_countries(df):
    print("\nStandardisation des pays...")
    country_col = next((col for col in df.columns if any(k in col.lower() for k in ['pays','country'])), None)
    if country_col is None:
        print("Aucune colonne pays trouvée")
        return df
    unique_before = df[country_col].nunique()
    df[f'{country_col}_original'] = df[country_col].copy()
    df[country_col] = df[country_col].apply(normalize_country)
    unique_after = df[country_col].nunique()
    print(f"  Variantes uniques avant: {unique_before}")
    print(f"  Variantes uniques après: {unique_after}")
    print(f"  Réduction de {unique_before - unique_after} variantes")
    return df

def clean_phones(df):
    print("\nNettoyage des téléphones...")
    phone_col = next((col for col in df.columns if any(k in col.lower() for k in ['tel','phone'])), None)
    if phone_col is None:
        print("Aucune colonne téléphone trouvée")
        return df
    valid_before = df[phone_col].notna().sum()
    df[f'{phone_col}_original'] = df[phone_col].copy()
    df[f'{phone_col}_normalise'] = df[phone_col].apply(normalize_phone_fr)
    valid_after = df[f'{phone_col}_normalise'].notna().sum()
    print(f"  Téléphones valides avant: {valid_before}/{len(df)}")
    print(f"  Téléphones valides après: {valid_after}/{len(df)}")
    print(f"  Téléphones invalides: {len(df) - valid_after}")
    return df

def clean_birthdates(df):
    print("\nValidation des dates de naissance...")
    birth_col = next((col for col in df.columns if any(k in col.lower() for k in ['naissance','birth','dob'])), None)
    if birth_col is None:
        print("Aucune colonne date de naissance trouvée")
        return df
    df[f'{birth_col}_original'] = df[birth_col].copy()
    df[birth_col] = df[birth_col].apply(normalize_date)
    df['date_naissance_valide'] = df[birth_col].apply(is_valid_birthdate)
    valid_count = df['date_naissance_valide'].sum()
    invalid_count = len(df) - valid_count
    print(f"  Dates valides: {valid_count}/{len(df)}")
    print(f"  Dates invalides (futures ou > 120 ans): {invalid_count}")
    df.loc[~df['date_naissance_valide'], birth_col] = None
    return df

def remove_duplicates(df):
    print("\nSuppression des doublons...")
    rows_before = len(df)
    key_columns = [col for col in df.columns if any(k in col.lower() for k in ['nom','name','prenom','firstname','lastname','email','courriel']) and 'original' not in col.lower()]
    if not key_columns:
        print("Impossible de détecter les colonnes clés pour déduplication")
        return df
    print(f"  Colonnes clés utilisées: {key_columns}")
    df = merge_duplicates(df, key_columns, keep='most_complete')
    rows_after = len(df)
    duplicates_removed = rows_before - rows_after
    print(f"  Lignes avant: {rows_before}")
    print(f"  Lignes après: {rows_after}")
    print(f"  Doublons supprimés: {duplicates_removed} ({duplicates_removed/rows_before*100:.1f}%)")
    return df

def save_results(df_clean, kpi_before, kpi_after):
    print("\nSauvegarde des résultats...")
    os.makedirs(os.path.dirname(CLEAN_DATA_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    df_clean.to_csv(CLEAN_DATA_PATH, index=False)
    print(f"  Données nettoyées sauvegardées: {CLEAN_DATA_PATH}")
    kpi_comparison = pd.DataFrame({
        'Métrique': ['Nombre de lignes', 'Taux complétude global (%)', 'Taux doublons (%)', 'Taux valeurs manquantes (%)'],
        'Avant': [kpi_before['total_rows'], kpi_before['global_completeness_rate'], kpi_before['duplicate_rate'], kpi_before['missing_rate']],
        'Après': [kpi_after['total_rows'], kpi_after['global_completeness_rate'], kpi_after['duplicate_rate'], kpi_after['missing_rate']]
    })
    kpi_comparison['Amélioration'] = kpi_comparison['Après'] - kpi_comparison['Avant']
    kpi_comparison.to_csv(REPORT_PATH, index=False)
    print(f"  Rapport KPI sauvegardé: {REPORT_PATH}")
    print("\nCOMPARAISON AVANT/APRÈS:")
    print(kpi_comparison.to_string(index=False))

# -------------------------
# PIPELINE PRINCIPAL
# -------------------------
def main():
    print("\n" + "="*70)
    print(" PROJET 1: CRM DE QUALITÉ OPTIMALE")
    print("="*70)
    df = load_data()
    if df is None:
        return

    # KPI avant nettoyage
    print("\n" + "="*70)
    print(" ÉTAT INITIAL DES DONNÉES")
    print("="*70)
    kpi_before = kpi_quality(df, "Clients (AVANT)")
    print_quality_report(kpi_before)

    # Nettoyage
    print("\n" + "="*70)
    print(" NETTOYAGE DES DONNÉES")
    print("="*70)
    df_clean = df.copy()
    df_clean = clean_emails(df_clean)
    df_clean = clean_countries(df_clean)
    df_clean = clean_phones(df_clean)
    df_clean = clean_birthdates(df_clean)
    df_clean = remove_duplicates(df_clean)

    # KPI après nettoyage
    print("\n" + "="*70)
    print(" ÉTAT FINAL DES DONNÉES")
    print("="*70)
    kpi_after = kpi_quality(df_clean, "Clients (APRÈS)")
    print_quality_report(kpi_after)

    # Sauvegarder résultats
    save_results(df_clean, kpi_before, kpi_after)
    print("\n" + "="*70)
    print(" NETTOYAGE TERMINÉ AVEC SUCCÈS!")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()
