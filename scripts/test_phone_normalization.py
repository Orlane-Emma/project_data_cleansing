
import pandas as pd
import sys
import os

# Ajouter le chemin des scripts
sys.path.append(os.path.dirname(__file__))
from utils import normalize_phone_fr

# Exemples de numéros à tester
test_phones = [
    "06 12 34 56 78",
    "0612345678",
    "+33612345678",
    "33612345678",
    "0033612345678",
    "612345678",
    "06.12.34.56.78",
    "06-12-34-56-78",
    "(06) 12 34 56 78",
    None,
    "",
    "123",
    "abcdefghij",
    "01 23 45 67 89",  # Téléphone fixe
]

print("="*60)
print("TEST DE NORMALISATION DES TÉLÉPHONES")
print("="*60)

for phone in test_phones:
    result = normalize_phone_fr(phone)
    status = "good" if result else "bad"
    print(f"{status} {str(phone):20s} → {result}")

print("="*60)

# Test sur un petit échantillon du fichier clients.csv
print("\nTest sur un échantillon de clients.csv...")
try:
    df = pd.read_csv("../data/raw/clients.csv", nrows=10)
    print(f"\nPremiers numéros du fichier:")
    print(df[['telephone']].head())
    
    print(f"\nAprès normalisation:")
    df['telephone_normalise'] = df['telephone'].apply(normalize_phone_fr)
    print(df[['telephone', 'telephone_normalise']].head())
    
    valid_count = df['telephone_normalise'].notna().sum()
    print(f"\nRésultat: {valid_count}/10 numéros valides")
    
except FileNotFoundError:
    print(" Fichier clients.csv non trouvé")
except Exception as e:
    print(f" Erreur: {e}")