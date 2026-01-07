import polars as pl
import os

# Configuration des chemins selon votre structure
BASE_PATH = os.path.join("..", "archive")
OUTPUT_DIR = "unified_python_parallel"

# Création du dossier de sortie
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 1. Chargement parallèle des données
# Polars lit et analyse les types de colonnes de façon multi-threadée
df_trans = pl.read_csv(os.path.join(BASE_PATH, "transactions_data.csv"))
df_cards = pl.read_csv(os.path.join(BASE_PATH, "cards_data.csv"))
df_users = pl.read_csv(os.path.join(BASE_PATH, "users_data.csv"))
df_fraud = pl.read_csv(os.path.join(BASE_PATH, "fraud_labels.csv"))
# Chargement du JSON
df_mcc = pl.read_json(os.path.join(BASE_PATH, "mcc_codes.json"))

# 2. Fusion Parallèle 
# Polars optimise automatiquement l'ordre des jointures
result = (
    df_trans
    .join(df_fraud, left_on="id", right_on="transaction_id", how="left")
    .join(df_cards, left_on="card_id", right_on="id", how="left")
    .join(df_users, left_on="user_id", right_on="id", how="left")
)

# 3. Écriture rapide
result.write_csv(os.path.join(OUTPUT_DIR, "unified_dataset_polars.csv"))
print(f"Fusion parallèle Python terminée dans {OUTPUT_DIR}")