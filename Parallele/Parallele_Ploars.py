import polars as pl
import json
import os
import time

# Définition des chemins des fichiers de données
BASE_PATH = os.path.join(os.path.dirname(__file__), "..", "archive")
UNIFIED_DATASET_PATH = os.path.join(os.path.dirname(__file__), "unified_py")

PATH_CARDS  = os.path.join(BASE_PATH, "cards_data.csv")
PATH_USERS  = os.path.join(BASE_PATH, "users_data.csv")
PATH_TRANS  = os.path.join(BASE_PATH, "transactions_data.csv")
PATH_MCC    = os.path.join(BASE_PATH, "mcc_codes.json")
PATH_FRAUD  = os.path.join(BASE_PATH, "train_fraud_labels.json")

# --- Benchmark : début ---
start_time = time.time()

print("--- ÉTAPE 1 : Chargement des fichiers de référence ---")

# Chargement CSV avec Polars
cards_df = pl.read_csv(PATH_CARDS) if os.path.exists(PATH_CARDS) else pl.DataFrame()
users_df = pl.read_csv(PATH_USERS) if os.path.exists(PATH_USERS) else pl.DataFrame()
trans_df = pl.read_csv(PATH_TRANS) if os.path.exists(PATH_TRANS) else pl.DataFrame()

# Chargement/normalisation du JSON de fraude
fraud_df = pl.DataFrame()
if os.path.exists(PATH_FRAUD):
    try:
        # Essayons de lire directement
        tmp = pl.read_json(PATH_FRAUD)
        # Si la colonne "transaction_id" n'existe pas, tentons de la reconstruire
        if "transaction_id" in tmp.columns:
            fraud_df = tmp
        else:
            # Charger brut via json pour déduire la structure
            with open(PATH_FRAUD, "r", encoding="utf-8") as f:
                raw = json.load(f)
            # Cas 1: dict {transaction_id: target}
            if isinstance(raw, dict):
                fraud_df = pl.DataFrame(
                    {"transaction_id": list(raw.keys()), "target": list(raw.values())}
                )
            # Cas 2: liste d'objets [{"transaction_id": "...", "target": ...}, ...]
            elif isinstance(raw, list) and len(raw) > 0 and isinstance(raw[0], dict):
                fraud_df = pl.from_dicts(raw)
                # Si "transaction_id" toujours absent mais "id" présent, renommer
                if "transaction_id" not in fraud_df.columns and "id" in fraud_df.columns:
                    fraud_df = fraud_df.rename({"id": "transaction_id"})
            else:
                # Dernier recours: si tmp contient juste "target", on ne peut pas joindre
                fraud_df = pl.DataFrame()
    except Exception:
        # En cas d'échec total, on désactive la jointure fraude
        fraud_df = pl.DataFrame()

# Chargement et préparation du MCC
mcc_lookup = {}
if os.path.exists(PATH_MCC):
    with open(PATH_MCC, 'r', encoding='utf-8') as f:
        mcc_lookup = json.load(f)

mcc_df = pl.DataFrame()
if mcc_lookup:
    # Certaines sources MCC sont en int; on aligne avec le type de trans_df["mcc"]
    mcc_keys = list(mcc_lookup.keys())
    # Déterminer le type souhaité: si "mcc" dans trans_df est str -> str, sinon int
    desired_str = "mcc" in trans_df.columns and trans_df["mcc"].dtype == pl.Utf8
    if desired_str:
        mcc_df = pl.DataFrame({
            "mcc": [str(k) for k in mcc_keys],
            "mcc_description": [mcc_lookup[k] for k in mcc_keys]
        })
    else:
        # Tenter int sinon garder str si conversion impossible
        try:
            mcc_df = pl.DataFrame({
                "mcc": [int(k) for k in mcc_keys],
                "mcc_description": [mcc_lookup[k] for k in mcc_keys]
            })
        except Exception:
            mcc_df = pl.DataFrame({
                "mcc": [str(k) for k in mcc_keys],
                "mcc_description": [mcc_lookup[k] for k in mcc_keys]
            })

print("\n--- ÉTAPE 2 : Fusion des transactions (parallèle avec Polars) ---")

dataset_final = trans_df

# Harmonisation des types clés pour les jointures
# id (transactions) vs transaction_id (fraude)
if dataset_final.shape[0] > 0 and fraud_df.shape[0] > 0:
    # Aligner dtypes
    if "id" in dataset_final.columns and "transaction_id" in fraud_df.columns:
        if dataset_final["id"].dtype != fraud_df["transaction_id"].dtype:
            fraud_df = fraud_df.with_columns(
                pl.col("transaction_id").cast(dataset_final["id"].dtype)
            )
        dataset_final = dataset_final.join(
            fraud_df, left_on="id", right_on="transaction_id", how="left"
        )

# Jointure Transaction + Carte (card_id -> cards.id)
if dataset_final.shape[0] > 0 and cards_df.shape[0] > 0:
    if "card_id" in dataset_final.columns and "id" in cards_df.columns:
        # Aligner dtypes
        if dataset_final["card_id"].dtype != cards_df["id"].dtype:
            cards_df = cards_df.with_columns(pl.col("id").cast(dataset_final["card_id"].dtype))
        dataset_final = dataset_final.join(
            cards_df, left_on="card_id", right_on="id", how="left"
        )

# Jointure Carte + Utilisateur (user_id déjà dans dataset_final après jointure cartes)
if dataset_final.shape[0] > 0 and users_df.shape[0] > 0:
    if "user_id" in dataset_final.columns and "id" in users_df.columns:
        if dataset_final["user_id"].dtype != users_df["id"].dtype:
            users_df = users_df.with_columns(pl.col("id").cast(dataset_final["user_id"].dtype))
        dataset_final = dataset_final.join(
            users_df, left_on="user_id", right_on="id", how="left"
        )

# Jointure MCC
if dataset_final.shape[0] > 0 and mcc_df.shape[0] > 0 and "mcc" in dataset_final.columns:
    if dataset_final["mcc"].dtype != mcc_df["mcc"].dtype:
        mcc_df = mcc_df.with_columns(pl.col("mcc").cast(dataset_final["mcc"].dtype))
    dataset_final = dataset_final.join(mcc_df, on="mcc", how="left")

print(f"\n Succès : {dataset_final.shape[0]} lignes ont été fusionnées.")
print(f"L'ensemble des fichiers a été traité en parallèle grâce à Polars.")

# --- ÉTAPE 3 : Écriture du fichier final ---
PATH_OUTPUT = os.path.join(UNIFIED_DATASET_PATH, "unified_financial_dataset.csv")
print(f"--- ÉTAPE 3 : Écriture du fichier final ---")

if dataset_final.shape[0] > 0:
    # Écriture directe
    dataset_final.write_csv(PATH_OUTPUT)
    print(f" Fichier généré avec succès ici : {PATH_OUTPUT}")
else:
    print(" Aucun enregistrement à écrire (vérifie le chemin des fichiers et leur contenu).")

# --- Benchmark : fin ---
end_time = time.time()
elapsed_time = end_time - start_time
print(f"\n⏱ Temps total d'exécution : {elapsed_time:.2f} secondes")
