import csv
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

def charger_csv_en_dict(chemin, cle_primaire):
    """Charge un CSV en mémoire sous forme de dictionnaire"""
    donnees = {}
    if not os.path.exists(chemin):
        print(f"Fichier introuvable : {chemin}")
        return donnees
    
    with open(chemin, mode='r', encoding='utf-8') as f:
        lecteur = csv.DictReader(f)
        for ligne in lecteur:
            # Utilisation de la clé de jointure (id de la carte) 
            donnees[ligne[cle_primaire]] = ligne
    return donnees

# --- Benchmark : début ---
start_time = time.time()

# 1. Chargement des données de référence (Lookups)
print("--- ÉTAPE 1 : Chargement des fichiers de référence ---")
cards_dict = charger_csv_en_dict(PATH_CARDS, 'id')
users_dict = charger_csv_en_dict(PATH_USERS, 'id')
fraud_dict = charger_csv_en_dict(PATH_FRAUD, 'transaction_id')

# Chargement du JSON MCC 
mcc_lookup = {}
if os.path.exists(PATH_MCC):
    with open(PATH_MCC, 'r') as f:
        mcc_lookup = json.load(f)

# 2. Processus de Fusion Séquentielle (Approche Locale) 
print("\n--- ÉTAPE 2 : Fusion des transactions ---")
dataset_final = []

if os.path.exists(PATH_TRANS):
    with open(PATH_TRANS, mode='r', encoding='utf-8') as f:
        transactions = csv.DictReader(f)
        for tx in transactions:
            # Jointure Transaction + Fraude 
            tx_id = tx.get('id')
            if tx_id in fraud_dict:
                tx.update(fraud_dict[tx_id])

            # Jointure Transaction + Carte (via card_id) 
            c_id = tx.get('card_id')
            if c_id in cards_dict:
                infos_carte = cards_dict[c_id]
                tx.update(infos_carte) 
                
                # Jointure Carte + Utilisateur (via user_id) 
                u_id = infos_carte.get('user_id')
                if u_id in users_dict:
                    tx.update(users_dict[u_id])

            # Jointure MCC (Catégories commerçants) 
            m_code = tx.get('mcc')
            if m_code in mcc_lookup:
                tx['mcc_description'] = mcc_lookup[m_code]

            dataset_final.append(tx)

print(f"\n Succès : {len(dataset_final)} lignes ont été fusionnées.")
print(f"L'ensemble des fichiers a été traité sur un seul thread.")

# 3. Écriture du fichier final
PATH_OUTPUT = os.path.join(UNIFIED_DATASET_PATH, "unified_financial_dataset.csv")

print(f"--- ÉTAPE 3 : Écriture du fichier final ---")

if dataset_final:
    # On récupère les noms de colonnes à partir du premier dictionnaire
    champs = dataset_final[0].keys()
    
    with open(PATH_OUTPUT, mode='w', encoding='utf-8', newline='') as f:
        scripteur = csv.DictWriter(f, fieldnames=champs)
        scripteur.writeheader()
        scripteur.writerows(dataset_final)
    
    print(f" Fichier généré avec succès ici : {PATH_OUTPUT}")

# --- Benchmark : fin ---
end_time = time.time()
elapsed_time = end_time - start_time
print(f"\n⏱ Temps total d'exécution : {elapsed_time:.2f} secondes")
