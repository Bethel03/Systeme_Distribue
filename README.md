

### README.md

```markdown
# Étude Comparative : Intégration et Fusion de Données Financières 

Ce projet implémente différentes stratégies de fusion de données massives pour un jeu de données bancaires (transactions, cartes, utilisateurs, fraudes et MCC). L'objectif est d'analyser l'impact de l'architecture logicielle sur les performances de traitement.

## 📂 Structure du Répertoire

```text
Systeme_Distribue/
├── archive/                # Sources de données brutes (CSV, JSON)
│   ├── transactions_data.csv
│   ├── cards_data.csv
│   ├── users_data.csv
│   ├── fraud_labels.csv
│   └── mcc_codes.json
├── locale/                 # Approche 1 : Séquentielle (Mono-thread)
│   ├── fusion_locale.py    # Implémentation Python (Dictionnaires)
│   └── FusionLocale.java   # Implémentation Java (HashMap)
├── parallele/              # Approche 2 : Parallélisme Local (Multi-thread)
│   ├── fusion_parallele.py # Implémentation Python (Polars)
│   └── FusionParallele.java# Implémentation Java (Parallel Streams)
└── README.md

```

## 🛠️ Détails des Approches

### 1. Dossier `locale/` (Approche Séquentielle)

Cette approche respecte la contrainte de n'utiliser qu'un **seul processeur et un seul thread**.

* **Mécanique** : Lecture des fichiers de référence en mémoire vive dans des structures de hachage.
* **Complexité** :  pour le temps, où  est le nombre de transactions.

### 2. Dossier `parallele/` (Approche Parallèle Légère)

Exploitation de la puissance de calcul de tous les cœurs du processeur local.

* **Python (Polars)** : Utilise des algorithmes de jointure multi-threadés.
* **Java (Parallel Streams)** : Divise le flux de données en segments traités simultanément.

## 📤 Génération des Résultats (Étape Finale)

À la fin de chaque exécution (qu'elle soit locale ou parallèle), le système est conçu pour exporter les données fusionnées dans des dossiers dédiés à la racine du projet :

1. **`unified_java/`** : Contient le fichier CSV final généré par les scripts Java.
2. **`unified_python/`** : Contient le fichier CSV final généré par les scripts Python.

**Note :** Ces dossiers sont créés automatiquement par le code s'ils n'existent pas encore. Ils regroupent l'ensemble des informations (Transaction + Carte + Utilisateur + Fraude + MCC) en une seule ligne cohérente.

## 🚀 Instructions d'exécution

### Pour Python

Les scripts s'attendent à trouver les données dans `../archive/`.

```bash
cd locale
python fusion_locale.py

```

### Pour Java

Compilez et lancez les classes depuis leurs dossiers respectifs.

```bash
cd parallele
javac FusionParallele.java
java FusionParallele

```

## 📊 Rapport de Performance

Le rapport synthétique joint analyse :

* **Temps de traitement** : Comparaison mono-thread vs multi-thread.
* **Occupation RAM** : Analyse de l'empreinte mémoire lors de la montée en charge.

```
