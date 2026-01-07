package Locale;
import java.io.*;
import java.util.*;

public class Fusion_Locale {

    // Chemins pour la lecture des sources
    private static final String BASE_PATH = "..\\archive\\";
    private static final String PATH_CARDS = BASE_PATH + "cards_data.csv";
    private static final String PATH_USERS = BASE_PATH + "users_data.csv";
    private static final String PATH_TRANS = BASE_PATH + "transactions_data.csv";
    private static final String PATH_FRAUD = BASE_PATH + "fraud_labels.csv";
    
    // Dossier et fichier de sortie 
    private static final String OUTPUT_DIR = "unified_java";
    private static final String OUTPUT_FILE = "unified_financial_dataset.csv";

    public static void main(String[] args) {
        try {
            // Création du dossier de sortie s'il n'existe pas
            File directory = new File(OUTPUT_DIR);
            if (!directory.exists()) {
                directory.mkdirs();
            }

            String fullOutputPath = OUTPUT_DIR + File.separator + OUTPUT_FILE;

            System.out.println("--- ÉTAPE 1 : Chargement des référentiels en RAM ---");
            // Chargement séquentiel sur un seul thread 
            Map<String, String> cards = chargerCSVEnMap(PATH_CARDS, "id");
            Map<String, String> users = chargerCSVEnMap(PATH_USERS, "id");
            Map<String, String> fraud = chargerCSVEnMap(PATH_FRAUD, "transaction_id");

            System.out.println("--- ÉTAPE 2 : Fusion et écriture dans " + fullOutputPath + " ---");
            fusionnerEtEcrire(cards, users, fraud, fullOutputPath);

            System.out.println("Terminé avec succès !");

        } catch (IOException e) {
            System.err.println("Erreur : " + e.getMessage());
        }
    }

    private static Map<String, String> chargerCSVEnMap(String chemin, String nomCle) throws IOException {
        Map<String, String> map = new HashMap<>();
        File file = new File(chemin);
        if (!file.exists()) return map;

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String header = br.readLine();
            int indexCle = trouverIndexColonne(header, nomCle);
            
            String ligne;
            while ((ligne = br.readLine()) != null) {
                String[] colonnes = ligne.split(",");
                if (indexCle < colonnes.length) {
                    map.put(colonnes[indexCle], ligne); // Stockage en mémoire vive 
                }
            }
        }
        return map;
    }

    private static void fusionnerEtEcrire(Map<String, String> cards, Map<String, String> users, Map<String, String> fraud, String outPath) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(PATH_TRANS));
             PrintWriter pw = new PrintWriter(new FileWriter(outPath))) {

            String headerTrans = br.readLine();
            // Création d'un dataset unifié et cohérent 
            pw.println(headerTrans + ",is_fraud,card_info,user_info");

            String ligne;
            while ((ligne = br.readLine()) != null) {
                String[] cols = ligne.split(",");
                String txId = cols[0]; 
                String cardId = cols[1]; 

                StringBuilder sb = new StringBuilder(ligne);

                // Jointure Fraude 
                sb.append(",").append(fraud.getOrDefault(txId, "0"));

                // Jointure Cartes 
                if (cards.containsKey(cardId)) {
                    String infoCard = cards.get(cardId);
                    sb.append(",").append(infoCard.replace(",", "|")); // Séparateur pipe pour éviter de casser le CSV
                    
                    // Jointure Utilisateurs 
                    String userId = infoCard.split(",")[1]; 
                    sb.append(",").append(users.getOrDefault(userId, "N/A").replace(",", "|"));
                } else {
                    sb.append(",N/A,N/A");
                }

                pw.println(sb.toString());
            }
        }
    }

    private static int trouverIndexColonne(String header, String nomCle) {
        String[] cols = header.split(",");
        for (int i = 0; i < cols.length; i++) {
            if (cols[i].trim().equalsIgnoreCase(nomCle)) return i;
        }
        return 0;
    }
}