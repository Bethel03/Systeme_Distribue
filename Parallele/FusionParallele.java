import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.functions;

import java.util.Map;
import java.util.HashMap;
import java.nio.file.Paths;

public class UnifiedFinancialDataset {
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        // --- Initialisation Spark ---
        SparkSession spark = SparkSession.builder()
                .appName("UnifiedFinancialDataset")
                .master("local[*]") // exécution locale parallèle
                .getOrCreate();

        // Définition des chemins
        String basePath = Paths.get("..", "archive").toString();
        String unifiedPath = Paths.get("unified_py").toString();

        String pathCards = Paths.get(basePath, "cards_data.csv").toString();
        String pathUsers = Paths.get(basePath, "users_data.csv").toString();
        String pathTrans = Paths.get(basePath, "transactions_data.csv").toString();
        String pathMcc   = Paths.get(basePath, "mcc_codes.json").toString();
        String pathFraud = Paths.get(basePath, "train_fraud_labels.json").toString();

        System.out.println("--- ÉTAPE 1 : Chargement des fichiers de référence ---");

        // Chargement CSV
        Dataset<Row> cardsDf = spark.read().option("header", true).csv(pathCards);
        Dataset<Row> usersDf = spark.read().option("header", true).csv(pathUsers);
        Dataset<Row> transDf = spark.read().option("header", true).csv(pathTrans);

        // Chargement JSON fraude
        Dataset<Row> fraudDf = spark.read().json(pathFraud);
        if (!fraudDf.columns().toString().contains("transaction_id")) {
            // Exemple : si structure différente, on peut adapter
            fraudDf = fraudDf.withColumnRenamed("id", "transaction_id");
        }

        // Chargement MCC
        Dataset<Row> mccDf = spark.read().json(pathMcc);

        System.out.println("\n--- ÉTAPE 2 : Fusion des transactions ---");

        Dataset<Row> datasetFinal = transDf;

        // Jointure Transaction + Fraude
        datasetFinal = datasetFinal.join(fraudDf,
                datasetFinal.col("id").equalTo(fraudDf.col("transaction_id")),
                "left");

        // Jointure Transaction + Carte
        datasetFinal = datasetFinal.join(cardsDf,
                datasetFinal.col("card_id").equalTo(cardsDf.col("id")),
                "left");

        // Jointure Carte + Utilisateur
        datasetFinal = datasetFinal.join(usersDf,
                datasetFinal.col("user_id").equalTo(usersDf.col("id")),
                "left");

        // Jointure MCC
        datasetFinal = datasetFinal.join(mccDf,
                datasetFinal.col("mcc").equalTo(mccDf.col("mcc")),
                "left");

        System.out.println("\nSuccès : " + datasetFinal.count() + " lignes ont été fusionnées.");
        System.out.println("L'ensemble des fichiers a été traité en parallèle grâce à Spark.");

        // --- ÉTAPE 3 : Écriture du fichier final ---
        String pathOutput = Paths.get(unifiedPath, "unified_financial_dataset.csv").toString();
        System.out.println("--- ÉTAPE 3 : Écriture du fichier final ---");

        datasetFinal.write().option("header", true).csv(pathOutput);

        long elapsedTime = System.currentTimeMillis() - startTime;
        System.out.printf("\n Temps total d'exécution : %.2f secondes%n", elapsedTime / 1000.0);

        spark.stop();
    }
}
