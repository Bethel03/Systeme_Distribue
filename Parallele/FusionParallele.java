package Parallele;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class FusionParallele {
    private static final String BASE_PATH = "..\\archive\\";
    private static final String OUTPUT_DIR = "unified_java_parallel";

    public static void main(String[] args) throws IOException {
        Files.createDirectories(Paths.get(OUTPUT_DIR));
        long start = System.currentTimeMillis();

        // 1. Chargement Parallèle des référentiels dans des Maps thread-safe
        Map<String, String> cards = chargerEnParallele("cards_data.csv", 0);
        Map<String, String> users = chargerEnParallele("users_data.csv", 0);
        Map<String, String> fraud = chargerEnParallele("fraud_labels.csv", 0);

        // 2. Fusion Parallèle des transactions
        Path pathTrans = Paths.get(BASE_PATH + "transactions_data.csv");
        List<String> result = Files.lines(pathTrans)
                .parallel() // Activation du parallélisme local 
                .skip(1)    // Sauter l'en-tête
                .map(line -> {
                    String[] cols = line.split(",");
                    String txId = cols[0];
                    String cardId = cols[1];
                    
                    String fraudStatus = fraud.getOrDefault(txId, "0");
                    String cardDetail = cards.getOrDefault(cardId, "N/A");
                    
                    return line + "," + fraudStatus + "," + cardDetail;
                })
                .collect(java.util.stream.Collectors.toList());

        // 3. Écriture
        Files.write(Paths.get(OUTPUT_DIR + "/unified_data.csv"), result);
        
        System.out.println("Temps Parallèle Java: " + (System.currentTimeMillis() - start) + "ms");
    }

    private static Map<String, String> chargerEnParallele(String filename, int keyIdx) throws IOException {
        // Utilisation de ConcurrentHashMap pour éviter les erreurs de concurrence
        Map<String, String> map = new ConcurrentHashMap<>();
        Files.lines(Paths.get(BASE_PATH + filename))
             .parallel()
             .skip(1)
             .forEach(line -> {
                 String[] cols = line.split(",");
                 if (cols.length > keyIdx) map.put(cols[keyIdx], line);
             });
        return map;
    }
}