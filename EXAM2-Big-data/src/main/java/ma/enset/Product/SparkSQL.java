package ma.enset.Product;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SparkSQL {
    public static void main(String[] args) {
        SparkSession ss= SparkSession.builder().appName("TP SPARK SQL").master("local[*]").getOrCreate();
        // Configurer les détails de la connexion MySQL
        // Charger les données depuis les tables MySQL
        Dataset<Row> clients = ss.read().format("jdbc")
                .option("url", "jdbc:mysql://localhost:4306/DB_COMMERCE")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", "CLIENTS")
                .option("user", "root")
                .option("password", "")
                .load();

        Dataset<Row> commandes = ss.read().format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/DB_COMMERCE")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", "COMMANDES")
                .option("user", "root")
                .option("password", "")
                .load();

        // Afficher le nombre total de commandes
        long totalCommandes = commandes.count();
        System.out.println("Nombre total de commandes : " + totalCommandes);

        // Afficher le client qui a dépensé le plus
        Row clientPlusDepense = commandes.groupBy("ID_CLIENT")
                .sum("MONTANT_TOTALE")
                .orderBy("sum(MONTANT_TOTALE)", "desc")
                .first();
        System.out.println("Le client qui a dépensé le plus : " + clientPlusDepense.getAs("ID_CLIENT"));

        // Afficher la moyenne des dépenses par client
        Dataset<Row> moyenneDepensesParClient = commandes.groupBy("ID_CLIENT")
                .avg("MONTANT_TOTALE")
                .withColumnRenamed("avg(MONTANT_TOTALE)", "MOYENNE_DEPENSES");
        moyenneDepensesParClient.show();

        // Arrêter la session Spark
        ss.stop();

    }
}
