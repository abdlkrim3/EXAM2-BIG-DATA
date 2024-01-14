package ma.enset.Product;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;


public class App1 {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder()
                .appName("Products Analysis")
                .master("spark://spark-master:7077")
                .getOrCreate();
        Dataset<Row> df1=ss.read().option("header",true).csv("/bitnami/products.csv");
        System.out.println("****************************************************************");
        System.out.println("--------------------------- DataSet------------------------------");
        df1.show();
        System.out.println("******************************************************************");
        System.out.println("Question 1 : Afficher le produit le plus vendu en termes de montant total");
        Dataset<Row> totalAmountByProduct = df1.groupBy("produit_id").agg(sum("montant").as("total_amount")).orderBy(desc("total_amount")).limit(1);
        totalAmountByProduct.show();
        System.out.println("*******************************************************************");
        System.out.println("Question 2 : Afficher les 3 produits les plus vendus dans l'ensemble des données");
        Dataset<Row> top3Products = df1.groupBy("produit_id").agg(sum("montant").as("total_amount")).orderBy(desc("total_amount")).limit(3);
        top3Products.show();
        System.out.println("Question 3 : Afficher le montant total des achats pour chaque produit");
        Dataset<Row> totalAmountPerProduct = df1.groupBy("produit_id").agg(sum("montant").as("total_amount"));
        totalAmountPerProduct.show();


    }
}