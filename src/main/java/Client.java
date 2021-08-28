import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Client {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("readdata").master("local").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> ds = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://10.11.6.23:2881/tpch_1g_part")
                .option("dbtable", "region")
                .option("user", "root@test_mrhanice")
                .option("password", "123456")
                .option("driver","com.mysql.jdbc.Driver")
                .load();
        long count = ds.count();
        System.out.println("count = " + count);
//        ds.printSchema();
//        ds.show();
    }
}
