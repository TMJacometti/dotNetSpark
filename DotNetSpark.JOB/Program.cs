using System;
using Microsoft.Spark.Sql;

namespace DotNetSpark.JOB
{
    class Program
    {
        static void Main(string[] args)
        {
            LoginSql();
        }

        private static void Exemplo1()
        {
            // Create a Spark session
            var spark = SparkSession
                .Builder()
                .AppName("word_count_sample")
                .GetOrCreate();

            // Create initial DataFrame
            DataFrame dataFrame = spark.Read().Text("input.txt");

            // Count words
            var words = dataFrame
                .Select(Functions.Split(Functions.Col("value"), " ").Alias("words"))
                .Select(Functions.Explode(Functions.Col("words"))
                .Alias("word"))
                .GroupBy("word")
                .Count()
                .OrderBy(Functions.Col("count").Desc());

            // Show results
            words.Show();

            // Stop Spark session
            spark.Stop();
        }
        private static void LoginSql()
        {
            string query = "select * from tblUser";
            var spark = SparkSession.Builder().GetOrCreate();
            DataFrameReader dataFrameReader = spark.Read();
            dataFrameReader = dataFrameReader.Format("jdbc");
            dataFrameReader = dataFrameReader.Option("Driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver");

            dataFrameReader = dataFrameReader.Option("url", "jdbc:sqlserver://localhost:1433;databaseName=UserSystem;user=sa;password=@@Error15;");
            dataFrameReader = dataFrameReader.Option("dbtable", query);
            DataFrame dataFrame = dataFrameReader.Load();
            dataFrame.Show();
            spark.Stop();

        }
    }
}
