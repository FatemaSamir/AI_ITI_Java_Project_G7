package com.example.Java_Project_G7;

import joinery.DataFrame;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;


import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;

public class DataProvider implements JobDAO{

    String path = "src/main/resources/Wuzzuf_Jobs_Cleand.csv";
    DataFrame<Object> jobs = null;


    public static void main(String[] args) {

        DataProvider p = new DataProvider( );
        Dataset<Row> DataFrame= p.readData_Spark();
//        p.dfStructure()
//        p.Most_pop_Skills(DataFrame);
//        p.readcsv();
//        p.displayHeader();
//        p.dfStructure();
//        p.count_jobs_company();
//p.dataSummary(DataFrame);
//p.dataDiscribee(DataFrame);
//p.removeDuplicate(DataFrame);
//p.removeNull(DataFrame);
//        p.CleanData(DataFrame);

    }


    @Override
    public  Dataset<Row> readData_Spark() {

        final SparkSession sparkSession = SparkSession.builder ().appName ("Spark WuZZuf Jobs Analysis ").master ("local[3]")
                .getOrCreate ();

        final DataFrameReader dataFrameReader = sparkSession.read ().option ("header", "true");

        final Dataset<Row> csvDataFrame = dataFrameReader.csv ("src/main/resources/Wuzzuf_Jobs.csv");
        System.out.println("==================Starting reading csv file==================");
//        for (Row r:csvDataFrame.collectAsList()
//             ) {
//            System.out.println(r.get(7).toString());
//
//        }

        csvDataFrame.show(30);

        return csvDataFrame;
    }

    @Override
    public  void readcsv() {

        System.out.println("==================Starting reading csv file==================");
         ;
        if (path != null)
        {
            try {

                jobs = DataFrame.readCsv(path)
                        .retain("Title","Company","Location","Type","Level","YearsExp","Country","Skills");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else
        {
            System.out.println("==================Reading csv file is failed==================");
        }
 }

    @Override
    public void displayHeader() {

        System.out.println("==================First 10 Rows of DataFrame==================");

        System.out.println("head\n\n"+jobs.head());


    }

    @Override
    public String  dfStructure(Dataset<Row> Data) {

//        System.out.println(
//                "\n--------------------------------File Summary--------------------------------\n"
//                        +"--This DataFrame has "+ jobs.size()+ " columns, and "+ jobs.length()+ " rows\n"
//                        +"--Columns Names are: "+ jobs.columns()+ " \n"
//                        +"--------------------------------***--------------------------------"
//        );

        Data.printSchema();
//        StructType d = Data.schema().catalogString();
//        return d.prettyJson();
return   Data.schema().treeString();
    }

    @Override
    public String dataSummary(Dataset<Row> Data ) {

        System.out.println("All Data is text can't do summary statistic on it!");

        Dataset<Row> d = Data.summary();
        List<Row> summary = d.collectAsList();
        return Htmlshow.displayrows(d.columns(), summary);
    }

    @Override
    public void dataDiscribee(Dataset<Row> Data) {
        // implemented by fatema samir
        System.out.println("============================================= Describe =============================================");
        Data.describe().show();

    }

    @Override
    public Dataset<Row> removeDuplicate(Dataset<Row> Data) {
// implemented by fatema samir
        System.out.println("========================================== dropDuplicates ============================================");

        Dataset<Row> dubl = Data.distinct();
//        Dataset<Row> dubl = Data.dropDuplicates();
        System.out.println("========================================== ");
        return dubl;
    }

    @Override
    public Dataset<Row> removeNull(Dataset<Row> Data) {
        // implemented by fatema samir

        System.out.println("========================================== na drop all ============================================");
//        Data = Data.filter("YearsExp != 'null Yrs of Exp'");
        System.out.println(" the size of Data befor nulllllllllllllllllllllll"+Data.count());
        Dataset<Row> cleanDF =  Data.na().drop();
        System.out.println(" the size of Data afterrrrrrrrrr nulllllllllllllllllllllll"+cleanDF.count());

//        cleanDF.show(10);
        System.out.println("========================================== ");

return cleanDF;
    }

    // remove null and duplicate Values
    public Dataset<Row> CleanData(Dataset<Row> Data) {
// implemented by fatema samir
        Dataset<Row> Delet_null =  Data.na().drop();
        Dataset<Row> CleanData = Delet_null.distinct();
//       Dataset<Row> dubl = Data.dropDuplicates();
        DataFrameWriter dataFrameWriter = new DataFrameWriter<>(CleanData);
//        dataFrameWriter.option("sep",",").option("header","true").csv("src/main/resources/Wuzzuf_Jobs_Cleand.csv");
//        CleanData.write().csv("src/main/resources/Wuzzuf_Jobs_Cleand.csv");

        return CleanData;
    }

    @Override
    public void count_jobs_company() {
        DataFrame df = jobs.groupBy("Company");
        System.out.println("----------------------------###------------------------------------");
        System.out.println(df.head());
    }

    @Override
    public void most_demanding_comp() {

    }

public  List<Map.Entry> Most_pop_Skills (Dataset<Row> Data){

// Clean Data First
    DataProvider p = new DataProvider( );
    Dataset<Row> CleanedData = p.CleanData(Data);

    JavaRDD<String> AllSkills = CleanedData.toJavaRDD().flatMap ((Row skill) -> Arrays.asList (skill.get(7).toString().toLowerCase ()
            .trim ()
            .replaceAll ("\"", "").trim()
            .split (", ")).iterator ());

    //Print All Skills
    System.out.println(AllSkills.toString ());
//        AllSkills.foreach(System.out::println); false Statement

    // COUNTING and Sort In descending order
    Map<String, Long> SkillsCounts = AllSkills.countByValue ();
    List<Map.Entry> sorted = SkillsCounts.entrySet ().stream ()
            .sorted (Map.Entry.comparingByValue (Comparator.reverseOrder())).collect (Collectors.toList ());


    // DISPLAY
    for (Map.Entry<String, Long> entry : sorted) {
        System.out.println (entry.getKey () + " : " + entry.getValue ());
    }

    return sorted.stream().limit(30).collect(Collectors.toList());

}
}
