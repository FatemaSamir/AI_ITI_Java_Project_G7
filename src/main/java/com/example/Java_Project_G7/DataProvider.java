package com.example.Java_Project_G7;

import com.aol.cyclops.control.LazyReact;
import joinery.DataFrame;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.knowm.xchart.*;


import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.clustering.KMeans;

// import org.apache.log4j.Level;
//import org.apache.log4j.Logger;

import org.apache.spark.mllib.linalg.Vectors;
import org.sparkproject.dmg.pmml.StringValue;
import org.threeten.extra.DayOfYear;

import static org.apache.spark.sql.functions.col;

public class DataProvider implements JobDAO {

    String path = "src/main/resources/Wuzzuf_Jobs.csv";
    DataFrame<Object> jobs = null;
    private SparkSession sparkSession ;
    public DataProvider(){
         this.sparkSession = SparkSession.builder().appName("Spark WuZZuf Jobs Analysis ").master("local[3]")
                .getOrCreate();
    }


    public static void main(String[] args) {

        DataProvider p = new DataProvider();
        Dataset<Row> DataFrame = p.readData_Spark();
        p.dataDiscribee(DataFrame);
//        p.dfStructure()
//        p.Most_pop_Skills(DataFrame);
//       p.readcsv();
//        p.displayHeader();
//        p.dfStructure();
//        p.count_jobs_company();
//p.dataSummary(DataFrame);
//p.dataDiscribee(DataFrame);
//p.removeDuplicate(DataFrame);
//p.removeNull(DataFrame);
//        p.CleanData(DataFrame);

//        p.factorizeYearsExp();
//        p.kmeanAlgorithm();

    }


    @Override
    public Dataset<Row> readData_Spark() {

        final DataFrameReader dataFrameReader = sparkSession.read().option("header", "true");

 //       JavaRDD<Row> rdd = dataFrameReader.csv("src/main/resources/Wuzzuf_Jobs.csv").toJavaRDD();
        final Dataset<Row> csvDataFrame = dataFrameReader.csv("src/main/resources/Wuzzuf_Jobs.csv");
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
    public void readcsv() {

        System.out.println("==================Starting reading csv file==================");
        ;
        if (path != null) {
            try {

                jobs = DataFrame.readCsv(path)
                        .retain("Title", "Company", "Location", "Type", "Level", "YearsExp", "Country", "Skills");
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("==================Reading csv file is failed==================");
        }
    }

    @Override
    public void displayHeader() {

        System.out.println("==================First 10 Rows of DataFrame==================");

        System.out.println("head\n\n" + jobs.head());


    }

    @Override
    public String dfStructure(Dataset<Row> Data) {

//        System.out.println(
//                "\n--------------------------------File Summary--------------------------------\n"
//                        +"--This DataFrame has "+ jobs.size()+ " columns, and "+ jobs.length()+ " rows\n"
//                        +"--Columns Names are: "+ jobs.columns()+ " \n"
//                        +"--------------------------------***--------------------------------"
//        );

        Data.printSchema();
//        StructType d = Data.schema().catalogString();
//        return d.prettyJson();
        return Data.schema().treeString();
    }

    @Override
    public String dataSummary(Dataset<Row> Data) {

//        System.out.println("All Data is text can't do summary statistic on it!");

        Dataset<Row> d = Data.summary();
        List<Row> summary = d.collectAsList();
        return Htmlshow.displayrows(d.columns(), summary);
    }

    @Override
    public String dataDiscribee(Dataset<Row> Data) {
        // implemented by fatema samir
        System.out.println("============================================= Describe =============================================");
        Data.describe().show();
        Dataset<Row> d = Data.describe();
        List<Row> summary = d.collectAsList();
        return Htmlshow.displayrows(d.columns(), summary);

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
        System.out.println(" the size of Data befor nulllllllllllllllllllllll" + Data.count());
        Dataset<Row> cleanDF = Data.na().drop();
        System.out.println(" the size of Data afterrrrrrrrrr nulllllllllllllllllllllll" + cleanDF.count());

//        cleanDF.show(10);
        System.out.println("========================================== ");

        return cleanDF;
    }

    // remove null and duplicate Values
    public Dataset<Row> CleanData(Dataset<Row> Data) {
// implemented by fatema samir
        Dataset<Row> Delet_null = Data.na().drop();
        Dataset<Row> CleanData = Delet_null.distinct();
//       Dataset<Row> dubl = Data.dropDuplicates();
        // Save Data After Cleaned
//        DataFrameWriter dataFrameWriter = new DataFrameWriter<>(CleanData);
//        dataFrameWriter.option("sep",",").option("header","true").csv("src/main/resources/Wuzzuf_Jobs_Cleand.csv");
//        CleanData.write().csv("src/main/resources/Wuzzuf_Jobs_Cleand.csv");

        return CleanData;
    }


    //*********************************************************************//
    //***************************Question 4(a) *******************************//
    //*********************************************************************//
    @Override
    public Map<String, Long> count_jobs_company() {
        List<Object> Title = jobs.col("Title");
        List<String> Title_String = LazyReact.sequentialBuilder()
                .from(Title)
                .cast(String.class)
                .toList();

        Map<String, Long> Title_counter = Title_String.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        return Title_counter;
    }


    //*********************************************************************//
    //***************************Question 4(b) *******************************//
    //*********************************************************************//
    @Override
    public LinkedHashMap<String, Long> SortedCompinesCount() {

        // sorted titles has title of job and count
        Map<String, Long> Title_counter = count_jobs_company();
        LinkedHashMap<String, Long> sortedTitles = new LinkedHashMap<>();
        String sep = "----------------------------------------------------------------------";
        int count;

        Title_counter.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> sortedTitles.put(x.getKey(), x.getValue()));

        count = 1;
        System.out.println("\n\n\n" + sep);
        System.out.println("Top 5 Most Demanded jobs are:");
        for (Map.Entry entry : sortedTitles.entrySet()) {
            System.out.println(count + " = " + entry.getKey() +
                    " with " + entry.getValue() + " jobs.");
            count++;
            if (count > 5) {
                break;
            }
        }
        System.out.println(sep);
        return sortedTitles;

    }


    //*********************************************************************//
    //***************************Question 5 *******************************//
    //*********************************************************************//

    // Function Get Piechart most demanding companies for jobs
    public  void  drawPieChartjobs( ) throws IOException {

        LinkedHashMap<String, Long> sortedCompanies = SortedCompinesCount();
        List<String> Company_counter_keys = new ArrayList<>(sortedCompanies.keySet());

        List<String> Company_keys_chart = new ArrayList<>();

        Company_keys_chart = Company_counter_keys.subList(0, 5);

        PieChart company_chart = new PieChartBuilder().width(800).height(600).title("Top 5 Most demanding Companies").build();

        for (String i : Company_keys_chart) {
            company_chart.addSeries(i, sortedCompanies.get(i));
        }
//        new SwingWrapper<PieChart>(company_chart).displayChart();
        try {
            BitmapEncoder.saveBitmapWithDPI(company_chart, "src/main/resources/Step5_pieChart", BitmapEncoder.BitmapFormat.PNG, 300);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    //*********************************************************************//
    //***************************Question 6 *******************************//
    //*********************************************************************//

    //  6 6.	Find out what are the most popular job titles.
    public LinkedHashMap<String, Long> GetMostPopularJobsTitle(){

        String sep = "----------------------------------------------------------------------";
        int count;
        List<Object> Title = jobs.col("Title");
        List<String> Title_String = LazyReact.sequentialBuilder()
                .from(Title)
                .cast(String.class)
                .toList();

        Map<String, Long> Title_counter = Title_String.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        // sorted titles has title of job and count
        LinkedHashMap<String, Long> sortedTitles = new LinkedHashMap<>();

        Title_counter.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> sortedTitles.put(x.getKey(), x.getValue()));

        count = 1;
        System.out.println("\n\n\n" + sep);
        System.out.println("Top 5 Most Demanded jobs are:");
        for (Map.Entry entry : sortedTitles.entrySet()) {
            System.out.println(count + " = " + entry.getKey() +
                    " with " + entry.getValue() + " jobs.");
            count++;
            if (count > 5) {
                break;
            }
        }
        System.out.println(sep);
        return sortedTitles ;
    }


    //*********************************************************************//
    //***************************Question 7 *******************************//
    //*********************************************************************//

    public  void drawBarChartjobs() throws IOException {
        LinkedHashMap<String, Long> sortedTitles = GetMostPopularJobsTitle();

        List<Long> Title_counter_values = new ArrayList<>(sortedTitles.values());
        List<Double> double_values = Title_counter_values.stream()
                .map(Double::valueOf)
                .collect(Collectors.toList());

        List<Double> double_values_arr = new ArrayList<>(double_values);

        List<String> Title_counter_keys = new ArrayList<>(sortedTitles.keySet());

        List<String> Title_keys_chart = new ArrayList<>();
        List<Double> vals_chart = new ArrayList<>();

        Title_keys_chart = Title_counter_keys.subList(0, 4);
        vals_chart = double_values_arr.subList(0, 4);

        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).title("Bar Chart").xAxisTitle("Jobs").yAxisTitle("Demands").build();
        chart.getStyler().setHasAnnotations(true);
        chart.addSeries("\"most popular jobs .\"", Title_keys_chart, vals_chart);
//        new SwingWrapper(chart).displayChart();
        try {
            BitmapEncoder.saveBitmapWithDPI(chart, "src/main/resources/Step7_BarChartjobs", BitmapEncoder.BitmapFormat.PNG, 300);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    //*********************************************************************//
    //***************************Question 8 *******************************//
    //*********************************************************************//

    // 8.	 function Get Find out the most popular areas
    @Override
    public LinkedHashMap<String, Long> GetMostPopularArea(){

        String sep = "----------------------------------------------------------------------";
        int count;
        List<Object> Area = this.jobs.col("Location");
        List<String> Area_String = LazyReact.sequentialBuilder()
                .from(Area)
                .cast(String.class)
                .toList();

        Map<String, Long> Area_counter = Area_String.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        LinkedHashMap<String, Long> sortedAreas = new LinkedHashMap<>();

        Area_counter.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> sortedAreas.put(x.getKey(), x.getValue()));

        count = 1;
        System.out.println("\n\n\n" + sep);
        System.out.println("Top 5 Most popular locations are:");
        for (Map.Entry entry: sortedAreas.entrySet()) {
            System.out.println(count + " = " + entry.getKey() +
                    " with " + entry.getValue() + " jobs.");
            count++;
            if (count > 5) {
                break;
            }
        }
        System.out.println(sep);
        return sortedAreas;
    }


    //*********************************************************************//
    //***************************Question 9 *******************************//
    //*********************************************************************//

    @Override
    public  void drawBarChartArea() {
        LinkedHashMap<String, Long> sortedAreas = GetMostPopularArea();

        List<Long> Area_counter_values = new ArrayList<>(sortedAreas.values());
        List<Double> Area_double_values = Area_counter_values.stream()
                .map(Double::valueOf)
                .collect(Collectors.toList());

        List<Double> Area_double_values_arr = new ArrayList<>(Area_double_values);

        List<String> Area_counter_keys = new ArrayList<>(sortedAreas.keySet());

        List<String> Area_keys_chart = new ArrayList<>();
        List<Double> Area_vals_chart = new ArrayList<>();

        Area_keys_chart = Area_counter_keys.subList(0, 5);
        Area_vals_chart = Area_double_values_arr.subList(0, 5);

        CategoryChart Area_chart = new CategoryChartBuilder().width(800).height(600).title("Bar Chart").xAxisTitle("Locations").yAxisTitle("Demands").build();
        Area_chart.getStyler().setHasAnnotations(true);
        Area_chart.addSeries("most popular areas .", Area_keys_chart, Area_vals_chart);
//        new SwingWrapper(Area_chart).displayChart();
        try {
            BitmapEncoder.saveBitmapWithDPI(Area_chart, "src/main/resources/Step9_BarChartArea", BitmapEncoder.BitmapFormat.PNG, 300);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }



    //*********************************************************************//
    //***************************Question 10 ******************************//
    //*********************************************************************//
    @Override
    public List<Map.Entry> Most_pop_Skills(Dataset<Row> Data) {


// Clean Data First
        DataProvider p = new DataProvider();
        Dataset<Row> CleanedData = p.CleanData(Data);

        JavaRDD<String> AllSkills = CleanedData.toJavaRDD().flatMap((Row skill) -> Arrays.asList(skill.get(7).toString().toLowerCase()
                .trim()
                .replaceAll("\"", "").trim()
                .split(", ")).iterator());

        //Print All Skills
        System.out.println(AllSkills.toString());
//        AllSkills.foreach(System.out::println); false Statement

        // COUNTING and Sort In descending order
        Map<String, Long> SkillsCounts = AllSkills.countByValue();
        List<Map.Entry> sorted = SkillsCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).collect(Collectors.toList());


//         // DISPLAY
//         for (Map.Entry<String, Long> entry : sorted) {
//             System.out.println(entry.getKey() + " : " + entry.getValue());
//         }

        return sorted.stream().limit(30).collect(Collectors.toList());


  }



    //*********************************************************************//
    //***************************Question 11 *******************************//
    //*********************************************************************//
    @Override
    public  Dataset<Row> factorizeYearsExp(Dataset<Row> Data) {

        List<Row> yearsExp = new LinkedList<>();
        List<String> years = new LinkedList<>();
//        Dataset<Row> data = readData_Spark();
        Dataset<Row> data = Data;
        List<Row> rows = data.collectAsList();
        for(Row r : rows){
            String[] s = r.toString().replace("[","").replace("]","")
                    .split(",");
            years.add(s[5]);
        }

        for (String x : years)
        {
            if(x.contains("-")){
                String [] s = x.split("-");
                int n1 = Integer.parseInt(String.valueOf(s[0]));
                int n2 = Integer.parseInt(String.valueOf(s[1].split(" ")[0]));
                int avg = (n1+n2)/2;

                yearsExp.add(RowFactory.create(avg));
            }else if(x.contains("+")){

                int i = x.indexOf('+');
                int n1 = Integer.parseInt(String.valueOf(x.substring(0,i)));
                yearsExp.add(RowFactory.create(n1));

            }else{
                yearsExp.add(RowFactory.create(0));
            }
        };

        System.out.println(" //**********************Factorization***********************//");

        List<org.apache.spark.sql.types.StructField> listOfStructField=
                new ArrayList<org.apache.spark.sql.types.StructField>();
        listOfStructField.add
                (DataTypes.createStructField("FYearsExp", DataTypes.IntegerType, true));

        StructType structType=DataTypes.createStructType(listOfStructField);
        Dataset<Row> Df =sparkSession.createDataFrame(yearsExp,structType);

        data = data.join(Df);
        data = data.drop("YearsExp");
        System.out.println(data.tail(30));

        return  data;
    }


    public  void kmeanAlgorithm() {

//        final SparkSession sparkSession = SparkSession.builder().appName("Spark WuZZuf Jobs Analysis ").master("local[3]")
//                .getOrCreate();
//        // Spark Session
//        SparkConf conf = new SparkConf().setAppName("JavaKMeansExample")
//                .setMaster("local[2]")
//                .set("spark.executor.memory","3g")
//                .set("spark.driver.memory", "3g");

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        // Load and parse data

        JavaRDD data = jsc.textFile(path);

        System.out.println("========================================================================================");
        System.out.println("========================================================================================");
        System.out.println(data);
//        JavaRDD parsedData = data.map(s -> {
//            String[] sarray = s.toString().replace("[","").replace("]","")
//                    .split(",");
//
//
//            return Vectors.;
//        });
//        parsedData.cache();
//
//
//        // Cluster the data into three classes using KMeans
//        int numClusters = 3;
//        int numIterations = 20;
//
//        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);
//
//        System.out.println("\n*****Training*****");
//        int clusterNumber = 0;
//        for (Vector center: clusters.clusterCenters()) {
//            System.out.println("Cluster center for Clsuter "+ (clusterNumber++) + " : " + center);
//        }
//        double cost = clusters.computeCost(parsedData.rdd());
//        System.out.println("\nCost: " + cost);
//
//        // Evaluate clustering by computing Within Set Sum of Squared Errors
//        double WSSSE = clusters.computeCost(parsedData.rdd());
//        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

    }
}
