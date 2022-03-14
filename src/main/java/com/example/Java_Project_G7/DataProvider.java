package com.example.Java_Project_G7;

import joinery.DataFrame;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;


import java.io.IOException;

public class DataProvider implements JobDAO{

    String path = "src/main/resources/Wuzzuf_Jobs.csv";
    DataFrame<Object> jobs = null;

//    Dataset<Row> csvDataFrame = null;
//    SparkSession sparkSession;


//    public DataProvider(String path)
//    {
//        this.path = path;
//    }
    public static void main(String []args) throws IOException {
        DataProvider p = new DataProvider( );
        Visualize v = new Visualize ();
        p.readcsv();
        p.displayHeader();
        p.dfStructure();
        p.count_jobs_company();
        v.drawPieChartjobs();
        v.drawBarChartArea();
        v.drawBarChartjobs();





    }


    @Override
    public  void readcsv() {

        System.out.println("==================Starting reading csv file==================");

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
    public void dfStructure() {

        System.out.println(
                "\n--------------------------------File Summary--------------------------------\n"
                        +"--This DataFrame has "+ jobs.size()+ " columns, and "+ jobs.length()+ " rows\n"
                        +"--Columns Names are: "+ jobs.columns()+ " \n"
                        +"--------------------------------***--------------------------------"
        );

    }

    @Override
    public void dataSummary() {
        System.out.println("All Data is text can't do summary statistic on it!");
    }

    @Override
    public void removeDuplicate() {

    }

    @Override
    public void removeNull() {

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
}
