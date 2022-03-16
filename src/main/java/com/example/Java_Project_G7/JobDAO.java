package com.example.Java_Project_G7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public interface JobDAO {

    public Dataset<Row> readData_Spark();

    public void readcsv();

    public void displayHeader();

    public String dfStructure(Dataset<Row> Data);

    public String dataSummary(Dataset<Row> Data);

    public void dataDiscribee(Dataset<Row> Data);

    public Dataset<Row> removeDuplicate(Dataset<Row> Data);

    public Dataset<Row> removeNull(Dataset<Row> Data);

    public Map<String, Long> count_jobs_company();

    public LinkedHashMap<String, Long> SortedCompinesCount();

    public LinkedHashMap<String, Long> GetMostPopularJobsTitle();

    public LinkedHashMap<String, Long> GetMostPopularArea();

    public  void drawBarChartArea();

    public List<Map.Entry> Most_pop_Skills(Dataset<Row> Data);

    public Dataset<Row> factorizeYearsExp();





}
