package com.example.Java_Project_G7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface JobDAO {

    public  Dataset<Row> readData_Spark();

    public void readcsv();

    public void displayHeader();

    public String dfStructure(Dataset<Row> Data);

    public String dataSummary(Dataset<Row> Data);

    public void dataDiscribee(Dataset<Row> Data);

    public Dataset<Row> removeDuplicate(Dataset<Row> Data);

    public Dataset<Row> removeNull(Dataset<Row> Data);

    public void count_jobs_company();

    public void most_demanding_comp();





}
