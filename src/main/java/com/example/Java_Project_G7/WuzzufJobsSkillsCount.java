package com.example.Java_Project_G7;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.*;
import java.util.stream.Collectors;

/*
 This file implemented by
 Author : --- Fatema Samir ---
 Date   : --- 7/3/2022 ---
 version : --- 1 ---
 */

public class WuzzufJobsSkillsCount {

    private static final String COMMA_DELIMITER = ",";
    public static void main (String [] arges){
        WuzzufJobsSkillsCount ob = new WuzzufJobsSkillsCount();
        ob.count_skills();

    }


public static  List<Map.Entry> count_skills(){
    Logger.getLogger ("org").setLevel (Level.ERROR);
    // CREATE SPARK CONTEXT
    SparkConf conf = new SparkConf ().setAppName ("SkillsCounts").setMaster ("local[3]");
    JavaSparkContext sparkContext = new JavaSparkContext (conf);

// LOAD DATASETS
    JavaRDD<String> WuzzufJobs = sparkContext.textFile ("src/main/resources/Wuzzuf_Jobs.csv");

// TRANSFORMATIONS

    JavaRDD<String> Skills_col = WuzzufJobs
            .map (WuzzufJobsSkillsCount::extractSKills)
            .filter (StringUtils::isNotBlank);

    JavaRDD<String> AllSkills = Skills_col.flatMap (skill -> Arrays.asList (skill
            .toLowerCase ()
            .trim ()
            .replaceAll ("\"", "").trim()
            .split (", ")).iterator ());


    //Print All Skills
//        System.out.println(AllSkills.toString ());
//        AllSkills.foreach(System.out::println); false Statement

    // COUNTING and Sort In descending order
    Map<String, Long> SkillsCounts = AllSkills.countByValue ();
    List<Map.Entry> sorted = SkillsCounts.entrySet ().stream ()
            .sorted (Map.Entry.comparingByValue (Comparator.reverseOrder())).collect (Collectors.toList ());


    // DISPLAY
    for (Map.Entry<String, Long> entry : sorted) {
        System.out.println (entry.getKey () + " : " + entry.getValue ());
    }

    return sorted;
}


// Extract Skills from DataFrame RDD

    public static String extractSKills(String WuzzufJobsRead) {
        try {
            return WuzzufJobsRead.split (COMMA_DELIMITER)[7];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }

}
