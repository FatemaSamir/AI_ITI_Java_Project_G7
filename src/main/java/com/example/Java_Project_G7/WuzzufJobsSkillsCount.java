package com.example.Java_Project_G7;

import org.apache.commons.lang3.StringUtils;
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.ml.feature.StringIndexer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.spark.sql.*;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/*
 This file implemented by
 Author : --- Fatema Samir ---
 Date   : --- 7/3/2022 ---
 version : --- 1 ---
 */

public class WuzzufJobsSkillsCount {

    private static final String COMMA_DELIMITER = ",";
    public static void main (String [] arges) throws IOException {

//        Logger.getLogger ("org").setLevel (Level.ERROR);
        WuzzufJobsSkillsCount ob = new WuzzufJobsSkillsCount();
        ob.count_skills();

    }


public static  List<Map.Entry> count_skills(){
//    Logger.getLogger ("org").setLevel (Level.ERROR);
    // CREATE SPARK CONTEXT
    SparkConf conf = new SparkConf ().setAppName ("SkillsCounts").setMaster ("local[3]");
    JavaSparkContext sparkContext = new JavaSparkContext (conf);

// LOAD DATASETS
    JavaRDD<String> WuzzufJobs_cleand = sparkContext.textFile ("src/main/resources/Wuzzuf_Jobs_Cleand.csv");

//    // try to remove dublicate
//    JavaRDD<String> WuzzufJobs_dublicate = WuzzufJobs.distinct();
//
//    // try to remove null data
//    JavaRDD<String> WuzzufJobs_null = WuzzufJobs_dublicate.filter(l -> !l.contains("null Yrs of Exp"));
////    for (String string : WuzzufJobs_null.collect()) {
////        System.out.println(string);
////    }

// TRANSFORMATIONS

    JavaRDD<String> Skills_col = WuzzufJobs_cleand
            .map (WuzzufJobsSkillsCount::extractSKills)
            .filter (StringUtils::isNotBlank);

    JavaRDD<String> AllSkills = Skills_col.flatMap (skill -> Arrays.asList (skill
            .toLowerCase ()
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


// Extract Skills from DataFrame RDD

    public static String extractSKills(String WuzzufJobsRead) {
        try {
            String s = WuzzufJobsRead.split (COMMA_DELIMITER,8)[7];
//            System.out.println(s);
            return s.substring(1,s.length()-1);

        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }



}
