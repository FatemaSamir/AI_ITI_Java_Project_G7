package com.example.Java_Project_G7;

import org.apache.commons.codec.binary.Base64;
import org.apache.spark.sql.Row;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
/*
implemented by
Author : --- Fatema Samir ---
        */
public class Htmlshow {
    private static TableBuilder builder ;

    public static String displayrows(String []head, List<Row> ls){

        builder=new TableBuilder(null,true,3,head.length);
        builder.addTableHeader(head);
        for (Row r : ls) {

            String[] s = r.toString().replace("[","").replace("]","")
                    .split(",", head.length);
            builder.addRowValues(s);

        }
        return builder.build();


    }

    public static String displayrows_fact(String []head, List<Row> ls){

        builder=new TableBuilder(null,true,3,head.length);
        builder.addTableHeader(head);
        for (Row r : ls) {
            String[] s  = new String[r.size()];
            for (int i =0 ;i< r.size();i++){
                s[i] = r.get(i).toString();}
                builder.addRowValues(s);
        }
        return builder.build();


    }

    public static String Skills(String []head, List<Map.Entry> ls){

        builder=new TableBuilder(null,true,3,head.length);
        builder.addTableHeader(head);
        for (Map.Entry<String, Long> entry : ls) {
            String[] s  = {entry.getKey () , entry.getValue ().toString()};
            builder.addRowValues(s);

        }
        return builder.build();


    }


}
