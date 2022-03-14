package com.example.Java_Project_G7;

import org.apache.commons.io.FileUtils;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;

@RestController

public class Test_Web_Service {

    @GetMapping("/drawPieChartjobs")


    public ResponseEntity<byte[]> drawPieChartjobs() throws IOException {

        Visualize v = new Visualize ();

        v.drawPieChartjobs();
        byte[] image = new byte[0];
        try {
            image = FileUtils.readFileToByteArray(new File("src/main/resources/Step5_pieChart.png"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ResponseEntity.ok().contentType(MediaType.IMAGE_JPEG).body(image);}

//    Step5_BieChart

    @GetMapping("/DrawBarChartAreas")

    public ResponseEntity<byte[]> getBarChartArea() throws IOException {
        Visualize v = new Visualize ();

        v.drawBarChartArea();
        byte[] image = new byte[0];
        try {
            image = FileUtils.readFileToByteArray(new File("src/main/resources/Step9_BarChartArea.png"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ResponseEntity.ok().contentType(MediaType.IMAGE_JPEG).body(image);}

    @GetMapping("/DrawBarChartJobs")


    public ResponseEntity<byte[]> getBarChartJobs() throws IOException {

        Visualize v = new Visualize ();

        v.drawBarChartjobs();
        byte[] image = new byte[0];
        try {
            image = FileUtils.readFileToByteArray(new File("src/main/resources/Step7_BarChartjobs.png"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ResponseEntity.ok().contentType(MediaType.IMAGE_JPEG).body(image);}







}
