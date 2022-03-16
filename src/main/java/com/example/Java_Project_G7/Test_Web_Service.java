package com.example.Java_Project_G7;

import org.apache.commons.io.FileUtils;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;

@RestController
//@RequestMapping("/api/user")
public class Test_Web_Service {
//    WuzzufJobsSkillsCount ob = new WuzzufJobsSkillsCount();
//    List<Map.Entry> skill = ob.count_skills();
    DataProvider p = new DataProvider( );
    Dataset<Row> DataFrame= p.readData_Spark();

    @GetMapping("/main_page")
    public String tes_fun(){
        return new TableBuilder().mainPage();
    }

    @GetMapping("/ShowStructure")
    public String ShowStructure(){
        String structre = p.dfStructure(DataFrame);
        return  "<html>"+"<body>\n"+"<div >"+structre.replace("|--","<p>\t|--</p>")+"<div >"+"</body>\n"+"</html>";
    }

    @GetMapping("/ShowSummary")
    public String ShowSchema(){
        return  p.dataSummary(DataFrame);
    }



    @GetMapping("/ShowData")
    public String ShowData(){

        List<Row> first_20_records = DataFrame.limit(20).collectAsList();
        return Htmlshow.displayrows(DataFrame.columns(), first_20_records);
    }

    @GetMapping("/ShowCleanedData")
    public String ShowCleanedData(){

        List<Row> first_20_records = p.CleanData(DataFrame).limit(20).collectAsList();
        return Htmlshow.displayrows(DataFrame.columns(), first_20_records);
    }


    @GetMapping("/Mostpopulerskills")
    public String most_po_skills(){

//       return Htmlshow.Skills(new String[]{"Skill", "Count"},ob.count_skills());
        return Htmlshow.Skills(new String[]{"Skill", "Count"},p.Most_pop_Skills(DataFrame));
    }


    @GetMapping("/Most_demanding_companies_for_jobs")
    public String Most_demanding_companies() throws IOException {
        Visualize v = new Visualize ();
        LinkedHashMap<String, Long> sortedCompanies = v.SortedCompinesCount();
        return Htmlshow.Skills(new String[]{"Company", "Count_demanding"},sortedCompanies.entrySet().stream().limit(10).collect(Collectors.toList()));
    }

    @GetMapping("/Most_popular_job_titles")
    public String Most_popular_job() throws IOException {
        Visualize v = new Visualize ();
        LinkedHashMap<String, Long> sortedTitles = v.GetMostPopularJobsTitle();
        return Htmlshow.Skills(new String[]{"Job Tittle", "Count Job"},sortedTitles.entrySet().stream().limit(10).collect(Collectors.toList()));
    }


    @GetMapping("/Most_popular_locations")
    public String Most_popular_locations() throws IOException {
        Visualize v = new Visualize ();
        LinkedHashMap<String, Long> sortedAreas = v.GetMostPopularArea();
        return Htmlshow.Skills(new String[]{"Areas", "Count"},sortedAreas.entrySet().stream().limit(10).collect(Collectors.toList()));
    }



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



    @GetMapping("/DataAfterFactorize")
    public String Data_after_Factorize(){
        Dataset<Row> data = p.factorizeYearsExp();
        List<Row > f = data.limit(20).collectAsList();
        return Htmlshow.displayrows(data.columns(), f);

    }


}
