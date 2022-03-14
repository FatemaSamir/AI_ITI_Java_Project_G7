package com.example.Java_Project_G7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

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

    @GetMapping("/DataAfterFactorize")
    public String Data_after_Factorize(){

        return "hi           ";
    }
}
