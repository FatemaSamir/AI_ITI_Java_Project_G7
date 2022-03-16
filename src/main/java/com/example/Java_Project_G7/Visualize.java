package com.example.Java_Project_G7;

import com.aol.cyclops.control.LazyReact;
import joinery.DataFrame;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.*;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;


public class Visualize {



    String path = "src/main/resources/Wuzzuf_Jobs.csv";
    String sep = "----------------------------------------------------------------------";
    int count;
    DataFrame<Object> df;


    public Visualize() throws IOException {
        this.df = DataFrame.readCsv(path);


    }



//   4. function Count the jobs for each company and display that in order
    public LinkedHashMap<String, Long> SortedCompinesCount(){


        //Task 4: Code to get most demanding Companies.
        List<Object> Company = df.col("Company");
        List<String> Company_String = LazyReact.sequentialBuilder()
                .from(Company)
                .cast(String.class)
                .toList();

        Map<String, Long> Company_counter = Company_String.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        LinkedHashMap<String, Long> sortedCompanies = new LinkedHashMap<>();

        Company_counter.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> sortedCompanies.put(x.getKey(), x.getValue()));

        count = 1;
        System.out.println("\n\n\n" + sep);
        System.out.println("Top 5 Most Demanding Companies are:");
        for (Map.Entry entry : sortedCompanies.entrySet()) {
            System.out.println(count + " = " + entry.getKey() +
                    " with " + entry.getValue() + " demands.");
            count++;
            if (count > 5) {
                break;
            }
        }

        Map<String, Long> TopsortedCompanies = new HashMap<>();


//        System.out.println(sep);
        return sortedCompanies;

    }
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

    //End of Task 5.


//  6 6.	Find out what are the most popular job titles.
    
    public LinkedHashMap<String, Long> GetMostPopularJobsTitle(){
        //Task 6: Code to get most popular Title.
        List<Object> Title = df.col("Title");
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
    //End of task 6


    //Task 8: Code to get most popular Area

    // 8.	 function Get Find out the most popular areas

    public LinkedHashMap<String, Long> GetMostPopularArea(){

        List<Object> Area = this.df.col("Location");
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
    //End of task 9



}