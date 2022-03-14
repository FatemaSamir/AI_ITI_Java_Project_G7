package com.example.Java_Project_G7;

public class TableBuilder {


        private int columns;
        private final StringBuilder table = new StringBuilder();
        public static String HTML_START = "<html>";
        public static String HTML_END = "</html>";
        public static String TABLE_START_BORDER = "<table border=\"2\">";
        public static String TABLE_START = "<table>";
        public static String TABLE_END = "</table>";
        public static String HEADER_START = "<th style=\"background-color:DodgerBlue;\">";
        public static String HEADER_END = "</th>";
        public static String ROW_START = "<tr>";
        public static String ROW_END = "</tr>";
        public static String COLUMN_START = "<td>";
        public static String COLUMN_END = "</td>";


        /**
         * @param header
         * @param border
         * @param rows
         * @param columns
         */


        public TableBuilder(String header, boolean border, int rows, int columns) {
            this.columns = columns;
            if (header != null) {
                table.append("<b style=\"background-color:DodgerBlue;\">");
                table.append(header);
                table.append("</b>");
            }
            table.append(HTML_START);
            table.append(border ? TABLE_START_BORDER : TABLE_START);
            table.append(TABLE_END);
            table.append(HTML_END);
        }
        public TableBuilder(){

        }

    public String mainPage(){
return
        "<html>\n" +
        "<head>\n" +
        "<style>\n" +
        ".button {\n" +
        "  border: none;\n" +
        "  color: white;\n" +
        "  padding: 16px 32px;\n" +
        "  text-align: center;\n" +
        "  text-decoration: none;\n" +
        "  display: inline-block;\n" +
        "  font-size: 20px;\n" +
        "  margin: 12px 10px;\n" +
        "  transition-duration: 0.4s;\n" +
        "  cursor: pointer;\n" +
        "}\n" +
        "\n" +".wrapper {\n" +
                "    text-align: center;\n" +
                "top: 50%;"+
                "}"+
        ".button1 {\n" +
        "  background-color: white; \n" +
        "  color: black; \n" +
        "  border: 2px solid #4CAF50;\n" +
        "}\n" +
        "\n" +
        ".button1:hover {\n" +
        "  background-color: #4CAF50;\n" +
        "  color: white;\n" +
        "}\n" +
        "\n" +
        ".button2 {\n" +
        "  background-color: white; \n" +
        "  color: black; \n" +
        "  border: 2px solid #008CBA;\n" +
        "}\n" +
        "\n" +
        ".button2:hover {\n" +
        "  background-color: #008CBA;\n" +
        "  color: white;\n" +
        "}\n" +
        "\n" +
        "</style>\n" +
        "</head>\n" +
        "<body>\n" +
        "\n" +
        "<h1 style=\"text-align:center\" >Welcome To Our Project Wuzzuf Jobs Analysis </h1>\n" +
        "\n" +
        "\n" +
                "<div class=\"wrapper\">"+
        "<button onclick=\"location.href = '/ShowStructure'\" class=\"button button2\">Display structure</button>\n" +
        "<button onclick=\"location.href = '/ShowSummary'\" class=\"button button1\">Display Summary</button>\n" +
        "\n" +
        "<button  onclick=\"location.href = '/ShowCleanedData'\" class=\"button button1\">Display Dataset</button>\n" +
        "<button onclick=\"location.href = '/ShowData'\" class=\"button button2\">Clean the Data</button>\n" +
        "<button onclick=\"location.href = '/ShowData'\" class=\"button button1\">Most Demanding Companies</button>\n" +
        "<button onclick=\"location.href = '/ShowData'\" class=\"button button2\">pie chart Most Companies</button>\n" +
        "<button onclick=\"location.href = '/ShowData'\" class=\"button button1\">popular job titles</button>\n" +
        "<button onclick=\"location.href = '/ShowData'\" class=\"button button2\">Bar chart popular job titles </button>\n" +
        "<button onclick=\"location.href = '/ShowData'\" class=\"button button1\">popular Areas</button>\n" +
        "<button onclick=\"location.href = '/ShowData'\" class=\"button button2\">Bar chart popular Areas</button>\n" +
        "<button onclick=\"location.href = '/Mostpopulerskills'\" class=\"button button1\">Most Important Skills</button>\n" +
        "<button onclick=\"location.href = '/DataAfterFactorize'\" class=\"button button2\">Factorize the YearsExp</button>\n" +
        " \n" +
                "</div>"+
        "\n" +
        "\n" +
        "\n" +
        "</body>\n" +
        "</html>\n";
    }

        /**
         * @param values
         */
        public void addTableHeader(String... values) {
            if (values.length != columns) {
                System.out.println("Error column lenth");
            } else {
                int lastIndex = table.lastIndexOf(TABLE_END);
                if (lastIndex > 0) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(ROW_START);
                    for (String value : values) {
                        sb.append(HEADER_START);
                        sb.append(value);
                        sb.append(HEADER_END);
                    }
                    sb.append(ROW_END);
                    table.insert(lastIndex, sb.toString());
                }
            }
        }


        /**
         * @param values
         */
        public void addRowValues(String... values) {
            if (values.length != columns) {
                System.out.println("Error column lenth");
            } else {
                int lastIndex = table.lastIndexOf(ROW_END);
                if (lastIndex > 0) {
                    int index = lastIndex + ROW_END.length();
                    StringBuilder sb = new StringBuilder();
                    sb.append(ROW_START);
                    for (String value : values) {
                        sb.append(COLUMN_START);
                        sb.append(value);
                        sb.append(COLUMN_END);
                    }
                    sb.append(ROW_END);
                    table.insert(index, sb.toString());
                }
            }
        }


        /**
         * @return
         */
        public String build() {
            return table.toString();
        }


        /**
         * @param args
         */
    }

