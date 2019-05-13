import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;

public class QueryDataServletRun {

    public static void main(String[] args) {
        try {
            String query = "select * where {\n" +
                    "<http://www.knoldus.com/PO17116> ?p ?o.\n" +
                    "}";

            String queryenc = URLEncoder.encode(query, "UTF-8");

            URL url = new URL("http://localhost:8080/web.rya/queryrdf?query=" + queryenc);
            URLConnection urlConnection = url.openConnection();
            urlConnection.setDoOutput(true);

            BufferedReader rd = new BufferedReader(new InputStreamReader(
                    urlConnection.getInputStream()));
            String line;
            while ((line = rd.readLine()) != null) {
                System.out.println(line);
            }
            rd.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
