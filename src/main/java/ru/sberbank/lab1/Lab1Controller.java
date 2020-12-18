package ru.sberbank.lab1;

import org.asynchttpclient.*;
import org.asynchttpclient.util.HttpConstants;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@RestController
@RequestMapping("/lab1")
public class Lab1Controller {

    private static final String URL = "http://export.rbc.ru/free/selt.0/free.fcgi?period=DAILY&tickers=USD000000TOD&separator=TAB&data_format=BROWSER";

    @GetMapping("/quotes")
    public List<Quote> quotes(@RequestParam("days") int days) throws ExecutionException, InterruptedException, ParseException {
        AsyncHttpClient client = AsyncHttpClientFactory.create(new AsyncHttpClientFactory.AsyncHttpClientConfig());
        Response response = client.prepareGet(URL + "&lastdays=" + days).execute().get();

        String body = response.getResponseBody();
        String[] lines = body.split("\n");

        List<Quote> quotes = new ArrayList<>();

        Map<String, Double> maxMap = new HashMap<>();

        for (int i = 0; i < lines.length; i++) {
            String[] line = lines[i].split("\t");
            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(line[1]);
            String year = line[1].split("-")[0];
            String month = line[1].split("-")[1];
            String monthYear = year + month;
            Double high = Double.parseDouble(line[3]);

            Double maxYear = maxMap.get(year);
            if (maxYear == null || maxYear < high) {
                maxMap.put(year, high);
                if (maxYear != null) {
                    List<Quote> newQuotes = new ArrayList<>();
                    for (Quote oldQuote : quotes) {
                        Calendar cal = Calendar.getInstance();
                        cal.setTime(oldQuote.getDate());
                        int oldYear = cal.get(Calendar.YEAR);
                        if (oldYear == Integer.parseInt(year)) {
                            if (oldQuote.getMaxInYear() < high) {
                                Quote newQuote = oldQuote.setMaxInYear(high);
                                newQuotes.add(newQuote);
                            } else {
                                newQuotes.add(oldQuote);
                            }
                        }
                    }
                    quotes.clear();
                    quotes.addAll(newQuotes);
                }
            }

            Double maxMonth = maxMap.get(monthYear);
            if (maxMonth == null || maxMonth < high) {
                maxMap.put(monthYear, high);
                if (maxMonth != null) {
                    List<Quote> newQuotes = new ArrayList<>();
                    for (Quote oldQuote : quotes) {
                        Calendar cal = Calendar.getInstance();
                        cal.setTime(oldQuote.getDate());
                        int oldYear = cal.get(Calendar.YEAR);
                        int oldMonth = cal.get(Calendar.MONTH);
                        if (oldYear == Integer.parseInt(year) && oldMonth == Integer.parseInt(month)) {
                            if (oldQuote.getMaxInMonth() < high) {
                                Quote newQuote = oldQuote.setMaxInMonth(high);
                                quotes.remove(oldQuote);
                                quotes.add(newQuote);
                            }
                        }
                    }
                }
            }

            Quote quote = new Quote(line[0],
                    new SimpleDateFormat("yyyy-MM-dd").parse(line[1]),
                    Double.parseDouble(line[2]),
                    Double.parseDouble(line[3]),
                    Double.parseDouble(line[4]),
                    Double.parseDouble(line[5]),
                    Long.parseLong(line[6]),
                    Double.parseDouble(line[7]));
            quote = quote.setMaxInMonth(maxMap.get(monthYear));
            quote = quote.setMaxInYear(maxMap.get(year));

            quotes.add(quote);
        }
        return quotes;
    }

    @GetMapping("/weather")
    public List<Double> getWeatherForPeriod(Integer days) {

        // asynchronously throw requests
        AsyncHttpClient client = Dsl.asyncHttpClient();
        List<Future<Response>> responseFutures = new ArrayList<>();
        for (int i = 0; i < days; i++) {
            Long currentDayInSec = Calendar.getInstance().getTimeInMillis() / 1000;
            Long oneDayInSec = 24 * 60 * 60L;
            Long curDateSec = currentDayInSec - i * oneDayInSec;

            Future<Response> responseFuture = requestAsyncForOneDay(client, curDateSec.toString());
            System.out.printf("Requested for day %s\n", curDateSec.toString());
            responseFutures.add(responseFuture);
        }

        // gather results for all
        List<Double> temperatures = new ArrayList<>();
        for (Future<Response> responseFuture : responseFutures) {
            try {
                // blocks here
                temperatures.add(extractTemperature(responseFuture.get()));
                System.out.printf("Completed future %s\n", responseFuture.toString());
            } catch (InterruptedException | ExecutionException | JSONException e) {
                throw new RuntimeException("Failed", e);
            }
        }

        return temperatures;
    }

    private Future<Response> requestAsyncForOneDay(AsyncHttpClient client, String dateInSeconds) {
        final String obligatoryForecastStart = "https://api.darksky.net/forecast/ac1830efeff59c748d212052f27d49aa/";
        final String LAcoordinates = "34.053044,-118.243750,";
        final String exclude = "exclude=daily";
        final String url = obligatoryForecastStart + LAcoordinates + dateInSeconds + "?" + exclude;

        Request request = new RequestBuilder(HttpConstants.Methods.GET)
                .setUrl(url)
                .build();

        return client.executeRequest(request);
    }

    private Double extractTemperature(Response response) throws JSONException {
        String responseJson = response.getResponseBody();

        return new JSONObject(responseJson)
                .getJSONObject("hourly")
                .getJSONArray("data")
                .getJSONObject(0)
                .getDouble("temperature");
    }
}

