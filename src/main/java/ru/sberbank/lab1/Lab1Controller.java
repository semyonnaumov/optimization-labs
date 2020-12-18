package ru.sberbank.lab1;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Response;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.emptyList;

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
        try {
            return getTemperatureForLastDays(days);
        } catch (JSONException e) {
        }

        return emptyList();
    }

    public List<Double> getTemperatureForLastDays(int days) throws JSONException {
        List<Double> temps = new ArrayList<>();

        for (int i = 0; i < days; i++) {
            Long currentDayInSec = Calendar.getInstance().getTimeInMillis() / 1000;
            Long oneDayInSec = 24 * 60 * 60L;
            Long curDateSec = currentDayInSec - i * oneDayInSec;
            Double curTemp = getTemperatureFromInfo(curDateSec.toString());
            temps.add(curTemp);
        }

        return temps;
    }

    public String getTodayWeather(String date) {
        String obligatoryForecastStart = "https://api.darksky.net/forecast/ac1830efeff59c748d212052f27d49aa/";
        String LAcoordinates = "34.053044,-118.243750,";
        String exclude = "exclude=daily";

        RestTemplate restTemplate = new RestTemplate();
        String fooResourceUrl = obligatoryForecastStart + LAcoordinates + date + "?" + exclude;
        System.out.println(fooResourceUrl);
        ResponseEntity<String> response = restTemplate.getForEntity(fooResourceUrl, String.class);
        String info = response.getBody();
        System.out.println(info);
        return info;
    }

    public Double getTemperatureFromInfo(String date) throws JSONException {
        String info = getTodayWeather(date);
        Double curTemp = getTemperature(info);
        return curTemp;
    }

    public Double getTemperature(String info) throws JSONException {
        // 1. Постоянная сериализация-десериализация JSON-а требует накладных расходов на парсинг,
        //    достаточно десериализовать один раз
        return new JSONObject(info)
                .getJSONObject("hourly")
                .getJSONArray("data")
                .getJSONObject(0)
                .getDouble("temperature");
    }
}

