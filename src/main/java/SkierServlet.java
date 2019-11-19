import com.google.gson.Gson;
import dao.ResortsLiftRidesDao;
import model.*;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Random;

public class SkierServlet extends javax.servlet.http.HttpServlet {

    private static final Gson gson = new Gson();

    protected void doPost(javax.servlet.http.HttpServletRequest req, HttpServletResponse res) throws IOException {
//        long startTime = System.currentTimeMillis();

        int pathLength;

        String urlPath = req.getPathInfo();

        res.setContentType("application/json");

        // check we have a URL!
        if (urlPath == null || urlPath.isEmpty()) {
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            ResponseMsg output = new ResponseMsg().message("Invalid inputs supplied");
            res.getWriter().write(gson.toJson(output));

//            int latency = (int) (System.currentTimeMillis() - startTime);
//            MyServletContextListener.stats.add(new RequestsLatencies("/skiers", "POST", latency));
            return;
        }

        String[] urlParts = urlPath.split("/");

        pathLength = urlParts.length;

        if (!isUrlValid(urlParts, pathLength)) {
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            ResponseMsg output = new ResponseMsg().message("Invalid inputs supplied");
            res.getWriter().write(gson.toJson(output));

//            int latency = (int) (System.currentTimeMillis() - startTime);
//            MyServletContextListener.stats.add(new RequestsLatencies("/skiers", "POST", latency));
            return;
        }

        BufferedReader reqBody = req.getReader();

        try {
            LiftRide reqLifeRide = gson.fromJson(reqBody, LiftRide.class);
            ResortsLiftRidesDao resortsLiftRidesDao = new ResortsLiftRidesDao();
            int resortId = Integer.parseInt(urlParts[1]);
            String seasonId = urlParts[3];
            String dayId = urlParts[5];
            int skierId = Integer.parseInt(urlParts[7]);
            int liftId = reqLifeRide.getLiftID();
            int rideTime = reqLifeRide.getTime();

            StringBuilder sb = new StringBuilder();
            sb.append(resortId).append(seasonId).append(dayId).append(skierId).append(rideTime);

            resortsLiftRidesDao
                    .createLiftRide(req, new ResortsLiftRides(sb.toString(), resortId, seasonId, dayId, skierId, liftId, rideTime, (liftId * 10)));

            res.setStatus(HttpServletResponse.SC_CREATED);
        } catch (ClassNotFoundException cex) {
            res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            ResponseMsg output = new ResponseMsg().message(cex.getMessage());
            res.getWriter().write(gson.toJson(output));
        } catch (SQLException se) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            ResponseMsg output = new ResponseMsg().message("Data Not Found: " + se.getMessage());
            res.getWriter().write(gson.toJson(output));
        } catch (Exception ex) {
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            ResponseMsg output = new ResponseMsg().message("Invalid inputs supplied");
            res.getWriter().write(gson.toJson(output));
        }
//        finally {
////            int latency = (int) (System.currentTimeMillis() - startTime);
////            MyServletContextListener.stats.add(new RequestsLatencies("/skiers", "POST", latency));
//        }
    }

    protected void doGet(javax.servlet.http.HttpServletRequest req, HttpServletResponse res) throws IOException {
//        long startTime = System.currentTimeMillis();
        int pathLength;
        int totalVertical = 0;
        String urlPath = req.getPathInfo();

        res.setContentType("application/json");

        // check we have a URL!
        if (urlPath == null || urlPath.isEmpty()) {
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            ResponseMsg output = new ResponseMsg().message("Invalid inputs supplied");
            res.getWriter().write(gson.toJson(output));

//            int latency = (int) (System.currentTimeMillis() - startTime);
//            MyServletContextListener.stats.add(new RequestsLatencies("/skiers", "GET", latency));
            return;
        }

        String[] urlParts = urlPath.split("/");

        pathLength = urlParts.length;

        if (!isUrlValid(urlParts, pathLength)) {
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            ResponseMsg output = new ResponseMsg().message("Invalid inputs supplied");
            res.getWriter().write(gson.toJson(output));

//            int latency = (int) (System.currentTimeMillis() - startTime);
//            MyServletContextListener.stats.add(new RequestsLatencies("/skiers", "GET", latency));
            return;
        } else {
            if (pathLength == 8) {
                try {
                    ResortsLiftRidesDao resortsLiftRidesDao = new ResortsLiftRidesDao();
                    totalVertical = resortsLiftRidesDao
                            .getTotalVertical(req, Integer.parseInt(urlParts[1]), urlParts[3], urlParts[5], Integer.parseInt(urlParts[7]));
                    res.setStatus(HttpServletResponse.SC_OK);
                    res.getWriter().write(gson.toJson(totalVertical));
                } catch (ClassNotFoundException cex) {
                    res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    ResponseMsg output = new ResponseMsg().message(cex.getMessage());
                    res.getWriter().write(gson.toJson(output));
                } catch (SQLException se) {
                    res.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    ResponseMsg output = new ResponseMsg().message("Data Not Found: " + se.getMessage());
                    res.getWriter().write(gson.toJson(output));
                }
//                finally {
//                    int latency = (int) (System.currentTimeMillis() - startTime);
//                    MyServletContextListener.stats.add(new RequestsLatencies("/skiers", "GET", latency));
//                }
                return;
            }

            String resort = req.getParameter("resort");
            String season = req.getParameter("season");

            if(resort == null) {
                res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                ResponseMsg output = new ResponseMsg().message("Invalid inputs supplied");
                res.getWriter().write(gson.toJson(output));
                return;
            }

            res.setStatus(HttpServletResponse.SC_OK);

            if(season == null) {
                SkierVertical output = new SkierVertical();
                SkierVerticalResorts skierVerticalResorts = new SkierVerticalResorts().seasonID("All");
                skierVerticalResorts.setTotalVert(totalVertical);
                output.addResortsItem(skierVerticalResorts);
                res.getWriter().write(gson.toJson(output));
            } else {
                SkierVertical output = new SkierVertical();
                SkierVerticalResorts skierVerticalResorts = new SkierVerticalResorts().seasonID(season);
                skierVerticalResorts.setTotalVert(totalVertical);
                output.addResortsItem(skierVerticalResorts);
                res.getWriter().write(gson.toJson(output));
            }
        }
    }

    private boolean isUrlValid(String[] urlPath, int pathLength) {
        // urlPath  = "/1/seasons/2019/day/1/skier/123"
        // urlParts = [, 1, seasons, 2019, day, 1, skier, 123]
        if (pathLength == 8) {
            return ("".equals(urlPath[0]) && isInteger(urlPath[1]) && isInteger(urlPath[7])
                    && "seasons".equals(urlPath[2]) && "days".equals(urlPath[4]) && "skiers".equals(urlPath[6])
                    && isValidId(urlPath[3]) && isValidId(urlPath[5]));
        }
        return ("".equals(urlPath[0]) && isInteger(urlPath[1]) && "vertical".equals(urlPath[2]));
    }

    private boolean isInteger(String s) {
        if(s.isEmpty()) return false;
        for(int i = 0; i < s.length(); i++) {
            if(i == 0 && s.charAt(i) == '-') {
                if(s.length() == 1) return false;
                else continue;
            }
            if(Character.digit(s.charAt(i), 10) < 0) return false;
        }
        return true;
    }

    private boolean isValidId(String s) {
        int inputLength = s.length();
        return (inputLength > 0 && inputLength <= 366);
    }

    private int getRandomNumber() {
        // create random object
        Random ran = new Random();

        // Print next int value
        // Returns number between 0-40
        return 1 + ran.nextInt(39);
    }
}
