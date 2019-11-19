import com.google.gson.Gson;
import dao.StatisticsDao;
import model.APIStats;
import model.ResponseMsg;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.SQLException;

public class StatisticsServlet extends javax.servlet.http.HttpServlet {

    private static final Gson gson = new Gson();

    protected void doGet(javax.servlet.http.HttpServletRequest req, HttpServletResponse res) throws IOException {
        res.setContentType("application/json");

        try {
            StatisticsDao statisticsDao = new StatisticsDao();
            APIStats resultStats = statisticsDao.getStats(req);
            res.setStatus(HttpServletResponse.SC_OK);
            res.getWriter().write(gson.toJson(resultStats));
        } catch (ClassNotFoundException cex) {
            res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            ResponseMsg output = new ResponseMsg().message(cex.getMessage());
            res.getWriter().write(gson.toJson(output));
        } catch (SQLException se) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            ResponseMsg output = new ResponseMsg().message("Data Not Found: " + se.getMessage());
            res.getWriter().write(gson.toJson(output));
        }
    }
}
