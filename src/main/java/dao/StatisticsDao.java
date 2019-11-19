package dao;

import model.APIStats;
import model.APIStatsEndpointStats;
import model.RequestsLatencies;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class StatisticsDao {

    public APIStats getStats(javax.servlet.http.HttpServletRequest req) throws SQLException, ClassNotFoundException {
        String getStatsQueryStatement = "SELECT url, operation, AVG(latency) as mean, MAX(latency) as max FROM requests_latencies GROUP BY url, operation";

        DataSource pool = (DataSource) req.getServletContext().getAttribute("my-pool");

        Connection conn = pool.getConnection();
        PreparedStatement preparedStatement;
        preparedStatement = conn.prepareStatement(getStatsQueryStatement);

        ResultSet result = preparedStatement.executeQuery();

        List<APIStatsEndpointStats> apiStatsEndpointStats = new ArrayList<>();

        while (result.next()) {
            String url = result.getString("url");
            String operation = result.getString("operation");
            int mean = result.getInt("mean");
            int max = result.getInt("max");

            APIStatsEndpointStats input = new APIStatsEndpointStats();
            input.setURL(url);
            input.setOperation(operation);
            input.setMean(mean);
            input.setMax(max);

            apiStatsEndpointStats.add(input);
        }

        conn.close();

        APIStats output = new APIStats();
        output.setEndpointStats(apiStatsEndpointStats);
        return output;
    }

    public void dumpStats(javax.servlet.http.HttpServletRequest req, List<RequestsLatencies> input) throws SQLException, ClassNotFoundException {
        if(!input.isEmpty()) {

            DataSource pool = (DataSource) req.getServletContext().getAttribute("my-pool");

            try (Connection conn = pool.getConnection();
                 PreparedStatement preparedStatement =
                         conn.prepareStatement("INSERT INTO requests_latencies (url, operation, latency) VALUES(?,?,?)")) {

                conn.setAutoCommit(false);

                for (RequestsLatencies requestsLatencies : input) {
                    // Add each parameter to the row.
                    preparedStatement.setString(1, requestsLatencies.getUrl());
                    preparedStatement.setString(2, requestsLatencies.getOperation());
                    preparedStatement.setInt(3, requestsLatencies.getLatency());
                    // Add row to the batch.
                    preparedStatement.addBatch();
                }

                preparedStatement.executeBatch();
                conn.commit();
            } catch (SQLException sq) {
                throw new SQLException(sq);
            }
        }
    }
}
