package model;

public class RequestsLatencies {

    private final String url;

    private final String operation;

    private final Integer latency;

    public RequestsLatencies(String url, String operation, Integer latency) {
        this.url = url;
        this.operation = operation;
        this.latency = latency;
    }

    public String getUrl() {
        return url;
    }

    public String getOperation() {
        return operation;
    }

    public Integer getLatency() {
        return latency;
    }
}
