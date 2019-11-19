package model;

public class ResortsLiftRides {

    private final String id;

    private final Integer resortId;

    private final String seasonId;

    private final String dayId;

    private final Integer skierId;

    private final Integer liftId;

    private final Integer ride_time;

    private final Integer vertical;

    public ResortsLiftRides(String id, Integer resortId, String seasonId, String dayId, Integer skierId,
                            Integer liftId, Integer ride_time, Integer vertical) {
        this.id = id;
        this.resortId = resortId;
        this.seasonId = seasonId;
        this.dayId = dayId;
        this.skierId = skierId;
        this.liftId = liftId;
        this.ride_time = ride_time;
        this.vertical = vertical;
    }


    public String getId() {
        return id;
    }

    public Integer getResortId() {
        return resortId;
    }

    public String getSeasonId() {
        return seasonId;
    }

    public String getDayId() {
        return dayId;
    }

    public Integer getSkierId() {
        return skierId;
    }

    public Integer getLiftId() {
        return liftId;
    }

    public Integer getRide_time() {
        return ride_time;
    }

    public Integer getVertical() {
        return vertical;
    }
}
