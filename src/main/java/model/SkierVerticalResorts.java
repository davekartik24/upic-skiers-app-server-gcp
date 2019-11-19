package model;

import java.util.Objects;

public class SkierVerticalResorts {

  private String seasonID = null;

  private Integer totalVert = null;

  public SkierVerticalResorts seasonID(String seasonID) {
    this.seasonID = seasonID;
    return this;
  }

   /**
   * Get seasonID
   * @return seasonID
  **/
  public String getSeasonID() {
    return seasonID;
  }

  public void setSeasonID(String seasonID) {
    this.seasonID = seasonID;
  }

  public SkierVerticalResorts totalVert(Integer totalVert) {
    this.totalVert = totalVert;
    return this;
  }

   /**
   * Get totalVert
   * @return totalVert
  **/
  public Integer getTotalVert() {
    return totalVert;
  }

  public void setTotalVert(Integer totalVert) {
    this.totalVert = totalVert;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SkierVerticalResorts skierVerticalResorts = (SkierVerticalResorts) o;
    return Objects.equals(this.seasonID, skierVerticalResorts.seasonID) &&
        Objects.equals(this.totalVert, skierVerticalResorts.totalVert);
  }

  @Override
  public int hashCode() {
    return Objects.hash(seasonID, totalVert);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class SkierVerticalResorts {\n");

    sb.append("    seasonID: ").append(toIndentedString(seasonID)).append("\n");
    sb.append("    totalVert: ").append(toIndentedString(totalVert)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

}