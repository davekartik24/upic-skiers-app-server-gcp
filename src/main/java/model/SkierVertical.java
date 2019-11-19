package model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SkierVertical {

  private List<SkierVerticalResorts> resorts = null;

  public SkierVertical resorts(List<SkierVerticalResorts> resorts) {
    this.resorts = resorts;
    return this;
  }

  public SkierVertical addResortsItem(SkierVerticalResorts resortsItem) {
    if (this.resorts == null) {
      this.resorts = new ArrayList<SkierVerticalResorts>();
    }
    this.resorts.add(resortsItem);
    return this;
  }

   /**
   * Get resorts
   * @return resorts
  **/
  public List<SkierVerticalResorts> getResorts() {
    return resorts;
  }

  public void setResorts(List<SkierVerticalResorts> resorts) {
    this.resorts = resorts;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SkierVertical skierVertical = (SkierVertical) o;
    return Objects.equals(this.resorts, skierVertical.resorts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resorts);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class SkierVertical {\n");

    sb.append("    resorts: ").append(toIndentedString(resorts)).append("\n");
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