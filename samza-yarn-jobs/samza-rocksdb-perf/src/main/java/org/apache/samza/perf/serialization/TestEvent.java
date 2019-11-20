package org.apache.samza.perf.serialization;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * A test Json event
 */
public class TestEvent {
  @JsonProperty("testString")
  private String testString;

  @JsonCreator
  public TestEvent(@JsonProperty("testString") String testString) {
    this.testString = testString;
  }

  public String getTestString() {
    return testString;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TestEvent that = (TestEvent) o;

    return testString.equals(that.testString);
  }

  @Override
  public int hashCode() {
    int result = testString.hashCode();
    result = 31 * result + testString.hashCode();
    return result;
  }
}