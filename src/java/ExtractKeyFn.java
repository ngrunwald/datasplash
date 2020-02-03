package datasplash.fns;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;

import clojure.lang.IFn;
import clojure.java.api.Clojure;

public class ExtractKeyFn implements ElasticsearchIO.Write.FieldValueExtractFn {

  private final IFn keyFn;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public ExtractKeyFn(IFn keyFunction) {
    super();
    keyFn = keyFunction;
  }

  @Override
  public String apply(JsonNode input) {
    String jsonString;
    try {
      if (input.isMissingNode()) {
	throw new RuntimeException("Unable to extract field: " + input.asText());
      }
      jsonString = MAPPER.writer().writeValueAsString(input);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    return String.valueOf(keyFn.invoke(jsonString));
  }
}
