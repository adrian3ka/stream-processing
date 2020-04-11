package beam.example.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ExampleUtils {
  /**
   * \p{L} denotes the category of Unicode letters, so this pattern will match on everything that is
   * not a letter.
   *
   * <p>It is used for tokenizing strings in the wordcount examples.
   */
  public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

  public static final String PROJECT_ID = "beam-tutorial-272917";

  // Create your own storage first on google storage
  public static final String GCS_TMP_LOCATION = "gs://beam-tutorial-272917/tmp";


  public static String[] appendArgs(String[] args) {
    List<String> listArgs = new ArrayList<>(Arrays.asList(args));

    listArgs.add("--project=" + PROJECT_ID);

    String[] itemsArray = new String[listArgs.size()];
    itemsArray = listArgs.toArray(itemsArray);

    return itemsArray;
  }
}
