package beam.example.common;

public class ExampleUtils {
  /**
   * \p{L} denotes the category of Unicode letters, so this pattern will match on everything that is
   * not a letter.
   *
   * <p>It is used for tokenizing strings in the wordcount examples.
   */
  public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";
}
