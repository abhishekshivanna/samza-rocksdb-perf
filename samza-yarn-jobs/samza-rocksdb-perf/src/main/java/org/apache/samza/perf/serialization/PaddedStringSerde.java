package org.apache.samza.perf.serialization;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;


/**
 * Pads keys to the given length.
 */
public class PaddedStringSerde extends StringSerde implements Serde<String> {
  private final int keyLength;

  public PaddedStringSerde(int keyLength) {
    this.keyLength = keyLength;
  }

  @Override
  public String fromBytes(byte[] bytes) {
    String padded = super.fromBytes(bytes);
    return unpad(padded);
  }

  @Override
  public byte[] toBytes(String unpaddedKey) {
    String padded = pad(unpaddedKey);
    return super.toBytes(padded);
  }

  /**
   * Removes padding from the key.
   */
  private String unpad(String key) {
    int padding = keyLength - key.length();
    if (padding > 0) {
      return key.substring(padding);
    } else {
      return key;
    }
  }

  /**
   * Pads the given key with preceding 0s to the configured key size.
   */
  private String pad(String key) {
    int padding = keyLength - key.length();
    if (padding > 0) {
      String pad = IntStream.range(0, padding).boxed()
          .map(i -> "0")
          .collect(Collectors.joining());
      return pad + key;
    } else {
      return key;
    }
  }
}