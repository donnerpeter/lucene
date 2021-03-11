/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.analysis.hunspell;

import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Assume;
import org.junit.AssumptionViolatedException;
import org.junit.BeforeClass;
import org.junit.Test;

@TestCaseOrdering(TestCaseOrdering.AlphabeticOrder.class)
public class TestPerformanceComparative extends LuceneTestCase {
  private static Path corporaDir;

  @BeforeClass
  public static void resolveCorpora() {
    String dir = System.getProperty("hunspell.corpora");
    Assume.assumeFalse("Requires test word corpora at -Dhunspell.corpora=...", dir == null);
    corporaDir = Paths.get(dir);
  }

  @Test
  public void en() throws Exception {
    checkPerformance("en_US", 1_000_000, 60);
  }

  @Test
  public void de() throws Exception {
    checkPerformance("de", 200_000, 30);
  }

  @Test
  public void fr() throws Exception {
    checkPerformance("fr", 200_000, 20);
  }

  @Test
  public void ca() throws Exception {
    checkPerformance("ca", 400_000, 20);
  }

  @Test
  public void es() throws Exception {
    checkPerformance("es", 700_000, 100);
  }

  @Test
  public void nl() throws Exception {
    checkPerformance("nl", 400_000, 30);
  }

  @Test
  public void pt() throws Exception {
    checkPerformance("pt", 250_000, 20);
  }

  @Test
  public void ru() throws Exception {
    checkPerformance("ru", 400_000, 30);
  }

  @Test
  public void uk() throws Exception {
    checkPerformance("uk", 250_000, 40);
  }

  private void checkPerformance(String dicHint, int wordCount, int checkSuggestions)
      throws Exception {
    Path aff =
        TestAllDictionaries.findAllAffixFiles()
            .filter(path -> path.toString().contains("/" + dicHint))
            .findFirst()
            .orElseThrow(
                () -> new AssumptionViolatedException("Cannot find aff/dic for: " + dicHint));

    Dictionary dictionary = TestAllDictionaries.loadDictionary(aff);
    System.out.println("\nLoaded " + aff);

    List<String> words = loadWords(Dictionary.extractLanguageCode(dicHint), wordCount, dictionary);

    CorpusTest.NativeHunspell nativeCheck =
        CorpusTest.nativeHunspell(
            aff, Path.of(aff.toString().substring(0, aff.toString().length() - 4) + ".dic"));

    Hunspell speller = new Hunspell(dictionary, TimeoutPolicy.THROW_EXCEPTION, () -> {});

    int expensiveSuggestions = 0;
    Set<String> misspelled = new LinkedHashSet<>();
    for (String word : words) {
      long start = System.currentTimeMillis();
      boolean ok = nativeCheck.spell(word);
      long elapsed = System.currentTimeMillis() - start;
      if (elapsed > 100) {
        System.out.println("Native checking took " + elapsed + "ms for " + word);
      } else if (!ok && misspelled.size() < checkSuggestions) {
        start = System.currentTimeMillis();
        try {
          speller.suggest(word);
          elapsed = System.currentTimeMillis() - start;
        } catch (SuggestionTimeoutException e) {
          elapsed = Hunspell.SUGGEST_TIME_LIMIT;
        }
        if (elapsed < Hunspell.SUGGEST_TIME_LIMIT * 9 / 10) {
          misspelled.add(word);
        } else {
          expensiveSuggestions++;
        }
      }
    }

    String code = Dictionary.extractLanguageCode(dicHint);
    System.out.println("Spellchecking " + words.size() + " words");
    compareTimes(
        "spell " + code,
        blackHole -> {
          for (String word2 : words) {
            blackHole.accept(nativeCheck.spell(word2));
          }
        },
        blackHole -> {
          for (String word3 : words) {
            blackHole.accept(speller.spell(word3));
          }
        });

    if (misspelled.isEmpty()) return;

    System.out.print("Suggestions for " + misspelled.size() + " words");
    if (expensiveSuggestions > 0) {
      System.out.print(", " + expensiveSuggestions + " expensive skipped");
    }
    System.out.println();

    compareTimes(
        "suggest " + code,
        blackHole -> {
          for (String word : misspelled) {
            blackHole.accept(nativeCheck.suggest(word));
          }
        },
        blackHole -> {
          for (String word1 : misspelled) {
            blackHole.accept(speller.suggest(word1));
          }
        });
  }

  private void compareTimes(String what, Iteration aNative, Iteration lucene) {
    long nativeTime = measure("Native " + what, aNative);
    long luceneTime = measure("Lucene " + what, lucene);
    assertTrue("Native " + what + " is still faster!", nativeTime >= luceneTime);
  }

  private List<String> loadWords(String code, int wordCount, Dictionary dictionary)
      throws IOException {
    Path dataPath = corporaDir.resolve(code + ".txt");
    if (!Files.isReadable(dataPath)) {
      throw new AssumptionViolatedException("Missing text corpora at: " + dataPath);
    }

    List<String> words = new ArrayList<>();
    try (InputStream stream = Files.newInputStream(dataPath)) {
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
      while (true) {
        String line = reader.readLine();
        if (line == null) break;

        for (String token : line.split("[^\\p{IsLetter}" + Pattern.quote(dictionary.wordChars) + "]+")) {
          String word = stripPunctuation(token);
          if (word != null) {
            words.add(word);
            if (words.size() == wordCount) {
              return words;
            }
          }
        }
      }
    }
    return words;
  }

  private long measure(String what, Iteration iteration) {
    Consumer<Object> consumer =
        o -> {
          if (o == null) {
            throw new AssertionError();
          }
        };

    // warmup
    for (int i = 0; i < 2; i++) {
      iteration.run(consumer);
    }

    List<Long> times = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      long start = System.currentTimeMillis();
      iteration.run(consumer);
      times.add(System.currentTimeMillis() - start);
    }
    long median = times.stream().mapToLong(Long::longValue).sorted().toArray()[times.size() / 2];

    System.out.println(what + ": median " + median + ", all times = " + times);
    return median;
  }

  private interface Iteration {
    void run(Consumer<Object> blackHole);
  }

  static String stripPunctuation(String token) {
    int start = 0;
    int end = token.length();
    while (start < end && isPunctuation(token.charAt(start))) start++;
    while (start < end - 1 && isPunctuation(token.charAt(end - 1))) end--;
    return start < end ? token.substring(start, end) : null;
  }

  private static boolean isPunctuation(char c) {
    return ".!?,\"'’‘".indexOf(c) >= 0;
  }
}
