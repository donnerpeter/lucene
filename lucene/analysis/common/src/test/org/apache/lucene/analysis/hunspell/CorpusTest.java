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

import static org.apache.lucene.analysis.hunspell.TestPerformance.stripPunctuation;
import static org.junit.Assert.assertEquals;

import dumonts.hunspell.bindings.HunspellLibrary;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.NamedThreadFactory;
import org.bridj.Pointer;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class CorpusTest {
  private static final String INCORRECT_BREAK_CASE = "[0-9\\p{Lu}]*[-.][0-9\\p{Lu}-.]*";
  private static final Map<String, Pattern> spellExcludes =
      Map.of(
          "fr", Pattern.compile(".*[a-z]+['’][A-Z][a-z]+|3d|" + INCORRECT_BREAK_CASE),
          "de", Pattern.compile(".*-\\p{Upper}.*|" + INCORRECT_BREAK_CASE),
          "pt", Pattern.compile("CM|KG|KM|" + INCORRECT_BREAK_CASE),
          "uk", Pattern.compile("[\\w-]+-Ñ|\\p{Lu}+-[\\w-]+|" + INCORRECT_BREAK_CASE), // see ICONV
          "nl", Pattern.compile("3d|3d-printen|3m|Powertraining|vrijetijd|.*-\\d\\d-.*"));
  private static final Map<String, Set<String>> suggestExcludes =
      Map.of(
          "en", Set.of("Reters"), // hash order
          "pt", Set.of("Balc", "Bjos"), // hash order
          "nl", Set.of("Oeko-Tex", "GS") // dubious dash strategy, smth with case
          );

  @Before
  public void setUp() throws Exception {
    ensureCorpusDir();
  }

  @Test
  public void en_US() throws Exception {
    assertEquals(
        0,
        compareImplementations(
            "/home/peter/work/dicts/wooorm/dictionaries/en/index", "en", 50_000, 1_000));
  }

  @Test
  public void es() throws Exception {
    assertEquals(
        0,
        compareImplementations(
            "/home/peter/work/dicts/wooorm/dictionaries/es/index", "es", 50_000, 100));
  }

  @Test
  public void fr() throws Exception {
    assertEquals(
        0,
        compareImplementations(
            "/home/peter/work/dicts/wooorm/dictionaries/fr/index", "fr", 50_000, 100));
  }

  @Test
  public void de() throws Exception {
    assertEquals(
        0,
        compareImplementations(
            "/home/peter/work/dicts/wooorm/dictionaries/de/index", "de", 50_000, 100));
  }

  @Test
  public void ru() throws Exception {
    assertEquals(
        0,
        compareImplementations(
            "/home/peter/work/dicts/wooorm/dictionaries/ru/index", "ru", Integer.MAX_VALUE, 200));
  }

  @Test
  public void uk() throws Exception {
    assertEquals(
        0,
        compareImplementations(
            "/home/peter/work/dicts/wooorm/dictionaries/uk/index", "uk", Integer.MAX_VALUE, 50));
  }

  @Test
  public void pt() throws Exception {
    assertEquals(
        0, compareImplementations("/home/peter/work/dicts/libre/pt_BR/pt_BR", "pt", 150_000, 20));
  }

  @Test
  public void nl() throws Exception {
    assertEquals(
        0,
        compareImplementations(
            "/home/peter/work/dicts/wooorm/dictionaries/nl/index", "nl", 130_000, 100));
    assertEquals(
        0, compareImplementations("/home/peter/work/dicts/libre/nl_NL/nl_NL", "nl", 500_000));
  }

  private static String ensureCorpusDir() {
    String corpusDir = System.getProperty("hunspell.corpora");
    Assume.assumeFalse("", corpusDir == null);
    return corpusDir;
  }

  private static List<String> loadWords(String code, int wordCount, Dictionary dictionary)
      throws IOException {
    String corpusDir = ensureCorpusDir();

    Path dataPath = Path.of(corpusDir).resolve(code + ".txt");
    Collection<String> words = new LinkedHashSet<>();
    try (InputStream stream = Files.newInputStream(dataPath)) {
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
      outer:
      while (true) {
        String line = reader.readLine();
        if (line == null) break;

        for (String token :
            line.split("[^\\p{IsLetter}" + Pattern.quote(dictionary.wordChars) + "]+")) {
          String word = stripPunctuation(token);
          if (word != null) {
            words.add(word);
            if (words.size() == wordCount) {
              break outer;
            }
          }
        }
      }
    }
    return new ArrayList<>(words);
  }

  private static int compareImplementations(String basePath, String code, int wordCount)
      throws Exception {
    return compareImplementations(basePath, code, wordCount, 0);
  }

  private static int compareImplementations(
      String basePath, String code, int spellWords, int suggestWords) throws Exception {
    Path affPath = Path.of(basePath + ".aff");
    Path dicPath = Path.of(basePath + ".dic");
    Dictionary dictionary =
        new Dictionary(
            new ByteBuffersDirectory(),
            "",
            Files.newInputStream(affPath),
            Files.newInputStream(dicPath));

    Dictionary dicWithoutBadSugs =
        new Dictionary(
            new ByteBuffersDirectory(),
            "",
            Files.newInputStream(affPath),
            Files.newInputStream(dicPath));
    dicWithoutBadSugs.onlyMaxDiff = true;

    AtomicInteger ignored = new AtomicInteger();
    AtomicInteger failures = new AtomicInteger();
    Set<String> misspelled = Collections.newSetFromMap(new ConcurrentHashMap<>());

    List<String> words = loadWords(code, spellWords, dictionary);
    int cpus = Runtime.getRuntime().availableProcessors();
    ExecutorService executor = Executors.newFixedThreadPool(cpus, new NamedThreadFactory("corpus"));

    ReadWriteLock lock = new ReentrantReadWriteLock();

    //    new Hunspell(dictionary, TimeoutPolicy.NO_TIMEOUT, () -> {}).suggest("Reters");

    Pattern spellExclusion = spellExcludes.get(code);
    Set<String> suggestExcludes =
        CorpusTest.suggestExcludes.getOrDefault(code, Collections.emptySet());

    Map<String, Set<String>> suggestions = new ConcurrentHashMap<>();

    AtomicInteger wordIndex = new AtomicInteger();
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < cpus; i++) {
      Runnable task =
          new Runnable() {
            final NativeHunspell nativeSpell = nativeHunspell(affPath, dicPath);
            final Hunspell speller =
                new Hunspell(dictionary, TimeoutPolicy.THROW_EXCEPTION, () -> {});

            @Override
            public void run() {
              while (true) {
                int index = wordIndex.getAndIncrement();
                if (index >= words.size()) break;

                compareResults(words.get(index));
              }
            }

            private void compareResults(String word) {
              lock.readLock().lock();
              boolean luceneGood;
              boolean nativeGood;
              try {
                luceneGood = speller.spell(word);
                nativeGood = nativeSpell.spell(word);
              } finally {
                lock.readLock().unlock();
              }
              if (luceneGood != nativeGood) {
                lock.writeLock().lock();
                try {
                  if (nativeSpell.spell(word) != nativeGood) {
                    System.out.println("Inconsistent Hunspell for " + word);
                    return;
                  }
                } finally {
                  lock.writeLock().unlock();
                }
                if (spellExclusion != null && spellExclusion.matcher(word).matches()) {
                  ignored.incrementAndGet();
                } else {
                  failures.incrementAndGet();
                  System.out.println(
                      "   " + word + " should be " + (nativeGood ? "OK" : "misspelled"));
                }
              } else if (!luceneGood) {
                misspelled.add(word);
                if (misspelled.size() <= suggestWords && !suggestExcludes.contains(word)) {
                  try {
                    suggestions.put(word, new LinkedHashSet<>(speller.suggest(word)));
                  } catch (SuggestionTimeoutException ignore) {
                  }
                }
              }
            }
          };
      futures.add(executor.submit(task));
    }
    for (Future<?> future : futures) {
      future.get();
    }
    executor.shutdown();

    if (!suggestions.isEmpty()) {
      NativeHunspell nativeSpell = nativeHunspell(affPath, dicPath);
      for (Map.Entry<String, Set<String>> entry : suggestions.entrySet()) {
        compareSuggestions(
            entry.getKey(), entry.getValue(), nativeSpell, dicWithoutBadSugs, ignored, failures);
      }
    }

    int misspelledRatio = (int) ((double) misspelled.size() * 100 / words.size());
    String msg =
        "   "
            + (failures.get() > 0 ? failures + " failures in " : "Checked ")
            + (words.size() + " words, ")
            + (ignored.get() == 0 ? "" : ignored + " ignored, ")
            + (misspelled.size() + "(" + misspelledRatio + "%) misspelled");
    System.out.println(msg);
    return failures.get();
  }

  private static void compareSuggestions(
      String word,
      Set<String> luceneSugs,
      NativeHunspell nativeSpell,
      Dictionary dicWithoutBadSugs,
      AtomicInteger ignored,
      AtomicInteger failures) {
    LinkedHashSet<String> nativeSugs = new LinkedHashSet<>(nativeSpell.suggest(word));

    if (!luceneSugs.containsAll(nativeSugs)) {
      long start = System.currentTimeMillis();
      LinkedHashSet<String> sug2 = new LinkedHashSet<>(nativeSpell.suggest(word));
      long elapsed = System.currentTimeMillis() - start;
      if (!nativeSugs.equals(sug2)) {
        System.out.println("Inconsistent suggestions for " + word + ", CPU time " + elapsed + "ms");
      }
      nativeSugs = sug2;
    }
    if (!luceneSugs.containsAll(nativeSugs)) {
      List<String> missing =
          nativeSugs.stream().filter(s -> !luceneSugs.contains(s)).collect(Collectors.toList());
      if (!ignoreSuggestionMismatch(word, luceneSugs, dicWithoutBadSugs, missing)) {
        System.out.println(
            ("   " + word + " should suggest " + nativeSugs)
                + (", found " + luceneSugs)
                + (", missing " + missing));
        failures.incrementAndGet();
      } else {
        ignored.incrementAndGet();
      }
    }
  }

  private static boolean ignoreSuggestionMismatch(
      String word, Set<String> luceneSugs, Dictionary nonBadDic, List<String> missing) {
    WordCase wc = WordCase.caseOf(word);
    if (wc == WordCase.UPPER && word.length() == 1) {
      return true; // Hunspell thinks it's INITCAP and converts suggestions to title case
    }

    if (wc == WordCase.UPPER
        && missing.stream()
            .allMatch(
                m -> luceneSugs.stream().anyMatch(s -> m.contains(s) && m.length() > s.length()))) {
      // ngram suggestion didn't find a substring in previous suggestions because it was in a
      // different case
      return true;
    }

    if (missing.size() == 1) {
      List<String> noBad =
          new Hunspell(nonBadDic, TimeoutPolicy.NO_TIMEOUT, () -> {}).suggest(word);
      // different same-rank single "bad" ngram suggestion
      //noinspection RedundantIfStatement
      if (noBad.stream().allMatch(s -> luceneSugs.contains(s) && !missing.contains(s))) {
        return true;
      }
    }
    return false;
  }

  interface NativeHunspell {
    boolean spell(String word);

    List<String> suggest(String word);
  }

  static synchronized NativeHunspell nativeHunspell(Path affPath, Path dicPath) {
    Pointer<Byte> affPointer = Pointer.pointerToCString(affPath.toString());
    Pointer<Byte> dicPointer = Pointer.pointerToCString(dicPath.toString());
    Pointer<HunspellLibrary.Hunhandle> handle =
        HunspellLibrary.Hunspell_create(affPointer, dicPointer);
    Charset charset =
        Charset.forName(HunspellLibrary.Hunspell_get_dic_encoding(handle).getCString());
    affPointer.release();
    dicPointer.release();
    return new NativeHunspell() {
      @Override
      public boolean spell(String word) {
        @SuppressWarnings("unchecked")
        Pointer<Byte> str =
            (Pointer<Byte>) Pointer.pointerToString(word, Pointer.StringType.C, charset);
        try {
          return HunspellLibrary.Hunspell_spell(handle, str) != 0;
        } finally {
          str.release();
        }
      }

      @Override
      public List<String> suggest(String word) {
        @SuppressWarnings("unchecked")
        Pointer<Byte> str =
            (Pointer<Byte>) Pointer.pointerToString(word, Pointer.StringType.C, charset);
        // Create pointer to native string array
        Pointer<Pointer<Pointer<Byte>>> nativeSuggestionArray =
            Pointer.allocatePointerPointer(Byte.class);
        // Hunspell will allocate the array and fill it with suggestions
        int suggestionCount = HunspellLibrary.Hunspell_suggest(handle, nativeSuggestionArray, str);
        if (suggestionCount == 0) {
          // Return early and don't try to free the array
          return new ArrayList<>();
        }
        // Ask bridj for a `java.util.List` that wraps `nativeSuggestionArray`
        List<Pointer<Byte>> nativeSuggestionList =
            nativeSuggestionArray.get().validElements(suggestionCount).asList();
        // Convert C Strings to java strings
        List<String> suggestions =
            nativeSuggestionList.stream()
                .map(p -> p.getStringAtOffset(0, Pointer.StringType.C, charset))
                .collect(Collectors.toList());

        // We can free the underlying buffer now because Java's `String` owns it's own memory
        HunspellLibrary.Hunspell_free_list(handle, nativeSuggestionArray, suggestionCount);
        return suggestions;
      }
    };
  }

  public static void main(String[] args) throws Exception {
    long totalStart = System.currentTimeMillis();
    int failures = 0;
    Set<String> supportedLangs = Set.of("en", "es", "fr", "ru", "de", "nl", "pt");
    for (Path aff : TestAllDictionaries.findAllAffixFiles().collect(Collectors.toList())) {
      Path parent = aff.getParent();
      String code = parent.getFileName().toString().substring(0, 2);
      if (supportedLangs.contains(code)) {
        if (code.equals("pt") && parent.toString().contains("wooorm")) continue; // wrong encoding

        System.out.println("Checking " + aff);
        long start = System.currentTimeMillis();
        String path = aff.toString();
        failures +=
            compareImplementations(path.substring(0, path.length() - 4), code, Integer.MAX_VALUE);
        System.out.println("Done in " + (System.currentTimeMillis() - start) + "ms");
      }
    }
    System.out.println("Total " + (System.currentTimeMillis() - totalStart) + "ms");
    assertEquals(0, failures);
  }
}
