/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.store;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;


/**
 * A mock journal that makes use of Journal in the background, and provides support for pausing and
 * resuming the addition of entries to the backing Journal. This can be used to simulate the race
 * condition of an entry getting added to the index but not yet to the journal.
 */
class MockJournal extends Journal {
  private final List<Long> savedCrcs = new ArrayList<>();
  private final List<JournalEntry> savedEntries = new ArrayList<>();
  boolean paused;

  public MockJournal(String dataDir, int maxEntriesToJournal, int maxEntriesToReturn) {
    super(dataDir, maxEntriesToJournal, maxEntriesToReturn);
    paused = false;
  }

  public void pause() {
    paused = true;
  }

  public void resume() {
    for (int i = 0; i < savedEntries.size(); i++) {
      JournalEntry entry = savedEntries.get(i);
      super.addEntry(entry.getOffset(), entry.getKey(), entry.getIndexEntryTypes(), savedCrcs.get(i));
    }
    paused = false;
  }

  @Override
  public void addEntry(Offset offset, StoreKey key, EnumSet<PersistentIndex.IndexEntryType> indexEntryTypes) {
    addEntry(offset, key, indexEntryTypes, null);
  }

  @Override
  public void addEntry(Offset offset, StoreKey key, EnumSet<PersistentIndex.IndexEntryType> indexEntryTypes, Long crc) {
    if (paused) {
      savedEntries.add(new JournalEntry(offset, key, indexEntryTypes));
      savedCrcs.add(crc);
    } else {
      super.addEntry(offset, key, indexEntryTypes, crc);
    }
  }
}
