/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.named;

/**
 * Class to convey information about a successful put in {@link NamedBlobDb}.
 */
public class PutResult {
  NamedBlobRecord insertedRecord;

  /**
   * @param insertedRecord the new record stored in the DB.
   */
  public PutResult(NamedBlobRecord insertedRecord) {
    this.insertedRecord = insertedRecord;
  }

  /**
   * @return the new record stored in the DB.
   */
  public NamedBlobRecord getInsertedRecord() {
    return insertedRecord;
  }
}