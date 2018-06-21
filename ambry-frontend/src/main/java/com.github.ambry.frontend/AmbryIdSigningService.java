/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.frontend;

import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.Pair;
import java.util.Map;


public class AmbryIdSigningService implements IdSigningService{
  private static final String SIGNED_ID_PREFIX = "signedId/";

  @Override
  public String getSignedId(String blobId, Map<String, String> metadata) throws RestServiceException {
    return SIGNED_ID_PREFIX + blobId + metadata;
  }

  @Override
  public boolean isIdSigned(String id) {
    return false;
  }

  @Override
  public Pair<String, Map<String, String>> parseSignedId(String signedId) throws RestServiceException {
    return null;
  }


}
