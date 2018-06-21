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
package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;


/**
 * Factory that instantiates an {@link IdConverter} implementation for the frontend.
 */
public class AmbryIdConverterFactory implements IdConverterFactory {
  private final IdSigningService idSigningService;
  private final FrontendMetrics frontendMetrics;

  public AmbryIdConverterFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      IdSigningService idSigningService) {
    this.idSigningService = idSigningService;
    frontendMetrics = new FrontendMetrics(metricRegistry);
  }

  @Override
  public IdConverter getIdConverter() {
    return new AmbryIdConverter(idSigningService, frontendMetrics);
  }

  private static class AmbryIdConverter implements IdConverter {
    private boolean isOpen = true;
    private final IdSigningService idSigningService;
    private final FrontendMetrics frontendMetrics;

    AmbryIdConverter(IdSigningService idSigningService, FrontendMetrics frontendMetrics) {
      this.idSigningService = idSigningService;
      this.frontendMetrics = frontendMetrics;
    }

    @Override
    public void close() {
      isOpen = false;
    }

    /**
     * {@inheritDoc}
     * On {@link RestMethod#POST}, adds a leading slash to indicate that the ID represents the path of the resource
     * created.
     * On any other {@link RestMethod}, removes the leading slash in order to convert the path into an ID that the
     * {@link com.github.ambry.router.Router} will understand.
     * @param restRequest {@link RestRequest} representing the request.
     * @param input the ID that needs to be converted.
     * @param callback the {@link Callback} to invoke once the converted ID is available. Can be null.
     * @return a {@link Future} that will eventually contain the converted ID.
     */
    @Override
    public Future<String> convert(RestRequest restRequest, String input, Callback<String> callback) {
      FutureResult<String> futureResult = new FutureResult<String>();
      String convertedId = null;
      Exception exception = null;
      frontendMetrics.idConverterRequestRate.mark();
      long startTimeInMs = System.currentTimeMillis();
      if (!isOpen) {
        exception = new RestServiceException("IdConverter is closed", RestServiceErrorCode.ServiceUnavailable);
      } else {
        try {
          if (restRequest.getRestMethod().equals(RestMethod.POST)) {
            convertedId = "/" + (requiresSignedId(restRequest) ? getSignedId(restRequest, input) : input);
          } else {
            String id = input.startsWith("/") ? input.substring(1) : input;
            convertedId = idSigningService.isIdSigned(id) ? idSigningService.parseSignedId(id).getFirst() : id;
          }
        } catch (Exception e) {
          exception = e;
        }
      }
      futureResult.done(convertedId, exception);
      if (callback != null) {
        callback.onCompletion(convertedId, exception);
      }
      frontendMetrics.idConverterProcessingTimeInMs.update(System.currentTimeMillis() - startTimeInMs);
      return futureResult;
    }

    /**
     * @param restRequest the POST {@link RestRequest}
     * @return {@code true} if the POST requires a signed ID to be returned in the response.
     * @throws RestServiceException if header parsing fails.
     */
    private boolean requiresSignedId(RestRequest restRequest) throws RestServiceException {
      return RestUtils.getBooleanHeader(restRequest.getArgs(), RestUtils.Headers.STITCHED_CHUNK, false);
    }

    private String getSignedId(RestRequest restRequest, String blobId) throws RestServiceException {
      Map<String, String> metadata = new HashMap<>();
      metadata.put(RestUtils.Headers.BLOB_SIZE, Long.toString(restRequest.getBytesReceived()));
      return idSigningService.getSignedId(blobId, metadata);
    }
  }
}
