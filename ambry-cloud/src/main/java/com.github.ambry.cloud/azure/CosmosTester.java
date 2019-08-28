package com.github.ambry.cloud.azure;

import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.PartitionKeyRange;
import java.util.List;


public class CosmosTester {

  public static void main(String[] args) throws Exception {
    // Set up CosmosDB connection, including any proxy setting
    String key = "passwordpassword";
    String endpoint = "https://blah.documents.azure.com:443/";
    String collectionLink = "/dbs/blah/colls/blah";
    ConnectionPolicy connectionPolicy = new ConnectionPolicy();
    connectionPolicy.setConnectionMode(ConnectionMode.DirectHttps);
    try (DocumentClient client = new DocumentClient(endpoint, key, connectionPolicy, ConsistencyLevel.Session)) {

      FeedResponse<PartitionKeyRange> partitionRangeQuery =
          client.readPartitionKeyRanges(collectionLink, new FeedOptions());
      System.out.println("Partition key ranges: " + partitionRangeQuery.getQueryIterable().toList());

      String partitionKeyRangeId = "0";   // Use client.readPartitionKeyRanges() to obtain the ranges.
      String checkpointContinuation = "36583";
      ChangeFeedOptions options = new ChangeFeedOptions();
      options.setPartitionKeyRangeId(partitionKeyRangeId);
      options.setRequestContinuation(checkpointContinuation);
      // options.setStartFromBeginning(true);
      FeedResponse<Document> query = client.queryDocumentChangeFeed(collectionLink, options);
      int i = 0;
      do {
        System.out.println();
        List<Document> docs = query.getQueryIterable().fetchNextBlock();
        System.out.println("# docs in block: " + docs.size());
        if (docs.size() > 1) {
          System.out.println("First doc in block: " + docs.get(0));
        }
        System.out.println("Continuation token: " + query.getResponseContinuation());
        // Process the documents
        // Checkpoint query.getResponseContinuation()
      } while (++i < 5 && query.getQueryIterator().hasNext());
    }
  }
}
