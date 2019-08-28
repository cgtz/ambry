package com.github.ambry.cloud.azure;

import com.microsoft.azure.cosmosdb.ChangeFeedOptions;
import com.microsoft.azure.cosmosdb.ConnectionMode;
import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.PartitionKey;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import java.util.List;
import rx.Observable;


public class CosmosTesterAsync {

  public static void main(String[] args) throws Exception {
    // Set up CosmosDB connection, including any proxy setting
    String key = "passwordpassword";
    String endpoint = "https://blah.documents.azure.com:443/";
    String collectionLink = "/dbs/blah/colls/blah";
    ConnectionPolicy policy = new ConnectionPolicy();
    policy.setConnectionMode(ConnectionMode.Direct);

    AsyncDocumentClient client = new AsyncDocumentClient.Builder()
        .withServiceEndpoint(endpoint)
        .withMasterKeyOrResourceToken(key)
        .withConnectionPolicy(policy)
        .withConsistencyLevel(ConsistencyLevel.Eventual)
        .build();
    try {
      String partitionKey = "118"; // ambry partition ID
      String checkpointContinuation = "241651";
      ChangeFeedOptions options = new ChangeFeedOptions();
      options.setPartitionKey(new PartitionKey(partitionKey));
      options.setRequestContinuation(checkpointContinuation);
      options.setStartFromBeginning(true);
      Observable<FeedResponse<Document>> responses = client.queryDocumentChangeFeed(collectionLink, options);
      responses.limit(3).toBlocking().forEach(feedResponse -> {
        System.out.println();
        List<Document> docs = feedResponse.getResults();
        System.out.println("# docs in block: " + docs.size());
        if (docs.size() > 1) {
          System.out.println("First doc in block: " + docs.get(0));
        }
        System.out.println("Continuation token: " + feedResponse.getResponseContinuation());
      });
    } finally {
      client.close();
    }
  }
}
