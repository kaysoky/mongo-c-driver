#include <bson.h>
#include <mongoc.h>
#include <stdio.h>
#include <stdlib.h>

#define BATCH_SIZE 1000000

static void GenerateID(char generatedId[25]) {
 bson_context_t* context = bson_context_new(
 (bson_context_flags_t) (BSON_CONTEXT_THREAD_SAFE | BSON_CONTEXT_DISABLE_PID_CACHE));

 bson_oid_t oid;
 bson_oid_init(&oid, context);
 bson_oid_to_string(&oid, generatedId);
}

static void EnsureIndex(mongoc_collection_t* collection, const char* name, bson_t* indexDef, bool unique) {
  bson_error_t error;
  mongoc_index_opt_t opts;
  mongoc_index_opt_init(&opts);
  
  opts.name = name;
  opts.unique = unique;
  opts.background = true;
  
  mongoc_collection_create_index(collection, indexDef, &opts, &error);
}

static void DoStuff(mongoc_client_t* client, const uint32_t payload_length) {
  bson_error_t error;
  
  // Get a clean database
  mongoc_database_t* database = mongoc_client_get_database(client, "test");
  mongoc_database_drop(database, &error);
  database = mongoc_client_get_database(client, "test");
  
  // Get a clean collection
  mongoc_collection_t* collection = mongoc_database_create_collection(database, "c", NULL, &error);
  
  // Start timing
  struct timeval startTime, beforeExecTime, currentTime;
  gettimeofday(&startTime, NULL);
  uint64_t startusec = startTime.tv_sec * 1000000 + startTime.tv_usec;
  
  // Build and insert the documents
  uint64_t totalExecuteTime = 0;
  mongoc_bulk_operation_t* bulk = NULL;
  for (uint32_t i = 0; i < payload_length; i++) {
    if (i % BATCH_SIZE == 0) {
      
      // Execute!
      if (bulk != NULL) {
          gettimeofday(&beforeExecTime, NULL);
          uint64_t beforeExecute = beforeExecTime.tv_sec * 1000000 + beforeExecTime.tv_usec;
          
          bson_t* reply = bson_new();
          mongoc_bulk_operation_execute(bulk, reply, &error);
          
          mongoc_bulk_operation_destroy(bulk);
          bulk = NULL;
    
          gettimeofday(&currentTime, NULL);
          uint64_t currentusec = currentTime.tv_sec * 1000000 + currentTime.tv_usec;
          // printf("%d,%llu\n", i, currentusec - startusec);
          
          totalExecuteTime += currentusec - beforeExecute;
      }
      
      bulk = mongoc_collection_create_bulk_operation(collection, true, NULL);
    }
  
    char generatedId[25];
    GenerateID(generatedId);
    
    bson_t* document = bson_new();
    // bson_append_int32(document, "a", 1, i);
    // bson_append_int32(document, "c", 1, i * 100);
    // bson_append_utf8(document, "_key", 4, generatedId, 25);
    // bson_append_utf8(document, "_user", 5, "nobody", 6);
    mongoc_bulk_operation_insert(bulk, document);
    bson_destroy(document);
  }
  
  if (bulk != NULL) {
      gettimeofday(&beforeExecTime, NULL);
      uint64_t beforeExecute = beforeExecTime.tv_sec * 1000000 + beforeExecTime.tv_usec;
      
      bson_t* reply = bson_new();
      mongoc_bulk_operation_execute(bulk, reply, &error);
      
      mongoc_bulk_operation_destroy(bulk);
      bulk = NULL;

      gettimeofday(&currentTime, NULL);
      uint64_t currentusec = currentTime.tv_sec * 1000000 + currentTime.tv_usec;
      // printf("%d,%llu\n", i, currentusec - startusec);
      
      totalExecuteTime += currentusec - beforeExecute;
  }
  
  // Create the _user + _key index definition
  bson_t* indexDef = bson_new();
  bson_append_int32(indexDef, "_user", 5, 1);
  bson_append_int32(indexDef, "_key", 4, 1);
  EnsureIndex(collection, "UserAndKeyUniqueIndex", indexDef, true);
  
  // Create the "a" and "c" indices (don't bother about the memory leak)
  indexDef = bson_new();
  bson_append_int32(indexDef, "a", 1, 1);
  EnsureIndex(collection, "a", indexDef, false);
  indexDef = bson_new();
  bson_append_int32(indexDef, "c", 1, 1);
  EnsureIndex(collection, "c", indexDef, false);
  
  gettimeofday(&currentTime, NULL);
  uint64_t currentusec = currentTime.tv_sec * 1000000 + currentTime.tv_usec;
  
  printf("Total Time: %llu usec\n", currentusec - startusec);
  printf("Total Execution Time: %llu usec\n", totalExecuteTime);
  printf("Total Inserts: %u\n", payload_length);
  printf("Time per Execution: %llu usec\n", totalExecuteTime / payload_length);
  printf("Time per Insert Total: %llu usec\n", (currentusec - startusec) / payload_length);
  printf("Inserts per Second: %f\n", ((double) payload_length) / totalExecuteTime * 1000000);
  printf("Inserts per Second Total: %f\n", ((double) payload_length) / (currentusec - startusec) * 1000000);
}

int main (int argc, char *argv[]) {
  mongoc_client_t *client;
  
  if (argc < 2) {
    fprintf(stderr, "Usage: %s NUMBER\n", argv[0]);
    exit(1);
  }
  
  mongoc_init();

  client = mongoc_client_new("mongodb://__system:q7nuJka03fBzSesACoIvPimacdCL8YaPOunCOvaB65slrz7PBBfJxYw2laYwvFaSQ7kVwaxshIaTHJlvWAL2Rgaa@localhost:8191/"
    "?authSource=local"
    "&w=0"
    "&journal=false");
  if (!client) {
    fprintf(stderr, "Invalid URI: \"%s\"\n", argv[1]);
    return EXIT_FAILURE;
  }
  
  DoStuff(client, strtol(argv[1], NULL, 10));

  mongoc_client_destroy(client);

  return 0;
}
