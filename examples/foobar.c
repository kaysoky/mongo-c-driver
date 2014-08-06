#include <bson.h>
#include <mongoc.h>
#include <stdio.h>
#include <stdlib.h>

static void GenerateID(char generatedId[25]) {
  bson_context_t* context = bson_context_new(
      (bson_context_flags_t) (BSON_CONTEXT_THREAD_SAFE | BSON_CONTEXT_DISABLE_PID_CACHE));
  
  bson_oid_t oid;
  bson_oid_init(&oid, context);
  bson_oid_to_string(&oid, generatedId);
}

static void PrintBSON(bson_t* obj) {
  size_t len;
  char* str = bson_as_json(obj, &len);
  printf("%s\n", str);
}

static void DoStuff(mongoc_client_t* client, const uint32_t payload_length) {
  struct timeval startTime, currentTime;
  gettimeofday(&startTime, NULL);
  uint64_t startusec = startTime.tv_sec * 1000000 + startTime.tv_usec;
  
  // Build an array!
  bson_t* array = bson_new();
  
  printf("Array Size,Usec Elapsed\n");
  
  const char* key;
  uint32_t index = 0;
  char snprintfStorage[16];
  for (uint32_t i = 0; i < payload_length; i++) {
    bson_uint32_to_string (index++, &key, snprintfStorage, sizeof(snprintfStorage));
    bson_append_int32(array, key, -1, i);
    
    gettimeofday(&currentTime, NULL);
    uint64_t currentusec = currentTime.tv_sec * 1000000 + currentTime.tv_usec;
    printf("%d,%llu\n", array->len, currentusec - startusec);
  }
}

int main (int argc, char *argv[]) {
  mongoc_client_t *client;
  
  if (argc < 2) {
    fprintf(stderr, "Usage: %s NUMBER\n", argv[0]);
    exit(1);
  }
  
  mongoc_init();

  client = mongoc_client_new("mongodb://__system:q7nuJka03fBzSesACoIvPimacdCL8YaPOunCOvaB65slrz7PBBfJxYw2laYwvFaSQ7kVwaxshIaTHJlvWAL2Rgaa@localhost:8191/?authSource=local");
  if (!client) {
    fprintf(stderr, "Invalid URI: \"%s\"\n", argv[1]);
    return EXIT_FAILURE;
  }

  DoStuff(client, strtol(argv[1], NULL, 10));

  mongoc_client_destroy(client);

  return 0;
}
