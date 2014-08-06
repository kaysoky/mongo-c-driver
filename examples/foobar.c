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

static bson_t* BuildInsert(uint32_t length) {
  // BSON object holding an ID
  bson_t* idObj = bson_new();
  char id_str[25];
  GenerateID(id_str);
  bson_append_utf8(idObj, "_id", 3, id_str, 25);
  
  // BSON object with the payload to insert
  bson_t* payload = bson_new();
  const char* key;
  char str[16];
  for (int i = 0; i < length; i++) {
    bson_uint32_to_string (i, &key, str, sizeof(str));
    bson_append_utf8(payload, key, -1, "b", 1);
  }
  
  // Build up this -> {q:{_id:<_id>}, u:<document>, upsert:true}
  bson_t* insert = bson_new();
  bson_append_document(insert, "q", 1, idObj);
  bson_append_document(insert, "u", 1, payload);
  bson_append_bool(insert, "upsert", 6, true);
  
  return insert;
}

static void DoStuff(mongoc_client_t* client, const uint32_t payload_length) {
  // Turn the insert into a one element array
  bson_t* array = bson_new();
  bson_append_document(array, "0", -1, BuildInsert(3));
  bson_append_document(array, "1", -1, BuildInsert(payload_length));
  
  // Build up the command to send
  bson_t* command = bson_new();
  bson_append_utf8(command, "update", 6, "c", 1);
  bson_append_array(command, "updates", 7, array);
  bson_append_bool(command, "ordered", 7, true);
  
  // Send the command
  bson_t reply;
  bson_error_t error;
  bool success = mongoc_client_command_simple(client, "TODO", command, NULL, &reply, &error);
  
  if (!success) {
    printf("Command failed (domain: %u) (code: %u) (message: %s)\n", error.domain, error.code, error.message);
    exit(1);
  }
  
  PrintBSON(&reply);
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
