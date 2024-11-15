// Definitions
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

// Hash Table Struct
typedef struct hash_struct
{
	uint32_t hash;
	char name[50];
	uint32_t salary;
	struct hash_struct* next;

} hashRecord;

// Function Prototypes
hashRecord** createTable();
hashRecord* createNode(uint8_t* key, uint32_t value, uint32_t hashValue);
uint32_t jenkinsOneAtATime(uint8_t* key, size_t length);
void insert(uint8_t* key, uint32_t value);
void delete(uint8_t* key);
uint32_t search(uint8_t* key);
void cleanupHashTable();
uint32_t search(uint8_t* key);
void parseCommand(FILE* commands, char destination[][50]);
void* handleCommand(void* arg);


// Global Variables
hashRecord** concurrentHashTable;
pthread_t* threadsArray;
size_t tableSize = 10;
int numThreads;
int lockAcquisitions = 0;
int lockReleases = 0;
pthread_mutex_t* write_locks;
pthread_rwlock_t* read_locks;
FILE* commands;
FILE* output;
