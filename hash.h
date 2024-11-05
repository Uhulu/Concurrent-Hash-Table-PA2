// Concurrent Hash Table definitions

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

// Linked List Node
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
void cleanupHashTable();
uint32_t search(uint8_t* key);


// Global Variables
hashRecord** concurrentHashTable;
size_t tableSize;