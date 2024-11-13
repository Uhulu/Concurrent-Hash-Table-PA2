#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h> 
#include <math.h>
#include "hash.h"
#define MAX_LINE_LENGTH 1000
#define tableSize 10 // Only for testing purposes

hashRecord** createTable() {

	concurrentHashTable = (hashRecord**)malloc(tableSize * sizeof(hashRecord*));

	if (!concurrentHashTable) {
		printf("\nError: couldn't allocate memory to hash table.");
		return NULL;
	}

	for (int i = 0; i < tableSize; i++)
		concurrentHashTable[i] = NULL;

	return concurrentHashTable;
}

// Function to get a current timestamp in seconds.
time_t currentTimestamp() {
	time_t seconds;
	seconds = time(NULL);
	return seconds;
}

hashRecord* createNode(uint8_t* key, uint32_t value, uint32_t hashValue) {


	hashRecord* node = (hashRecord*)malloc(sizeof(hashRecord));


	if (node == NULL) {
		printf("\nError: couldn't allocate memory to node.");
		return NULL;
	}

	node->hash = hashValue;
	strcpy(node->name, (const char*)key);
	node->salary = value;
	node->next = NULL;

	return node;
}

uint32_t jenkinsOneAtATime(uint8_t* key, size_t length) {
	size_t i = 0;
	uint32_t hash = 0;
	while (i != length) {
		hash += key[i++];
		hash += hash << 10;
		hash ^= hash >> 6;
	}
	hash += hash << 3;
	hash ^= hash >> 11;
	hash += hash << 15;
	return hash;
}

void insert(uint8_t* key, uint32_t value) {

	time_t timestamp = currentTimestamp();
	int keyLen = strlen((char*)key);
	uint32_t hashValue = jenkinsOneAtATime(key, keyLen);
	int index = hashValue % tableSize;


	// Acquire the write lock   
	pthread_mutex_lock(&write_locks[index]);
	timestamp = currentTimestamp();
	fprintf(output, "%ld: WRITE LOCK ACQUIRED\n", timestamp);
	// Print the insert operation
	fprintf(output, "%ld: INSERT,%u,%s,%u\n", timestamp, hashValue, key, value);
	lockAcquisitions++;

	if (concurrentHashTable[index] != NULL) {
		hashRecord* current = concurrentHashTable[index];

		while (current->next && (current->hash != hashValue || strncmp((char*)current->name, (char*)key, MAX_LINE_LENGTH) != 0)) {
			current = current->next;
		}

		if (current->hash == hashValue && strncmp((char*)current->name, (char*)key, MAX_LINE_LENGTH) == 0) {
			current->salary = value;
			// Release the write lock and return since the value is updated
			pthread_mutex_unlock(&write_locks[index]);
			timestamp = currentTimestamp();
			fprintf(output, "%ld: WRITE LOCK RELEASED\n", timestamp);
			lockReleases++;
			return;
		}
	}

	hashRecord* node = createNode(key, value, hashValue);

	if (node == NULL) {
		fprintf(stderr, "Memory allocation failed\n");
		pthread_mutex_unlock(&write_locks[index]);
		timestamp = currentTimestamp();
		fprintf(output, "%ld: WRITE LOCK RELEASED\n", timestamp);
		lockReleases++;
		return;
	}
	node->next = concurrentHashTable[index];
	concurrentHashTable[index] = node;

	// Release the write lock after inserting   
	pthread_mutex_unlock(&write_locks[index]);
	timestamp = currentTimestamp();
	fprintf(output, "%ld: WRITE LOCK RELEASED\n", timestamp);
	lockReleases++;

}

void delete(uint8_t* key) {

	int keyLen = strlen((char*)key);
	uint32_t hashValue = jenkinsOneAtATime(key, keyLen);
	int index = hashValue % tableSize;

	time_t timestamp = time(NULL);

	pthread_mutex_lock(&write_locks[index]);
	fprintf(output, "%ld: WRITE LOCK ACQUIRED\n", timestamp);
	fprintf(output, "%ld: DELETE,%u,%s\n", timestamp, hashValue, key);

	lockAcquisitions++;


	// Pointer to traverse the linked list at hashTable[index]
	hashRecord* current = concurrentHashTable[index];
	hashRecord* previous = NULL;

	// Traverse the list to find the node to delete
	while (current != NULL && (current->hash != hashValue || strncmp((char*)current->name, (char*)key, MAX_LINE_LENGTH) != 0)) {
		previous = current;
		current = current->next;
	}

	// If the node was found, delete it
	if (current != NULL) {
		if (previous == NULL) {
			// Node to delete is the first node in the list
			concurrentHashTable[index] = current->next;
		}
		else {
			// Node to delete is in the middle or end of the list
			previous->next = current->next;
		}

		free(current);  // Free the memory of the deleted node
	}

	timestamp = time(NULL);
	fprintf(output, "%ld: WRITE LOCK RELEASED\n", timestamp);
	pthread_mutex_unlock(&write_locks[index]);
	lockReleases++;
}

uint32_t search(uint8_t* key) {

	time_t timestamp = currentTimestamp();
	int keyLen = strlen((char*)key);
	uint32_t hashValue = jenkinsOneAtATime(key, keyLen);
	int index = hashValue % tableSize;

	fprintf(output, "%ld: READ LOCK ACQUIRED\n", timestamp);
	fprintf(output, "%ld: SEARCH,%u,%s\n", timestamp, hashValue, key);

	// Acquire read lock for concurrent access
	pthread_rwlock_rdlock(&read_locks[index]);
	lockAcquisitions++;

	hashRecord* current = concurrentHashTable[index];

	while (current != NULL) {
		if (current->hash == hashValue && strncmp((char*)current->name, (char*)key, MAX_LINE_LENGTH) == 0) {
			uint32_t salary = current->salary;
			// Release read lock after reading
			// fprintf(output, "SEARCH: %s FOUND with salary %u\n", current->name, current->salary);
			// fprintf(output, "%ld: READ LOCK RELEASED\n", timestamp);
			pthread_rwlock_unlock(&read_locks[index]);
			lockReleases++;
			return salary;
		}
		current = current->next;
	}

	// Release read lock after reading
	pthread_rwlock_unlock(&read_locks[index]);
	timestamp = currentTimestamp();
	// fprintf(output, "SEARCH: NOT FOUND\n");
	// fprintf(output, "%ld: READ LOCK RELEASED\n", timestamp);
	lockReleases++;

	// Key not found
	return 0;
}

// Helper function for qsort to compare hash values of two hashRecord structs
int compareHashRecords(const void* a, const void* b) {
	hashRecord* recordA = *(hashRecord**)a;
	hashRecord* recordB = *(hashRecord**)b;
	return (recordA->hash - recordB->hash);
}

void printTable() {

	time_t timestamp = time(NULL);

	fprintf(output, "Number of lock acquisitions: %d\n", lockAcquisitions);
	fprintf(output, "Number of lock releases: %d\n", lockReleases);

	fprintf(output, "%ld: READ LOCK ACQUIRED\n", timestamp);
	lockAcquisitions++;

	// Step 1: Gather all entries into a list
	hashRecord** records = malloc(tableSize * sizeof(hashRecord*)); // Allocate a dynamic list
	int count = 0;

	for (int i = 0; i < tableSize; i++) {
		hashRecord* current = concurrentHashTable[i];
		while (current != NULL) {
			records[count++] = current;
			current = current->next;
		}
	}

	// Step 2: Sort the list by hash values
	qsort(records, count, sizeof(hashRecord*), compareHashRecords);

	// Step 3: Print sorted entries
	for (int i = 0; i < count; i++) {
		fprintf(output, "%u,%s,%u\n", records[i]->hash, records[i]->name, records[i]->salary);
	}

	// Clean up the temporary list
	free(records);

	timestamp = time(NULL);
	lockReleases++;
	fprintf(output, "%ld: READ LOCK RELEASED\n", timestamp);
}


void cleanupHashTable() {
	for (int i = 0; i < tableSize; i++) {
		hashRecord* current = concurrentHashTable[i];
		while (current != NULL) {
			hashRecord* temp = current;
			current = current->next;
			free(temp);
		}
	}
}

// read next line and split around commas
void parseCommand(FILE* commands, char destination[][20]) {
	int c;
	int i = 0;
	for (; (c = fgetc(commands)) != ','; i++)
	{
		destination[0][i] = c;
	}
	destination[0][i] = '\0';
	for (i = 0; (c = fgetc(commands)) != ','; i++)
	{
		destination[1][i] = c;
	}
	destination[1][i] = '\0';
	for (i = 0; (c = fgetc(commands)) != '\n' && c != EOF; i++)
	{
		destination[2][i] = c;
	}
	destination[2][i] = '\0';
}

void* handleCommand(void* arg) {
	char** cmdPieces = (char**)arg;

	if (!strcmp(cmdPieces[0], "insert")) {
		insert((uint8_t*)cmdPieces[1], (uint32_t)atoi(cmdPieces[2]));
	}
	else if (!strcmp(cmdPieces[0], "delete")) {
		delete((uint8_t*)cmdPieces[1]);

	}
	else if (!strcmp(cmdPieces[0], "search")) {

		uint32_t salary = search((uint8_t*)cmdPieces[1]);

		 if (salary != 0) {
		     fprintf(output, "SEARCH: %s FOUND with salary %u\n", cmdPieces[1], salary);			 
		 }
		 else {
		     fprintf(output, "SEARCH: %s NOT FOUND\n", cmdPieces[1]);
		}

		 fprintf(output, "%ld: READ LOCK RELEASED\n", time(NULL));
	}

	return NULL;
}

int main() {

	concurrentHashTable = createTable();


	// Initialize Locks
	read_locks = (pthread_rwlock_t*)malloc(tableSize * sizeof(pthread_rwlock_t));
	write_locks = (pthread_mutex_t*)malloc(tableSize * sizeof(pthread_mutex_t));

	for (int i = 0; i < tableSize; i++) {
		pthread_rwlock_init(&read_locks[i], NULL);
		pthread_mutex_init(&write_locks[i], NULL);
	}

	// read from file
	commands = fopen("commands.txt", "r");

	// open output file
	output = fopen("output.txt", "w");

	// initialize command reader
	int cmdParamLength = 20;
	int cmdParameters = 3;
	char cmdPieces[cmdParameters][cmdParamLength];

	// get number of threads
	parseCommand(commands, cmdPieces);
	int threads = atoi(cmdPieces[1]);
	fprintf(output, "Running %d threads\n", threads);

	threadsArray = (pthread_t*)malloc(threads * sizeof(pthread_t));

	// Loop through the commands
	for (int i = 0; i < threads; i++)
	{
		// scan command
		parseCommand(commands, cmdPieces);

		// Allocate command arguments for each thread
		char** cmdArgs = (char**)malloc(3 * sizeof(char*));

		for (int j = 0; j < 3; j++) {
			cmdArgs[j] = strdup(cmdPieces[j]);  // Use strdup to simplify allocation
		}

		// Create a thread to handle each command
		pthread_create(&threadsArray[i], NULL, handleCommand, (void*)cmdArgs);


	}

	// Join threads
	for (int i = 0; i < threads; i++) {
		pthread_join(threadsArray[i], NULL);
	}

	fprintf(output, "Finished all threads.\n");
	printTable();

	for (int i = 0; i < tableSize; i++) {
		pthread_rwlock_destroy(&read_locks[i]);
		pthread_mutex_destroy(&write_locks[i]);
	}

	free(threadsArray);
	free(read_locks);
	free(write_locks);
	fclose(commands);
	fclose(output);
	cleanupHashTable();



	return 0;
}
