#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <semaphore.h>
#include <math.h>
#include <stdint.h>
#include <time.h> 
#define MAX_LINE_LENGTH 1000

typedef struct {
	uint32_t hash;
	char name[50];
	uint32_t salary;
} SortedRecord;

// Hash Table Struct
typedef struct hash_struct
{
	uint32_t hash;
	char name[50];
	uint32_t salary;
	struct hash_struct* next;

} hashRecord;

typedef struct {
	char command[MAX_LINE_LENGTH];
	char name[50];
	uint32_t salary;
} Command;

// Define the semaphore globally
hashRecord** concurrentHashTable;
sem_t semaphore;
pthread_t* threads;
int numThreads;
size_t tableSize = 10;
int lockAcquisitions = 0;
int lockReleases = 0;

// Function Prototypes
void insert(uint8_t* key, uint32_t value);
void delete(uint8_t* key, uint32_t value);
uint32_t search(uint8_t* key);
uint32_t jenkinsOneAtATime(uint8_t* key, size_t length);
hashRecord** createTable();
hashRecord* createNode(uint8_t* key, uint32_t value, uint32_t hashValue);
void* threadFunction(void* arg);
void printHashTable();
int compareHash(const void* a, const void* b);
time_t currentTimestamp();

#include <time.h>

// Function to get a current timestamp in seconds
time_t currentTimestamp() {
	time_t seconds;
	seconds = time(NULL);
	return seconds;
}

void insert(uint8_t* key, uint32_t value) {
	time_t timestamp = currentTimestamp();
	int keyLen = strlen((char*)key);
	uint32_t hashValue = jenkinsOneAtATime(key, keyLen);
	int index = hashValue % tableSize;

	// Print the insert operation
	printf("%ld: INSERT,%u,%s,%u\n", timestamp, hashValue, key, value);

	// Acquire the write lock   
	sem_wait(&semaphore);
	timestamp = currentTimestamp();
	printf("%ld: WRITE LOCK ACQUIRED\n", timestamp);
	lockAcquisitions++;

	if (concurrentHashTable[index] != NULL) {
		hashRecord* current = concurrentHashTable[index];

		while (current->next && (current->hash != hashValue || strncmp((char*)current->name, (char*)key, MAX_LINE_LENGTH) != 0)) {
			current = current->next;
		}

		if (current->hash == hashValue && strncmp((char*)current->name, (char*)key, MAX_LINE_LENGTH) == 0) {
			current->salary = value;
			// Release the write lock and return since the value is updated
			sem_post(&semaphore);
			timestamp = currentTimestamp();
			printf("%ld: WRITE LOCK RELEASED\n", timestamp);
			lockReleases++;
			return;
		}
	}

	hashRecord* node = createNode(key, value, hashValue);
	if (node == NULL) {
		fprintf(stderr, "Memory allocation failed\n");
		sem_post(&semaphore);
		timestamp = currentTimestamp();
		printf("%ld: WRITE LOCK RELEASED\n", timestamp);
		lockReleases++;
		return;
	}
	node->next = concurrentHashTable[index];
	concurrentHashTable[index] = node;

	// Release the write lock after inserting   
	sem_post(&semaphore);
	timestamp = currentTimestamp();
	printf("%ld: WRITE LOCK RELEASED\n", timestamp);
	lockReleases++;
}

void delete(uint8_t* key, uint32_t value) {

	time_t timestamp = currentTimestamp();
	int keyLen = strlen((char*)key);
	uint32_t hashValue = jenkinsOneAtATime(key, keyLen);
	int index = hashValue % tableSize;

	// Print the delete operation
	printf("%ld: DELETE,%u,%s,%u\n", timestamp, hashValue, key, value);

	// Acquire write lock for exclusive access
	sem_wait(&semaphore);
	timestamp = currentTimestamp();
	printf("%ld: WRITE LOCK ACQUIRED\n", timestamp);
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

	// Release the write lock after deletion
	sem_post(&semaphore);
	timestamp = currentTimestamp();
	printf("%ld: WRITE LOCK RELEASED\n", timestamp);
	lockReleases++;
}

uint32_t search(uint8_t* key) {
	int keyLen = strlen((char*)key);
	uint32_t hashValue = jenkinsOneAtATime(key, keyLen);
	int index = hashValue % tableSize;

	// Acquire read lock for concurrent access
	sem_wait(&semaphore);
	lockAcquisitions++;

	hashRecord* current = concurrentHashTable[index];

	while (current != NULL) {
		if (current->hash == hashValue && strncmp((char*)current->name, (char*)key, MAX_LINE_LENGTH) == 0) {
			uint32_t salary = current->salary;
			// Release read lock after reading
			sem_post(&semaphore);
			lockReleases++;
			return salary;
		}
		current = current->next;
	}

	// Release read lock after reading
	sem_post(&semaphore);
	lockReleases++;

	// Key not found
	return 0;
}

// Function to compare hash values for qsort
int compareHash(const void* a, const void* b) {
	SortedRecord* recordA = (SortedRecord*)a;
	SortedRecord* recordB = (SortedRecord*)b;
	return (recordA->hash - recordB->hash);
}

void printHashTable() {
	SortedRecord* sortedRecords = (SortedRecord*)malloc(tableSize * sizeof(SortedRecord));
	int recordCount = 0;

	printf("Current contents of the hash table:\n");

	for (int i = 0; i < tableSize; i++) {
		// Acquire read lock before accessing the hash table
		sem_wait(&semaphore);
		lockAcquisitions++;

		hashRecord* current = concurrentHashTable[i];
		while (current != NULL) {
			sortedRecords[recordCount].hash = current->hash;
			strcpy(sortedRecords[recordCount].name, current->name);
			sortedRecords[recordCount].salary = current->salary;
			recordCount++;
			current = current->next;
		}

		// Release read lock after accessing the hash table
		lockReleases++;
		sem_post(&semaphore);
	}

	// Sort the records by hash value
	qsort(sortedRecords, recordCount, sizeof(SortedRecord), compareHash);

	// Print sorted records
	for (int i = 0; i < recordCount; i++) {
		printf("%u,%s,%u\n", sortedRecords[i].hash, sortedRecords[i].name, sortedRecords[i].salary);
	}

	free(sortedRecords);
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

hashRecord* createNode(uint8_t* key, uint32_t value, uint32_t hashValue) {


	hashRecord* node = (hashRecord*)malloc(sizeof(hashRecord));


	if (node == NULL) {
		printf("\nError: couldn't allocate memory to node.");
		return NULL;
	}

	node->hash = hashValue;
	strcpy(node->name, key);
	node->salary = value;
	node->next = NULL;

	return node;
}

void* threadFunction(void* arg) {
	Command* cmd = (Command*)arg;
	if (strcmp(cmd->command, "insert") == 0) {
		insert((uint8_t*)cmd->name, cmd->salary);
	}
	else if (strcmp(cmd->command, "delete") == 0) {
		delete((uint8_t*)cmd->name, cmd->salary);
	}
	else if (strcmp(cmd->command, "search") == 0) {
		uint32_t salary = search((uint8_t*)cmd->name);
		if (salary != 0) {
			printf("%s,%u\n", cmd->name, salary);
		}
		else {
			printf("No Record Found\n");
		}
	}
	pthread_exit(NULL);
}

int main() {
	// Initialize semaphore
	sem_init(&semaphore, 0, 1);

	// Create and initialize hash table
	concurrentHashTable = createTable();
	if (!concurrentHashTable) {
		return 1; // Exit if table creation failed
	}

	// Open command file
	FILE* file = fopen("commands.txt", "r");
	if (!file) {
		perror("Error opening file");
		return 1;
	}

	// Read number of threads
	fscanf(file, "threads,%d,0\n", &numThreads);
	threads = (pthread_t*)malloc(numThreads * sizeof(pthread_t));
	Command* commands = (Command*)malloc(numThreads * sizeof(Command));

	// Read commands and create threads
	for (int i = 0; i < numThreads; i++) {
		fscanf(file, "%[^,],%[^,],%u\n", commands[i].command, commands[i].name, &commands[i].salary);
		pthread_create(&threads[i], NULL, threadFunction, &commands[i]);
	}

	// Join threads
	for (int i = 0; i < numThreads; i++) {
		pthread_join(threads[i], NULL);
	}


	// Print the number of lock acquisitions and releases 
	printf("Number of lock acquisitions: %d\n", lockAcquisitions); 
	printf("Number of lock releases: %d\n", lockReleases);

	// Print hash table
	printHashTable();

	// Clean up
	free(commands);
	free(threads);
	fclose(file);
	sem_destroy(&semaphore);
	free(concurrentHashTable); // Don't forget to free the hash table memory

	return 0;
}