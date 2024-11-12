#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "hash.h"
#define MAX_LINE_LENGTH 1000
#define tableSize 10 // Only for testing purposes

int lockAcquisitions = 0; // Counter for lock acquisitions
int lockReleases = 0;

pthread_rwlock_t bucketLocks[tableSize];
pthread_mutex_t* write_locks;
pthread_t* threads = NULL; // Array to store thread IDs

// Global hash table
hashRecord** concurrentHashTable = NULL;

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
    int keyLen = strlen((char*)key);
    uint32_t hashValue = jenkinsOneAtATime(key, keyLen);
    int index = hashValue % tableSize;

    time_t timestamp = time(NULL);
    
    // Acquire the write lock
    //pthread_rwlock_wrlock(&bucketLocks[index]);
    pthread_mutex_lock(&write_locks[index]);
    lockAcquisitions++;

    // Log the lock acquisition
    timestamp = time(NULL);
    printf("%ld,WRITE LOCK ACQUIRED\n", timestamp);

    // Traverse to check if the key exists in the hash table
    hashRecord* current = concurrentHashTable[index];
    while (current != NULL) {
        if (current->hash == hashValue && strncmp((char*)current->name, (char*)key, MAX_LINE_LENGTH) == 0) {
            // Key exists, update the value
            current->salary = value;

            // Log the update (not an insertion)
            timestamp = time(NULL);
            printf("%ld: Key %s already exists, updated value to %u\n", timestamp, key, value);
            //pthread_rwlock_unlock(&bucketLocks[index]);
            pthread_mutex_unlock(&write_locks[index]);
            lockReleases++;
            // Log the lock release
            timestamp = time(NULL);
            printf("%ld,WRITE LOCK RELEASED\n", timestamp);
            return; // Exit after updating the value
        }
        current = current->next;
    }

    // Key does not exist, create a new node and insert it
    hashRecord* node = createNode(key, value, hashValue);
    node->next = concurrentHashTable[index];
    concurrentHashTable[index] = node;

    // Log the insertion
    timestamp = time(NULL);
    printf("%ld: INSERT,%u,%s,%u\n", timestamp, hashValue, key, value);

    // Release the lock after the insertion
    //pthread_rwlock_unlock(&bucketLocks[index]);
    pthread_mutex_unlock(&write_locks[index]);
    lockReleases++;

    // Log the lock release
    timestamp = time(NULL);
    printf("%ld,WRITE LOCK RELEASED\n", timestamp);
}



void delete(uint8_t* key, uint32_t value) {
    int keyLen = strlen((char*)key);
    uint32_t hashValue = jenkinsOneAtATime(key, keyLen);
    int index = hashValue % tableSize;
    time_t timestamp = time(NULL);
    printf("%ld: DELETE,%u,%s,%u\n", timestamp, hashValue, key, value);

    
    printf("%ld,WRITE LOCK ACQUIRED\n", timestamp);
    pthread_mutex_lock(&write_locks[index]);
    //pthread_rwlock_wrlock(&bucketLocks[index]);
    lockAcquisitions++;

    hashRecord* current = concurrentHashTable[index];
    hashRecord* previous = NULL;

    while (current != NULL && (current->hash != hashValue || strncmp((char*)current->name, (char*)key, MAX_LINE_LENGTH) != 0)) {
        previous = current;
        current = current->next;
    }

    if (current != NULL) {
        if (previous == NULL) {
            concurrentHashTable[index] = current->next;
        }
        else {
            previous->next = current->next;
        }
        free(current);
    }

    timestamp = time(NULL);
    printf("%ld,WRITE LOCK RELEASED\n", timestamp);
    //pthread_rwlock_unlock(&bucketLocks[index]);
    pthread_mutex_unlock(&write_locks[index]);
    lockReleases++;
}

uint32_t search(uint8_t* key, uint32_t value) {
    int keyLen = strlen((char*)key);
    uint32_t hashValue = jenkinsOneAtATime(key, keyLen);
    int index = hashValue % tableSize;

    pthread_rwlock_rdlock(&bucketLocks[index]);
    time_t timestamp = time(NULL);
    printf("%ld,READ LOCK ACQUIRED\n", timestamp);
    lockAcquisitions++;
    printf("%ld: SEARCH,%u,%s,%u\n", timestamp, hashValue, key, value);

    hashRecord* current = concurrentHashTable[index];
    while (current != NULL) {
        if (current->hash == hashValue && strncmp((char*)current->name, (char*)key, MAX_LINE_LENGTH) == 0) {
            uint32_t salary = current->salary;
            pthread_rwlock_unlock(&bucketLocks[index]);
            timestamp = time(NULL);
            printf("%ld,READ LOCK RELEASED\n", timestamp);
            return salary;
        }
        current = current->next;
    }

    pthread_rwlock_unlock(&bucketLocks[index]);
    timestamp = time(NULL);
    printf("%ld,READ LOCK RELEASED\n", timestamp);
    lockReleases++;
    return 0;
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

// Read next line and split around commas
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

// Thread function to handle the commands
void* handleCommand(void* arg) {
    char** cmdPieces = (char**)arg;

    if (!strcmp(cmdPieces[0], "insert")) {
        insert((uint8_t*)cmdPieces[1], atoi(cmdPieces[2]));
    }
    else if (!strcmp(cmdPieces[0], "delete")) {
        delete((uint8_t*)cmdPieces[1], atoi(cmdPieces[2]));
        printf("hi");
    }
    else if (!strcmp(cmdPieces[0], "search")) {
        search((uint8_t*)cmdPieces[1], atoi(cmdPieces[2]));
        printf("hi");
    }

    return NULL;
}

void printFinalHashTable(FILE* output) {
    hashRecord* current;
    
    // Print lock acquisition and release counts
    fprintf(output, "Number of lock acquisitions:  %d\n", lockAcquisitions);
    fprintf(output, "Number of lock releases:  %d\n", lockReleases);
    
    // Print the records in the desired format
    for (int i = 0; i < tableSize; i++) {
        current = concurrentHashTable[i];
        while (current != NULL) {
            fprintf(output, "%u,%s,%u\n", current->hash, current->name, current->salary);
            current = current->next;
        }
    }
}

int main() {
    concurrentHashTable = createTable();

    // Initialize write locks for each bucket
    write_locks = (pthread_mutex_t*)malloc(tableSize * sizeof(pthread_mutex_t));
    for (int i = 0; i < tableSize; i++) {
        pthread_mutex_init(&write_locks[i], NULL);
    }

    // Open command and output files
    FILE* commands = fopen("commands.txt", "r");
    FILE* output = fopen("output.txt", "w");

    // Initialize command reader and parse the first line
    char cmdPieces[3][20];
    parseCommand(commands, cmdPieces);
    int numThreads = atoi(cmdPieces[1]); // Number of threads to spawn
    fprintf(output, "Running %d threads\n", numThreads);

    // Allocate thread array and process commands
    threads = (pthread_t*)malloc(numThreads * sizeof(pthread_t));
    for (int i = 0; i < numThreads; i++) {
        parseCommand(commands, cmdPieces);

        // Allocate command arguments for each thread
        char** cmdArgs = (char**)malloc(3 * sizeof(char*));
        for (int j = 0; j < 3; j++) {
            cmdArgs[j] = strdup(cmdPieces[j]);  // Use strdup to simplify allocation
        }

        // Create a thread to handle each command
        pthread_create(&threads[i], NULL, handleCommand, (void*)cmdArgs);
    }

    // Join threads and free command arguments
    for (int i = 0; i < numThreads; i++) {
        pthread_join(threads[i], NULL);
    }

    // Print the final hash table to the output
    printFinalHashTable(output);

    // Cleanup resources
    for (int i = 0; i < tableSize; i++) {
        pthread_mutex_destroy(&write_locks[i]);
    }
    free(write_locks);
    free(threads);
    fclose(commands);
    fclose(output);
    cleanupHashTable();

    return 0;
}