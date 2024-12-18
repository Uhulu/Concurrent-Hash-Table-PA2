/*
Matthew Itskovich
Luke Knowles
Julian Nutovits
Jesuel Rosado Arroyo
Elizabeth Teter
*/
#include "hash.h"

// Function that creates the hash table.
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

// Function that creates a node.
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

// Hash function.
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

// Function that inserts into the hash table.
void insert(uint8_t* key, uint32_t value) {

    // Get the current timestamp
    time_t timestamp = currentTimestamp();
    int keyLen = strlen((char*)key);

    // Compute the hash value of the key using the Jenkins one-at-a-time hash function
    uint32_t hashValue = jenkinsOneAtATime(key, keyLen);

    // Compute the index in the hash table
    int index = hashValue % tableSize;

    // Acquire the write lock to ensure exclusive access for writing
    pthread_mutex_lock(&write_locks[index]);
    timestamp = currentTimestamp();
    lockAcquisitions++;
    fprintf(output, "%ld: WRITE LOCK ACQUIRED\n", timestamp);

    // Print the insert operation to the output file
    fprintf(output, "%ld: INSERT,%u,%s,%u\n", timestamp, hashValue, key, value);
    

    // Check if there is an existing entry in the hash table at the computed index
    if (concurrentHashTable[index] != NULL) {
        hashRecord* current = concurrentHashTable[index];

        // Traverse the linked list to find the node with the same hash and key
        while (current->next && (current->hash != hashValue || strncmp((char*)current->name, (char*)key, MAX_LINE_LENGTH) != 0)) {
            current = current->next;
        }

        // If the node with the same hash and key is found, update its salary
        if (current->hash == hashValue && strncmp((char*)current->name, (char*)key, MAX_LINE_LENGTH) == 0) {
            current->salary = value;

            // Release the write lock and return as the value is updated
            pthread_mutex_unlock(&write_locks[index]);
            timestamp = currentTimestamp();
            fprintf(output, "%ld: WRITE LOCK RELEASED\n", timestamp);
            lockReleases++;
            return;
        }
    }

    // If the node is not found, create a new node and insert it into the hash table
    hashRecord* node = createNode(key, value, hashValue);

    // Check if memory allocation for the new node failed
    if (node == NULL) {
        fprintf(stderr, "Memory allocation failed\n");

        // Release the write lock in case of failure
        pthread_mutex_unlock(&write_locks[index]);
        timestamp = currentTimestamp();
        fprintf(output, "%ld: WRITE LOCK RELEASED\n", timestamp);
        lockReleases++;
        return;
    }

    // Insert the new node at the beginning of the linked list at the computed index
    node->next = concurrentHashTable[index];
    concurrentHashTable[index] = node;

    // Release the write lock after inserting the new node
    pthread_mutex_unlock(&write_locks[index]);
    timestamp = currentTimestamp();
    fprintf(output, "%ld: WRITE LOCK RELEASED\n", timestamp);
    lockReleases++;
}

// Function that deletes from the hash table.
void delete(uint8_t* key) {
    // Calculate the key length
    int keyLen = strlen((char*)key);

    // Compute the hash value of the key using the Jenkins one-at-a-time hash function
    uint32_t hashValue = jenkinsOneAtATime(key, keyLen);

    // Compute the index in the hash table
    int index = hashValue % tableSize;

    // Get the current timestamp
    time_t timestamp = time(NULL);

    // Acquire the write lock to ensure exclusive access for writing
    pthread_mutex_lock(&write_locks[index]);
    lockAcquisitions++;
    fprintf(output, "%ld: WRITE LOCK ACQUIRED\n", timestamp);

    // Print the delete operation to the output file
    fprintf(output, "%ld: DELETE,%u,%s\n", timestamp, hashValue, key);
    

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
        } else {
            // Node to delete is in the middle or end of the list
            previous->next = current->next;
        }

        free(current);  // Free the memory of the deleted node
    }

    // Get the current timestamp
    timestamp = time(NULL);

    // Release the write lock after deletion
    fprintf(output, "%ld: WRITE LOCK RELEASED\n", timestamp);
    lockReleases++;
    pthread_mutex_unlock(&write_locks[index]);
}

// Function that searches in the hash table.
uint32_t search(uint8_t* key) {
    // Get the current timestamp
    time_t timestamp = currentTimestamp();

    // Calculate the key length
    int keyLen = strlen((char*)key);

    // Compute the hash value of the key using the Jenkins one-at-a-time hash function
    uint32_t hashValue = jenkinsOneAtATime(key, keyLen);

    // Compute the index in the hash table
    int index = hashValue % tableSize;

    // Log the read lock acquisition and search operation
    fprintf(output, "%ld: READ LOCK ACQUIRED\n", timestamp);
    fprintf(output, "%ld: SEARCH,%u,%s\n", timestamp, hashValue, key);

    // Acquire read lock for concurrent access
    pthread_rwlock_rdlock(&read_locks[index]);
    lockAcquisitions++;

    // Pointer to traverse the linked list at hashTable[index]
    hashRecord* current = concurrentHashTable[index];

    // Traverse the list to find the node with the matching hash and key
    while (current != NULL) {
        if (current->hash == hashValue && strncmp((char*)current->name, (char*)key, MAX_LINE_LENGTH) == 0) {
            uint32_t salary = current->salary;

            // Release read lock after reading
            pthread_rwlock_unlock(&read_locks[index]);
            lockReleases++;
            return salary;
        }
        current = current->next;
    }

    // Release read lock after reading
    pthread_rwlock_unlock(&read_locks[index]);
    timestamp = currentTimestamp();
    lockReleases++;

    // Key not found, return 0
    return 0;
}

// Helper function for qsort to compare hash values of two hashRecord structs
int compareHashRecords(const void* a, const void* b) {
	hashRecord* recordA = *(hashRecord**)a;
	hashRecord* recordB = *(hashRecord**)b;
	return (recordA->hash - recordB->hash);
}

// Function that print the whole hashtable.
void printTable() {
    // Get the current timestamp
    time_t timestamp = time(NULL);    

    // Log the read lock acquisition
    pthread_rwlock_rdlock(&read_locks[0]);
    fprintf(output, "%ld: READ LOCK ACQUIRED\n", timestamp);
    lockAcquisitions++;

    // Step 1: Gather all entries into a list
    // Allocate a dynamic list to store hash records
    hashRecord** records = malloc(tableSize * sizeof(hashRecord*));
    int count = 0;

    // Traverse the hash table and gather all entries
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

    // Get the current timestamp
    timestamp = time(NULL);

    // Log the read lock release
    pthread_rwlock_unlock(&read_locks[0]);    
    lockReleases++;
    fprintf(output, "%ld: READ LOCK RELEASED\n", timestamp);
}

// Function that clears the hashtable
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

// Function that reads next line and splits around commas.
void parseCommand(FILE* commands, char destination[][50]) {
    int c;
    int i = 0;

    // Read the first part of the command
    for (; (c = fgetc(commands)) != ','; i++) {
        destination[0][i] = c;
    }
    destination[0][i] = '\0';

    // If the command is 'print', handle it differently
    if (strcmp(destination[0], "print") == 0) {
        strcpy(destination[1], "0");
        strcpy(destination[2], "0");
		while ((c = fgetc(commands)) != '\n' && c != EOF);  // Move to the end of the line
        return;
    }

    // Read the second part of the command
    for (i = 0; (c = fgetc(commands)) != ','; i++) {
        destination[1][i] = c;
    }
    destination[1][i] = '\0';

    // Read the third part of the command
    for (i = 0; (c = fgetc(commands)) != '\n' && c != EOF; i++) {
        destination[2][i] = c;
    }
    destination[2][i] = '\0';
}

// Funtion that handles the command function calls.
void* handleCommand(void* arg) {
    char** cmdPieces = (char**)arg;

    if (strcmp(cmdPieces[0], "insert") == 0) {
        insert((uint8_t*)cmdPieces[1], (uint32_t)atoi(cmdPieces[2]));
    }
    else if (strcmp(cmdPieces[0], "delete") == 0) {
        delete((uint8_t*)cmdPieces[1]);
    }
    else if (strcmp(cmdPieces[0], "search") == 0) {
        uint32_t salary = search((uint8_t*)cmdPieces[1]);

        if (salary != 0) {
            fprintf(output, "SEARCH: %s FOUND with salary %u\n", cmdPieces[1], salary);
        }
        else {
            fprintf(output, "SEARCH: %s NOT FOUND\n", cmdPieces[1]);
        }

        fprintf(output, "%ld: READ LOCK RELEASED\n", time(NULL));
    }
    else if (strcmp(cmdPieces[0], "print") == 0) {        
	printTable();		
    }

    return NULL;
}

// Main function.
int main() {
    // Open command file for reading
    commands = fopen("commands.txt", "r");

    // Open output file for writing
    output = fopen("output.txt", "w");

    // Initialize command reader parameters
    int cmdParamLength = 50;
    int cmdParameters = 3;
    char cmdPieces[cmdParameters][cmdParamLength];

    // Read the number of threads from the first command
    parseCommand(commands, cmdPieces);
    int threads = atoi(cmdPieces[1]);
    tableSize = threads;
    fprintf(output, "Running %d threads\n", threads);

    // Create and initialize the hash table
    concurrentHashTable = createTable();

    // Initialize read and write locks
    read_locks = (pthread_rwlock_t*)malloc(tableSize * sizeof(pthread_rwlock_t));
    write_locks = (pthread_mutex_t*)malloc(tableSize * sizeof(pthread_mutex_t));

    for (int i = 0; i < tableSize; i++) {
        pthread_rwlock_init(&read_locks[i], NULL);
        pthread_mutex_init(&write_locks[i], NULL);
    }

    // Allocate memory for threads
    threadsArray = (pthread_t*)malloc(threads * sizeof(pthread_t));

    // Loop through the commands and create threads to handle each command
    for (int i = 0; i < threads; i++) {
        // Parse the next command
        parseCommand(commands, cmdPieces);		

        // Allocate memory for command arguments for each thread
        char** cmdArgs = (char**)malloc(3 * sizeof(char*));
        for (int j = 0; j < 3; j++) {
            cmdArgs[j] = strdup(cmdPieces[j]);  // Use strdup to simplify allocation
        }

        // Create a thread to handle each command
        pthread_create(&threadsArray[i], NULL, handleCommand, (void*)cmdArgs);
    }

    // Join all threads
    for (int i = 0; i < threads; i++) {
        pthread_join(threadsArray[i], NULL);
    }

    // Log that all threads have finished
    fprintf(output, "Finished all threads.\n\n");

    // Print the number of lock acquisitions and releases
    fprintf(output, "Number of lock acquisitions: %d\n", lockAcquisitions);
    fprintf(output, "Number of lock releases: %d\n", lockReleases);

    // Print the hash table
    printTable();

    // Clean up resources
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

