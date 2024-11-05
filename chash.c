#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
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
    
    

    if (concurrentHashTable[index] != NULL) {

        hashRecord* current = concurrentHashTable[index];

        while (current->next && (current->hash != hashValue || strncmp((char*)current->name, (char*)key, MAX_LINE_LENGTH) != 0)) {
            current = current->next;
        }

        if (current->hash == hashValue && strncmp((char*)current->name, (char*)key, MAX_LINE_LENGTH) == 0) {
            current->salary = value;
            return;
        }

    }

    hashRecord* node = createNode(key, value, hashValue);
    node->next = concurrentHashTable[index];
    concurrentHashTable[index] = node;
   
}

void delete(uint8_t* key) {

    int keyLen = strlen((char*)key);
    uint32_t hashValue = jenkinsOneAtATime(key, keyLen);
    int index = hashValue % tableSize;

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
}


uint32_t search(uint8_t* key) {

    int keyLen = strlen((char*)key);
    uint32_t hashValue = jenkinsOneAtATime(key, keyLen);
    int index = hashValue % tableSize;

    hashRecord* current = concurrentHashTable[index];

    while (current != NULL) {

        if (current->hash == hashValue && strncmp((char*)current->name, (char*)key, MAX_LINE_LENGTH) == 0) {
            uint32_t salary = current->salary;
            return salary;
        }
        current = current->next;
    }

    // Key not found
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

int main() {

    concurrentHashTable = createTable();
    
    // Insert entries
    printf("Inserting entries...\n");
    insert((uint8_t*)"Alice", 50000);
    insert((uint8_t*)"Bob", 60000);
    insert((uint8_t*)"Charlie", 70000);

    
    // Search entries
    printf("\nSearching entries...\n");
    uint32_t salary = search((uint8_t*)"Alice");
    if (salary != 0) {
        printf("Found Alice with salary: %u\n", salary);
    }
    else {
        printf("No record found for Alice\n");
    }

    salary = search((uint8_t*)"Bob");
    if (salary != 0) {
        printf("Found Bob with salary: %u\n", salary);
    }
    else {
        printf("No record found for Bob\n");
    }

    salary = search((uint8_t*)"Eve");
    if (salary != 0) {
        printf("Found Eve with salary: %u\n", salary);
    }
    else {
        printf("No record found for Eve\n");
    }    

    // Delete an entry
    printf("\nDeleting entry for Bob...\n");
    delete((uint8_t*)"Bob");

    // Search after deletion
    printf("\nSearching for Bob after deletion...\n");
    salary = search((uint8_t*)"Bob");
    if (salary != 0) {
        printf("Found Bob with salary: %u\n", salary);
    }
    else {
        printf("No record found for Bob\n");
    }

    // Cleanup
    cleanupHashTable();
    printf("\nHash table cleanup complete.\n");
    

    return 0;
}