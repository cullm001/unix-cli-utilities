#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <pthread.h>
#define MAXCHAR 1000

void* fileWrite(void* vargp);


int main(int argc, char *argv[]) {
    if (argc == 1) {
        printf("Error: File argument not passed in\n");
        return 1;
    }

    int numCSV = 0;
    char bytes[10] = "";
    char outdir[30] = "./";
    int threads = 0;
    long long fileCap;

    for (int i = 1; i < argc; i++) {
        if (strstr(argv[i], "--bytes=") != NULL) {
            strncpy(bytes, argv[i] + 8, strlen(argv[i]) - 8);
            bytes[strlen(argv[i]) - 8] = '\0';
            fileCap = atoll(bytes);

            if (bytes[strlen(bytes) - 1] == 'G') {
                fileCap *= 1024 * 1024 * 1024;
            } else if (bytes[strlen(bytes) - 1] == 'M') {
                fileCap *= 1024 * 1024;
            } else if (bytes[strlen(bytes) - 1] == 'K') {
                fileCap *= 1024;
            } else if (bytes[strlen(bytes) - 1] != 'B') {
                printf("Error: incorrect file size format\n");
                return 1;
            }

            printf("File Cap: %lld\n", fileCap);
        }
        else if (strstr(argv[i], "--threads") != NULL) {
            threads = 1;
        } 
        else if (strstr(argv[i], "--outdir=") != NULL) {
            strncpy(outdir, argv[i] + 9, strlen(argv[i]) - 9);
            outdir[strlen(argv[i]) - 9] = '\0';
        } 
        else {
            numCSV++;
        }
    }

    printf("Number of Inputted files: %d\n", numCSV);

    char row[MAXCHAR];
    char header[MAXCHAR];

    FILE *stream = fopen(argv[1], "r");
    if (stream == NULL) {
        printf("Error: file %s NOT found\n", argv[1]);
        return 1;
    }
    fgets(header, MAXCHAR, stream);
    fclose(stream);

    for (int i = 2; i <= numCSV; i++) {
        stream = fopen(argv[i], "r");
        if (stream == NULL) {
            printf("Error: file %s not found\n", argv[i]);
            return 1;
        }
        fgets(row, MAXCHAR, stream);
        if (strcmp(header, row) != 0) {
            printf("Error: %s contains a different header schema\n", argv[i]);
            return 1;
        }
        fclose(stream);
    }

    if (strlen(bytes) == 0) {  
        printf("%s", header);
        for (int i = 1; i <= numCSV; i++) {
            stream = fopen(argv[i], "r");
            fgets(row, MAXCHAR, stream); 
            while (fgets(row, MAXCHAR, stream) != NULL) {
                printf("%s", row);
            }
            fclose(stream);
        }
    } else if (threads == 0) {
        printf("Break up into files\n");

        

        int currentSize = 0;
        int currentInputFile = 1;
        int currentOutputFile = 1;
        char fileName[100];

        FILE *ifstream = fopen(argv[currentInputFile], "r");
        if (ifstream == NULL) {
            printf("Error: file %s not found\n", argv[currentInputFile]);
            return 1;
        }

        sprintf(fileName, "%s/output_%d.csv", outdir, currentOutputFile);
        FILE *ofstream = fopen(fileName, "w");

        fprintf(ofstream, "%s", header); 

        while (true) {
            if (fgets(row, MAXCHAR, ifstream) == NULL) {
                fclose(ifstream);
                currentInputFile++;
                if (currentInputFile > numCSV) {
                    break;
                }
                ifstream = fopen(argv[currentInputFile], "r");
                if (ifstream == NULL) {
                    printf("Error: file %s not found\n", argv[currentInputFile]);
                    break;
                }
                fgets(row, MAXCHAR, ifstream);  
                continue;
            }

            int lineSize = strlen(row);

            if (lineSize + currentSize <= fileCap) {
                fprintf(ofstream, "%s", row);
                currentSize += lineSize;
            } else {
                fclose(ofstream);
                currentOutputFile++;
                sprintf(fileName, "%s/output_%d.csv", outdir, currentOutputFile);
                ofstream = fopen(fileName, "w");
                fprintf(ofstream, "%s", header); 
                currentSize = lineSize;
                fprintf(ofstream, "%s", row);
            }
        }

        fclose(ofstream);
    }

    else if (threads == 1){
        // printf("HELLO\n");
        //total size of a1l files
        long long totalSize = 0;
        FILE* fp ;
        struct stat st;
        for (int i = 1; i <= numCSV; i++){
            stat(argv[i], &st);
            totalSize += st.st_size;
            // printf("%lli\n", st.st_size);
        }



        fileCap = 60;
        totalSize = 180;

        int numThreads;
        if (totalSize % fileCap != 0){
            numThreads = totalSize/fileCap + 1;
        }
        else{
            numThreads = totalSize/fileCap;
        }
        printf("Number of threads:%i\n", numThreads);

        long long* startingBytes = malloc(numThreads * sizeof(long long));
        for (int i = 0; i < numThreads; i++) {
            startingBytes[i] = (long long)(fileCap*i);
            printf("Starting Byte: %lld\n", startingBytes[i]);
        }


        pthread_t threads[numThreads];
        for (int i = 0; i < numThreads; i++){
            long long* arg = malloc(sizeof(long long));
            *arg = startingBytes[i];
            pthread_create(&threads[i], NULL, fileWrite, (void*)arg);
        }

        for (int i = 0; i < numThreads; i++) {
            pthread_join(threads[i], NULL);
        }

        free(startingBytes);


        for (int i = 0; i < numThreads; i++) {
            pthread_join(threads[i], NULL);
            // Free allocated memory after thread is done
        }
        /*
        chunks = total size / bytes
        n threads
        where each thread starts
        total size/bytes * thread #

        bytes/total size * thread
        3/10
        
        */
        

    }

    return 0;
}


void* fileWrite(void* arg){
    long long value = *(long long*)arg;
    printf("Thread received value: %lli\n", value);
    return NULL;
}