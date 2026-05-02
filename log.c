#include <stdlib.h>
#include <string.h>
#include "log.h"
#include <pthread.h>
#include <stdio.h>


// MAKE SURE WHEN PASSING LOG AS AN ARGUMENT ALL FUNCTION ARE LOCKING AND UNLOCKING THE SAME LOG!!

// There is no problem of "putting the lock in the safe" becuse all updates of the log 
// are done by the functions below and they hold the key before uptading and reading




// Creates a new server log instance (stub)
server_log create_log() {
    server_log log = malloc(sizeof(struct Server_Log));
    if (!log) {
        app_error("Memory allocation for server log failed");
    }
    log->capacity = 1024;
    log->length = 0;
    log->debug_sleep_time = 0; // 2026 new feature
    log->buffer = malloc(sizeof(char) * log->capacity);
    if (!log->buffer) {
        free(log);
        app_error("Memory allocation for log buffer failed");
    }
    readers_writers_init(log);
    return log;
}

// Destroys and frees the log (stub)
void destroy_log(server_log log) {
    if (!log) return;
    free(log->buffer);
    pthread_mutex_destroy(&log->log_lock);        // The destroy functions the chat suggested, i didnt find them in the tirgul
    pthread_cond_destroy(&log->read_allowed);
    pthread_cond_destroy(&log->write_allowed);
    free(log);
}

int get_log(server_log log, char** dst){
    reader_lock(log);
    if (log->debug_sleep_time > 0) sleep(log->debug_sleep_time); // 2026 new feature
    *dst = malloc(sizeof(char) * (log->length + 1));
    if(*dst == NULL){
        reader_unlock(log);
        app_error("Failed to allocate memory for log content");
    }
    memcpy(*dst, log->buffer, log->length);
    (*dst)[log->length] = '\0';
    int len = log->length;
    reader_unlock(log);
    return len;
}

// Appends a new entry to the log (no-op stub)
void add_to_log(server_log log, const char* data, int data_len) {
    // writer_lock(log);

    // if (log->debug_sleep_time > 0) sleep(log->debug_sleep_time); // 2026 new feature

    if(log->length + data_len >= log->capacity){
        int new_capacity = (log->length + data_len) * 2;
        char* new_buf = realloc(log->buffer, new_capacity);
        if(!new_buf){
            // writer_unlock(log);
            app_error("Memory allocation for log buffer failed");
        }
        log->buffer = new_buf;
        log->capacity = new_capacity;
    }
    memcpy(log->buffer + log->length, data, data_len);
    log->length += data_len;
    log->buffer[log->length] = '\n';
    log->length++; // increment length for the newline character
    log->buffer[log->length] = '\0'; // null-terminate the string
    
    // writer_unlock(log);
}


/*********************** Helper Functions ************************/

void readers_writers_init(server_log log){
    pthread_mutex_init(&log->log_lock, NULL);
    pthread_cond_init(&log->read_allowed, NULL);
    pthread_cond_init(&log->write_allowed, NULL);
    log->readers_inside = 0;
    log->writers_inside = 0;
    log->writers_waiting = 0;
}

void reader_lock(server_log log){
    pthread_mutex_lock(&log->log_lock);
    while(log->writers_inside > 0 || log->writers_waiting > 0){
        pthread_cond_wait(&log->read_allowed, &log->log_lock);
    }
    log->readers_inside++;
    pthread_mutex_unlock(&log->log_lock);    // Once I increment readers_inside++ the writers won't write so its "locked" for the writers, but I want to let other readers in so we perform mutex_unlock(&log->log_lock)
}

void reader_unlock(server_log log){
    pthread_mutex_lock(&log->log_lock);
    log->readers_inside--;
    if(log->readers_inside == 0){
        pthread_cond_signal(&log->write_allowed); 
    }
    pthread_mutex_unlock(&log->log_lock);    
}

void writer_lock(server_log log){
    pthread_mutex_lock(&log->log_lock);
    log->writers_waiting++;
    while(log->writers_inside + log->readers_inside > 0){
        pthread_cond_wait(&log->write_allowed, &log->log_lock);
    }
    log->writers_waiting--;
    log->writers_inside++;
    pthread_mutex_unlock(&log->log_lock);
}

void writer_unlock(server_log log){
    pthread_mutex_lock(&log->log_lock);
    log->writers_inside--;
    if(log->writers_inside == 0){
        pthread_cond_broadcast(&log->read_allowed);
        pthread_cond_signal(&log->write_allowed);
    }
    pthread_mutex_unlock(&log->log_lock);  
}
