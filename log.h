#ifndef SERVER_LOG_H
#define SERVER_LOG_H

// TODO:
// Implement a thread-safe server log system.
// - The log should support concurrent access from multiple threads.
// - You must implement a multiple-readers/single-writer synchronization model.
// - Writers must have priority over readers.
//   This means that if a writer is waiting, new readers should be blocked until the writer is done.
// - Use appropriate synchronization primitives (e.g., pthread mutexes and condition variables).
// - The log should allow appending entries and returning the full log content.

//typedef struct Server_Log* server_log;

typedef struct Server_Log {
    char* buffer;
    int capacity;
    int length;

    pthread_mutex_t log_lock;     // protects access to the log
    pthread_cond_t read_allowed;  // for readers to wait
    pthread_cond_t write_allowed; // for writers to wait

    int readers_inside;           // number of active readers
    int writers_inside;           // number of active writers 
    int writers_waiting;          // number of waiting writers (for priority)

    int debug_sleep_time; // 2026 new feature
}* server_log; 


// Creates a new server log instance
server_log create_log();

// Destroys and frees the log
void destroy_log(server_log log);

// Returns the log contents as a string (null-terminated)
// NOTE: caller is responsible for freeing dst
int get_log(server_log log, char** dst);

// Appends a new entry to the log
void add_to_log(server_log log, const char* data, int data_len);


// Reader-writer synchronization helper functions
void readers_writers_init(server_log log);
void reader_lock(server_log log);
void reader_unlock(server_log log);
void writer_lock(server_log log);
void writer_unlock(server_log log);


#endif // SERVER_LOG_H
