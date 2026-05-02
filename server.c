#include "segel.h"
#include "request.h"
#include "log.h"


typedef struct Request{
    int request_id;
    time_stats times;
} Request;

typedef struct QueueRequest {
    Request* queue;
    int front;
    int rear;
    int count;
    int MAX_QUEUE_SIZE;
} *QueueRequest;

// Global variables to be accessable to all threads

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_work = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond_not_full = PTHREAD_COND_INITIALIZER;

server_log global_log; // Global server log
QueueRequest request_queue; // Global request queue + initialization
int p_count; // Global counter for requests that still processed
int Listenfd; // Global listen file descriptor
threads_stats* thread_stats_array; // Array to hold thread stats
int thread_pool_size; // Number of threads in the pool
pthread_t* thread_pool; // Thread pool array

QueueRequest create_queue(int max_size) {
    QueueRequest q = malloc(sizeof(struct QueueRequest));
    if(q == NULL) {
        app_error("Memory allocation for queue structure failed");
    }
    q->queue = malloc(sizeof(Request) * max_size);
    if(q->queue == NULL) {
        free(q); // Free the queue structure if allocation fails
        app_error("Memory allocation for queue failed");
    }
    q->front = 0;
    q->rear = 0;
    q->count = 0;
    q->MAX_QUEUE_SIZE = max_size;
    return q;
}


// --------------- Queue Functions --------------- //

void enqueue(int client_fd, struct timeval arrival_time) {
    request_queue->queue[request_queue->rear].request_id = client_fd; // Assuming client_fd is used as request_id
    //request_queue->queue[request_queue->rear].arrival_time.tv_sec = arrival_time.tv_sec;
    //request_queue->queue[request_queue->rear].arrival_time.tv_usec = arrival_time.tv_usec;
    request_queue->queue[request_queue->rear].times.task_arrival.tv_sec = arrival_time.tv_sec;
    request_queue->queue[request_queue->rear].times.task_arrival.tv_usec = arrival_time.tv_usec;
    request_queue->rear = (request_queue->rear + 1) % request_queue->MAX_QUEUE_SIZE;
    request_queue->count++;
}

Request* dequeue() {
    Request* req = malloc(sizeof(Request));
    if(req == NULL) {
        app_error("Memory allocation for request failed");
    }
    req->request_id = request_queue->queue[request_queue->front].request_id;
    req->times.task_arrival = request_queue->queue[request_queue->front].times.task_arrival;
    gettimeofday(&req->times.task_dispatch, NULL); // Get the current time for dispatch
    request_queue->front = (request_queue->front + 1) % request_queue->MAX_QUEUE_SIZE;
    request_queue->count--;
    return req;
}



//
// server.c: A very, very simple web server
//
// To run:
//  ./server <portnum (above 2000)>
//
// Repeatedly handles HTTP requests sent to this port number.
// Most of the work is done within routines written in request.c
//

// Parses command-line arguments
void getargs(int *port, int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr, "Usage: %s <port>\n", argv[0]);
        exit(1);
    }
    *port = atoi(argv[1]);
}

void* worker_thread(void *arg) {
    // create thread stats structure for this thread + initialize it
    threads_stats ts = (threads_stats)arg;

    ts->stat_req = 0;
    ts->dynm_req = 0;
    ts->post_req = 0;
    ts->total_req = 0;

    while (1) {
        pthread_mutex_lock(&mutex);
        while (request_queue->count == 0){
            pthread_cond_wait(&cond_work, &mutex);
        }
        p_count++; // Increment the global processing request counter
        Request* req = dequeue();
        pthread_mutex_unlock(&mutex);

        // Initialize log stats to prevent garbage values if request fails early
        req->times.log_enter.tv_sec = 0;
        req->times.log_enter.tv_usec = 0;
        req->times.log_exit.tv_sec = 0;
        req->times.log_exit.tv_usec = 0;

        
        requestHandle(req->request_id, req->times, ts, global_log);
        Close(req->request_id); // Close the connection
        free(req); // Free the request structure
        pthread_mutex_lock(&mutex);
        p_count--;
        pthread_cond_signal(&cond_not_full); // Signal that the queue is not full anymore
        pthread_mutex_unlock(&mutex);
    }
    free(ts); // Free the dynamically allocated memory for thread stats
    return NULL;
}

void cleanUp(){
    // Clean up the request queue
    free(request_queue->queue);
    request_queue->queue = NULL;
    request_queue->count = 0;
    request_queue->front = 0;
    request_queue->rear = 0;
    free(request_queue); // Free the queue structure
    request_queue = NULL;

    // Clean up the thread stats array
    for (int i = 0; i < thread_pool_size ; i++) {
        if(thread_stats_array[i] != NULL){
            free(thread_stats_array[i]);
            thread_stats_array[i] = NULL;
        }
    }
    free(thread_stats_array);
    thread_stats_array = NULL;
    
    // Clean up the thread pool
    if (thread_pool != NULL) {
        free(thread_pool);
        thread_pool = NULL;
    }

    // Destroy mutex and condition variables
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&cond_work);
    pthread_cond_destroy(&cond_not_full);

}

void ctrlCHandler(int sig_num) {
    printf("got ctrl-C\n");
    Close(Listenfd); // Close the listening socket
    destroy_log(global_log); // Clean up the server log
    cleanUp(); // Clean up the request queue and mutexes
    _exit(0);
}



// TODO: HW3 — Initialize thread pool and request queue
// This server currently handles all requests in the main thread.
// You must implement a thread pool (fixed number of worker threads)
// that process requests from a synchronized queue.

int main(int argc, char *argv[])
{
    // Create the global server log
    global_log = create_log();

    p_count = 0; // Initialize the global processing request counter


    int listenfd, connfd, port, clientlen;
    struct sockaddr_in clientaddr;

    getargs(&port, argc, argv);

    // Initialize the request queue
    thread_pool_size = atoi(argv[2]);
    int max_queue_size = atoi(argv[3]);
    int debug_sleep_time = atoi(argv[4]); // 2026 new feature

    if(debug_sleep_time > 0) global_log->debug_sleep_time = debug_sleep_time; // 2026 new feature
    // check later - check the debug

    if(thread_pool_size <= 0 || max_queue_size <= 0) {
        app_error("Thread pool size and max queue size must be positive integers");
    }

    request_queue = create_queue(max_queue_size);

    // Initialize the thread stats array
    thread_stats_array = malloc(sizeof(threads_stats) * thread_pool_size);
    if (thread_stats_array == NULL) {
        app_error("Memory allocation for thread_stats_array failed");
    }

    // Create a thread pool
    pthread_t* threads = malloc(sizeof(pthread_t) * thread_pool_size);
    if (threads == NULL) {
        app_error("Memory allocation for threads failed");
    }

    for(int i = 0; i < thread_pool_size; i++) {
        threads_stats ts = malloc(sizeof(struct Threads_stats));
        if (ts == NULL) {
            app_error("Memory allocation for thread_stats failed");
        }
        thread_stats_array[i] = ts; // Store the thread stats in the global array
        ts->id = i + 1; // Set thread ID
        int rc = pthread_create(&threads[i], NULL, worker_thread, ts);
        if(rc != 0){
            posix_error(rc, "pthread_create error");
        }
    }


    listenfd = Open_listenfd(port);

    Listenfd = listenfd; // Store the listen file descriptor globally

    while (1) {
        //clientlen = sizeof(clientaddr);
        //connfd = Accept(listenfd, (SA *)&clientaddr, (socklen_t *) &clientlen);
        struct timeval arrival;
        // here we will to wait on cond_not_full if the queue is full
        pthread_mutex_lock(&mutex);
        while (request_queue->count + p_count >= max_queue_size) {
            pthread_cond_wait(&cond_not_full, &mutex);
        }
        pthread_mutex_unlock(&mutex); // 2026

        clientlen = sizeof(clientaddr);
        connfd = Accept(listenfd, (SA *)&clientaddr, (socklen_t *) &clientlen);
        gettimeofday(&arrival, NULL); // Record the request arrival time
        pthread_mutex_lock(&mutex);
        enqueue(connfd, arrival);
        pthread_cond_signal(&cond_work);
        pthread_mutex_unlock(&mutex);
    }

    // Clean up the server log before exiting
    destroy_log(global_log);

    cleanUp();
    // TODO: HW3 — Add cleanup code for thread pool and queue
}
