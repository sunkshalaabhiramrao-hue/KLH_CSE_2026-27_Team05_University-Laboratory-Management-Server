#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <arpa/inet.h>
#include <time.h>
#include <dirent.h>
#include <sys/types.h>

#define PORT 8080

#define BUFFER_SIZE 1024
#define MAX_QUEUE_SIZE 20

#define WORKER_COUNT 3

#define TOTAL_COMPUTERS 3
#define TOTAL_PROGRAM_SLOTS 2
#define TOTAL_FILE_SLOTS 2

#define MAX_REQUEST_HISTORY 100

#define LOG_FILE "logs/server.log"


/* =========================================================
   RESOURCE TYPES
   ========================================================= */

typedef enum
{
    RESOURCE_COMPUTER,
    RESOURCE_PROGRAM,
    RESOURCE_FILE

} ResourceType;


/* =========================================================
   CLIENT REQUEST
   ========================================================= */

typedef struct
{
    int client_socket;
    int client_number;

} Request;


/* =========================================================
   RESOURCE ALLOCATION
   ========================================================= */

typedef struct
{
    int student_id;

    ResourceType resource_type;

    int resource_number;

    int active;

} Allocation;


/* =========================================================
   REQUEST HISTORY
   ========================================================= */

typedef struct
{
    int student_id;

    char action[20];

    char resource[20];

    char result[100];

    char time[30];

} RequestRecord;


/* =========================================================
   REQUEST QUEUE
   ========================================================= */

Request request_queue[MAX_QUEUE_SIZE];

int queue_front = 0;
int queue_rear = 0;
int queue_count = 0;

pthread_mutex_t queue_mutex =
    PTHREAD_MUTEX_INITIALIZER;

pthread_cond_t queue_condition =
    PTHREAD_COND_INITIALIZER;


/* =========================================================
   SEMAPHORES
   ========================================================= */

sem_t computer_semaphore;
sem_t program_semaphore;
sem_t file_semaphore;


/* =========================================================
   RESOURCE ALLOCATIONS
   ========================================================= */

Allocation computer_allocations[TOTAL_COMPUTERS];

Allocation program_allocations[TOTAL_PROGRAM_SLOTS];

Allocation file_allocations[TOTAL_FILE_SLOTS];

pthread_mutex_t resource_mutex =
    PTHREAD_MUTEX_INITIALIZER;


/* =========================================================
   REQUEST HISTORY
   ========================================================= */

RequestRecord request_history[MAX_REQUEST_HISTORY];

int request_history_count = 0;

pthread_mutex_t history_mutex =
    PTHREAD_MUTEX_INITIALIZER;


/* =========================================================
   LOGGING
   ========================================================= */

pthread_mutex_t log_mutex =
    PTHREAD_MUTEX_INITIALIZER;


/* =========================================================
   SERVER CONTROL
   ========================================================= */

volatile sig_atomic_t server_running = 1;


/* =========================================================
   LOG SERVER ACTIVITY
   ========================================================= */

void log_activity(
    const char *event,
    const char *message
)
{
    FILE *log_file;

    time_t current_time;

    struct tm *time_info;

    char time_string[30];


    pthread_mutex_lock(
        &log_mutex
    );


    log_file =
        fopen(
            LOG_FILE,
            "a"
        );


    if (log_file == NULL)
    {
        perror(
            "[LOG] Unable to open log file"
        );

        pthread_mutex_unlock(
            &log_mutex
        );

        return;
    }


    current_time =
        time(NULL);


    time_info =
        localtime(
            &current_time
        );


    strftime(
        time_string,
        sizeof(time_string),
        "%Y-%m-%d %H:%M:%S",
        time_info
    );


    fprintf(
        log_file,
        "%s | %-20s | %s\n",
        time_string,
        event,
        message
    );


    fflush(
        log_file
    );


    fclose(
        log_file
    );


    pthread_mutex_unlock(
        &log_mutex
    );
}


/* =========================================================
   RESOURCE NAME
   ========================================================= */

const char *resource_name(
    ResourceType type
)
{
    switch (type)
    {
        case RESOURCE_COMPUTER:
            return "COMPUTER";

        case RESOURCE_PROGRAM:
            return "PROGRAM";

        case RESOURCE_FILE:
            return "FILE";

        default:
            return "UNKNOWN";
    }
}


/* =========================================================
   GET SEMAPHORE
   ========================================================= */

sem_t *get_semaphore(
    ResourceType type
)
{
    switch (type)
    {
        case RESOURCE_COMPUTER:
            return &computer_semaphore;

        case RESOURCE_PROGRAM:
            return &program_semaphore;

        case RESOURCE_FILE:
            return &file_semaphore;

        default:
            return NULL;
    }
}


/* =========================================================
   GET ALLOCATION ARRAY
   ========================================================= */

Allocation *get_allocations(
    ResourceType type,
    int *total
)
{
    switch (type)
    {
        case RESOURCE_COMPUTER:

            *total =
                TOTAL_COMPUTERS;

            return computer_allocations;


        case RESOURCE_PROGRAM:

            *total =
                TOTAL_PROGRAM_SLOTS;

            return program_allocations;


        case RESOURCE_FILE:

            *total =
                TOTAL_FILE_SLOTS;

            return file_allocations;


        default:

            *total = 0;

            return NULL;
    }
}


/* =========================================================
   ADD REQUEST HISTORY
   ========================================================= */

void add_request_history(
    int student_id,
    const char *action,
    const char *resource,
    const char *result
)
{
    pthread_mutex_lock(
        &history_mutex
    );


    if (
        request_history_count <
        MAX_REQUEST_HISTORY
    )
    {
        RequestRecord *record =
            &request_history[
                request_history_count
            ];


        record->student_id =
            student_id;


        snprintf(
            record->action,
            sizeof(record->action),
            "%s",
            action
        );


        snprintf(
            record->resource,
            sizeof(record->resource),
            "%s",
            resource
        );


        snprintf(
            record->result,
            sizeof(record->result),
            "%s",
            result
        );


        time_t now =
            time(NULL);


        struct tm *current_time =
            localtime(
                &now
            );


        strftime(
            record->time,
            sizeof(record->time),
            "%Y-%m-%d %H:%M:%S",
            current_time
        );


        request_history_count++;
    }


    pthread_mutex_unlock(
        &history_mutex
    );
}


/* =========================================================
   ADD REQUEST TO QUEUE
   ========================================================= */

void enqueue_request(
    Request request
)
{
    pthread_mutex_lock(
        &queue_mutex
    );


    if (
        queue_count >=
        MAX_QUEUE_SIZE
    )
    {
        printf(
            "[QUEUE] Queue is full.\n"
        );


        log_activity(
            "QUEUE_FULL",
            "Request rejected because queue is full"
        );


        pthread_mutex_unlock(
            &queue_mutex
        );


        close(
            request.client_socket
        );


        return;
    }


    request_queue[
        queue_rear
    ] =
        request;


    queue_rear =
        (
            queue_rear + 1
        )
        % MAX_QUEUE_SIZE;


    queue_count++;


    printf(
        "[QUEUE] Client %d added to queue. "
        "Requests waiting: %d\n",
        request.client_number,
        queue_count
    );


    char message[100];


    snprintf(
        message,
        sizeof(message),
        "Client %d added to request queue",
        request.client_number
    );


    log_activity(
        "QUEUE_ADD",
        message
    );


    pthread_cond_signal(
        &queue_condition
    );


    pthread_mutex_unlock(
        &queue_mutex
    );
}


/* =========================================================
   REMOVE REQUEST FROM QUEUE
   ========================================================= */

Request dequeue_request(
    void
)
{
    Request request;


    pthread_mutex_lock(
        &queue_mutex
    );


    while (
        queue_count == 0 &&
        server_running
    )
    {
        pthread_cond_wait(
            &queue_condition,
            &queue_mutex
        );
    }


    if (
        !server_running &&
        queue_count == 0
    )
    {
        request.client_socket =
            -1;

        request.client_number =
            -1;


        pthread_mutex_unlock(
            &queue_mutex
        );


        return request;
    }


    request =
        request_queue[
            queue_front
        ];


    queue_front =
        (
            queue_front + 1
        )
        % MAX_QUEUE_SIZE;


    queue_count--;


    printf(
        "[QUEUE] Client %d removed from queue. "
        "Requests waiting: %d\n",
        request.client_number,
        queue_count
    );


    pthread_mutex_unlock(
        &queue_mutex
    );


    return request;
}


/* =========================================================
   ALLOCATE RESOURCE
   ========================================================= */

int allocate_resource(
    int student_id,
    ResourceType resource_type
)
{
    sem_t *semaphore =
        get_semaphore(
            resource_type
        );


    int total;


    Allocation *allocations =
        get_allocations(
            resource_type,
            &total
        );


    if (
        semaphore == NULL ||
        allocations == NULL
    )
    {
        return -1;
    }


    pthread_mutex_lock(
        &resource_mutex
    );


    for (
        int i = 0;
        i < total;
        i++
    )
    {
        if (
            allocations[i].active &&
            allocations[i].student_id ==
                student_id
        )
        {
            pthread_mutex_unlock(
                &resource_mutex
            );


            return -2;
        }
    }


    if (
        sem_trywait(
            semaphore
        ) != 0
    )
    {
        pthread_mutex_unlock(
            &resource_mutex
        );


        return 0;
    }


    for (
        int i = 0;
        i < total;
        i++
    )
    {
        if (
            !allocations[i].active
        )
        {
            allocations[i].student_id =
                student_id;


            allocations[i].resource_type =
                resource_type;


            allocations[i].resource_number =
                i + 1;


            allocations[i].active =
                1;


            printf(
                "[RESOURCE] Student %d "
                "allocated %s-%d\n",
                student_id,
                resource_name(
                    resource_type
                ),
                i + 1
            );


            char message[150];


            snprintf(
                message,
                sizeof(message),
                "Student %d allocated %s-%d",
                student_id,
                resource_name(
                    resource_type
                ),
                i + 1
            );


            log_activity(
                "RESOURCE_ALLOCATED",
                message
            );


            pthread_mutex_unlock(
                &resource_mutex
            );


            return i + 1;
        }
    }


    sem_post(
        semaphore
    );


    pthread_mutex_unlock(
        &resource_mutex
    );


    return -1;
}


/* =========================================================
   RELEASE RESOURCE
   ========================================================= */

int release_resource(
    int student_id,
    ResourceType resource_type
)
{
    sem_t *semaphore =
        get_semaphore(
            resource_type
        );


    int total;


    Allocation *allocations =
        get_allocations(
            resource_type,
            &total
        );


    if (
        semaphore == NULL ||
        allocations == NULL
    )
    {
        return -1;
    }


    pthread_mutex_lock(
        &resource_mutex
    );


    for (
        int i = 0;
        i < total;
        i++
    )
    {
        if (
            allocations[i].active &&
            allocations[i].student_id ==
                student_id
        )
        {
            int resource_number =
                allocations[i]
                    .resource_number;


            allocations[i].active =
                0;


            allocations[i].student_id =
                0;


            sem_post(
                semaphore
            );


            printf(
                "[RESOURCE] Student %d "
                "released %s-%d\n",
                student_id,
                resource_name(
                    resource_type
                ),
                resource_number
            );


            char message[150];


            snprintf(
                message,
                sizeof(message),
                "Student %d released %s-%d",
                student_id,
                resource_name(
                    resource_type
                ),
                resource_number
            );


            log_activity(
                "RESOURCE_RELEASED",
                message
            );


            pthread_mutex_unlock(
                &resource_mutex
            );


            return resource_number;
        }
    }


    pthread_mutex_unlock(
        &resource_mutex
    );


    return 0;
}


/* =========================================================
   RESOURCE STATUS
   ========================================================= */

void print_resource_status(
    void
)
{
    int computers;

    int programs;

    int files;


    sem_getvalue(
        &computer_semaphore,
        &computers
    );


    sem_getvalue(
        &program_semaphore,
        &programs
    );


    sem_getvalue(
        &file_semaphore,
        &files
    );


    printf(
        "\n========== RESOURCE STATUS ==========\n"
    );


    printf(
        "Computers     : %d/%d available\n",
        computers,
        TOTAL_COMPUTERS
    );


    printf(
        "Program Slots : %d/%d available\n",
        programs,
        TOTAL_PROGRAM_SLOTS
    );


    printf(
        "File Slots    : %d/%d available\n",
        files,
        TOTAL_FILE_SLOTS
    );


    printf(
        "=====================================\n\n"
    );
}


/* =========================================================
   STUDENT STATUS
   ========================================================= */

void print_student_status(
    int student_id,
    char *output,
    size_t output_size
)
{
    int position = 0;

    int found = 0;


    pthread_mutex_lock(
        &resource_mutex
    );


    position += snprintf(
        output + position,
        output_size - position,
        "Student %d resources:\n",
        student_id
    );


    for (
        int i = 0;
        i < TOTAL_COMPUTERS;
        i++
    )
    {
        if (
            computer_allocations[i].active &&
            computer_allocations[i].student_id ==
                student_id
        )
        {
            position += snprintf(
                output + position,
                output_size - position,
                "- COMPUTER-%d\n",
                computer_allocations[i]
                    .resource_number
            );


            found = 1;
        }
    }


    for (
        int i = 0;
        i < TOTAL_PROGRAM_SLOTS;
        i++
    )
    {
        if (
            program_allocations[i].active &&
            program_allocations[i].student_id ==
                student_id
        )
        {
            position += snprintf(
                output + position,
                output_size - position,
                "- PROGRAM-%d\n",
                program_allocations[i]
                    .resource_number
            );


            found = 1;
        }
    }


    for (
        int i = 0;
        i < TOTAL_FILE_SLOTS;
        i++
    )
    {
        if (
            file_allocations[i].active &&
            file_allocations[i].student_id ==
                student_id
        )
        {
            position += snprintf(
                output + position,
                output_size - position,
                "- FILE-%d\n",
                file_allocations[i]
                    .resource_number
            );


            found = 1;
        }
    }


    if (!found)
    {
        position += snprintf(
            output + position,
            output_size - position,
            "No resources allocated.\n"
        );
    }


    pthread_mutex_unlock(
        &resource_mutex
    );
}


/* =========================================================
   REQUEST HISTORY DISPLAY
   ========================================================= */

void print_request_history(
    void
)
{
    pthread_mutex_lock(
        &history_mutex
    );


    printf(
        "\n=========== REQUEST HISTORY "
        "===========\n"
    );


    if (
        request_history_count == 0
    )
    {
        printf(
            "No requests recorded.\n"
        );
    }


    for (
        int i = 0;
        i < request_history_count;
        i++
    )
    {
        RequestRecord *record =
            &request_history[i];


        printf(
            "Student: %d | Action: %s | "
            "Resource: %s | Result: %s | Time: %s\n",
            record->student_id,
            record->action,
            record->resource,
            record->result,
            record->time
        );
    }


    printf(
        "========================================\n\n"
    );


    pthread_mutex_unlock(
        &history_mutex
    );
}


/* =========================================================
   GET ACTIVE THREAD COUNT
   ========================================================= */

int get_active_thread_count(
    void
)
{
    DIR *directory;

    struct dirent *entry;

    int thread_count = 0;


    directory =
        opendir(
            "/proc/self/task"
        );


    if (
        directory == NULL
    )
    {
        return -1;
    }


    while (
        (entry = readdir(directory))
        != NULL
    )
    {
        if (
            entry->d_name[0] == '.'
        )
        {
            continue;
        }


        thread_count++;
    }


    closedir(
        directory
    );


    return thread_count;
}


/* =========================================================
   GENERATE MONITOR REPORT
   ========================================================= */

void generate_monitor_report(
    char *output,
    size_t output_size
)
{
    int computers;

    int programs;

    int files;

    int threads;

    int waiting_requests;


    sem_getvalue(
        &computer_semaphore,
        &computers
    );


    sem_getvalue(
        &program_semaphore,
        &programs
    );


    sem_getvalue(
        &file_semaphore,
        &files
    );


    threads =
        get_active_thread_count();


    pthread_mutex_lock(
        &queue_mutex
    );


    waiting_requests =
        queue_count;


    pthread_mutex_unlock(
        &queue_mutex
    );


    snprintf(
        output,
        output_size,

        "\n"
        "========== SERVER MONITOR ==========\n"
        "Server PID        : %d\n"
        "Active threads    : %d\n"
        "Worker threads    : %d\n"
        "Waiting requests  : %d\n"
        "\n"
        "RESOURCE USAGE\n"
        "Computers         : %d/%d available\n"
        "Program slots     : %d/%d available\n"
        "File slots        : %d/%d available\n"
        "====================================\n",

        getpid(),

        threads,

        WORKER_COUNT,

        waiting_requests,

        computers,
        TOTAL_COMPUTERS,

        programs,
        TOTAL_PROGRAM_SLOTS,

        files,
        TOTAL_FILE_SLOTS
    );
}


/* =========================================================
   PROCESS STUDENT REQUEST
   ========================================================= */

void process_request(
    int client_socket,
    char *request_text
)
{
    int student_id;

    char action[30];

    char resource_text[30];


    /* =====================================================
       MONITOR
       ===================================================== */

    if (
        strcmp(
            request_text,
            "MONITOR"
        ) == 0
    )
    {
        char monitor_report[
            BUFFER_SIZE
        ];


        generate_monitor_report(
            monitor_report,
            sizeof(monitor_report)
        );


        printf(
            "%s",
            monitor_report
        );


        log_activity(
            "MONITOR",
            "Server monitoring report requested"
        );


        send(
            client_socket,
            monitor_report,
            strlen(monitor_report),
            0
        );


        return;
    }


    /* =====================================================
       STATUS
       ===================================================== */

    if (
        strcmp(
            request_text,
            "STATUS"
        ) == 0
    )
    {
        print_resource_status();


        log_activity(
            "STATUS",
            "Resource status requested"
        );


        const char *response =
            "Resource status displayed "
            "on server.\n";


        send(
            client_socket,
            response,
            strlen(response),
            0
        );


        return;
    }


    /* =====================================================
       STUDENT STATUS
       ===================================================== */

    if (
        sscanf(
            request_text,
            "STUDENT %d",
            &student_id
        ) == 1
    )
    {
        char response[
            BUFFER_SIZE
        ];


        print_student_status(
            student_id,
            response,
            sizeof(response)
        );


        char message[100];


        snprintf(
            message,
            sizeof(message),
            "Student %d status requested",
            student_id
        );


        log_activity(
            "STUDENT_STATUS",
            message
        );


        send(
            client_socket,
            response,
            strlen(response),
            0
        );


        return;
    }


    /* =====================================================
       NORMAL REQUEST
       ===================================================== */

    int values =
        sscanf(
            request_text,
            "%d %29s %29s",
            &student_id,
            action,
            resource_text
        );


    if (
        values != 3
    )
    {
        const char *response =
            "Invalid command.\n"
            "\n"
            "Examples:\n"
            "101 REQUEST COMPUTER\n"
            "101 RELEASE COMPUTER\n"
            "101 REQUEST PROGRAM\n"
            "101 RELEASE PROGRAM\n"
            "101 REQUEST FILE\n"
            "101 RELEASE FILE\n"
            "STATUS\n"
            "MONITOR\n"
            "STUDENT 101\n";


        log_activity(
            "INVALID_REQUEST",
            "Client sent an invalid command"
        );


        send(
            client_socket,
            response,
            strlen(response),
            0
        );


        return;
    }


    /* =====================================================
       DETERMINE RESOURCE
       ===================================================== */

    ResourceType resource_type;


    if (
        strcmp(
            resource_text,
            "COMPUTER"
        ) == 0
    )
    {
        resource_type =
            RESOURCE_COMPUTER;
    }
    else if (
        strcmp(
            resource_text,
            "PROGRAM"
        ) == 0
    )
    {
        resource_type =
            RESOURCE_PROGRAM;
    }
    else if (
        strcmp(
            resource_text,
            "FILE"
        ) == 0
    )
    {
        resource_type =
            RESOURCE_FILE;
    }
    else
    {
        const char *response =
            "Unknown resource.\n"
            "Use COMPUTER, PROGRAM, or FILE.\n";


        log_activity(
            "INVALID_RESOURCE",
            resource_text
        );


        send(
            client_socket,
            response,
            strlen(response),
            0
        );


        return;
    }


    /* =====================================================
       REQUEST RESOURCE
       ===================================================== */

    if (
        strcmp(
            action,
            "REQUEST"
        ) == 0
    )
    {
        char request_message[150];


        snprintf(
            request_message,
            sizeof(request_message),
            "Student %d requested %s",
            student_id,
            resource_name(
                resource_type
            )
        );


        log_activity(
            "RESOURCE_REQUEST",
            request_message
        );


        int resource_number =
            allocate_resource(
                student_id,
                resource_type
            );


        char response[
            BUFFER_SIZE
        ];


        if (
            resource_number > 0
        )
        {
            snprintf(
                response,
                sizeof(response),
                "Student %d allocated %s-%d successfully.\n",
                student_id,
                resource_name(
                    resource_type
                ),
                resource_number
            );


            add_request_history(
                student_id,
                "REQUEST",
                resource_name(
                    resource_type
                ),
                "ALLOCATED"
            );
        }
        else if (
            resource_number == 0
        )
        {
            snprintf(
                response,
                sizeof(response),
                "No %s resource is currently available.\n",
                resource_name(
                    resource_type
                )
            );


            add_request_history(
                student_id,
                "REQUEST",
                resource_name(
                    resource_type
                ),
                "NOT AVAILABLE"
            );


            char unavailable_message[150];


            snprintf(
                unavailable_message,
                sizeof(unavailable_message),
                "No %s available for student %d",
                resource_name(
                    resource_type
                ),
                student_id
            );


            log_activity(
                "RESOURCE_UNAVAILABLE",
                unavailable_message
            );
        }
        else if (
            resource_number == -2
        )
        {
            snprintf(
                response,
                sizeof(response),
                "Student %d already has a %s resource.\n",
                student_id,
                resource_name(
                    resource_type
                )
            );


            add_request_history(
                student_id,
                "REQUEST",
                resource_name(
                    resource_type
                ),
                "ALREADY ALLOCATED"
            );
        }
        else
        {
            snprintf(
                response,
                sizeof(response),
                "Unable to allocate resource.\n"
            );


            add_request_history(
                student_id,
                "REQUEST",
                resource_name(
                    resource_type
                ),
                "FAILED"
            );


            log_activity(
                "ALLOCATION_FAILED",
                "Resource allocation failed"
            );
        }


        send(
            client_socket,
            response,
            strlen(response),
            0
        );


        return;
    }


    /* =====================================================
       RELEASE RESOURCE
       ===================================================== */

    if (
        strcmp(
            action,
            "RELEASE"
        ) == 0
    )
    {
        int resource_number =
            release_resource(
                student_id,
                resource_type
            );


        char response[
            BUFFER_SIZE
        ];


        if (
            resource_number > 0
        )
        {
            snprintf(
                response,
                sizeof(response),
                "Student %d released %s-%d successfully.\n",
                student_id,
                resource_name(
                    resource_type
                ),
                resource_number
            );


            add_request_history(
                student_id,
                "RELEASE",
                resource_name(
                    resource_type
                ),
                "RELEASED"
            );
        }
        else
        {
            snprintf(
                response,
                sizeof(response),
                "Student %d does not have a %s resource allocated.\n",
                student_id,
                resource_name(
                    resource_type
                )
            );


            add_request_history(
                student_id,
                "RELEASE",
                resource_name(
                    resource_type
                ),
                "NOT ALLOCATED"
            );


            char message[150];


            snprintf(
                message,
                sizeof(message),
                "Student %d attempted to release unallocated %s",
                student_id,
                resource_name(
                    resource_type
                )
            );


            log_activity(
                "RELEASE_FAILED",
                message
            );
        }


        send(
            client_socket,
            response,
            strlen(response),
            0
        );


        return;
    }


    /* =====================================================
       UNKNOWN ACTION
       ===================================================== */

    const char *response =
        "Unknown action.\n"
        "Use REQUEST or RELEASE.\n";


    log_activity(
        "UNKNOWN_ACTION",
        action
    );


    send(
        client_socket,
        response,
        strlen(response),
        0
    );
}


/* =========================================================
   WORKER THREAD
   ========================================================= */

void *worker_function(
    void *argument
)
{
    int worker_id =
        *((int *)argument);


    free(
        argument
    );


    printf(
        "[WORKER %d] Worker thread started.\n",
        worker_id
    );


    char worker_message[100];


    snprintf(
        worker_message,
        sizeof(worker_message),
        "Worker thread %d started",
        worker_id
    );


    log_activity(
        "WORKER_STARTED",
        worker_message
    );


    while (
        server_running
    )
    {
        Request request =
            dequeue_request();


        if (
            request.client_socket == -1
        )
        {
            break;
        }


        char buffer[
            BUFFER_SIZE
        ];


        memset(
            buffer,
            0,
            sizeof(buffer)
        );


        ssize_t bytes_received =
            recv(
                request.client_socket,
                buffer,
                sizeof(buffer) - 1,
                0
            );


        if (
            bytes_received <= 0
        )
        {
            close(
                request.client_socket
            );


            continue;
        }


        buffer[
            strcspn(
                buffer,
                "\r\n"
            )
        ] =
            '\0';


        printf(
            "[WORKER %d] Client %d request: %s\n",
            worker_id,
            request.client_number,
            buffer
        );


        process_request(
            request.client_socket,
            buffer
        );


        close(
            request.client_socket
        );


        printf(
            "[WORKER %d] Client %d completed.\n",
            worker_id,
            request.client_number
        );
    }


    printf(
        "[WORKER %d] Worker thread stopped.\n",
        worker_id
    );


    snprintf(
        worker_message,
        sizeof(worker_message),
        "Worker thread %d stopped",
        worker_id
    );


    log_activity(
        "WORKER_STOPPED",
        worker_message
    );


    return NULL;
}


/* =========================================================
   SIGNAL HANDLER
   ========================================================= */

void handle_signal(
    int signal_number
)
{
    if (
        signal_number == SIGINT
    )
    {
        printf(
            "\n[SIGNAL] SIGINT received.\n"
        );


        printf(
            "[SERVER] Starting controlled shutdown...\n"
        );


        log_activity(
            "SHUTDOWN",
            "SIGINT received - controlled shutdown started"
        );


        server_running =
            0;


        pthread_cond_broadcast(
            &queue_condition
        );
    }
}


/* =========================================================
   MAIN
   ========================================================= */

int main(void)
{
    int server_socket;


    struct sockaddr_in server_address;


    /* =====================================================
       SIGNAL
       ===================================================== */

    signal(
        SIGINT,
        handle_signal
    );


    /* =====================================================
       SEMAPHORES
       ===================================================== */

    if (
        sem_init(
            &computer_semaphore,
            0,
            TOTAL_COMPUTERS
        ) != 0
    )
    {
        perror(
            "sem_init computer"
        );


        return 1;
    }


    if (
        sem_init(
            &program_semaphore,
            0,
            TOTAL_PROGRAM_SLOTS
        ) != 0
    )
    {
        perror(
            "sem_init program"
        );


        return 1;
    }


    if (
        sem_init(
            &file_semaphore,
            0,
            TOTAL_FILE_SLOTS
        ) != 0
    )
    {
        perror(
            "sem_init file"
        );


        return 1;
    }


    /* =====================================================
       SOCKET
       ===================================================== */

    server_socket =
        socket(
            AF_INET,
            SOCK_STREAM,
            0
        );


    if (
        server_socket == -1
    )
    {
        perror(
            "socket"
        );


        return 1;
    }


    /* =====================================================
       SOCKET REUSE
       ===================================================== */

    int reuse = 1;


    if (
        setsockopt(
            server_socket,
            SOL_SOCKET,
            SO_REUSEADDR,
            &reuse,
            sizeof(reuse)
        ) == -1
    )
    {
        perror(
            "setsockopt"
        );


        close(
            server_socket
        );


        return 1;
    }


    /* =====================================================
       ADDRESS
       ===================================================== */

    memset(
        &server_address,
        0,
        sizeof(server_address)
    );


    server_address.sin_family =
        AF_INET;


    server_address.sin_addr.s_addr =
        INADDR_ANY;


    server_address.sin_port =
        htons(PORT);


    /* =====================================================
       BIND
       ===================================================== */

    if (
        bind(
            server_socket,
            (struct sockaddr *)&server_address,
            sizeof(server_address)
        ) == -1
    )
    {
        perror(
            "bind"
        );


        close(
            server_socket
        );


        return 1;
    }


    /* =====================================================
       LISTEN
       ===================================================== */

    if (
        listen(
            server_socket,
            MAX_QUEUE_SIZE
        ) == -1
    )
    {
        perror(
            "listen"
        );


        close(
            server_socket
        );


        return 1;
    }


    /* =====================================================
       SERVER INFORMATION
       ===================================================== */

    printf(
        "\n"
        "============================================\n"
        " University Laboratory Management Server\n"
        "============================================\n"
    );


    printf(
        "Server listening on port %d\n",
        PORT
    );


    printf(
        "Server PID: %d\n",
        getpid()
    );


    printf(
        "Worker threads: %d\n",
        WORKER_COUNT
    );


    printf(
        "Computers: %d\n",
        TOTAL_COMPUTERS
    );


    printf(
        "Program slots: %d\n",
        TOTAL_PROGRAM_SLOTS
    );


    printf(
        "File slots: %d\n",
        TOTAL_FILE_SLOTS
    );


    printf(
        "Request history: enabled\n"
    );


    printf(
        "Process/thread monitoring: enabled\n"
    );


    printf(
        "Activity logging: enabled\n"
    );


    printf(
        "Log file: %s\n",
        LOG_FILE
    );


    printf(
        "Press Ctrl+C for controlled shutdown.\n"
    );


    printf(
        "\nWaiting for student clients...\n\n"
    );


    log_activity(
        "SERVER_STARTED",
        "University Laboratory Management Server started"
    );


    /* =====================================================
       WORKER THREADS
       ===================================================== */

    for (
        int i = 0;
        i < WORKER_COUNT;
        i++
    )
    {
        pthread_t worker_thread;


        int *worker_id =
            malloc(
                sizeof(int)
            );


        if (
            worker_id == NULL
        )
        {
            perror(
                "malloc"
            );


            return 1;
        }


        *worker_id =
            i + 1;


        if (
            pthread_create(
                &worker_thread,
                NULL,
                worker_function,
                worker_id
            ) != 0
        )
        {
            perror(
                "pthread_create"
            );


            free(
                worker_id
            );


            return 1;
        }


        pthread_detach(
            worker_thread
        );
    }


    /* =====================================================
       ACCEPT CLIENTS
       ===================================================== */

    int client_number = 0;


    while (
        server_running
    )
    {
        struct sockaddr_in client_address;


        socklen_t client_address_length =
            sizeof(client_address);


        int client_socket =
            accept(
                server_socket,
                (struct sockaddr *)&client_address,
                &client_address_length
            );


        if (
            client_socket == -1
        )
        {
            if (
                !server_running
            )
            {
                break;
            }


            perror(
                "accept"
            );


            continue;
        }


        client_number++;


        printf(
            "[SERVER] Client %d connected.\n",
            client_number
        );


        char client_message[100];


        snprintf(
            client_message,
            sizeof(client_message),
            "Client %d connected",
            client_number
        );


        log_activity(
            "CLIENT_CONNECTED",
            client_message
        );


        Request request;


        request.client_socket =
            client_socket;


        request.client_number =
            client_number;


        enqueue_request(
            request
        );
    }


    /* =====================================================
       CONTROLLED SHUTDOWN
       ===================================================== */

    printf(
        "\n[SERVER] Closing server socket...\n"
    );


    close(
        server_socket
    );


    pthread_cond_broadcast(
        &queue_condition
    );


    log_activity(
        "SERVER_STOPPED",
        "Server socket closed and server shutdown completed"
    );


    sem_destroy(
        &computer_semaphore
    );


    sem_destroy(
        &program_semaphore
    );


    sem_destroy(
        &file_semaphore
    );


    printf(
        "[SERVER] Server shutdown complete.\n"
    );


    return 0;
}
