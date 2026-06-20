#define _GNU_SOURCE  
#include <stdio.h>
#include <math.h>
#include <stdint.h>
#include <sched.h>
#include <string.h> 
#include <errno.h>  
#include <stdlib.h>
#include <sys/mman.h>
#include <mqueue.h>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>

/* MACRO DEFINITIONS */
//#define AFFINITY_SET

#define MSG_HEADER          (0xA5)
#define MAX_PAYLOAD_SIZE    (10000U)
#define WARM_UP             (1000U)

#define HANDSHAKE_MSG           ("SEND ME ANOTHER MESSAGE")
#define DATA_MSQ_NAME           "/data_queue_name"
#define ACK_MSQ_NAME            "/ack_queue_name"

/* TYPE DECLARATIONS */
#pragma pack(push, 1)
typedef struct tag_MSG{
    uint8_t header;
    uint32_t sequence;
    uint64_t time_ns;
    uint32_t payload_size;
}message_header_t;
#pragma pack(pop)

#pragma pack(push, 1)
typedef struct {
    message_header_t message_header;
    uint8_t payload[]; 
} full_message_t;
#pragma pack(pop)

/* FUNCTION PROTOTYPES */
static void producer(void);
static void consumer(void);
static int parse_command_line_arguments(int argc, char* argv[]);
static long double standard_deviation(uint64_t *arr, int n);
static double calculate_median(uint64_t *data, size_t n);
static void exit_sys(const char *msg);

/* GLOBAL VARIABLES */
pid_t pid;
mqd_t data_queue_handle, ack_queue_handle;
struct mq_attr data_queue_attr, ack_queue_attr;
uint32_t number_of_message;
uint32_t payload_size;
struct sched_param consumer_param;
struct sched_param producer_param;

/* Usage: ./main <number_of_messages> <payload_size> <consumer_priority> <producer_priority>*/
int main(int argc, char* argv[])
{
    if(parse_command_line_arguments(argc, argv) == -1)
        return -1;

    mq_unlink(DATA_MSQ_NAME);
    mq_unlink(ACK_MSQ_NAME);

    /* DATA QUEUE PROPERTIES, ONE MESSAGE SIZE and MAX MESSAGE NUMBER IN QUEUE */
    data_queue_attr.mq_maxmsg = 10;
    data_queue_attr.mq_msgsize = payload_size + sizeof(message_header_t);

    /* ACK QUEUE PROPERTIES, ONE MESSAGE SIZE and MAX MESSAGE NUMBER IN QUEUE */
    ack_queue_attr.mq_maxmsg = 10;
    ack_queue_attr.mq_msgsize = sizeof(HANDSHAKE_MSG);

    /* DATA QUQUE FOR COMMUNICATION BETWEEN PRODUCER AND CONSUMER */
    if((data_queue_handle = mq_open(DATA_MSQ_NAME, O_RDWR | O_CREAT | O_EXCL, S_IRWXU | S_IRGRP, &data_queue_attr)) == -1)
    {
        exit_sys("MQ_OPEN FOR DATA UNSUCCESSFUL");
    }

    /* HANDSHAKE QUQUE FOR COMMUNICATION BETWEEN PRODUCER AND CONSUMER */
    if((ack_queue_handle = mq_open(ACK_MSQ_NAME, O_RDWR | O_CREAT | O_EXCL, S_IRWXU | S_IRGRP, &ack_queue_attr)) == -1)
    {
        exit_sys("MQ_OPEN FOR ACK UNSUCCESSFUL");
    }

    if ((pid = fork()) == -1) 
        exit_sys("FORK UNSUCCESSFUL COMPLETION");

    if(pid != 0) /* Parent Process */
    {
        #ifdef AFFINITY_SET
        cpu_set_t producer_cpu_set;     /* Linux Specific, not portable */
        CPU_ZERO(&producer_cpu_set);    /* Linux Specific, not portable */
	    CPU_SET(1, &producer_cpu_set);  /* Linux Specific, not portable */

        if (sched_setaffinity(0, sizeof(producer_cpu_set), &producer_cpu_set) == -1) /* Linux Specific, not portable */
		    exit_sys("SCHED_AFFINITY UNSUCCESSFUL COMPLETION FOR PRODUCER");

        printf("#CPU AFFINITY IS AVAILABLE\n");
        #endif

        if (mlockall(MCL_CURRENT | MCL_FUTURE) == -1) 
            exit_sys("mlockall producer");

        if (sched_setscheduler(0, SCHED_FIFO, &producer_param) == -1)
             exit_sys("sched_setscheduler producer");

        producer();
        waitpid(pid, NULL, 0);
        mq_close(data_queue_handle);
        mq_close(ack_queue_handle);
        
    }
    else /* Child Process */
    {
        #ifdef AFFINITY_SET
        cpu_set_t consumer_cpu_set;    /* Linux Specific, not portable */
        CPU_ZERO(&consumer_cpu_set);   /* Linux Specific, not portable */
	    CPU_SET(2, &consumer_cpu_set); /* Linux Specific, not portable */

        if (sched_setaffinity(0, sizeof(consumer_cpu_set), &consumer_cpu_set) == -1)  /* Linux Specific, not portable */
		    exit_sys("SCHED_AFFINITY UNSUCCESSFUL COMPLETION FOR CONSUMER");
        #endif

        if (mlockall(MCL_CURRENT | MCL_FUTURE) == -1) 
            exit_sys("mlockall consumer");
        
        if (sched_setscheduler(0, SCHED_FIFO, &consumer_param) == -1)
             exit_sys("sched_setscheduler consumer");
             
        consumer();
        mq_close(data_queue_handle);
        mq_close(ack_queue_handle);
        mq_unlink(DATA_MSQ_NAME);
        mq_unlink(ACK_MSQ_NAME);
    }

    return 0;

}

void producer(void)
{
    char handshake_arr[sizeof(HANDSHAKE_MSG)] = {0};

    struct timespec ts_start_time;
    full_message_t* full_msg = NULL;
    size_t total_size = sizeof(message_header_t) + payload_size;

    if((full_msg = malloc(total_size)) == NULL)
        exit_sys("MALLOC ALLOCATION IN PRODUCER");

    full_msg->message_header.header = MSG_HEADER;
    full_msg->message_header.payload_size = payload_size;
    (void)memset(full_msg->payload, 'A', payload_size);

    for(uint32_t i = 0; i < number_of_message + WARM_UP; ++i)
    {
        full_msg->message_header.sequence = i;

        if (clock_gettime(CLOCK_MONOTONIC, &ts_start_time) == -1)
            exit_sys("CLOCK_GETTIME");
        
        full_msg->message_header.time_ns = ((uint64_t)ts_start_time.tv_sec * 1000000000ULL) + (uint64_t)ts_start_time.tv_nsec;

        if(mq_send(data_queue_handle, (const char *)full_msg, total_size, 0) == -1)
            exit_sys("MQ_SEND PRODUCER");

        if(mq_receive(ack_queue_handle, handshake_arr, sizeof(handshake_arr), NULL) == -1)
            exit_sys("MQ_RECEIVE PRODUCER");
        
        if(memcmp(HANDSHAKE_MSG, handshake_arr, sizeof(handshake_arr)))
        {
            fprintf(stderr, "HANDSHAKE MISMATCH FAILURE\n");
            break;
        }

        (void)memset(handshake_arr, 0, sizeof(handshake_arr));
        
    }

    if(mq_send(data_queue_handle, (const char *)full_msg, 0, 0) == -1)
        exit_sys("MQ_SEND PRODUCER");

    free(full_msg);
}

void consumer(void)
{
    int err_check = 0;
    const char handshake_arr[] = HANDSHAKE_MSG;

    struct timespec ts1;
    full_message_t* full_msg = NULL;
    uint64_t* elapsed_time = NULL;

    size_t total_size = sizeof(message_header_t) + payload_size;
    uint64_t local_sequence = 0;
    ssize_t mq_receive_ret_val = 0;

    if((full_msg = malloc(total_size)) == NULL)
        exit_sys("MALLOC ALLOCATION     ");

    if((elapsed_time = calloc(number_of_message, sizeof(uint64_t))) == NULL)
        exit_sys("CALLOC ALLOCATION IN CONSUMER");

    (void)memset(full_msg, 0, total_size);

    while(1)
    {
        if((mq_receive_ret_val = mq_receive(data_queue_handle, (char*)full_msg, total_size, NULL)) == -1)
            exit_sys("MQ_RECEIVE CONSUMER");

        if (mq_receive_ret_val == 0)
            break;

        if (mq_receive_ret_val != total_size)
        {
            fprintf(stderr, "CONSUMER - RECEIVE SIZE FAILURE\n");
            err_check = 1;
            break;
        }

        if (clock_gettime(CLOCK_MONOTONIC, &ts1) == -1)
            exit_sys("CLOCK_GETTIME");
        
        if (MSG_HEADER != full_msg->message_header.header )
        {
            fprintf(stderr, "CONSUMER - HEADER FAILURE\n");
            err_check = 1;
            break;
        }

        if((local_sequence++) != full_msg->message_header.sequence)
        {
            fprintf(stderr, "CONSUMER - SEQUENCE FAILURE\n");
            err_check = 1;
            break;
        }
        
        if(full_msg->message_header.sequence >= (number_of_message + WARM_UP))
        {
            fprintf(stderr, "CONSUMER - ARRAY INDEX OVERFLOW\n");
            err_check = 1;
            break;
        }

        if (payload_size != full_msg->message_header.payload_size)
        {
            fprintf(stderr, "CONSUMER - PAYLOAD SIZE FAILURE\n");
            err_check = 1;
            break;
        }
        
        if (full_msg->payload[0] != 'A' || full_msg->payload[full_msg->message_header.payload_size - 1] != 'A')
        {
           fprintf(stderr, "PAYLOAD CORRUPTION\n");
           err_check = 1;
           break;
        }

        if(mq_send(ack_queue_handle, handshake_arr,  sizeof(handshake_arr), 0) == -1)
             exit_sys("MQ_SEND CONSUMER");

        if(full_msg->message_header.sequence < WARM_UP) 
            continue;
        
        elapsed_time[full_msg->message_header.sequence - WARM_UP] = ((uint64_t)ts1.tv_sec * 1000000000ULL) + (uint64_t)ts1.tv_nsec - full_msg->message_header.time_ns;


    }

    if(local_sequence != (uint64_t)(number_of_message + WARM_UP))
    {
        fprintf(stderr, "COUNT FAILURE\n");
        err_check = 1;
    }
         
    if(err_check == 0)
    {
        uint64_t min_val = elapsed_time[0];
        uint64_t max_val = elapsed_time[0];
        long double average = 0;

        for (uint32_t i = 0; i < number_of_message; ++i)
        {
            printf("%llu\n", (unsigned long long)elapsed_time[i]);

            average += elapsed_time[i];

            if (elapsed_time[i] < min_val)
            {
                min_val = elapsed_time[i];
            }

            if (elapsed_time[i] > max_val)
            {
                max_val = elapsed_time[i];
            }
        }

        printf("#Min: %llu\n", (unsigned long long)min_val);
        printf("#Max: %llu\n", (unsigned long long)max_val);
        printf("#Average: %.3Lf\n", average / number_of_message);
        printf("#Standard Deviation: %.3Lf\n", standard_deviation(elapsed_time, number_of_message));
        printf("#Median: %.3Lf\n", (long double)calculate_median(elapsed_time, number_of_message));

    }
   
    free(elapsed_time);
    free(full_msg);
}

int parse_command_line_arguments(int argc, char* argv[])
{
    if(argc != 5)
    {
        fprintf(stderr, "INVALID ARGUMENTS\n");
        return -1;
    }

    /* Number of Message */
    errno = 0;
    number_of_message = strtoul(argv[1], NULL, 10);

    if(0 != errno)
        exit_sys("STRTOUL FAILURE");

    if(number_of_message < 1)
    {
        fprintf(stderr, "NUMBER OF MESSAGE INVALID VALUE\n");
        return -1;
    } 

    printf("#number of message : %u\n", number_of_message);

    /* Payload Size */
    errno = 0;
    payload_size = strtoul(argv[2], NULL, 10);

    if(0 != errno)
        exit_sys("STRTOUL FAILURE");

    if((payload_size > MAX_PAYLOAD_SIZE) || (payload_size < 1))
    {
        fprintf(stderr, "MAX PAYLOAD SIZE\n");
        return -1;
    } 

    printf("#payload size : %u\n", payload_size);

    /* Consumer Priority */
    errno = 0;
    consumer_param.sched_priority = strtoul(argv[3], NULL, 10);

    if(0 != errno)
        exit_sys("STRTOUL");

    if((consumer_param.sched_priority > 99) || (consumer_param.sched_priority == 0))
    {
        fprintf(stderr, "CONSUMER PRIORITY FAILURE\n");
        return -1;
    }

    printf("#consumer priority : %d\n", consumer_param.sched_priority);

    /* Producer Priority */
    errno = 0;
    producer_param.sched_priority = strtoul(argv[4], NULL, 10);

    if(0 != errno)
        exit_sys("STRTOUL");

    if((producer_param.sched_priority > 99) || (producer_param.sched_priority == 0))
    {
        fprintf(stderr, "PRODUCER PRIORITY FAILURE\n");
        return -1;
    } 

    printf("#producer priority : %d\n", producer_param.sched_priority);

    fflush(stdout);
    
    return 0;
}

long double standard_deviation(uint64_t *arr, int n) 
{
    if (n < 2) return 0.0;

    long double sum = 0;

    for (int i = 0; i < n; i++) 
    {
        sum += arr[i];
    }

    long double average = sum / n;
    long double square_sum = 0.0;

    for (int i = 0; i < n; i++) 
    {
        long double diff = arr[i] - average;
        square_sum += diff * diff;
    }

    long double variance = square_sum / (n - 1);

    return sqrtl(variance);
}

void exit_sys(const char *msg)
{
	perror(msg);
    exit(EXIT_FAILURE);
}

static int compare_uint64(const void *a, const void *b)
{
    uint64_t x = *(const uint64_t *)a;
    uint64_t y = *(const uint64_t *)b;

    if (x < y) return -1;
    if (x > y) return 1;
    return 0;
}

double calculate_median(uint64_t *data, size_t n)
{
    if (data == NULL || n == 0) 
    {
        fprintf(stderr, "INVALID INPUT FOR MEDIAN\n");
        exit(EXIT_FAILURE);
    }

    qsort(data, n, sizeof(uint64_t), compare_uint64);

    if (n % 2 == 1) 
    {
        return (double)data[n / 2];
    } else 
    {
        return ((double)data[(n / 2) - 1] + (double)data[n / 2]) / 2.0;
    }
}

