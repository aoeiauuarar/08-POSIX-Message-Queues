#include "msgq_lab.h"

#include <errno.h>
#include <fcntl.h>
#include <mqueue.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

/* One queue name per task — prevents cross-contamination between runs */
#define Q_EXCHANGE "/msgq_lab_exchange"
#define Q_PRIO     "/msgq_lab_prio"
#define Q_FULL     "/msgq_lab_full"
#define Q_SIZE     "/msgq_lab_size"

/*
 * mq_msgsize used when opening every queue.
 * Must be >= sizeof(struct SensorReading) so a struct fits in one message.
 */
#define MSG_BUF_SIZE 128


//Task A — task_struct_exchange()

int task_struct_exchange(void) {
    // 1. Remove leftover queue
    mq_unlink(Q_EXCHANGE); 
    
    // 2. Open the queue
    struct mq_attr attr = { .mq_maxmsg = 4, .mq_msgsize = MSG_BUF_SIZE };
    mqd_t mqd = mq_open(Q_EXCHANGE, O_CREAT | O_RDWR, 0660, &attr);
    if (mqd == (mqd_t)-1) return 1;

    // 3. Fork one child process
    pid_t pid = fork();
    if (pid < 0) return 1;

    if (pid == 0) {
        // 5. Child Process
        char buf[MSG_BUF_SIZE];
        unsigned int prio;
        
        // a. Blocks until message is received
        if (mq_receive(mqd, buf, MSG_BUF_SIZE, &prio) == -1) exit(1);
        
        // b. Cast buf to SensorReading * and print
        struct SensorReading *r = (struct SensorReading *)buf;
        printf("child received: sensor id=%d value=%d\n", r->sensor_id, r->value);
        fflush(stdout);
        
        // c-e. Cleanup and exit
        mq_close(mqd);
        mq_unlink(Q_EXCHANGE);
        exit(0);
    } else {
        // 4. Parent Process
        // a. Build SensorReading
        struct SensorReading r = { .sensor_id = 7, .value = 42 };
        
        // b. Send message (priority 0)
        mq_send(mqd, (char *)&r, sizeof(r), 0);
        
        // c. Close queue
        mq_close(mqd);
        
        // d. Wait for child
        waitpid(pid, NULL, 0);
        
        // e. Print exactly after waitpid
        printf("parent: sent reading\n");
        fflush(stdout);
    }
    return 0;
}


// Task B — task_priority_order()

int task_priority_order(void) {
    // 1. Clean slate
    mq_unlink(Q_PRIO);
    
    // 2. Open the queue
    struct mq_attr attr = { .mq_maxmsg = 4, .mq_msgsize = MSG_BUF_SIZE };
    mqd_t mqd = mq_open(Q_PRIO, O_CREAT | O_RDWR, 0660, &attr);
    if (mqd == (mqd_t)-1) return 1;

    // 3. Send TWO messages before forking
    struct SensorReading r1 = { .sensor_id = 1, .value = 10 };
    struct SensorReading r2 = { .sensor_id = 2, .value = 99 };
    
    mq_send(mqd, (char *)&r1, sizeof(r1), 1); // priority 1
    mq_send(mqd, (char *)&r2, sizeof(r2), 5); // priority 5
    
    // 4. Close queue, then fork
    mq_close(mqd);

    fflush(stdout);
    pid_t pid = fork();
    if (pid < 0) return 1;

    if (pid == 0) {
        // 5. Child Process
        // a. Re-open O_RDONLY
        mqd_t child_mqd = mq_open(Q_PRIO, O_RDONLY);
        char buf[MSG_BUF_SIZE];
        unsigned int prio;
        
        // b. First receive -> highest priority wins (value=99)
        mq_receive(child_mqd, buf, MSG_BUF_SIZE, &prio);
        struct SensorReading *hi_r = (struct SensorReading *)buf;
        printf("child received hi-pri: value=%d\n", hi_r->value);
        
        // c. Second receive -> remaining message (value=10)
        mq_receive(child_mqd, buf, MSG_BUF_SIZE, &prio);
        struct SensorReading *lo_r = (struct SensorReading *)buf;
        printf("child received lo-pri: value=%d\n", lo_r->value);
        fflush(stdout);
        
        // d. Cleanup and exit
        mq_close(child_mqd);
        mq_unlink(Q_PRIO);
        exit(0);
    } else {
        // 6. Parent Process waits
        waitpid(pid, NULL, 0);
    }
    return 0;
}

// Task C — task_full_queue()

int task_full_queue(void) {
    // 1. Clean slate
    mq_unlink(Q_FULL);
    
    // 2. Open queue with O_NONBLOCK and mq_maxmsg = 2
    struct mq_attr attr = { .mq_maxmsg = 2, .mq_msgsize = MSG_BUF_SIZE };
    mqd_t mqd = mq_open(Q_FULL, O_CREAT | O_RDWR | O_NONBLOCK, 0660, &attr);
    if (mqd == (mqd_t)-1) return 1;

    // 3. Send two messages to fill the queue
    char dummy_msg[1] = {0};
    mq_send(mqd, dummy_msg, sizeof(dummy_msg), 0);
    mq_send(mqd, dummy_msg, sizeof(dummy_msg), 0);

    // 4. Attempt a third send which must fail immediately
    if (mq_send(mqd, dummy_msg, sizeof(dummy_msg), 0) == -1) {
        // 5. Detect EAGAIN
        if (errno == EAGAIN) {
            printf("queue full: EAGAIN detected\n");
            fflush(stdout);
        }
    }

    // 6. Cleanup
    mq_close(mqd);
    mq_unlink(Q_FULL);
    return 0;
}


 //Task D — task_size_limit()
 
int task_size_limit(void) {
    // 1. Clean slate
    mq_unlink(Q_SIZE);
    
    // 2. Open queue with exact mq_msgsize
    struct mq_attr attr = { .mq_maxmsg = 4, .mq_msgsize = sizeof(struct SensorReading) };
    mqd_t mqd = mq_open(Q_SIZE, O_CREAT | O_WRONLY, 0660, &attr);
    if (mqd == (mqd_t)-1) return 1;

    // 3. Prepare an oversized buffer (+1 byte)
    char oversized_buf[sizeof(struct SensorReading) + 1] = {0};

    // 4. mq_send must fail
    if (mq_send(mqd, oversized_buf, sizeof(oversized_buf), 0) == -1) {
        // 5. Detect EMSGSIZE
        if (errno == EMSGSIZE) {
            printf("oversized send: EMSGSIZE detected\n");
            fflush(stdout);
        }
    }

    // 6. Cleanup
    mq_close(mqd);
    mq_unlink(Q_SIZE);
    return 0;
}

/* ------------------------------------------------------------------ */
/* main — do not modify                                                */
/* ------------------------------------------------------------------ */
int main(void) {
    if (task_struct_exchange() != 0) return EXIT_FAILURE;
    if (task_priority_order()  != 0) return EXIT_FAILURE;
    if (task_full_queue()      != 0) return EXIT_FAILURE;
    if (task_size_limit()      != 0) return EXIT_FAILURE;
    printf("all tasks done\n");
    return EXIT_SUCCESS;
}