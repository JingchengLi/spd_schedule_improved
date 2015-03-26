#include "scheduler.h"
#include "time.h"
#include "linkedlist.h"

#define MALLOC_DEBUG 1
struct scheduler_context * sch_con;
pthread_t timer_sched_t;

int test_callback_1(void* data)
{
    int res = 100;
    printf("test_callback_1!\n");
    return res;  // re-schedule after 100ms (only when flag is 1)
}

int test_callback_2(void* data)
{
    int res = 500;
    printf("test_callback_2!\n");
    return res;  // re-schedule after 500ms (only when flag is 1)
}

int test_callback_3(void* data)
{
    int res = 500;
    printf("test_callback_3!\n");
    return 0;  // return 0, will not be re-scheduled
}

int test_callback_4(void* data)
{
    int res = 1000;
    printf("test_callback_4!\n");
    return res;
}

int test_callback_5(void* data)
{
    int res = 200;
    printf("test_callback_5!\n");
    return res;  // re-schedule after 200ms (only when flag is 1)
}


void start_timer_schedule(struct scheduler_context * const con)
{
    int wait_ms;
    while (1)
    {
        wait_ms = spd_sched_cond_wait(con);
        if (0 == wait_ms)
        {
            spd_sched_runall(con);
        }
    }
}

int add_tasks()
{
    /* flag is 1. the next schedule interval time according to the ms value returned by this callback. */
    void* data = LOG_MALLOC(64);
    spd_sched_add_flag(sch_con, 300, test_callback_1, data, 1, 5); // max retry is 5, when flag is 1, reschedule value(300) is ignored.

    data = LOG_MALLOC(64);
    spd_sched_add_flag(sch_con, 400, test_callback_2, data, 1, 20);// max retry is 20 

    data = LOG_MALLOC(64);
    spd_sched_add_flag(sch_con, 500, test_callback_3, data, 1, 30);// max retry is 30 

    /* flag is 0. will re-schedule every 100 ms util the callback return 0.*/
    data = LOG_MALLOC(64);
    spd_sched_add_flag(sch_con, 100, test_callback_4, data, 0, 40);// max retry is 40 

    /* flag is 1. the next schedule interval time according to the ms value returned by this callback. */
    data = LOG_MALLOC(64);
    spd_sched_add_flag(sch_con, 100, test_callback_5, data, 1, 50);// max retry is 50 
}

int main()
{
    sch_con = spd_sched_context_create();
    pthread_create(&timer_sched_t, NULL, (void *)start_timer_schedule, sch_con);
    add_tasks();

    pthread_join(timer_sched_t, NULL);
    spd_sche_context_destroy(sch_con);
}

