#include "scheduler.h"
#include "time.h"
#include "linkedlist.h"

struct scheduler_context * sch_con;
pthread_t timer_sched_t;

spd_scheduler_cb test_callback = NULL;
void* data = NULL;

void test(void* data)
{
    int res = 1000;
    printf("do test!\n");
    return res;  // re-schedule after 1000ms
}

void start_timer_schedule(struct scheduler_context * const con)
{
    int wait_ms;
    while (g_running_task)
    {
        wait_ms = spd_sched_cond_wait(con);
        if (0 == wait_ms)
        {
            spd_sched_runall(con);
        }
    }
}

int test()
{
    test_callback = test;
    spd_sched_add_flag(sch_con, 0, test_callback, data, 1, 100);
}

int main()
{
    sch_con = spd_sched_context_create();
    pthread_create(&timer_sched_t, &attr, (void *)start_timer_schedule, sch_con);

    pthread_join(timer_sched_t, NULL);
    spd_sche_context_destroy(sch_con);
}

