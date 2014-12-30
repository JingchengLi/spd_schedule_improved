/*
 * Spider -- An open source C language toolkit.
 *
 * Copyright (C) 2011 , Inc.
 *
 * lidp <openser@yeah.net>
 *
 * This program is free software, distributed under the terms of
 * the GNU General Public License Version 2. See the LICENSE file
 * at the top of the source tree.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <syslog.h>

#include "scheduler.h"
#include "linkedlist.h"
#include "times.h"

#define spd_log(priority, fmt, ...) ({ \
#ifdef DEBUG_SCHEDULER
    printf("[file: %s line:%d func:%s]" fmt, __FILE__, __LINE__, __PRETTY_FUNCTION__, ##__VA_ARGS__); \
#else
    syslog((priority),"[file: %s line:%d func:%s]" fmt, __FILE__, __LINE__, __PRETTY_FUNCTION__, ##__VA_ARGS__); \
#endif
    })   

#define SAFE_FREE(buf) if (buf) {\
        free(buf);\
        buf = NULL;}\
       
#define ONE_MILLION    1000000

#define DEBUG_M(a) {\
    (a);  \
}

#ifdef DEBUG_SCHEDULER
    #define DEBUG(a) do { \
    if(option_debug) \
        DEBUG_M(a)  \
        } while(0)
#else 
#define DEBUG(a)
#endif

struct scheduler {
    int flag;              /*!< Use return value from callback to reschedule */
    int reschedule;        /*!< When to reschedule (only if flag is false). */
    int id;                /*!< ID number of event */
    int retry_times;       /*!< Total retry times, negative value will always retry. */
    struct timeval when;   /*!< Absolute time event should take place */
    spd_scheduler_cb callback;
    void *data;
    SPD_LIST_ENTRY(scheduler)list;
};

struct scheduler_context {
    pthread_mutex_t lock;
    unsigned int processedcnt;                         /*!< Number of events processed */
    unsigned int schedsnt;                             /*!< Number of outstanding schedule events */
    SPD_LIST_HEAD_NOLOCK(, scheduler)schedulerq;       /*!< Schedule entry and main queue */
#ifdef SPD_SCHED_MA_CACHE
    SPD_LIST_HEAD_NOLOCK(, scheduler)schedulerc;
    unsigned int schedccnt;
#endif
#ifdef USE_COND_WAIT
    pthread_cond_t cond;
#endif
};

struct timeval spd_tvadd(struct timeval a, struct timeval b);
struct timeval spd_tvsub(struct timeval a, struct timeval b);
/*
 * put timeval in a valid range. usec is 0..999999
 * negative values are not allowed and truncated.
 */
static struct timeval tvfix(struct timeval a);

struct scheduler_context *spd_sched_context_create(void)
{
    struct scheduler_context *sc;

    if(!(sc = calloc(1, sizeof(*sc)))) {
        return NULL;
    }

    pthread_mutex_init(&sc->lock, NULL /*&attr*/);
#ifdef USE_COND_WAIT
    pthread_cond_init(&sc->cond, NULL);
#endif
    
    sc->processedcnt = 1;
    sc->schedsnt = 0;
    SPD_LIST_HEAD_INIT_NOLOCK(&sc->schedulerq);
#ifdef SPD_SCHED_MA_CACHE
    SPD_LIST_HEAD_INIT_NOLOCK(&sc->schedulerc);
    sc->schedccnt = 0;
#endif
    return sc;
}

void spd_sche_context_destroy(struct scheduler_context *sc)
{
    struct scheduler *s;

    pthread_mutex_lock(&sc->lock);    
#ifdef USE_COND_WAIT
    pthread_cond_destroy(&sc->cond);
#endif

#ifdef SPD_SCHED_MA_CACHE
    while((s = SPD_LIST_REMOVE_HEAD(&sc->schedulerc, list)))
        SAFE_FREE(s);
#endif

    while((s = SPD_LIST_REMOVE_HEAD(&sc->schedulerq, list)))
        SAFE_FREE(s);

    pthread_mutex_unlock(&sc->lock);

    pthread_mutex_destroy(&sc->lock);

    SAFE_FREE(sc);
}

static struct scheduler *sched_alloc(struct scheduler_context *con)
{
    struct scheduler *tmp;

#ifdef SPD_SCHED_MA_CACHE
    if((tmp = SPD_LIST_REMOVE_HEAD(&con->schedulerc, list))) {
            con->schedccnt--;
                return tmp;
        }
#endif 
    if(!(tmp = calloc(1, sizeof(*tmp))))
        return NULL;

    return tmp;
}

static void scheduler_release(struct scheduler_context *con, struct scheduler *sc)
{
        if (!sc) {
                return;
        }        
        SAFE_FREE(sc->data);
#ifdef SPD_SCHED_MA_CACHE
        if(con->schedccnt < SPD_SCHED_MA_CACHE) {
            SPD_LIST_INSERT_HEAD(&con->schedulerc, sc, list);
            con->schedccnt++;
                    return;
        }
#endif
        SAFE_FREE(sc);
}

/* To support new scheduler inform when add a new scheduler. */
#ifdef USE_COND_WAIT
/* To support new scheduler inform when add a new scheduler.*/
int spd_sched_cond_wait(struct scheduler_context * c)
{
    int ms;
    struct timespec wait;
    pthread_mutex_lock(&c->lock);
    while (SPD_LIST_EMPTY(&c->schedulerq))
    {
        pthread_cond_wait(&c->cond, &c->lock);
    }

    ms = spd_tvdiff_ms(SPD_LIST_FIRST(&c->schedulerq)->when, spd_tvnow());
    if (ms < 0)
    {
        ms = 0;
    }
    else if(ms > 0)
    {
        if (ms < 1000)
        {
            /* if wait time is less than 1 sec, just do a sleep*/
            usleep((ms+1)*1000);
        }
        else
        {
            /* if wait time is more than 1 sec, use pthread_cond_timedwait so we can waken up in time to process new scheduler 
                        because of  a pthread_cond_signal call. */
            wait.tv_sec = (ms+1)/1000;
            wait.tv_nsec = (ms+1) % 1000 * 1000000;
            pthread_cond_timedwait(&c->cond, &c->lock, &wait);
        }
        ms = 0;
    }
    pthread_mutex_unlock(&c->lock);

    return ms;
}

#else
/*! \brief
 * Return the number of milliseconds 
 * until the next scheduled event
 */
int spd_sched_wait(struct scheduler_context * c)
{
    int ms;
    //DEBUG(spd_log(LOG_DEBUG, "ast_sched_wait()\n"));
    //spd_log(LOG_DEBUG, "ast_sched_wait()\n");
    pthread_mutex_lock(&c->lock);
    if(SPD_LIST_EMPTY(&c->schedulerq)){
        ms = -1;
    } else {
        ms = spd_tvdiff_ms(SPD_LIST_FIRST(&c->schedulerq)->when, spd_tvnow());
        if(ms < 0)
            ms = 0;
    }
    pthread_mutex_unlock(&c->lock);

    return ms;
}
#endif

/*! \brief
 * Take a sched structure and put it in the
 * queue, such that the soonest event is
 * first in the list.
 */
static void add_scheduler(struct scheduler_context *c, struct scheduler *s)
{
    struct scheduler *cur = NULL;

    if (!s) {
        return;
    }

    SPD_LIST_TRAVERSE_SAFE_BEGIN(&c->schedulerq, cur, list) {
        if(spd_tvcmp(s->when, cur->when)  == -1){
            SPD_LIST_INSERT_BEFORE_CURRENT(s, list);
            break;
        }
    }
    SPD_LIST_TRAVERSE_SAFE_END

    if(!cur) {
        SPD_LIST_INSERT_TAIL(&c->schedulerq, s, list);
    }

    c->schedsnt++;    
}

/*! \brief
 * given the last event *tv and the offset in milliseconds 'when',
 * computes the next value,
 */
static int sched_settime(struct timeval *tv, int when)
{
    struct timeval now = spd_tvnow();
    if(spd_tvzero(*tv))
        *tv = now;
    *tv = spd_tvadd(*tv, spd_samp2tv(when, 1000));
    if(spd_tvcmp(*tv, now) < 0) {
        *tv = now;
    }
    return 0;
}

/*! \brief
 * Schedule callback(data) to happen when ms into the future
 */
#if 0
int spd_sched_add_flag(struct scheduler_context * con, int when, spd_scheduler_cb callback, void* data, int flag)
#else
int spd_sched_add_flag(struct scheduler_context * con, int when, spd_scheduler_cb callback, void* data, int flag, int retry_times)
#endif
{
    struct scheduler *tmp;
    int res = -1;

    if (NULL == con)
    {
        spd_log(LOG_DEBUG, "scheduler context is NULL!");
        return -1;
    }
    spd_log(LOG_DEBUG,"Enter spd_sched_add \n");
    if(!flag && !when) {
        spd_log(LOG_DEBUG," scheduled event in 0 ms ? \n");
        return -1;
    }

    pthread_mutex_lock(&con->lock);
    
    if((tmp = sched_alloc(con))) {
        tmp->id = con->processedcnt++;
        tmp->callback = callback;
        tmp->data = data;
        tmp->reschedule = when;
        tmp->flag = flag;
        tmp->when = spd_tv(0,0);
        tmp->retry_times = retry_times ? retry_times : 1; /* retry_times is at least 1*/
        if(sched_settime(&tmp->when, when)) {
            scheduler_release(con, tmp);
        } else {
            struct timeval tv= spd_tvnow();
            struct timeval delta = spd_tvsub(tmp->when, tv);    
            spd_log(LOG_DEBUG, "=============================================================\n"
                                 "|ID    Callback          Data              Time  (sec:ms)   |\n"
                                 "+-----+-----------------+-----------------+-----------------+\n");
            spd_log(LOG_DEBUG, "|%.4d | %-15p | %-15p | %.6ld : %.6ld |\n",
                tmp->id,
                tmp->callback,
                tmp->data,
                delta.tv_sec,
                (long int)delta.tv_usec);
            add_scheduler(con, tmp);
            res = tmp->id;
        }
    }

#ifdef SPD_SCHED_MA_CACHE  
    spd_log(LOG_DEBUG, " Schedule Dump (%d in Q, %d Total, %d Cache)\n", con->schedsnt, con->processedcnt- 1, con->schedccnt);
#else
    spd_log(LOG_DEBUG, " Schedule Dump (%d in Q, %d Total)\n", con->schedsnt, con->processedcnt- 1);
#endif

#ifdef USE_COND_WAIT
    pthread_cond_signal(&con->cond);
#endif
    
    pthread_mutex_unlock(&con->lock);
    
    spd_log(LOG_DEBUG,"Exit spd_sched_add \n");
    return res;
}

int spd_sched_add(struct scheduler_context * con, int when, spd_scheduler_cb callback, void * data)
{
    return spd_sched_add_flag(con, when, callback,data, 0, -1);
}

/*! \brief
 * Delete the schedule entry with number
 * "id".  It's nearly impossible that there
 * would be two or more in the list with that
 * id.
 */
int spd_sched_del(struct scheduler_context * c, int id)
{
    struct scheduler *s;

    pthread_mutex_lock(&c->lock);
    SPD_LIST_TRAVERSE_SAFE_BEGIN(&c->schedulerq, s, list) {
        if(s->id == id) {
            SPD_LIST_REMOVE_CURRENT(&c->schedulerq, list);
            c->schedsnt--;
            scheduler_release(c, s);
            break;
        }
    }
    SPD_LIST_TRAVERSE_SAFE_END
    pthread_mutex_unlock(&c->lock);

    if(!s) {
        //spd_log(LOG_WARNING, "ask to delete null schedule\n");
        spd_log(LOG_DEBUG,"ask to delete null schedule\n");
        return -1;
    }

    return 0;
}

void spd_sched_dump(const struct scheduler_context *con)
{
    struct scheduler *q;
    struct timeval tv= spd_tvnow();

#ifdef SPD_SCHED_MA_CACHE  
    spd_log(LOG_DEBUG, " Schedule Dump (%d in Q, %d Total, %d Cache)\n", con->schedsnt, con->processedcnt- 1, con->schedccnt);
#else
    spd_log(LOG_DEBUG, " Schedule Dump (%d in Q, %d Total)\n", con->schedsnt, con->processedcnt- 1);
#endif

    spd_log(LOG_DEBUG, "=============================================================\n");
    spd_log(LOG_DEBUG, "|ID    Callback          Data              Time  (sec:ms)   |\n");
    spd_log(LOG_DEBUG, "+-----+-----------------+-----------------+-----------------+\n");
    SPD_LIST_TRAVERSE(&con->schedulerq, q, list) {
    struct timeval delta = spd_tvsub(q->when, tv);
        spd_log(LOG_DEBUG, "|%.4d | %-15p | %-15p | %.6ld : %.6ld |\n", 
            q->id,
            q->callback,
            q->data,
            delta.tv_sec,
            (long int)delta.tv_usec);
    }
    spd_log(LOG_DEBUG, "=============================================================\n");
}

/*! \brief
 * Launch all events which need to be run at this time.
 */
int spd_sched_runall(struct scheduler_context * c)
{
    struct scheduler *cur;

    struct timeval tv;
    int numevents;
    int res;

    if (!SPD_LIST_EMPTY(&c->schedulerq))
        pthread_mutex_lock(&c->lock);
    
    for(numevents = 0; !SPD_LIST_EMPTY(&c->schedulerq); numevents++) {
        /* schedule all events which are going to expire within 1ms.
         * We only care about millisecond accuracy anyway, so this will
         * help us get more than one event at one time if they are very
         * close together.
         */
        tv = spd_tvadd(spd_tvnow(), spd_tv(0, 1000));
        if(spd_tvcmp(SPD_LIST_FIRST(&c->schedulerq)->when, tv) != -1)
        {
            
            pthread_mutex_unlock(&c->lock);
            break;
        }

        cur = SPD_LIST_REMOVE_HEAD(&c->schedulerq, list);
        c->schedsnt--;


        /*
         * At this point, the schedule queue is still intact.  We
         * have removed the first event and the rest is still there,
         * so it's permissible for the callback to add new events, but
         * trying to delete itself won't work because it isn't in
         * the schedule queue.  If that's what it wants to do, it 
         * should return 0.
         */

        pthread_mutex_unlock(&c->lock);
        res = cur->callback(cur->data);
        if (cur->retry_times > 0)
        {
            cur->retry_times -= 1;
        }
        pthread_mutex_lock(&c->lock);

#if 0        
        if(res) {
#else
        if(res && cur->retry_times) {
#endif
            /*
             * If they return non-zero, we should schedule them to be
             * run again.
             */
             if(sched_settime(&cur->when, cur->flag ? res : cur->reschedule)) {
                scheduler_release(c,cur);
             } else {
                add_scheduler(c, cur);
                         }
        } else {
            scheduler_release(c, cur);
        }
        pthread_mutex_unlock(&c->lock);
    }

    return numevents;
}

long spd_sched_when(struct scheduler_context * con, int id)
{
    struct scheduler *s;
    long secs = -1;
    DEBUG(spd_log(LOG_DEBUG, "spd_sched_when()\n"));

    pthread_mutex_lock(&con->lock);
    SPD_LIST_TRAVERSE(&con->schedulerq, s, list) {
        if (s->id == id)
            break;
    }
    if (s) {
        struct timeval now = spd_tvnow();
        secs = s->when.tv_sec - now.tv_sec;
    }
    pthread_mutex_unlock(&con->lock);
    
    return secs;
}


struct timeval spd_tvadd(struct timeval a, struct timeval b)
{
    /* consistency checks to guarantee usec in 0..999999 */
    a = tvfix(a);
    b = tvfix(b);
    a.tv_sec += b.tv_sec;
    a.tv_usec += b.tv_usec;
    if (a.tv_usec >= ONE_MILLION) {
        a.tv_sec++;
        a.tv_usec -= ONE_MILLION;
    }
    return a;
}

struct timeval spd_tvsub(struct timeval a, struct timeval b)
{
    /* consistency checks to guarantee usec in 0..999999 */
    a = tvfix(a);
    b = tvfix(b);
    a.tv_sec -= b.tv_sec;
    a.tv_usec -= b.tv_usec;
    if (a.tv_usec < 0) {
        a.tv_sec-- ;
        a.tv_usec += ONE_MILLION;
    }
    return a;
}


#define ONE_MILLION    1000000
/*
 * put timeval in a valid range. usec is 0..999999
 * negative values are not allowed and truncated.
 */
static struct timeval tvfix(struct timeval a)
{
    if (a.tv_usec >= ONE_MILLION) {
        spd_log(LOG_WARNING, "warning too large timestamp %ld.%ld\n",
            (long)a.tv_sec, (long int) a.tv_usec);
        a.tv_sec += a.tv_usec / ONE_MILLION;
        a.tv_usec %= ONE_MILLION;
    } else if (a.tv_usec < 0) {
        spd_log(LOG_WARNING, "warning negative timestamp %ld.%ld\n",
            (long)a.tv_sec, (long int) a.tv_usec);
        a.tv_usec = 0;
    }
    return a;
}

