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

#ifndef _SPIDER_TIMES_H
#define _SPIDER_TIMES_H

#include <sys/time.h>

#if defined (__cplusplus) || defined(c_plusplus)
extern "C" {
#endif
# if __WORDSIZE == 64
    typedef long int  int64_t;
# else
    __extension__
    typedef long long int  int64_t;
# endif



/* We have to let the compiler learn what types to use for the elements of a
   struct timeval since on linux, it's time_t and suseconds_t, but on *BSD,
   they are just a long. */
extern struct timeval tv;
typedef time_t spd_time_t;
typedef suseconds_t spd_suseconds_t;

/*!
 * \brief Computes the difference (in seconds) between two \c struct \c timeval instances.
 * \param end the end of the time period
 * \param start the beginning of the time period
 * \return the difference in seconds
 */
static inline int64_t spd_tvdiff_sec(struct timeval end, struct timeval start)
{
	int64_t result = end.tv_sec - start.tv_sec;
	if (result > 0 && end.tv_usec < start.tv_usec)
		result--;
	else if (result < 0 && end.tv_usec > start.tv_usec)
		result++;

	return result;
}

/*!
 * \brief Computes the difference (in microseconds) between two \c struct \c timeval instances.
 * \param end the end of the time period
 * \param start the beginning of the time period
 * \return the difference in microseconds
 */
static inline int64_t spd_tvdiff_us(struct timeval end, struct timeval start)
{
	return (end.tv_sec - start.tv_sec) * (int64_t) 1000000 +
		end.tv_usec - start.tv_usec;
}

/*!
 * \brief Computes the difference (in milliseconds) between two \c struct \c timeval instances.
 * \param end end of the time period
 * \param start beginning of the time period
 * \return the difference in milliseconds
 */
static inline int64_t spd_tvdiff_ms(struct timeval end, struct timeval start)
{
	/* the offset by 1,000,000 below is intentional...
	   it avoids differences in the way that division
	   is handled for positive and negative numbers, by ensuring
	   that the divisor is always positive
	*/
	return  ((end.tv_sec - start.tv_sec) * 1000) +
		(((1000000 + end.tv_usec - start.tv_usec) / 1000) - 1000);
}

/*!
 * \brief Returns true if the argument is 0,0
 */
static inline int spd_tvzero(const struct timeval t)
{
	return (t.tv_sec == 0 && t.tv_usec == 0);
}

/*!
 *\brief get current time 
 */
static inline struct timeval spd_tvnow()
{
	struct timeval t;
	gettimeofday(&t, NULL);

	return t;
}


/*!
 * \brief Compres two \c struct \c timeval instances returning
 * -1, 0, 1 if the first arg is smaller, equal or greater to the second.
 */
static inline int spd_tvcmp(struct timeval a, struct timeval b)
{
	if (a.tv_sec < b.tv_sec)
		return -1;
	if (a.tv_sec > b.tv_sec)
		return 1;
	/* now seconds are equal */
	if (a.tv_usec < b.tv_usec)
		return -1;
	if (a.tv_usec > b.tv_usec)
		return 1;
	return 0;
}

/*!
 * \brief Returns true if the two \c struct \c timeval arguments are equal.
 */
static inline int spd_tveq(struct timeval a, struct timeval b)
{
	return (a.tv_sec == b.tv_sec && a.tv_usec == b.tv_usec);
}

static inline struct timeval spd_tv(spd_time_t sec, spd_suseconds_t su)
{
	struct timeval tv;

	tv.tv_sec = sec;
	tv.tv_usec = su;

	return tv;
}

/*!
 * \brief Returns a timeval corresponding to the duration of n samples at rate r.
 * Useful to convert samples to timevals, or even milliseconds to timevals
 * in the form spd_samp2tv(milliseconds, 1000)
 */
static inline struct timeval spd_samp2tv(unsigned int _nsamp, unsigned int _rate)
{
	return spd_tv(_nsamp / _rate, (_nsamp % _rate) * (1000000 / _rate));
}

struct timeval spd_tvsub(struct timeval a, struct timeval b);

struct timeval spd_tvadd(struct timeval a, struct timeval b);

#if defined (__cplusplus) || defined(c_plusplus)
}
#endif

#endif
