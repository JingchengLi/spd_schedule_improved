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


 #ifndef _SPIDER_LINKEDLIST_H
 #define _SPIDER_LINKEDLIST_H

 //#include "lock.h"

 /*!
  * \file linkedlist.h
  * \ brief A set of macros to manage forword-linked list
  */

/*!
 * \brief Locks a list.
 * \param head a pointor to the list head structure
 * 
 * This macro attempts to place an exclusive lock in the
 * list head structure pointed to by head.
 * \retval 0 on success
 * \retval non-zero on failure
 */
#define SPD_LIST_LOCK(head) \
    pthread_mutex_lock(&(head)->lock)

#define typeof __typeof__
/*!
 * \brief Write locks a list.
 * \param head This is a pointer to the list head structure
 *
 * This macro attempts to place an exclusive write lock in the
 * list head structure pointed to by head.
 * \retval 0 on success
 * \retval non-zero on failure
 */

#define SPD_RWLIST_WRLOCK(head)   \
    pthread_rwlock_wrlock(&(head)->lock)

/*!
 * \brief Write locks a list, with timeout.
 * \param head This is a pointer to the list head structure
 * \param ts Pointer to a timespec structure
 *
 * This macro attempts to place an exclusive write lock in the
 * list head structure pointed to by head.
 * \retval 0 on success
 * \retval non-zero on failure
 */
#define SPD_RWLIST_TIMEDWRLOCK(head, ts)   \
    pthread_rwlock_timedwrlock(&(head)->lock, ts)

/*!
 * \brief Read locks a list.
 * \param head This is a pointer to the list head structure
 * 
 * This macro attempts to place a read lock in the
 * list head structure pointed to by head.
 * \retval 0 on success
 * \retval non-zero on failure
 */

#define SPD_RWLIST_RDLOCK(head)     \
    pthread_rwlock_rdlock(&(head)->lock)

/*!
 * \brief Read locks a list, with timeout.
 * \param head This is a pointer to the list head structure
 * \param ts Pointer to a timespec structure
 *
 * This macro attempts to place a read lock in the
 * list head structure pointed to by head.
 * \retval 0 on success
 * \retval non-zero on failure
 */
#define SPD_RWLISK_TIMEDRDLOCK(head, ts)        \
    pthread_rwlock_timedrdlock(&(head)->lock, ts)

/*!
 * \brief Locks a list without blocking if the list is locked.
 * \param head This is a pointer to the list head structure
 * 
 * This macro attempts to place an exclusive lock in the
 * list head structure pointed to by head.
 * \retval 0 on success
 * \retval non-zero on failure
 */
#define SPD_LIST_TRYLOCK(head)    \
    pthread_mutex_trylock(&(head)->lock)

/*!
 * \brief Write locks a list, without blocking if the list is locked.
 * \param head This is a pointer to the list head structure
 *
 * This macro attempts to place an exclusive write lock in the
 * list head structure pointed to by head.
 * \retval 0 on success
 * \retval non-zero on failure
 */
 #define SPD_RWLISK_TRYWRLOCK(head)         \
    pthread_rwlock_trywrlock(&(head)->lock)

/*!
 * \brief Read locks a list, without blocking if the list is locked.
 * \param head This is a pointer to the list head structure
 *
 * This macro attempts to place a read lock in the
 * list head structure pointed to by head.
 * \retval 0 on success
 * \retval non-zero on failure
 */
 #define SPD_RWLIST_TRYRDLOCK(head)        \
    pthread_rwlock_tryrdlock(&(head)->lock)

/*!
 * \brief Attempts to unlock a list.
 * \param head This is a pointer to the list head structure
 *
 * This macro attempts to remove an exclusive lock from the
 * list head structure pointed to by head. If the list
 * was not locked by this thread, this macro has no effect.
 */
#define SPD_LIST_UNLOCK(head)           \
    pthread_mutex_unlock(&(head)->lock)

/*!
 * \brief Attempts to unlock a read/write based list.
 * \param head This is a pointer to the list head structure
 *
 * This macro attempts to remove a read or write lock from the
 * list head structure pointed to by head. If the list
 * was not locked by this thread, this macro has no effect.
 */
 #define SPD_RWLIST_UNLOCK(head)           \
    pthread_rwlock_unlock(&(head)->lock)

/*!
 * \brief Define a structure to be used to hold a list of spycified type.
 * \param name This will be the name if the defined structure.
 * \type this is the type of each list entry.
 *
 * This Macro creates a structure definition tha can be used to hold a list 
 * of entries of type type, it does not actually declare a structure of the instance
 * you wish to declare or use the specified name to declare instances elsewhere.
 *
 * Example usage:
 * \code
 * static SPD_LIST_HEAD(entry_list, entry) entries;
 *
 * This would define struct entry_list, and declare an instance of it named
 */
 #define SPD_LIST_HEAD(name, type)   \
    struct name {                  \
        struct type *first;        \
        struct type *last;         \
        pthread_mutex_t lock;          \
    }
/*!
 * \brief Defines a structure to be used to hold a read/write list of specified type.
 * \param name This will be the name of the defined structure.
 * \param type This is the type of each list entry.
 *
 * This macro creates a structure definition that can be used
 * to hold a list of the entries of type \a type. It does not actually
 * declare (allocate) a structure; to do that, either follow this
 * macro with the desired name of the instance you wish to declare,
 * or use the specified \a name to declare instances elsewhere.
 *
 * Example usage:
 * \code
 * static SPD_RWLIST_HEAD(entry_list, entry) entries;
 * \endcode
 *
 * This would define \c struct \c entry_list, and declare an instance of it named
 * \a entries, all intended to hold a list of type \c struct \c entry.
 */
#define SPD_RWLIST_HEAD(name, type) \
    struct name {                  \
    struct type *first;\
    struct type *last;\
        pthread_rwlock_t lock; \
    }

/*!
 * \brief Defines a structure to be used to hold a list of specified type (with no lock).
 * \param name This will be the name of the defined structure.
 * \param type This is the type of each list entry.
 *
 * This macro creates a structure definition that can be used
 * to hold a list of the entries of type \a type. It does not actually
 * declare (allocate) a structure; to do that, either follow this
 * macro with the desired name of the instance you wish to declare,
 * or use the specified \a name to declare instances elsewhere.
 *
 * Example usage:
 * \code
 * static SPD_LIST_HEAD_NOLOCK(entry_list, entry) entries;
 * \endcode
 *
 * This would define \c struct \c entry_list, and declare an instance of it named
 * \a entries, all intended to hold a list of type \c struct \c entry.
 */
#define SPD_LIST_HEAD_NOLOCK(name, type)                \
    struct name {                               \
        struct type *first;                     \
        struct type *last;                      \
    }

/*!
 * \brief  Initial values for a declaration of SPD_LIST_HEAD
 */
 #define SPD_LIST_HEAD_INIT_VALUE   {            \
    .first = NULL,                       \
    .last  = NULL,                         \
    .lock  = SPD_MUTEX_INIT_VALUE,           \
    }

/*!
 * \brief  Initial values for a declaration of SPD_RWLIST_HEAD
 */
 #define SPD_RWLIST_HEAD_INIT_VALUE {        \
    .first = NULL,                          \
    .last = NULL,                           \
    .lock = SPD_RWLOCK_INIT_VALUE,          \
    }

/*!
 * \brief Defines initial values for a declaration of SPD_LIST_HEAD_NOLOCK
 */
#define SPD_LIST_HEAD_NOLOCK_INIT_VALUE {       \
    .first = NULL,                       \
    .last = NULL,                        \
    }

/*!
 * \brief Defines a structure to be used to hold a list of specified type, statically 
 * nitialized.
 * \param name This will be the name of the defined structure.
 * \param type This is the type of each list entry.
 *
 * This macro creates a structure definition that can be used
 * to hold a list of the entries of type \a type, and allocates an instance
 * of it, initialized to be empty.
 *
 * Example usage:
 * \code
 * static SPD_LIST_HEAD_STATIC(entry_list, entry);
 * \endcode
 *
 * type \c struct \c entry.
 */
#if defined(SPD_MUTEX_INIT_WITH_CONSTRUCTORS)
#define SPD_LIST_HEAD_STATIC(name, type)      \
    struct name {                      \
        struct type *first;            \
        struct type *last;             \
        pthread_mutex_t lock;              \
    } name;                            \
static void __attribute__((constructor)) __init_##name(void)  \
{                                               \
    SPD_LIST_HEAD_INIT(&name);                 \
}                                                 \
static void __attribute__((destructor)) __fini_##name(void)       \
{                                                                     \
    SPD_LIST_HEAD_DESTROY(&name);                           \
}                                                       \
struct __dummy_##name
#else
#define SPD_LIST_HEAD_STATIC(name, type)                    \
    struct name {                     \
        struct type *first;             \
        struct type *last;               \
        pthread_mutex_t lock;                \
    } name = SPD_LIST_HEAD_INIT_VALUE
#endif

/*!
 * \brief Defines a structure to be used to hold a read/write list of specified type, 
    statically initialized.
 * \param name This will be the name of the defined structure.
 * \param type This is the type of each list entry.
 *
 * This macro creates a structure definition that can be used
 * to hold a list of the entries of type \a type, and allocates an instance
 * of it, initialized to be empty.
 *
 * Example usage:
 * \code
 * static SPD_RWLIST_HEAD_STATIC(entry_list, entry);
 * \endcode
 *
 * This would define \c struct \c entry_list, intended to hold a list of
 * type \c struct \c entry.
 */
#define SPD_RWLIST_HEAD_STATIC(name, type)           \
    struct name {                       \
        struct type *first;             \
        struct type *last;             \
        pthread_rwlock_t lock;             \
    } name = SPD_RWLIST_HEAD_INIT_VALUE

/*!
 * \brief Defines a structure to be used to hold a list of specified type, statically 
 *   initialized.
 *
 * This is the same as SPD_LIST_HEAD_STATIC, except without the lock included.
 */
 #define SPD_LIST_HEAD_NOLOCK_STATIC(name, type)      \
    struct name {                                      \
        struct type *first;                            \
        struct type *last;                             \
    } name = SPD_LIST_HEAD_NOLOCK_INIT_VALUE

/*!
 * \brief Initializes a list head structure with a specified first entry.
 * \param head This is a pointer to the list head structure
 * \param entry pointer to the list entry that will become the head of the list
 *
 * This macro initializes a list head structure by setting the head
 * entry to the supplied value and recreating the embedded lock.
 */
#define SPD_LIST_HEAD_SET(head, entry) do { \
    (head)->first = (entry);                  \
    (head)->last  =(entry);                  \
    pthread_mutex_init(&(head)->lock);             \
} while (0)

/*!
 * \brief Initializes an rwlist head structure with a specified first entry.
 * \param head This is a pointer to the list head structure
 * \param entry pointer to the list entry that will become the head of the list
 *
 * This macro initializes a list head structure by setting the head
 * entry to the supplied value and recreating the embedded lock.
 */
 #define SPD_RWLIST_HEAD_SET(head, entry) do {            \
    (head)->first = (entry);                      \
    (head)->last  = (entry);                  \
    pthread_rwlock_init(&(head)->lock);              \
 } while (0)

/*!
 * \brief Initializes a list head structure with a specified first entry.
 * \param head This is a pointer to the list head structure
 * \param entry pointer to the list entry that will become the head of the list
 *
 * This macro initializes a list head structure by setting the head
 * entry to the supplied value.
 */
 #define SPD_LIST_HEAD_SET_NOLOCK(head, entry) do {          \
    (head)->first = (entry);                  \
    (head)->last = (entry);                            \
 } while (0)

/*!
 * \brief Declare a forward link structure inside a list entry.
 * \param type This is the type of each list entry.
 *
 * This macro declares a structure to be used to link list entries together.
 * It must be used inside the definition of the structure named in
 * \a type, as follows:
 *
 * \code
 * struct list_entry {
       ...
       SPD_LIST_ENTRY(list_entry) list;
 * }
 * \endcode
 *
 * The field name \a list here is arbitrary, and can be anything you wish.
 */
 #define SPD_LIST_ENTRY(type)            \
    struct {                          \
        struct type *next;            \
    }

#define SPD_RWLIST_ENTRY  SPD_LIST_ENTRY

/*!
 * \brief get the first entry contained in a lisk.
 * \param pointer to the list head structure
 */
#define SPD_LIST_FIRST(head)  ((head)->first)

#define SPD_RWLIST_FIRST SPD_LIST_FIRST

/*!
 * \brief Returns the last entry contained in a list.
 * \param head This is a pointer to the list head structure
 */
#define    SPD_LIST_LAST(head)    ((head)->last)

#define SPD_RWLIST_LAST SPD_LIST_LAST

/*!
 * \brief Returns the next entry in the list after the given entry.
 * \param elm This is a pointer to the current entry.
 * \param field This is the name of the field (declared using SPD_LIST_ENTRY())
 * used to link entries of this list together.
 */
 #define SPD_LIST_NEXT(cur, field) ((cur)->field.next)

 #define SPD_RWLIST_NEXT SPD_LIST_NEXT

/*!
 * \brief Checks whether the specified list contains any entries.
 * \param head This is a pointer to the list head structure
 *
 * \return zero if the list has entries
 * \return non-zero if not.
 */
 #define SPD_LIST_EMPTY(head)     (SPD_LIST_FIRST(head) == NULL)

 #define SPD_RWLIST_EMPTY(head)  SPD_LIST_EMPTY(head)


 /*!
  * \brief Loops over (traverses) the entries in a list.
  * \param head This is a pointer to the list head structure
  * \param var This is the name of the variable that will hold a pointer to the
  * current list entry on each iteration. It must be declared before calling
  * this macro.
  * \param field This is the name of the field (declared using SPD_LIST_ENTRY())
  * used to link entries of this list together.
  *
  * This macro is use to loop over (traverse) the entries in a list. It uses a
  * \a for loop, and supplies the enclosed code with a pointer to each list
  * entry as it loops. It is typically used as follows:
  * \code
  * static SPD_LIST_HEAD(entry_list, list_entry) entries;
  * ...
  * struct list_entry {
       ...
       SPD_LIST_ENTRY(list_entry) list;
  * }
  * ...
  * struct list_entry *current;
  * ...
  * SPD_LIST_TRAVERSE(&entries, current, list) {
      (do something with current here)
  * }
  * \endcode
  * \warning If you modify the forward-link pointer contained in the \a current entry 
 while
  * inside the loop, the behavior will be unpredictable. At a minimum, the following
  * macros will modify the forward-link pointer, and should not be used inside
  * SPD_LIST_TRAVERSE() against the entry pointed to by the \a current pointer without
  * careful consideration of their consequences:
  * \li SPD_LIST_NEXT() (when used as an lvalue)
  * \li SPD_LIST_INSERT_AFTER()
  * \li SPD_LIST_INSERT_HEAD()
  * \li SPD_LIST_INSERT_TAIL()
  * \li SPD_LIST_INSERT_SORTALPHA()
  */
#define SPD_LIST_TRAVERSE(head, cur, field)   \
    for((cur) = (head)->first; (cur); (cur) = (cur)->field.next)

#define SPD_RWLISt_TRAVERSE  SPD_LIST_TRAVERSE

/*!
 * \brief Loops safely over (traverses) the entries in a list.
 * \param head This is a pointer to the list head structure
 * \param var This is the name of the variable that will hold a pointer to the
 * current list entry on each iteration. It must be declared before calling
 * this macro.
 * \param field This is the name of the field (declared using SPD_LIST_ENTRY())
 * used to link entries of this list together.
 *
 * This macro is used to safely loop over (traverse) the entries in a list. It
 * uses a \a for loop, and supplies the enclosed code with a pointer to each list
 * entry as it loops. It is typically used as follows:
 *
 * \code
 * static SPD_LIST_HEAD(entry_list, list_entry) entries;
 * ...
 * struct list_entry {
      ...
      SPD_LIST_ENTRY(list_entry) list;
 * }
 * ...
 * struct list_entry *current;
 * ...
 * SPD_LIST_TRAVERSE_SAFE_BEGIN(&entries, current, list) {
     (do something with current here)
 * }
 * SPD_LIST_TRAVERSE_SAFE_END;
 * \endcode
 *
 * It differs from SPD_LIST_TRAVERSE() in that the code inside the loop can modify
 * (or even free, after calling SPD_LIST_REMOVE_CURRENT()) the entry pointed to by
 * the \a current pointer without affecting the loop traversal.
 */

#define SPD_LIST_TRAVERSE_SAFE_BEGIN(head, cur, field) {        \
    typeof((head)) __list_head = head;                         \
    typeof(__list_head->first) __list_next;                    \
    typeof(__list_head->first) __list_prev = NULL;              \
    typeof(__list_head->first) __new_prev = NULL;               \
    for((cur) = __list_head->first, __new_prev = (cur),      \
        __list_next = (cur) ? (cur)->field.next : NULL;      \
        (cur); __list_prev = __new_prev, (cur) = __list_next, \
        __new_prev = (cur),                                   \
        __list_next = (cur) ? (cur)->field.next : NULL           \
        )

#define SPD_RWLIST_TRAVERSE_SAFE_BEGIN    SPD_LIST_TRAVERSE_SAFE_BEGIN
    
/*!
 * \brief Removes the \a current entry from a list during a traversal.
 * \param field This is the name of the field (declared using SPD_LIST_ENTRY())
 * used to link entries of this list together.
 *
 * \note This macro can \b only be used inside an SPD_LIST_TRAVERSE_SAFE_BEGIN()
 * block; it is used to unlink the current entry from the list without affecting
 * the list traversal (and without having to re-traverse the list to modify the
 * previous entry, if any).
 
 #define SPD_LIST_REMOVE_CURRENT(cur) do { \
    __new_prev->cur.next = NULL;         \
    __new_prev-= __list_prev;           \
    if(__list_prev)       \
        __list_prev->cur.next = __list_next; \
    else                                        \
        __list_prev->first = __list_prev;        \
    if(!__list_next)        \
        __list_head->last = __list_prev;         \
    } while (0)

#define SPD_RWLIST_REMOVE_CURRENT SPD_LIST_REMOVE_CURRENT
*/

/*!
  \brief Removes the \a current entry from a list during a traversal.
  \param head This is a pointer to the list head structure
  \param field This is the name of the field (declared using SPD_LIST_ENTRY())
  used to link entries of this list together.

  \note This macro can \b only be used inside an SPD_LIST_TRAVERSE_SAFE_BEGIN()
  block; it is used to unlink the current entry from the list without affecting
  the list traversal (and without having to re-traverse the list to modify the
  previous entry, if any).
 */
#define SPD_LIST_REMOVE_CURRENT(head, field)                        \
    __new_prev->field.next = NULL;                            \
    __new_prev = __list_prev;                            \
    if (__list_prev)                                \
        __list_prev->field.next = __list_next;                    \
    else                                        \
        (head)->first = __list_next;                        \
    if (!__list_next)                                \
        (head)->last = __list_prev;

#define SPD_RWLIST_REMOVE_CURRENT SPD_LIST_REMOVE_CURRENT  


#define SPD_LIST_MOVE_CURRENT(head, cur) do {    \
    typeof ((head)->first) __list_cur = __ new_prev;          \
    SPD_LIST_REMOVE_CURRENT(cur);                             \
    SPD_LIST_INSERT_TAIL((head), __list_cur, cur);            \
    } while (0)

#define SPD_RWLIST_MOVE_CURRENT SPD_LIST_MOVE_CURRENT

/*!
 * \brief Inserts a list entry before the current entry during a traversal.
 * \param head this is a pointer to the entry to be inserted.
 * \param cur this is the name of cur,
 * 
 * \note this macro can be only used inside an SPD_LIST_TRAVRSE_SAFE_BEGIN().
 */
 #define SPD_LIST_INSERT_BEFORE_CURRENT(head, cur) do {              \
    if(__list_prev) {                                              \
        (head)->cur.next = __list_prev->cur.next;                  \
        __list_prev->cur.next = head;                               \
    } else {                                                      \
        (head)->cur.next = __list_head->first;                 \
        __list_head->first = (head);                        \
    }                                                    \
    __list_prev = (head);                                 \
 } while (0)

#define SPD_RWLIST_INSERT_BEFORE_CURRENT SPD_LIST_INSERT_BEFORE_CURRENT

/*!
 * \brief Closes a safe loop traversal block.
 */
#define SPD_LIST_TRAVERSE_SAFE_END  }

#define SPD_RWLIST_TRAVERSE_SAFE_END SPD_LIST_TRAVERSE_SAFE_END

/*!
 * \brief Initializes a list head structure.
 * \param head This is a pointer to the list head structure
 *
 * This macro initializes a list head structure by setting the head
 * entry to \a NULL (empty list) and recreating the embedded lock.
 */
 #define SPD_LIST_HEAD_INIT(head) {              \
    (head)->first = NULL;                      \
    (head)->last = NULL;                      \
    pthread_mutex_init(&(head)->lock);             \
    }
 
/*!
 * \brief Initializes an rwlist head structure.
 * \param head This is a pointer to the list head structure
 *
 * This macro initializes a list head structure by setting the head
 * entry to \a NULL (empty list) and recreating the embedded lock.
 */
#define SPD_RWLIST_HEAD_INIT(head) {                                    \
         (head)->first = NULL;                                           \
         (head)->last = NULL;                                            \
         pthread_rwlock_init(&(head)->lock);                                 \
 }

/*!
 * \brief Destroys a list head structure.
 * \param head This is a pointer to the list head structure
 *
 * This macro destroys a list head structure by setting the head
 * entry to \a NULL (empty list) and destroying the embedded lock.
 * It does not free the structure from memory.
 */
 #define SPD_LIST_HEAD_DESTROY(head) {           \
    (head)->first = NULL;                      \
    (head)->last = NULL;                      \
    pthread_mutex_destroy(&(head)->lock)          \
}

/*!
 * \brief Destroys an rwlist head structure.
 * \param head This is a pointer to the list head structure
 *
 * This macro destroys a list head structure by setting the head
 * entry to \a NULL (empty list) and destroying the embedded lock.
 * It does not free the structure from memory.
 */
#define SPD_RWLIST_HEAD_DESTROY(head) {                                 \
        (head)->first = NULL;                                           \
        (head)->last = NULL;                                            \
        pthread_rwlock_destroy(&(head)->lock);                              \
}

/*!
 * \brief Initializes a list head structure.
 * \param head This is a pointer to the list head structure
 *
 * This macro initializes a list head structure by setting the head
 * entry to \a NULL (empty list). There is no embedded lock handling
 * with this macro.
 */
#define SPD_LIST_HEAD_INIT_NOLOCK(head) {                \
    (head)->first = NULL;                        \
    (head)->last = NULL;                        \
}

/*!
 * \brief Inserts a list entry after a given entry.
 * \param head This is a pointer to the list head structure
 * \param listelm This is a pointer to the entry after which the new entry should
 * be inserted.
 * \param elm This is a pointer to the entry to be inserted.
 * \param field This is the name of the field (declared using SPD_LIST_ENTRY())
 * used to link entries of this list together.
 */

#define SPD_LIST_INSERT_AFTER(head, cur,nextel, field) do {       \
    (nextel)->field.next = (cur)->field.next;             \
    (cur)->field.next = (nextel);                     \
    if((head)->last == (cur))                       \
        (head)->last = (nextel);                    \
} while (0)

#define SPD_RWLIST_INSERT_AFTER  SPD_LIST_INSERT_AFTER

/*!
 * \brief Inserts a list entry at the head of a list.
 * \param head This is a pointer to the list head structure
 * \param cur This is a pointer to the entry to be inserted.
 * \param field This is the name of the field (declared using SPD_LIST_ENTRY())
 * used to link entries of this list together.
 */
#define SPD_LIST_INSERT_HEAD(head, cur, field) do { \
    (cur)->field.next = (head)->first;             \
    (head)->first = (cur);                        \
    if(!(head)->last)                             \
        (head)->last = (cur);                     \
} while (0)
        
#define SPD_RWLIST_INSERT_HEAD SPD_LIST_INSERT_HEAD

/*!
 * \brief Appends a list entry to the tail of a list.
 * \param head This is a pointer to the list head structure
 * \param elm This is a pointer to the entry to be appended.
 * \param field This is the name of the field (declared using SPD_LIST_ENTRY())
 * used to link entries of this list together.
 *
 * Note: The link field in the appended entry is \b not modified, so if it is
 * actually the head of a list itself, the entire list will be appended
 * temporarily (until the next SPD_LIST_INSERT_TAIL is performed).
 */
 #define SPD_LIST_INSERT_TAIL(head, cur, field) do {       \
    if(!(head)->first) {                                \
        (head)->first = (cur);                      \
        (head)->last = (cur);                       \
    } else {                                \
        (head)->last->field.next = (cur);             \
        (head)->last = (cur);                 \
    }                         \
} while (0)

#define SPD_RWLIST_INSERT_TAIL SPD_LIST_INSERT_TAIL

/*!
 * \brief Inserts a list entry into a alphabetically sorted list
 * \param head Pointer to the list head structure
 * \param elm Pointer to the entry to be inserted
 * \param field Name of the list entry field (declared using SPD_LIST_ENTRY())
 * \param sortfield Name of the field on which the list is sorted
 *
 */
 #define SPD_LIST_INSERT_SORTALPHA(head, elm, field, sortfield) do {   \
    if(!(head)->first) {                                   \
        (head)->first = (elm);                               \
        (head)->last = (elm);                \
    } else {                            \
        typeof((head)->first) cur = (head)->first, prev = NULL;    \
        while (cur && strcmp(cur->sortfield, elm->sortfield) < 0) { \
            prev = cur;                                              \
            cur = cur->field.next;                                     \
        }                                                         \
        if(!prev) {                                                   \
            SPD_LIST_INSERT_HEAD(head, elm, field);                  \
        } else if(!cur) {                                     \
            SPD_LIST_INSERT_TAIL(head, elm, field);             \
        } else {                                                \
            SPD_LIST_INSERT_AFTER(head, prev, elm, field);          \
        }                                                        \
    }                                                           \
} while(0)                                                      \

#define SPD_RWLIST_INSERT_SORTALPHA    SPD_LIST_INSERT_SORTALPHA

/*!
 * \brief Appends a whole list to the tail of a list.
 * \param head This is a pointer to the list head structure
 * \param list This is a pointer to the list to be appended.
 * \param field This is the name of the field (declared using SPD_LIST_ENTRY())
 * used to link entries of this list together.
 *
 * Note: The source list (the \a list parameter) will be empty after
 * calling this macro (the list entries are \b moved to the target list).
 */
#define SPD_LIST_APPEND_LIST(head, list, field) do {            \
    if (!(list)->first) {                        \
        break;                            \
    }                                \
    if (!(head)->first) {                        \
        (head)->first = (list)->first;                \
        (head)->last = (list)->last;                \
    } else {                            \
        (head)->last->field.next = (list)->first;        \
        (head)->last = (list)->last;                \
    }                                \
    (list)->first = NULL;                        \
    (list)->last = NULL;                        \
} while (0)

#define SPD_RWLIST_APPEND_LIST SPD_LIST_APPEND_LIST

/*!
  \brief Inserts a whole list after a specific entry in a list
  \param head This is a pointer to the list head structure
  \param list This is a pointer to the list to be inserted.
  \param elm This is a pointer to the entry after which the new list should
  be inserted.
  \param field This is the name of the field (declared using SPD_LIST_ENTRY())
  used to link entries of the lists together.

  Note: The source list (the \a list parameter) will be empty after
  calling this macro (the list entries are \b moved to the target list).
 */
#define SPD_LIST_INSERT_LIST_AFTER(head, list, elm, field) do {        \
    (list)->last->field.next = (elm)->field.next;            \
    (elm)->field.next = (list)->first;                \
    if ((head)->last == elm) {                    \
        (head)->last = (list)->last;                \
    }                                \
    (list)->first = NULL;                        \
    (list)->last = NULL;                        \
} while(0)

#define SPD_RWLIST_INSERT_LIST_AFTER SPD_LIST_INSERT_LIST_AFTER

/*!
 * \brief Removes and returns the head entry from a list.
 * \param head This is a pointer to the list head structure
 * \param field This is the name of the field (declared using SPD_LIST_ENTRY())
 * used to link entries of this list together.
 *
 * Removes the head entry from the list, and returns a pointer to it.
 * This macro is safe to call on an empty list.
 */
#define SPD_LIST_REMOVE_HEAD(head, field) ({                \
        typeof((head)->first) cur = (head)->first;        \
        if (cur) {                        \
            (head)->first = cur->field.next;        \
            cur->field.next = NULL;                \
            if ((head)->last == cur)            \
                (head)->last = NULL;            \
        }                            \
        cur;                            \
    })

#define SPD_RWLIST_REMOVE_HEAD SPD_LIST_REMOVE_HEAD

/*!
 * \brief Removes a specific entry from a list.
 * \param head This is a pointer to the list head structure
 * \param elm This is a pointer to the entry to be removed.
 * \param field This is the name of the field (declared using SPD_LIST_ENTRY())
 * used to link entries of this list together.
 * \warning The removed entry is \b not freed nor modified in any way.
 */
#define SPD_LIST_REMOVE(head, elm, field) ({                    \
    __typeof(elm) __res = NULL; \
    if ((head)->first == (elm)) {                    \
        __res = (head)->first;                      \
        (head)->first = (elm)->field.next;            \
        if ((head)->last == (elm))            \
            (head)->last = NULL;            \
    } else {                                \
        typeof(elm) curelm = (head)->first;            \
        while (curelm && (curelm->field.next != (elm)))            \
            curelm = curelm->field.next;            \
        if (curelm) { \
            __res = (elm); \
            curelm->field.next = (elm)->field.next;            \
            if ((head)->last == (elm))                \
                (head)->last = curelm;                \
        } \
    }                                \
    (elm)->field.next = NULL;                                       \
    (__res); \
})

#define SPD_RWLIST_REMOVE SPD_LIST_REMOVE

#endif /* _SPIDER_LINKEDLIST_H */

 






 
