
#ifndef __LOGGING_H__
#define __LOGGING_H__


#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <stdarg.h>

struct _logdata 
{
    pthread_spinlock_t lock;
    char path[1024];
    char name[256];
    char date[32];
    FILE *fd;
};

enum log_t
{
	LOG_ERROR,
	LOG_WARNING,
	LOG_INFO,
	LOG_LEVEL   
};
char *log_array[LOG_LEVEL];

#define MAX_LINE        100000     

#define _log0(level, fmt, ...) NULL 

#define _log1(level, fmt, ...) \
    do \
    { \
        if (level <= LOG_LEVEL) \
        { \
            log1(log_array[level],fmt, ##__VA_ARGS__); \
        } \
    } while (0)

#define logging _log1

#define log_printf(loglevel, fmt, ...) do{\
    char buff[2048] = {0};\
    int level = loglevel <= LOG_LEVEL?loglevel:LOG_INFO;\
	sprintf(buff, fmt, ##__VA_ARGS__); \
    char date[24] = {0};\
    time_t tm = time(NULL);\
    strftime(date, sizeof(date), "[%m/%d-%H:%M:%S]", localtime(&tm));\
    printf("%s %s %s",date, log_array[level], buff);\
}while(0)\

#define FMT_SEC "_%Y%m%d%H%M%S"
#define FMT_MIN "_%Y%m%d%H%M"
#define FMT_HOU "_%Y%m%d%H"
#define FMT_DAY "_%Y%m%d"
#define FMT_MON "_%Y%m"

#define FORMAT FMT_DAY
int log_init(struct _logdata *log, char *logpath);
int log_exit(struct _logdata *log);

int logging_init(char *logpath);
int logging_exit(void);
int log1(char *loglevel, const char *fmt, ...);

#ifdef __cplusplus
}
#endif

#endif
