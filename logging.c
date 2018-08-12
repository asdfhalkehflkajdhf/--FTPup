
#include <stdio.h>
#include <string.h>
#include <pthread.h>

#include "logging.h"
#include "utils.h"

static struct _logdata run_logdata;

char *log_array[LOG_LEVEL]={"[ERROR]", "[WARNING]", "[INFO]"};

static inline int get_running_name(char *name, char *date){
    snprintf(run_logdata.name, sizeof(run_logdata.name), "running%s.log", run_logdata.date);
	return 0;
}

int log_init(struct _logdata *log, char *logpath)
{
    if (NULL == log || NULL == logpath)
    {
        log_printf(LOG_ERROR, "%s: logpath or filename is NULL\n", __func__);
        exit(2);
    }

    if (strlen(logpath) >= sizeof(log->path))
    {
        log_printf(LOG_ERROR, "%s: logpath %s larger than %lu\n", __func__, log->path, sizeof(log->path));
        exit(2);
    }

    strncpy(log->path, logpath, sizeof(log->path));

    time_t tm = time(NULL);
    strftime(log->date, sizeof(log->date), FORMAT, localtime(&tm));
	log->fd = NULL;
    pthread_spin_init(&log->lock, PTHREAD_PROCESS_PRIVATE);

    return 0;
}
int log_exit(struct _logdata *log)
{
    if (NULL != log->fd)
    {
        fclose(log->fd);
    }
    pthread_spin_destroy(&log->lock);

    return 0;
}

int logging_init(char *logpath)
{
    if (NULL == logpath)
    {
        log_printf(LOG_ERROR, "%s: logpath or filename is NULL\n", __func__);
        return -1;
    }
    if (recursive_make_dir(logpath, DIR_MODE) != 0) 
    {
        log_printf(LOG_ERROR, "%s: create logpath %s error\n", __func__, logpath);
        return -1;
    }

	log_init(&run_logdata, logpath);
    return 0;
}

int logging_exit(void)
{
	log_exit(&run_logdata);
    return 0;
}
int log1(char *loglevel, const char *fmt, ...)
{
    char buff[2048] = {0};
    va_list args;
    va_start(args, fmt);
    vsnprintf(buff, sizeof(buff), fmt, args);
    va_end(args);

    char date[24] = {0};
    char date2[32] = {0};
    time_t tm = time(NULL);
    strftime(date, sizeof(date), "[%m/%d-%H:%M:%S]", localtime(&tm));

    pthread_spin_lock(&run_logdata.lock);
    strftime(date2, 32, FORMAT, localtime(&tm));
    if (NULL == run_logdata.fd || strcmp(date2, run_logdata.date))
    {
    	if(NULL != run_logdata.fd)
	        fclose(run_logdata.fd);
        run_logdata.fd = NULL;

        strcpy(run_logdata.date, date2);
		get_running_name(run_logdata.name,run_logdata.date);
		char new_path[4096] = {'\0'};
		snprintf(new_path, sizeof(new_path), "%s/%s", run_logdata.path, run_logdata.name);
        run_logdata.fd = fopen(new_path, "a+");
        if (NULL == run_logdata.fd)
        {
            log_printf(LOG_ERROR, "%s: logging file open error: %s\n", __func__, new_path);
            exit (0);
        }
    }
	
    fprintf(run_logdata.fd, "%s %s %s",date, loglevel, buff);
    fflush(run_logdata.fd);
    pthread_spin_unlock(&run_logdata.lock);

    return 0;
}


