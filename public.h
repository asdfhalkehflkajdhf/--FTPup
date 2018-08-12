
#ifndef __GLOBAL_H_
#define __GLOBAL_H_


#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <zlib.h>
#include "queue.h" 
#include "encapsulate_queue.h"

#define NEWIUP_OK 0
#define NEWIUP_FAIL -1
#define NEWIUP_INVALID_ID -1

#define NEWIUP_PATH_LEN 1024
#define NEWIUP_NAME_LEN 32



//1:same-move 2:keep 3:delete 4:same-move-time 5:diff-move 6:diff-move-time
enum end_process_e
{
	UPLOAD_END_PROCESS_SAME_MOVE = 1,
	UPLOAD_END_PROCESS_KEEP,
	UPLOAD_END_PROCESS_DELETE,
	UPLOAD_END_PROCESS_SAME_MOVE_TIME,
	UPLOAD_END_PROCESS_DIFF_MOVE,
	UPLOAD_END_PROCESS_DIFF_MOVE_TIME,
	UPLOAD_END_PROCESS_MAX
};

typedef struct global_config_t
{
	int affinity;
	int max_upload_num;
	int compress_buff_size;
	long upload_timeout;
	long connect_timeout;
	char log_dir_path[NEWIUP_PATH_LEN];
}global_config_s;


extern global_config_s global_config;

int ConfGetChildNumber(char *name);
int ConfNodeNameCheck(char *name);


extern int ThreadSetCPUAffinity(pthread_t pid, int cpuid);
extern int ThreadGetCPUAffinity(pthread_t pid, int cpuid);

#ifdef __cplusplus
}
#endif

#endif 
