
#ifndef __MONITOR_H__
#define __MONITOR_H__

#include "compress.h"


enum type_process_s
{
	UPLOAD_TYPE_PROCESS_KEEP = 1,
	UPLOAD_TYPE_PROCESS_REPLACE,
	UPLOAD_TYPE_PROCESS_ADD,
	UPLOAD_TYPE_PROCESS_CUT,
	UPLOAD_TYPE_PROCESS_MAX
};
enum host_and_back_e
{
	UPLOAD_RSL_PRIME,
	UPLOAD_RSL_BACK,
	UPLOAD_RSL_MAX
};

typedef struct server_group_queue_t
{
	int rs_count_prime;
	int rs_count_back;
	int rr_queue_prime;
	int rr_queue_back;
	real_server **rs_prime;
	real_server **rs_back;
}server_group_queue;


typedef struct upload_monitor_group {
	int index;                          //group id
	int enable;
	char name[NEWIUP_NAME_LEN];         //group name
	char monitor_dir[NEWIUP_PATH_LEN];  //monitor root directory 
	char monitor_file_type[16];                  //monitor file type
	int monitor_to_upload_act;                   //1:keep 2:replace 3:add 4:cut
	char upload_file_type[16];       //upload new file type
	int finish_act;
	char move_to[NEWIUP_PATH_LEN];
	queue_s queue;				     //need upload file queue
	Wdqueue wd_list;
	pthread_t monitor_pid;           //monitor thread id
	pthread_t sche_pid;              //monitor thread id
	int sgl_count;
	server_group_queue *server_group_list;
	compress_group compress_cfg;
	int compress;
	
	struct _logdata upload_log;
	struct _logdata upload_txt;
} monitor_group;



int monitor_group_init(void);
int monitor_group_exit(void);

void monitor_handle_pthread_start();
void monitor_handle_pthread_end();
void set_monitor_run_signal(int signal);
char * get_monitor_name_by_id(int index);
int do_upload_file_finish_single(char * server_name,FileQueue * node);
void set_monitor_sche_debug_switch();

struct _logdata * get_monitor_logdata_log_by_id(int index);
struct _logdata * get_monitor_logdata_txt_by_id(int index);
int UPLOG_LOG(int monitor_id, const char *fmt, ...);
int UPLOG_TXT(int monitor_id, const char *fmt, ...);

#endif
