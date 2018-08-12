
#ifndef __SERVERS_H__
#define __SERVERS_H__


#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <stdarg.h>


enum upload_protocol
{
	UPLOAD_PROT_FTP,
	UPLOAD_PROT_SFTP,
	UPLOAD_PROT_SCP,
	UPLOAD_PROT_MAX
};

enum upload_archive
{
	UPLOAD_ARCHIVE_DATE,
	UPLOAD_ARCHIVE_FULLPATH,
	UPLOAD_ARCHIVE_MAX
};

enum ftp_t_mode
{
	FTP_PORT,
	FTP_PASV
};

typedef struct real_server_
{
	int index;
	int make_citations;
	char name[NEWIUP_NAME_LEN];
	char addr[24];
	char username[NEWIUP_NAME_LEN];
	char passwd[32];
	int protocol;
	int ftp_mode;
	char finish_confirm[32];				//1: do not confirm 0: confirm
	char dir[NEWIUP_PATH_LEN];
	int retry_count;				//faile retry count
	int fail_sleep_time;
	int cur_rc;
	int cur_fst;
	int debug;		//0,is not debug
	char file_suffix[32];
	int thread_num;
	int compress;
	int archive;
	pthread_t *pid;
	queue_s queue;				//need upload file queue
	void *(*func)(void *);
}real_server;

extern real_server *g_server;
extern int g_server_num;
int servers_init(void);
int servers_exit(void);
int get_server_id_by_name(char *name);
real_server *get_server_by_index(int index);
real_server * get_server_by_name(char *name);
char * get_server_name_by_index(int index);
void set_upload_run_signal(int signal);
void server_upload_handle_pthread_start();
void server_upload_handle_pthread_end();
int get_server_compress_by_id(int index);
queue_s * get_server_queue_by_index(int index);
int get_server_available_state(int index);


#ifdef __cplusplus
}
#endif

#endif
