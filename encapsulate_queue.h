
#ifndef __ENCAPSULATE_QUEUE_H__
#define __ENCAPSULATE_QUEUE_H__


#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <stdarg.h>

#define QUEUE_EMPTY_SLEEP   10
#define WDQUEUE_PATH_LEN 512

typedef struct upload_queue_t
{
    int size;                     		//queue file number
    char name[32];
    pthread_spinlock_t lock;           //compress queue lock
    TAILQ_HEAD(,file_queue) head;       //file queue for upload
}queue_s;


typedef struct file_queue {
	int wd;                             //monitor handle number
	int monitor_id;
	int copy;
	int up_count_s;
	int re_count_s;
	int up_size;
	int *up_count;
	int *retain_count;
	char *monitorfile;
 	char *dstfile;
	char *src;
	char *src_com;
	char *sg_name;
	char *up_subpath;
	queue_s *sche_queue;
	TAILQ_ENTRY(file_queue) next;
} FileQueue;

int queue_node_push(queue_s *queue, FileQueue *node);
FileQueue *queue_node_pop(queue_s *queue);
void queue_node_free(FileQueue *node);
FileQueue * queue_node_add(int index, queue_s *queue, int wd, char *monitorfile, char *src);
FileQueue *queue_node_copy(FileQueue *oldnode);
int queue_node_is_retain(FileQueue *node);

typedef struct wd_node_t {
    int wd;                             //monitor dir fd ; defaul 0
    char path[WDQUEUE_PATH_LEN];                			//monitor dir path
	int suboffset;
	char *subpath;
	TAILQ_ENTRY(wd_node_t) next;
} Wd_node;
typedef struct wd_queue_t
{
    int size;                     		//queue file number
    TAILQ_HEAD(,wd_node_t) head;       //file queue for upload
}Wdqueue;


char *get_wd_path_by_wd(Wdqueue *head, int wd);
char *get_wd_subpath_by_wd(Wdqueue *head, int wd);
int get_wd_wd_by_path(Wdqueue *head, char *path);
int show_monitor_dir(int index, char *name, Wdqueue *head);
int del_monitor_dir(int fd, int index, char *name, Wdqueue *head, int wd, char *path);
int add_monitor_dir(int fd, int index, char *name,	unsigned int mask, Wdqueue *head, char *path, Wd_node *fnode);
Wd_node *get_wd_by_wd(Wdqueue *head,int wd);


#ifdef __cplusplus
}
#endif

#endif
