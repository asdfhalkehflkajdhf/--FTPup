
#include <pthread.h>
#include <string.h>
#include <sys/inotify.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/select.h>
#include "conf.h"

#include "logging.h"
#include "public.h"
#include "compress.h"
#include "utils.h"
#include "servers.h"
#include "monitor.h"

static monitor_group *g_monitorgroup = NULL;
static int g_monitorgroup_num = 0;

//return 1:keep 2:replace 3:add 3:cut -1:fail
static int get_type_process(char *str) 
{
    if (str == NULL || *str=='\0' )
    {
        return NEWIUP_FAIL;
    }

    if (!strncmp(str, "keep", strlen("keep")))
    {
        return UPLOAD_TYPE_PROCESS_KEEP; 
    }
    else if (!strncmp(str, "replace", strlen("replace")))
    {
        return UPLOAD_TYPE_PROCESS_REPLACE; 
    }
    else if (!strncmp(str, "add", strlen("add")))
    {
        return UPLOAD_TYPE_PROCESS_ADD; 
    }
    else if (!strncmp(str, "cut", strlen("cut")))
    {
        return UPLOAD_TYPE_PROCESS_CUT; 
    }

    return NEWIUP_FAIL;
}
static int show_upload_server_list(int count, server_group_queue *server_group_list)
{
	int j,i;
	real_server *node;

	logging(LOG_INFO, "server primer and backup info:\n");
	for(j=0;j<count; j++)
	{
		for(i=0; i<server_group_list[j].rs_count_prime; i++){
			node = server_group_list[j].rs_prime[i];
			if(node)
				logging(LOG_INFO, "\trs_name:%s, rs index:%d, primer\n",
					node->name, node->index); 
		}

		for(i=0; i<server_group_list[j].rs_count_back; i++)
		{
			node = server_group_list[j].rs_back[i];
			if(node)
				logging(LOG_INFO, "\trs_name:%s, rs index:%d, backup\n",
					node->name, node->index); 
		}
		logging(LOG_INFO, "\t----------------\n");
	}
	return NEWIUP_OK;
}

static server_group_queue * 
get_upload_group_server_group_list(
	char * primer, char *back, int *count)
{
	struct str_split split_primer={0, NULL, NULL};
	struct str_split split_back={0, NULL, NULL};
	struct str_split split_primer_g={0, NULL, NULL};
	struct str_split split_back_g={0, NULL, NULL};
	server_group_queue *head =NULL;
	int i=0, j=0;
	*count = 0;
	
	if(primer == NULL || *primer=='\0')
	{
		logging(LOG_ERROR, "don't servergroup primer is NULL error: --%s|%d\n",  __func__, __LINE__);
		return NULL;
	}

	str_split_func(&split_primer_g, primer, '|');
	str_split_func(&split_back_g, back, '|');
	if(0 != split_back_g.count 
		&& split_primer_g.count != split_back_g.count)
	{
		logging(LOG_ERROR, "servers-primer number != servers-backup number\n");
		logging(LOG_ERROR, "If the standby servers, must have the same number with the primary server\n");
		logging(LOG_ERROR, "For the server configuration cannot begin with a '|', and with the primary server number is not corresponding to the '|' completion\n");
		goto error;
	}

	head = (server_group_queue *)malloc(split_primer_g.count * sizeof(server_group_queue));
	if(head == NULL)
	{
		logging(LOG_ERROR, "don't servergroup malloc error: --%s|%d\n",  __func__, __LINE__);
		goto error;
	}
	memset(head, 0, split_primer_g.count * sizeof(server_group_queue));

	for(i=0; i<split_primer_g.count; i++)
	{

		str_split_func(&split_primer, split_primer_g.str_array[i], ',');
		if(split_primer.count) {
			head[i].rs_prime = (real_server **)malloc(split_primer.count * sizeof(real_server*));
			if(head[i].rs_prime== NULL)
			{
				logging(LOG_WARNING, "have real server is NULL: --%s|%d\n",  __func__, __LINE__);
				goto error;
			}
			head[i].rs_count_prime = split_primer.count;
			for(j=0;j<split_primer.count;j++){
				head[i].rs_prime[j] = get_server_by_name(split_primer.str_array[j]);
				if(!head[i].rs_prime[j]){
					logging(LOG_ERROR, "server %s is not find\n", split_primer.str_array[j]);
					goto error;
				}

			}
		}else
			goto error;

		str_split_free(&split_primer);
		
		if(split_back_g.count){
			str_split_func(&split_back, split_back_g.str_array[i], ',');
			if(split_back.count) {
				head[i].rs_back= (real_server **)malloc(split_primer.count * sizeof(real_server *));
				if(head[i].rs_back == NULL)
				{
					logging(LOG_WARNING, "have real server is NULL: --%s|%d\n",  __func__, __LINE__);
				}
				head[i].rs_count_back = split_back.count;
				for(j=0;j<split_primer.count;j++){
					if(split_back_g.str_array[j][0]=='-')
						head[i].rs_back[j] = NULL;
					else{
						head[i].rs_back[j] = get_server_by_name(split_back.str_array[j]);
						if(!head[i].rs_back[j]){
							logging(LOG_ERROR, "server %s is not find\n", split_primer.str_array[j]);
							goto error;
						}
					}
				}
			}
			str_split_free(&split_back);
		}


	}
	
	*count = split_primer_g.count;
	//logging(LOG_INFO, "head count:%d\n",*count);
error:
	str_split_free(&split_primer_g);
	str_split_free(&split_back_g);
	return head;
}

//return 1:same-move 2:keep 3:delete 4:same-move-time 5:diff-move 6:diff-move-time
//(same: same partition move-time: store to subdirectory by time)
int get_end_process_id(char *str) 
{
    if (str == NULL)
    {
        return NEWIUP_FAIL;
    }
    
    if (!strcmp(str, "same-move"))
    {
        return UPLOAD_END_PROCESS_SAME_MOVE;
    }
    else if (!strcmp(str, "keep"))
    {
        return UPLOAD_END_PROCESS_KEEP;
    }
    else if (!strcmp(str, "delete"))
    {
        return UPLOAD_END_PROCESS_DELETE;
    }
    else if (!strcmp(str, "same-move-time"))
    {
        return UPLOAD_END_PROCESS_SAME_MOVE_TIME;
    }
    else if (!strcmp(str, "diff-move"))
    {
        return UPLOAD_END_PROCESS_DIFF_MOVE;
    }
    else if (!strcmp(str, "diff-move-time"))
    {
        return UPLOAD_END_PROCESS_DIFF_MOVE_TIME;
    }

    return NEWIUP_FAIL; 
}
char * get_monitor_name_by_id(int index)
{
	return g_monitorgroup[index].name;
}
struct _logdata * get_monitor_logdata_log_by_id(int index)
{
	return &g_monitorgroup[index].upload_log;
}
struct _logdata * get_monitor_logdata_txt_by_id(int index)
{
	return &g_monitorgroup[index].upload_txt;
}

Wdqueue * get_monitor_wdqueue_by_id(int index)
{
	return &g_monitorgroup[index].wd_list;
}

char * get_monitor_move_to_by_id(int index)
{
	return g_monitorgroup[index].move_to;
}
int get_monitor_finish_act_by_id(int index)
{
	return g_monitorgroup[index].finish_act;
}

static int g_monitor_run_signal=NEWIUP_OK;
static int get_monitor_run_signal(void)
{
	return g_monitor_run_signal;
}
void set_monitor_run_signal(int signal)
{
	g_monitor_run_signal=signal;
	return ;
}


void get_upload_file_k_by_monitorfile(char *upload_file_k, char *monitorfile)
{
	snprintf(upload_file_k, NEWIUP_PATH_LEN, "%s-upload-k", monitorfile);
	return;
}


//return 0:sucess -1:fail
static int get_monitor_group_info(void)
{
    logging(LOG_INFO, "-- %s\n", __func__);
    int i;
    ConfNode *child = NULL;
    ConfNode *node = NULL;
    char *str = NULL;
    char *str2 = NULL;
    uint32_t count = 0;

    node = ConfGetNode("monitor-group");
    TAILQ_FOREACH(child, &node->head, next) 
    {

		g_monitorgroup[count].index = count; 
		if(ConfNodeNameCheck(child->val)){
            logging(LOG_ERROR, "--%s [%s]-[name] Name repetition error\n", __func__, child->val);
            return NEWIUP_FAIL;
		}

		strncpy(g_monitorgroup[count].name, child->val, NEWIUP_NAME_LEN); 
		if (ConfGetChildValue(child, "enable", &str) == 0)
		{
			g_monitorgroup[count].enable= NEWIUP_OK; 
		}else if(strcmp(str, "yes")== 0 || strcmp(str, "y")== 0){
			g_monitorgroup[count].enable= NEWIUP_OK; 
		}else if(strcmp(str, "no")== 0 || strcmp(str, "n")== 0){
			g_monitorgroup[count].enable= NEWIUP_FAIL; 
		}else{
		    logging(LOG_ERROR, "--%s [%s]-[enable] error: not support %s [yes/no]\n", __func__, child->val, str);
		    return NEWIUP_FAIL;
		}
		//monitor-dir
        if (ConfGetChildValue(child, "monitor-dir", &str) == 0)
        {
            logging(LOG_ERROR, "--%s [%s]-[monitor-dir] error\n", __func__, child->val);
            return NEWIUP_FAIL;
        }
		strncpy(g_monitorgroup[count].monitor_dir, str, NEWIUP_PATH_LEN); 
		//file-type
		if (ConfGetChildValue(child, "monitor-file-type", &str) == 0)
		{
		    logging(LOG_ERROR, "--%s [%s]-[monitor-file-type] error\n", __func__, child->val);
		    return NEWIUP_FAIL;
		}
		strncpy(g_monitorgroup[count].monitor_file_type, str, sizeof(g_monitorgroup[count].monitor_file_type)); 

		//upload-find-act
		if (ConfGetChildValue(child, "upload-find-act", &str) == 0)
		{
		    logging(LOG_ERROR, "--%s [%s]-[upload-find-act] error\n", __func__, child->val);
		    return NEWIUP_FAIL;
		}
		g_monitorgroup[count].monitor_to_upload_act = get_type_process(str);
		if (g_monitorgroup[count].monitor_to_upload_act == NEWIUP_FAIL)
		{
		    logging(LOG_ERROR, "--%s [%s]-[upload-find-act] error: not support %s [keep/replace/add/cut]\n", __func__, child->val, str);
		    return NEWIUP_FAIL;
		}

		//new_type
		if (ConfGetChildValue(child, "upload-file-type", &str) == 0)
		{
		    logging(LOG_ERROR, "--%s [%s]-[upload-file-type] error\n", __func__, child->val);
		    return NEWIUP_FAIL;
		}
		strncpy(g_monitorgroup[count].upload_file_type, str, sizeof(g_monitorgroup[count].upload_file_type)); 
		//compress-type
		if (ConfGetChildValue(child, "compress-type", &str) == 0)
		{
			logging(LOG_ERROR, "--%s [%s]-[compress-type] error\n", __func__, child->val);
			return NEWIUP_FAIL;
		}
		g_monitorgroup[count].compress_cfg.compress_type= get_compress_type_id(str); 
		if (g_monitorgroup[count].compress_cfg.compress_type == NEWIUP_FAIL)
		{
			logging(LOG_ERROR, "--%s [%s]-[compress-type] error: not support %s \n", __func__, child->val, str);
			return NEWIUP_FAIL;
		}
		
		//compress_pcount
		if (ConfGetChildValue(child, "compress-pcount", &str) == 0)
		{
			logging(LOG_ERROR, "--%s [%s]-[compress-pcount] error\n", __func__, child->val);
			return NEWIUP_FAIL;
		}
		g_monitorgroup[count].compress_cfg.compress_pcount = atoi(str);
		if (g_monitorgroup[count].compress_cfg.compress_pcount < 1 
			|| g_monitorgroup[count].compress_cfg.compress_pcount > 15) {
			logging(LOG_ERROR, "--%s [%s]-[compress-pcount] error: not support %s [1-15]\n", __func__, child->val, str);
			return NEWIUP_FAIL;
		}
		
		g_monitorgroup[count].compress_cfg.pid= (pthread_t *)malloc(sizeof(pthread_t)*g_monitorgroup[count].compress_cfg.compress_pcount);
		if(g_monitorgroup[count].compress_cfg.pid == NULL){
			logging(LOG_ERROR, "--%s [%s]-[compress-pcount] error: pid malloc\n", __func__, child->val);
			return NEWIUP_FAIL;
		}
		//move
		if (ConfGetChildValue(child, "finish-act", &str) == 0)
		{
			logging(LOG_ERROR, "--%s [%s]-[finish-act] error\n", __func__, child->val);
			return NEWIUP_FAIL;
		}
		g_monitorgroup[count].finish_act= get_end_process_id(str);
		if (g_monitorgroup[count].finish_act== NEWIUP_FAIL)
		{
		   logging(LOG_ERROR, "--%s [%s]-[finish-act] error: not support %s\n", __func__, child->val, str);
			return NEWIUP_FAIL; 
		}
		
		
		//move-to
		if (ConfGetChildValue(child, "move-to", &str) == 0)
		{
			logging(LOG_ERROR, "--%s [%s]-[move-to] error\n", __func__, child->val);
			return NEWIUP_FAIL;
		}
		if((g_monitorgroup[count].finish_act != UPLOAD_END_PROCESS_DELETE
			&&g_monitorgroup[count].finish_act != UPLOAD_END_PROCESS_KEEP)
			&& strlen(str) == 0){
			logging(LOG_ERROR, "--%s [%s]-[move-to] error: is empty \n", __func__, child->val);
			return NEWIUP_FAIL;
		}
		if(g_monitorgroup[count].finish_act == UPLOAD_END_PROCESS_KEEP 
			&& g_monitorgroup[count].monitor_to_upload_act == UPLOAD_TYPE_PROCESS_KEEP){
			logging(LOG_ERROR, "--%s [%s]-[finish-act] and [upload-find-act] is not at same time KEEP\n", __func__, child->val);
			return NEWIUP_FAIL;
		}
		strcpy(g_monitorgroup[count].move_to, str);

		//servers-primer backup
		if (ConfGetChildValue(child, "servers-primer", &str) == 0)
		{
			logging(LOG_ERROR, "--%s [%s]-[servers-primer] error\n", __func__, child->val);    
			return NEWIUP_FAIL;
		}
		
		if (ConfGetChildValue(child, "servers-backup", &str2) == 0)
		{
			logging(LOG_ERROR, "--%s [%s]-[servers-backup] error\n", __func__, child->val);  
			return NEWIUP_FAIL;
		}
		g_monitorgroup[count].server_group_list = get_upload_group_server_group_list(str, str2, &g_monitorgroup[count].sgl_count);
		if(g_monitorgroup[count].server_group_list == NULL 
			|| g_monitorgroup[count].sgl_count == 0)
		{
			logging(LOG_ERROR, "--%s [%s]-[servers-primer or servers-backup] error\n", __func__, child->val);	 
			return NEWIUP_FAIL;
		}
		//sech queue
		pthread_spin_init(&g_monitorgroup[count].queue.lock, PTHREAD_PROCESS_PRIVATE);
		g_monitorgroup[count].queue.size=0;
		TAILQ_INIT(&g_monitorgroup[count].queue.head);
		strcpy(g_monitorgroup[count].queue.name, g_monitorgroup[count].name);

		//compress queue
		pthread_spin_init(&g_monitorgroup[count].compress_cfg.queue.lock, PTHREAD_PROCESS_PRIVATE);
		g_monitorgroup[count].compress_cfg.queue.size=0;
		TAILQ_INIT(&g_monitorgroup[count].compress_cfg.queue.head);
		strcpy(g_monitorgroup[count].compress_cfg.queue.name, g_monitorgroup[count].name);
		g_monitorgroup[count].compress_cfg.pid=(pthread_t *)malloc(sizeof(pthread_t) *g_monitorgroup[count].compress_cfg.compress_pcount);
		if(g_monitorgroup[count].compress_cfg.pid == NULL){
			logging(LOG_ERROR, "--%s [%s]-malloc error\n",__func__, g_monitorgroup[count].name);
			return NEWIUP_FAIL;
		}
		g_monitorgroup[count].compress_cfg.index = count;
		g_monitorgroup[count].compress_cfg.name = g_monitorgroup[count].name;
		g_monitorgroup[count].compress = NEWIUP_FAIL;

		//monitor queue
		g_monitorgroup[count].wd_list.size= 0;
		TAILQ_INIT(&g_monitorgroup[count].wd_list.head);

		//log init
		log_init(&g_monitorgroup[count].upload_log, global_config.log_dir_path);
		log_init(&g_monitorgroup[count].upload_txt, global_config.log_dir_path);
		
		count++;
    }
    for (i = 0; i < g_monitorgroup_num; i++)
    {
        logging(LOG_INFO, "-- %s %d\n", g_monitorgroup[i].name, g_monitorgroup[i].index); 
        logging(LOG_INFO, "index            =%d\n", g_monitorgroup[i].index); 
        logging(LOG_INFO, "enable           =%d[0:yes/-1:no]\n", g_monitorgroup[i].enable); 
        logging(LOG_INFO, "name             =%s\n", g_monitorgroup[i].name); 
        logging(LOG_INFO, "monitor_dir      =%s\n", g_monitorgroup[i].monitor_dir); 
        logging(LOG_INFO, "monitor-file-type=%s\n", g_monitorgroup[i].monitor_file_type); 
        logging(LOG_INFO, "upload_find_act  =%d[1:keep/2:replace/3:add/4:cut]\n", g_monitorgroup[i].monitor_to_upload_act); 
        logging(LOG_INFO, "upload-file-type =%s\n", g_monitorgroup[i].upload_file_type); 
        logging(LOG_INFO, "finish_act       =%d[1:same-move/2:keep/3:delete/4:same-move-time/5:diff-move/6:diff-move-time]\n", 
			g_monitorgroup[i].finish_act); 
        logging(LOG_INFO, "move-to          =%s\n", g_monitorgroup[i].move_to); 
        logging(LOG_INFO, "compress_pcount  =%d[1-15]\n", g_monitorgroup[i].compress_cfg.compress_pcount); 
        logging(LOG_INFO, "compress_type    =%d[gz/bz/bz2/Z/zip/7z/lzma/tar/tar.gz/tar.bz/tar.bz2/tar.Z/]\n", 
			g_monitorgroup[i].compress_cfg.compress_type); 
			
			show_upload_server_list(g_monitorgroup[i].sgl_count,g_monitorgroup[i].server_group_list);
			logging(LOG_INFO, "-- end\n"); 
    }
	logging(LOG_INFO, "++++++++++++++++++++\n");

    return 0;
} 


//return group_number:sucess -1:fail
int ConfGetChildNumber(char *name)
{
    ConfNode *child = NULL;
    ConfNode *node = NULL;
    uint32_t count = 0;

    node = ConfGetNode(name);
    TAILQ_FOREACH(child, &node->head, next) 
    {
        count++;
    }

    return count;
} 


//return 0:sucess -1:fail(exit)
int monitor_group_init(void)
{
	logging(LOG_INFO, "++++++++++++++++++++\n");
    g_monitorgroup_num = ConfGetChildNumber("monitor-group");
    logging(LOG_INFO, "%s: monitor-group count: %d\n", __func__, g_monitorgroup_num);
    if (g_monitorgroup_num <= 0)
    {
        logging(LOG_ERROR, " --%s get [monitor_group_number] error(exit)\n", __func__);
        return NEWIUP_FAIL;
    }   
        
    g_monitorgroup = (monitor_group *)malloc(sizeof(monitor_group) * g_monitorgroup_num);
    if (g_monitorgroup == NULL)
    {
        logging(LOG_ERROR, " --%s malloc groups error(exit)\n", __func__);
        return NEWIUP_FAIL;
    }   

	memset(g_monitorgroup, 0, sizeof(monitor_group) * g_monitorgroup_num);
	int ret = get_monitor_group_info();
    if (ret != 0)
    {
        monitor_group_exit();
        return NEWIUP_FAIL;
    }   
    
    return NEWIUP_OK;
}   

int monitor_group_exit(void)
{
    int i;
    if (g_monitorgroup == NULL)
    {
        return NEWIUP_OK;
    }

    for (i=0; i<g_monitorgroup_num; i++)
    {
		Wd_node *wd_node = NULL;
        while ((wd_node = TAILQ_FIRST(&g_monitorgroup[i].wd_list.head)) != NULL)
        {
            TAILQ_REMOVE(&g_monitorgroup[i].wd_list.head, wd_node, next);
			free(wd_node);
            wd_node = NULL;
        }
		
		FileQueue *node = NULL;
        while ((node = TAILQ_FIRST(&g_monitorgroup[i].queue.head)) != NULL)
        {
            TAILQ_REMOVE(&g_monitorgroup[i].queue.head, node, next);
			queue_node_free(node);
        }
		
        while ((node = TAILQ_FIRST(&g_monitorgroup[i].compress_cfg.queue.head)) != NULL)
        {
            TAILQ_REMOVE(&g_monitorgroup[i].compress_cfg.queue.head, node, next);
			queue_node_free(node);
        }
		
		log_exit(&g_monitorgroup[i].upload_log);
		log_exit(&g_monitorgroup[i].upload_txt);
    }
	free(g_monitorgroup);
	g_monitorgroup = NULL;
	g_monitorgroup_num = 0;
	//logging(LOG_INFO, "%s OK\n", __func__); 
    return NEWIUP_OK;    
}

int UPLOG_LOG(int monitor_id, const char *fmt, ...)
{
    char buff[2048] = {0};
    va_list args;
    va_start(args, fmt);
    vsnprintf(buff, sizeof(buff), fmt, args);
    va_end(args);
	struct _logdata *log=get_monitor_logdata_log_by_id(monitor_id);

    char date[24] = {0};
    char date2[32] = {0};
    time_t tm = time(NULL);
    strftime(date, sizeof(date), "[%m/%d-%H:%M:%S]", localtime(&tm));
    strftime(date2, 32, FORMAT, localtime(&tm));

    pthread_spin_lock(&log->lock);
    if (NULL == log->fd || strcmp(date2, log->date)!=0)
    {
    	if(NULL != log->fd)
	        fclose(log->fd);
        log->fd = NULL;
		char *monitor_name= get_monitor_name_by_id(monitor_id);
        strcpy(log->date, date2);
		snprintf(log->name, sizeof(log->name), "upload_%s%s.log", monitor_name, log->date);
		char new_path[4096] = {'\0'};
		snprintf(new_path, 4096, "%s/%s", log->path, log->name);
        log->fd = fopen(new_path, "a+");
        if (NULL == log->fd)
        {
            log_printf(LOG_ERROR, "%s: logging file open error: %s\n", __func__, new_path);
            exit (0);
        }
    }
	
    fprintf(log->fd, "%s %s",date, buff);
    fflush(log->fd);
    pthread_spin_unlock(&log->lock);

    return 0;
}

int UPLOG_TXT(int monitor_id, const char *fmt, ...)
{
    char buff[2048] = {0};
    va_list args;
    va_start(args, fmt);
    vsnprintf(buff, sizeof(buff), fmt, args);
    va_end(args);
	struct _logdata *log=get_monitor_logdata_txt_by_id(monitor_id);

    char date[24] = {0};
    char date2[32] = {0};
    time_t tm = time(NULL);
    strftime(date, sizeof(date), "[%m/%d-%H:%M:%S]", localtime(&tm));
    strftime(date2, 32, FORMAT, localtime(&tm));

    pthread_spin_lock(&log->lock);
    if (NULL == log->fd || strcmp(date2, log->date))
    {
    	if(NULL != log->fd)
        	fclose(log->fd);
        log->fd = NULL;
		char *monitor_name= get_monitor_name_by_id(monitor_id);
        strcpy(log->date, date2);
		snprintf(log->name, sizeof(log->name), "upload_%s%s.txt", monitor_name, log->date);
		char new_path[4096] = {'\0'};
		snprintf(new_path, sizeof(new_path), "%s/%s", log->path, log->name);
        log->fd = fopen(new_path, "a+");
        if (NULL == log->fd)
        {
            log_printf(LOG_ERROR, "%s: logging file open error: %s\n", __func__, new_path);
            exit (0);
        }
    }
	
    fprintf(log->fd, "%s %s",date, buff);
    fflush(log->fd);
    pthread_spin_unlock(&log->lock);

    return 0;
}

#define NAME_LEN 512
#define EVENT_SIZE              ( sizeof (struct inotify_event) )
#define EVENT_BUF_LEN           ( 8 * ( EVENT_SIZE + NAME_LEN ) )
#define NEWIUP_QUEUE_SIZE_MAX 100

//return 0:yes -1:no
int is_monitor_type(char *filename, char *type) 
{
    char *ptr = NULL;    
    ptr = rindex(filename, '.');
    if (ptr == NULL)
    {
        return -1;
    }

    if ( strcmp(ptr+1, type) )
    {
        return -1;    
    }
    return 0;
}
char * add_file_suffix_process(int monitor_gid, char *monitorfile, char *dstfile)
{
	char temp;
	char *ptr=NULL;
	int new_path_len;
	int type_process = g_monitorgroup[monitor_gid].monitor_to_upload_act;
	switch(type_process)
	{
		case UPLOAD_TYPE_PROCESS_KEEP:
			strcpy(dstfile, monitorfile);
			break;
		case UPLOAD_TYPE_PROCESS_ADD:
			new_path_len = strlen(monitorfile)+1+strlen(g_monitorgroup[monitor_gid].upload_file_type);
			snprintf(dstfile, new_path_len+1, "%s.%s",monitorfile, g_monitorgroup[monitor_gid].upload_file_type);
			break;
		case UPLOAD_TYPE_PROCESS_CUT:
			ptr = rindex(monitorfile, '.');
			temp = *ptr; *ptr = '\0';
			strcpy(dstfile, monitorfile);
			*ptr = temp;
			break;
		case UPLOAD_TYPE_PROCESS_REPLACE:
			ptr = rindex(monitorfile, '.');
			temp = *ptr; *ptr = '\0';
			new_path_len = strlen(monitorfile)+1+strlen(g_monitorgroup[monitor_gid].upload_file_type);
			snprintf(dstfile, new_path_len+1, "%s.%s",monitorfile, g_monitorgroup[monitor_gid].upload_file_type);
			*ptr = temp;
			break;
		default:
			break;
	}
	return dstfile;
}


static int add_upload_file(int monitor_gid, char *path)
{
    int wd = get_wd_wd_by_path(&g_monitorgroup[monitor_gid].wd_list, path);
    if (wd < 0)
    {
        logging(LOG_ERROR, "get monitor dir [%s] error --%s|%d\n", path, __func__, __LINE__);
        exit (-1);
    }

    DIR *pdir = opendir(path);
    if (pdir == NULL)
    {
        logging(LOG_ERROR, "opendir:%s error\n", path);
        exit (-1);    
    }

    char monitorfile[NEWIUP_PATH_LEN] = {0};
    struct dirent *pdirent = NULL; 
    while ((pdirent = readdir(pdir)) != NULL)
    {
        if (pdirent->d_name[0] == '.')
		{
			continue;
		}

        snprintf(monitorfile, sizeof(monitorfile), "%s/%s", path, pdirent->d_name);

		char dstfile[NEWIUP_PATH_LEN] = {0};
		if(!is_file(monitorfile))
		{
			if (is_monitor_type(monitorfile, g_monitorgroup[monitor_gid].monitor_file_type) != 0)
			{
				continue;
			}
			add_file_suffix_process(monitor_gid,monitorfile, dstfile);
			queue_node_add(monitor_gid, &g_monitorgroup[monitor_gid].queue, wd, monitorfile, dstfile);
			/*
				logging(LOG_INFO, "++add to %s sche|queue_size=%d wd=%d %s\n",
					g_monitorgroup[monitor_gid].name,
					g_monitorgroup[monitor_gid].queue.size, wd, dstfile);
		*/	
		}
		else if (is_dir(monitorfile) == 0)
		{
			add_upload_file(monitor_gid, monitorfile);
		}
    }

    closedir(pdir);

    return NEWIUP_OK;
}

static void *monitor_handle(void *arg)
{
	int index=*((int *)arg);

    int fd;
    int ret;
	int offset;
    char buffer[EVENT_BUF_LEN];
    int length;
    struct inotify_event *event; 

	fd= inotify_init();
    if (fd < 0)
    {
        logging(LOG_ERROR, "--%s inotify_init error(exit) |%d\n", __func__, __LINE__);
        exit (-1);
    }
	ThreadGetCPUAffinity(pthread_self(), global_config.affinity);

    add_monitor_dir(fd, index, g_monitorgroup[index].name,
		IN_CREATE|IN_MOVED_TO|IN_MOVED_FROM|IN_DELETE|IN_DELETE_SELF,
		&g_monitorgroup[index].wd_list ,g_monitorgroup[index].monitor_dir, NULL);
	show_monitor_dir(index, g_monitorgroup[index].name,&g_monitorgroup[index].wd_list);
	
    add_upload_file(index, g_monitorgroup[index].monitor_dir);

    fd_set fd_sets;
	struct timeval tv;
	char monitorfile[NEWIUP_PATH_LEN] = {0};
	char dstfile[NEWIUP_PATH_LEN] = {0};

    logging(LOG_INFO, "MONITOR [%s] thread start\n", g_monitorgroup[index].name);
    while (get_monitor_run_signal()== NEWIUP_OK)
    {
		tv.tv_sec=3;
		tv.tv_usec=10;
		FD_ZERO(&fd_sets);
        FD_SET(fd,&fd_sets);
        ret = select(fd+1, &fd_sets, NULL, NULL, &tv);
        if (ret <= 0 || !FD_ISSET(fd, &fd_sets))
		{
			continue;
		}

        length = read(fd, buffer, EVENT_BUF_LEN);
        if (length < 0)
        {
            logging(LOG_ERROR, "--%s read error |%d\n", __func__, __LINE__);
            continue;
        }
        offset = 0;
        while (offset < length)
        {
            event = (struct inotify_event*)(buffer + offset); 
            if (event->len == 0 )
            {
				offset += EVENT_SIZE + event->len;
            	continue;
            }
            snprintf(monitorfile, sizeof(monitorfile), "%s/%s", 
				get_wd_path_by_wd(&g_monitorgroup[index].wd_list, event->wd), event->name);

			if(!is_file(monitorfile) && ((event->mask & IN_CREATE) || (event->mask & IN_MOVED_TO)))
			{
				if (is_monitor_type(monitorfile, g_monitorgroup[index].monitor_file_type) != 0)
				{
					offset += EVENT_SIZE + event->len;
					continue;
				}

				add_file_suffix_process(index,monitorfile, dstfile);

				queue_node_add(index, &g_monitorgroup[index].queue, event->wd, monitorfile, dstfile);
				/**/
					logging(LOG_INFO, "++add to %s | queue_size=%d wd=%d %s\n",
						g_monitorgroup[index].name,
						g_monitorgroup[index].queue.size, event->wd, dstfile);
				
			}
			else if (event->mask & IN_ISDIR)
			{
				if((event->mask & IN_CREATE) || (event->mask & IN_MOVED_TO))
				{
					add_monitor_dir(fd, index, g_monitorgroup[index].name,
						IN_CREATE|IN_MOVED_TO|IN_MOVED_FROM|IN_DELETE|IN_DELETE_SELF,
						&g_monitorgroup[index].wd_list ,monitorfile, get_wd_by_wd(&g_monitorgroup[index].wd_list,event->wd));
					//When can have add directory, have to monitor file, and skipped. Need to add scan files
					add_upload_file(index, monitorfile);
					show_monitor_dir(index, g_monitorgroup[index].name,&g_monitorgroup[index].wd_list);
				}
				else if ((event->mask & IN_DELETE) || (event->mask & IN_DELETE_SELF) || (event->mask & IN_MOVED_FROM))
				{
					del_monitor_dir(fd, index, g_monitorgroup[index].name,&g_monitorgroup[index].wd_list, event->wd, monitorfile);
					show_monitor_dir(index, g_monitorgroup[index].name,&g_monitorgroup[index].wd_list);
				}
			}
			//To speed up the exit thread, prevent the select waiting for long time
			if(get_monitor_run_signal()!= NEWIUP_OK){
				logging(LOG_INFO, "MONITOR [%s] thread exit\n", g_monitorgroup[index].name);
				pthread_exit(NULL);
			}
            offset += EVENT_SIZE + event->len;
        }
    }

    logging(LOG_INFO, "MONITOR [%s] thread exit\n", g_monitorgroup[index].name);
	pthread_exit(NULL);
}

volatile int upload_count = 0;               //upload file count number/max number
#define SAME_PARTITION 0
#define DIFF_PARTITION 1

//note: Only each new group of the first node of the service
// cmcc2104123.ctr.minitor_gname.server_name
int create_upload_state_file(char *moni_name, char *monitorfile, int sgl_count, server_group_queue *server_group_list)
{
	char filepath[NEWIUP_PATH_LEN]={0};
	int i;
	real_server *server;
	if(server_group_list == NULL)
	{
		return NEWIUP_FAIL;
	}
	for(i=0; i<sgl_count; i++)
	{
		server = server_group_list[i].rs_prime[0];
		snprintf(filepath, NEWIUP_PATH_LEN, "%s-%s-%s",
			monitorfile, moni_name, server->name);
		create_file(filepath);
	}
	get_upload_file_k_by_monitorfile(filepath, monitorfile);
	create_file(filepath);
	return NEWIUP_OK;
}

static int store_by_time(int monitor_id, char *name, char *oldpath, char *dir, int flag) 
{
    char *ptr = rindex(oldpath, '/');    

    char date[24] = {0};
    time_t tm = time(NULL);
    strftime(date, sizeof(date), "%Y%m%d", localtime(&tm));

    char newdir[NEWIUP_PATH_LEN] = {0};
    snprintf(newdir, sizeof(newdir), "%s/%s", dir, date);

    if (recursive_make_dir(newdir, DIR_MODE) != 0)
    {
        logging(LOG_ERROR, " move_to create directory error 1:%s \n", newdir);
        return -1;
    }

    char newpath[NEWIUP_PATH_LEN] = {0};
    snprintf(newpath, sizeof(newpath), "%s/%s", newdir, ptr+1);

    if (flag == SAME_PARTITION)
    {
        rename(oldpath, newpath);
		UPLOG_LOG(monitor_id, "[FINISH] %s same-move-time %s --> %s\n", name, oldpath, newpath);
	}
	else  
	{
		move_file(oldpath, newpath);
		UPLOG_LOG(monitor_id, "[FINISH] %s diff-move-time %s --> %s\n", name, oldpath, newpath);
	}
    
    return 0;
}

static int do_move_to(int monitor_id, char *name, int act, char *move_to, char *fullpath)
{
    //1:same-move 2:keep 3:delete 4:same->move-time 5:diff-move 6:diff-move-time
    
    char newpath[NEWIUP_PATH_LEN] = {0};
    char *ptr = rindex(fullpath, '/');
	if(act== UPLOAD_END_PROCESS_KEEP){
		return NEWIUP_OK;
	}
    snprintf(newpath, sizeof(newpath), "%s/%s", move_to, ptr+1);
    if ((act!= UPLOAD_END_PROCESS_DELETE) 
		&& (act != UPLOAD_END_PROCESS_SAME_MOVE_TIME) 
		&& (is_dir(move_to) != 0))
    {
        if (recursive_make_dir(move_to, DIR_MODE) != 0)
        {
            logging(LOG_ERROR, " move_to create directory error2:%s \n", move_to);
            return NEWIUP_FAIL;
        }
    }

    if (act == UPLOAD_END_PROCESS_SAME_MOVE)
    {
        rename(fullpath, newpath); 
        UPLOG_LOG(monitor_id, "[FINISH] %s same-move %s --> %s\n", name, fullpath, newpath);
    }
    else if (act == UPLOAD_END_PROCESS_DELETE)
    {
        remove(fullpath);    
        UPLOG_LOG(monitor_id, "[FINISH] %s delete %s\n", name, fullpath);
    }
    else if (act == UPLOAD_END_PROCESS_SAME_MOVE_TIME)
    {
        store_by_time(monitor_id, name, fullpath, move_to, SAME_PARTITION); 
    }
    else if (act == UPLOAD_END_PROCESS_DIFF_MOVE)
    {
        UPLOG_LOG(monitor_id, "[FINISH] %s diff-move %s --> %s\n", name, fullpath, newpath);
        move_file(fullpath, newpath);    
    }
    else if (act == UPLOAD_END_PROCESS_DIFF_MOVE_TIME)
    {
        store_by_time(monitor_id, name, fullpath, move_to, DIFF_PARTITION); 
    }else {
        UPLOG_LOG(monitor_id, "[FINISH] %s move error %d \n", name, act);
	}

    return NEWIUP_OK;
}

static int do_upload_file_limit()
{
	if (0 != global_config.max_upload_num
		&& __sync_fetch_and_add(&upload_count, 1) > global_config.max_upload_num)
	{
		set_monitor_run_signal(NEWIUP_FAIL);
		set_compress_run_signal(NEWIUP_FAIL);
		set_upload_run_signal(NEWIUP_FAIL);
	
		logging(LOG_INFO, "reach max-upload-count:%d exit \n", upload_count);
	}
	return NEWIUP_OK;
}

int do_upload_file_finish_all(FileQueue *node)
{
	char filepath[NEWIUP_PATH_LEN];
	char *move_to = get_monitor_move_to_by_id(node->monitor_id);
	int finish_act = get_monitor_finish_act_by_id(node->monitor_id);
	//the file upload sucess and Must first deal with the source file
	if(node->src){
		do_move_to(node->monitor_id, node->sg_name, finish_act, move_to, node->src);
	}
	if(file_exists_state(node->src_com) == 0){
		delete_file(node->src_com);
		UPLOG_LOG(node->monitor_id, "[FINISH] %s delete upload compress file: %s\n", node->sg_name, node->src_com);
	}
	
	delete_file(node->monitorfile);
	UPLOG_LOG(node->monitor_id, "[FINISH] %s delete upload monitor file: %s\n", node->sg_name, node->monitorfile);
	
	do_upload_file_limit();
	
	get_upload_file_k_by_monitorfile(filepath,node->monitorfile);
	delete_file(filepath);
	get_compress_file_k_by_monitorfile(filepath,node->monitorfile);
	if(file_exists_state(filepath) == 0){
		delete_file(filepath);
	}
	
	queue_node_free(node);
	return NEWIUP_OK;
}

int do_upload_file_finish_single(char *server_name, FileQueue *node)
{
	char filepath[NEWIUP_PATH_LEN]={0};
	char *monitor_name=get_monitor_name_by_id(node->monitor_id);
	
	snprintf(filepath, NEWIUP_PATH_LEN, "%s-%s-%s",node->monitorfile, monitor_name, node->sg_name);
	delete_file(filepath);
	UPLOG_LOG(node->monitor_id, "[FINISH] %s delete upload flag file: %s\n", server_name, filepath);

	return NEWIUP_OK;
}
/*
return NEWIUP_FAIL: state file is exists
return NEWIUP_OK  : state file is not exists . (That has been uploaded)
*/
int do_upload_state_file_check(char *name, char *monitorfile, int sgl_count, server_group_queue *server_group_list)
{
	char filepath[NEWIUP_PATH_LEN]={0};
	real_server *server;
	if(server_group_list == NULL)
	{
		return NEWIUP_FAIL;
	}
	if(sgl_count == 0){
		server = server_group_list->rs_prime[0];
		snprintf(filepath, NEWIUP_PATH_LEN, "%s-%s-%s",
			monitorfile, name, server->name);
		if(file_exists_state(filepath) == NEWIUP_OK){
			//is exists return -1
			return NEWIUP_FAIL;
		}
	}else{
		int i;
		for(i=0; i<sgl_count; i++)
		{
			server = server_group_list[i].rs_prime[0];
			snprintf(filepath, NEWIUP_PATH_LEN, "%s-%s-%s",
				monitorfile, name, server->name);
			if(file_exists_state(filepath) == NEWIUP_OK){
				//is exists return -1
				return NEWIUP_FAIL;
			}
		}
	}
	return NEWIUP_OK;
}
static queue_s *  upload_real_server_sche_rr(server_group_queue *cur_sgl, int type)
{
	int i;
	int server_count = cur_sgl->rs_count_prime;
	int *rr_queue = &cur_sgl->rr_queue_prime;
	queue_s * queue = NULL;
	queue_s * re_queue = NULL;
	int queue_size_max=10000000;
	int cur_index = 0;
	real_server **rserver=cur_sgl->rs_prime;
	if(UPLOAD_RSL_PRIME != type)
	{
		server_count = cur_sgl->rs_count_back;
		rr_queue = &cur_sgl->rr_queue_back;
		rserver = cur_sgl->rs_back;
	}
	if(server_count != 0){
		cur_index = (__sync_fetch_and_add(rr_queue, 1))%server_count;
	}
	if(rserver == NULL)
		return NULL;
	for(i=0; i<server_count; i++)
	{
		if(rserver[i] == NULL)
			continue;
		if(get_server_available_state(rserver[i]->index)==NEWIUP_OK){
			queue = get_server_queue_by_index(rserver[i]->index);
			if(queue_size_max > queue->size){
				queue_size_max = queue->size;
				re_queue = queue;
			} 
			//return rs_index;
		}
		cur_index = (cur_index+1) % server_count;
	}

	return re_queue;
}
static int sche_debug_switch = 0;
void set_monitor_sche_debug_switch()
{
	sche_debug_switch = 100;
}
void *sche_handle(void *arg)
{
	int index=*((int *)arg);
	
	FileQueue *node = NULL;
	int ret;
	int i;
	char filename[NEWIUP_PATH_LEN]={0};
	int sgl_count = g_monitorgroup[index].sgl_count;
	server_group_queue *sgl = g_monitorgroup[index].server_group_list;
	FileQueue *newnode=NULL;
	queue_s * queue = NULL;

    logging(LOG_INFO, "SRCH [%s] thread start(The same group and id, data is load)\n",
		g_monitorgroup[index].name);

    while (get_monitor_run_signal() == NEWIUP_OK)
    {
		//When all servers bad, 100% CPU utilization
		usleep(1);
        node = queue_node_pop(&g_monitorgroup[index].queue);
        if (node == NULL)
        {
            sleep (QUEUE_EMPTY_SLEEP);
            continue;
        }

		if(queue_node_is_retain(node)){
			queue_node_push(node->sche_queue, node);
			continue;
		}
		
		if(file_exists_state(node->src) != 0){
            logging(LOG_WARNING, "%s file [%s] not exist, drop and process next.\n",
		   		g_monitorgroup[index].name,node->src);
			queue_node_free(node);
			continue;
		}

		get_upload_file_k_by_monitorfile(filename, node->monitorfile);
		if(file_exists_state(filename) != NEWIUP_OK){
			create_upload_state_file(g_monitorgroup[index].name, node->monitorfile, sgl_count, sgl);
		}else{
			ret = do_upload_state_file_check(g_monitorgroup[index].name, node->monitorfile,sgl_count, sgl);
			if(ret == NEWIUP_OK){
				do_upload_file_finish_all( node);
				continue;
			}
		}
		
		//get compress dest file name and fullpath
		if(g_monitorgroup[index].compress == NEWIUP_OK){
			char compress_file_k[NEWIUP_PATH_LEN]={0};			
			if(node->src_com == NULL){
				char compress_file[NEWIUP_PATH_LEN]={0};
				snprintf(compress_file, NEWIUP_PATH_LEN, "%s.%s", 
					node->src, get_compress_suffix_by_id(g_monitorgroup[index].compress_cfg.compress_type));
				node->src_com = strdup(compress_file);
			}
			get_compress_file_k_by_monitorfile(compress_file_k, node->monitorfile);
			if(file_exists_state(compress_file_k) != 0){
				newnode=queue_node_copy(node);
				queue_node_push(&g_monitorgroup[index].compress_cfg.queue,newnode);
			}
		}
		for(i = 0; i<sgl_count; i++){
			ret = do_upload_state_file_check(g_monitorgroup[index].name, node->monitorfile,0, &sgl[i]);
			if(ret == NEWIUP_OK){
				continue;
			}

			node->sg_name = sgl[i].rs_prime[0]->name;
			queue = upload_real_server_sche_rr(&sgl[i], UPLOAD_RSL_PRIME);
			if(queue == NULL)
			{
				queue = upload_real_server_sche_rr(&sgl[i], UPLOAD_RSL_BACK);
				if(queue == NULL){
					continue;
				}
			}
			/**/
			if(sche_debug_switch != 0){
				logging(LOG_INFO, "++add %s to %s sche|queue_size=%d | %s\n",
					g_monitorgroup[index].name, queue->name, queue->size,  node->src);
				sche_debug_switch--;
			}
			newnode=queue_node_copy(node);
			queue_node_push(queue,newnode);
		}
		
		queue_node_push(node->sche_queue, node);
		
	}
    logging(LOG_INFO, "SCHE [%s] thread exit\n", g_monitorgroup[index].name);
	return NULL; 
}

static int monitor_compress_check(server_group_queue *sgl, int sgl_count)
{
	int j,i;
	real_server *server=NULL;

	for(i=0; i<sgl_count; i++){
		for(j=0; j<sgl[i].rs_count_prime; j++){
			server = sgl[i].rs_prime[j];
			if(server && server->compress==NEWIUP_OK)
				return NEWIUP_OK;
		}
		for(j=0; j<sgl[i].rs_count_back; j++){
			server = sgl[i].rs_back[j];
			if(server && server->compress==NEWIUP_OK)
				return NEWIUP_OK;
		}
	}
	return NEWIUP_FAIL;
}
 

void monitor_handle_pthread_start()
{
	int i;
	int k=0;
	int j,x;
	int ret;
	server_group_queue *node;
	
	for(i=0; i<g_monitorgroup_num; i++){
		if(g_monitorgroup[i].enable == NEWIUP_FAIL){
			k++;
			continue;
		}

		ret = monitor_compress_check(g_monitorgroup[i].server_group_list,g_monitorgroup[i].sgl_count);
		if(NEWIUP_OK == ret ){
			g_monitorgroup[i].compress = NEWIUP_OK;
			compress_handle_pthread_start(&g_monitorgroup[i].compress_cfg);
		}
		pthread_create(&g_monitorgroup[i].monitor_pid, NULL, monitor_handle, &g_monitorgroup[i].index);
		pthread_create(&g_monitorgroup[i].sche_pid, NULL, sche_handle, &g_monitorgroup[i].index);
		for(j=0; j<g_monitorgroup[i].sgl_count; j++){
			node = &g_monitorgroup[i].server_group_list[j];
			for(x=0; x<node->rs_count_prime; x++)
				server_upload_handle_pthread_start(node->rs_prime[x]);
			for(x=0; x<node->rs_count_back; x++)
				server_upload_handle_pthread_start(node->rs_back[x]);
		}
	}
	//Prevent configured to monitor group, but without the enable all set
	if(k == g_monitorgroup_num){
		logging(LOG_ERROR, "monitor-group number [%d], But there is no thread starts .check enable state!\n", g_monitorgroup_num);
		exit(2);
	}
}
void monitor_handle_pthread_end()
{
	int i;
	int j,x;
	int ret;
	server_group_queue *node;

	for(i=0; i<g_monitorgroup_num; i++){
		if(g_monitorgroup[i].enable == NEWIUP_FAIL){
			continue;
		}
		ret = monitor_compress_check(g_monitorgroup[i].server_group_list, g_monitorgroup[i].sgl_count);
		if(NEWIUP_OK == ret){
			compress_handle_pthread_end(&g_monitorgroup[i].compress_cfg);
		}
		pthread_join(g_monitorgroup[i].monitor_pid, NULL);
		pthread_join(g_monitorgroup[i].sche_pid, NULL);
		for(j=0; j<g_monitorgroup[i].sgl_count; j++){
			node = &g_monitorgroup[i].server_group_list[j];
			for(x=0; x<node->rs_count_prime; x++)
				server_upload_handle_pthread_end(node->rs_prime[x]);
			for(x=0; x<node->rs_count_back; x++)
				server_upload_handle_pthread_end(node->rs_back[x]);
		}	}
}
