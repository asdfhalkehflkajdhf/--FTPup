#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <curl/curl.h>

#include "logging.h"
#include "conf.h"
#include "public.h"
#include "utils.h"

#include "servers.h"
#include "monitor.h"

#define NEWIUP_UP_ERROR_SKIP_TIME 60
#define NEWIUP_UP_ERROR_CUR_WV_TIMES 1


real_server *g_server = NULL;
int g_server_num = 0;


static int get_server_protocol_id(char *prot)
{
	int prot_id = NEWIUP_FAIL;
    if (prot == NULL)
    {
        return NEWIUP_FAIL;
    }
	if(strcmp("ftp", prot) == 0)
	{
		prot_id = UPLOAD_PROT_FTP;
	}else if(strcmp("sftp", prot) == 0)
	{
		prot_id = UPLOAD_PROT_SFTP;
	}

    return prot_id;
} 

char * get_server_name_by_index(int index)
{
    return g_server[index].name;
} 

real_server *get_server_by_index(int index)
{
    return &g_server[index];
} 

int get_server_id_by_name(char *name)
{
	int i = 0;
	for(i=0; i<g_server_num; i++)
	{
		if(strcmp(g_server[i].name, name)==0)
		{
			return i;
		}
	}

    return NEWIUP_FAIL;
} 

real_server * get_server_by_name(char *name)
{
	int i = 0;
	for(i=0; i<g_server_num; i++)
	{
		if(strcmp(g_server[i].name, name)==0)
		{
			return &g_server[i];
		}
	}

    return NULL;
} 

int get_server_available_state(int index)
{
	if(g_server[index].cur_fst>= 0 && time(NULL) - g_server[index].cur_fst <= g_server[index].fail_sleep_time)
	{
		return NEWIUP_FAIL;
	}
	__sync_lock_test_and_set(&g_server[index].cur_fst , 0);
	return NEWIUP_OK;
}
queue_s * get_server_queue_by_index(int index)
{
	return &g_server[index].queue;
}
int get_server_compress_by_id(int index)
{
	return g_server[index].compress;
}
static volatile int g_upload_run_signal=NEWIUP_OK;
static int get_upload_run_signal(void)
{
	return g_upload_run_signal;
}
void set_upload_run_signal(int signal)
{
	g_upload_run_signal=signal;
	return ;
}
extern void *upload_handle(void *arg);

//return: 0:sucess -1:fail
static int get_server_info(void)
{
    logging(LOG_INFO, "-- %s\n", __func__);

    ConfNode *child = NULL;
    ConfNode *node = NULL;
    uint32_t count = 0;
    char *str = NULL;

    node = ConfGetNode("servers");
    TAILQ_FOREACH(child, &node->head, next) 
    {
        g_server[count].index = count;
		if(ConfNodeNameCheck(child->val)){
            logging(LOG_ERROR, "--%s [%s]-[name] Name repetition error\n", __func__, child->val);
            return NEWIUP_FAIL;
		}
        strncpy(g_server[count].name, child->val, NEWIUP_NAME_LEN);
		//addr
        if (ConfGetChildValue(child, "addr", &str) == 0)
        {
            logging(LOG_ERROR, "--%s [%s]-[addr] error \n", __func__, g_server[count].name);
            return NEWIUP_FAIL;
        }
        strncpy(g_server[count].addr, str, sizeof(g_server[count].addr));
		//username
        if (ConfGetChildValue(child, "username", &str) == 0)
        {
            logging(LOG_ERROR, "--%s [%s]-[username] error\n", __func__, g_server[count].name);
            return NEWIUP_FAIL;
        }
        strncpy(g_server[count].username, str, NEWIUP_NAME_LEN);
		//passwd
        if (ConfGetChildValue(child, "passwd", &str) == 0)
        {
            logging(LOG_ERROR, "--%s [%s]-[passwd] error\n",__func__, g_server[count].name);
            return NEWIUP_FAIL;
        }
        strncpy(g_server[count].passwd, str, sizeof(g_server[count].passwd));
		//prototcl
        if (ConfGetChildValue(child, "protocol", &str) == 0)
        {
            logging(LOG_ERROR, "--%s [%s]-[protocol] error\n", __func__, g_server[count].name);
            return NEWIUP_FAIL;
        }
        g_server[count].protocol = get_server_protocol_id(str);
		if( NEWIUP_FAIL == g_server[count].protocol)
		{
            logging(LOG_ERROR, "--%s [%s]-[protocol] error: not support %s [ftp/sftp]\n",__func__, g_server[count].name,str);
			return NEWIUP_FAIL;
		}
		//ftp-mode
        if (ConfGetChildValue(child, "ftp-mode", &str) == 0)
        {
            logging(LOG_ERROR, "--%s [%s]-[ftp-mode] error\n", __func__, g_server[count].name);
            return NEWIUP_FAIL;
        }
		if(strcmp(str, "initiative") == 0){
			g_server[count].ftp_mode = FTP_PORT;
		}else if(strcmp(str, "passive") == 0){
			g_server[count].ftp_mode = FTP_PASV;
		}else{
            logging(LOG_ERROR, "--%s [%s]-[ftp-mode] error: not support %s [ftp/sftp]\n",__func__, g_server[count].name,str);
			return NEWIUP_FAIL;
		}
		//finish-confirm
        if (ConfGetChildValue(child, "finish-flag", &str) == 0)
        {
            logging(LOG_ERROR, "--%s [%s]-[finish-flag] error\n",__func__, g_server[count].name);
            return NEWIUP_FAIL;
        }
		g_server[count].finish_confirm[0]='\0';
		if(str[0]!='\0'){
			strcpy(g_server[count].finish_confirm, str);	
		}
		//dir
        if (ConfGetChildValue(child, "dir", &str) == 0)
        {
            logging(LOG_ERROR, "--%s [%s]-[dir] error\n",__func__, g_server[count].name);
            return NEWIUP_FAIL;
        }
		if(str[strlen(str)-1] != '/')
		{
			snprintf(g_server[count].dir,NEWIUP_PATH_LEN, "%s/",str);
		}
		else
		{
			snprintf(g_server[count].dir,NEWIUP_PATH_LEN, "%s",str);
		}
		//debug
		if (ConfGetChildValue(child, "debug", &str) == 0)
		{
			g_server[count].debug=NEWIUP_OK;
		}else{
			g_server[count].debug=NEWIUP_FAIL;
		}
		//file-suffix: 
		if (ConfGetChildValue(child, "file-suffix", &str) == 0)
		{
			logging(LOG_ERROR, "--%s [%s]-[file-suffix] error\n",__func__, g_server[count].name);
			return NEWIUP_FAIL;
		}
		g_server[count].file_suffix[0]='\0';
		if(str[0]!='\0'){
			strcpy(g_server[count].file_suffix, str);	
		}
		//thread_num: 
		if (ConfGetChildValue(child, "thread-num", &str) == 0)
		{
			logging(LOG_ERROR, "--%s [%s]-[thread-num] error\n",__func__, g_server[count].name);
			return NEWIUP_FAIL;
		}
		g_server[count].thread_num=atoi(str);
		if(g_server[count].thread_num < 1 || g_server[count].thread_num >15){
			logging(LOG_ERROR, "--%s [%s]-[thread-num] error: not support %s [1-15]\n",__func__, g_server[count].name, str);
			return NEWIUP_FAIL;
		}
		//compress: 
		if (ConfGetChildValue(child, "compress", &str) == 0)
		{
			logging(LOG_ERROR, "--%s [%s]-[compress] error\n",__func__, g_server[count].name);
			return NEWIUP_FAIL;
		}
		if(strcmp(str, "no") == 0){
			g_server[count].compress =NEWIUP_FAIL;
		}else if(strcmp(str, "yes") == 0){
			g_server[count].compress =NEWIUP_OK;
		}else{
			logging(LOG_ERROR, "--%s [%s]-[compress] error: not support %s [yes/no]\n",__func__, g_server[count].name, str);
			return NEWIUP_FAIL;
		}
		//archive: 
		if (ConfGetChildValue(child, "archive", &str) == 0)
		{
			logging(LOG_ERROR, "--%s [%s]-[archive] error\n",__func__, g_server[count].name);
			return NEWIUP_FAIL;
		}
		if(strcmp(str, "date") == 0){
			g_server[count].archive =UPLOAD_ARCHIVE_DATE;
		}else if(strcmp(str, "path") == 0){
			g_server[count].archive =UPLOAD_ARCHIVE_FULLPATH;
		}else if(str[0]=='\0'){
			g_server[count].archive =UPLOAD_ARCHIVE_MAX;
		}else{
			logging(LOG_ERROR, "--%s [%s]-[archive] error: not support %s [date/path]\n",__func__, g_server[count].name, str);
			return NEWIUP_FAIL;
		}
		//attempts: 
		g_server[count].retry_count= NEWIUP_UP_ERROR_CUR_WV_TIMES;
		if (ConfGetChildValue(child, "attempts", &str) != 0)
		{
			g_server[count].retry_count = atoi(str);
		}
		//fsleep_time: 
		g_server[count].fail_sleep_time = NEWIUP_UP_ERROR_SKIP_TIME;
		if (ConfGetChildValue(child, "fsleep-time", &str) != 0)
		{
			g_server[count].fail_sleep_time= atoi(str);
		}

		g_server[count].make_citations = NEWIUP_FAIL;

		g_server[count].cur_rc= 0;
		g_server[count].cur_fst= NEWIUP_FAIL;

		pthread_spin_init(&g_server[count].queue.lock, PTHREAD_PROCESS_PRIVATE);
		g_server[count].queue.size=0;
		TAILQ_INIT(&g_server[count].queue.head);
		strcpy(g_server[count].queue.name, g_server[count].name);

		g_server[count].pid=(pthread_t *)malloc(sizeof(pthread_t) * g_server[count].thread_num);
		if(g_server[count].pid == NULL){
			logging(LOG_ERROR, "--%s [%s]-malloc error\n",__func__, g_server[count].name);
			return NEWIUP_FAIL;
		}
		g_server[count].func = upload_handle;
        count++;
    }

    int i;
    for (i = 0; i < g_server_num; i++) 
    {
        logging(LOG_INFO, "----number--%d\n", g_server[i].index);
        logging(LOG_INFO, "name(srv)     =%s\n", g_server[i].name);
        logging(LOG_INFO, "addr          =%s\n", g_server[i].addr);
        logging(LOG_INFO, "username      =%s\n", g_server[i].username);
        logging(LOG_INFO, "passwd        =%s\n", g_server[i].passwd);
        logging(LOG_INFO, "protocol      =%d 0,ftp|1,sftp\n", g_server[i].protocol);
        logging(LOG_INFO, "finish-flag   =%s\n", g_server[i].finish_confirm);
        logging(LOG_INFO, "dir           =%s \n", g_server[i].dir);
        logging(LOG_INFO, "file-suffix   =%s\n", g_server[i].file_suffix);
        logging(LOG_INFO, "thread_num    =%d\n", g_server[i].thread_num);
        logging(LOG_INFO, "compress      =%d [0:yes/-1:no]\n", g_server[i].compress);
        logging(LOG_INFO, "archive       =%d [0:data/1:path/2:no]\n", g_server[i].archive);
        logging(LOG_INFO, "attempts      =%d\n", g_server[i].retry_count);
        logging(LOG_INFO, "fsleep-time   =%d [s]\n", g_server[i].fail_sleep_time);
        logging(LOG_INFO, "debug         =%d 0:no|1:yes\n", g_server[i].debug);
    }

    return NEWIUP_OK;
} 


//return: 0:sucess -1:fail(exit)
int servers_init(void)
{
	logging(LOG_INFO, "++++++++++++++++++++\n");
    g_server_num = ConfGetChildNumber("servers");
    logging(LOG_INFO, "%s: server number = %d\n", __func__, g_server_num);
    if (g_server_num <= 0)
    {
        logging(LOG_ERROR, " --%s get [server_number] error(exit)\n", __func__);
        return NEWIUP_FAIL;
    }

    g_server = (real_server *)malloc(sizeof(real_server) * g_server_num);
    if (g_server == NULL)
    {
        logging(LOG_ERROR, " --%s malloc servers error(exit)\n", __func__);
        return NEWIUP_FAIL;
    }
    memset(g_server, 0, sizeof(real_server) * g_server_num);
    if (get_server_info() != NEWIUP_OK)
    {
		servers_exit();
        return NEWIUP_FAIL;
    }

    return NEWIUP_OK;
}

int servers_exit(void)
{
	int i;
	FileQueue *node = NULL;
	for(i=0; i<g_server_num; i++){
		if(g_server[i].pid)
			free(g_server[i].pid);
		
        while ((node = TAILQ_FIRST(&g_server[i].queue.head)) != NULL)
        {
            TAILQ_REMOVE(&g_server[i].queue.head, node, next);
			queue_node_free(node);
        }
		
	}
    if (g_server)
        free(g_server);
	g_server = NULL;
	g_server_num = 0;

    return 0;    
}

char *res[2]={"FAIL", "SUCESS"};

// Is it really need this function ?
static size_t curl_read_callback_func(void *ptr, size_t size, size_t nmemb, void *stream)
{
    size_t retcode = fread(ptr, size, nmemb, (FILE*)stream);    
    return retcode;
}

// Is it really need this function ?
static int curl_debug_callback_func(CURL *curl, curl_infotype type, char *ch, size_t size, void *data)
{
    return 0;
}
#define MB (1024*1024)
#define KB (1024)

int curl_upload_log(real_server *server, FileQueue *node,
	int upload_time, char *dstfile, char *server_path, int ret, char *errbuf)
{
	if(node != NULL)
	{
		UPLOG_LOG(node->monitor_id, "-----\n");
		UPLOG_LOG(node->monitor_id, "%s %s | %s\n", server->name, server_path, errbuf);
		if(ret == 0)
		{
			if(node->up_size >= MB)
				UPLOG_TXT(node->monitor_id, 
					"%s |upcount=%2d |uptime=%2d |upsize=%6.1f MB |%s\n",
					server->name, *node->up_count, upload_time, 
					node->up_size/1.0/MB, dstfile);
			else if(node->up_size)
				UPLOG_TXT(node->monitor_id, 
					"%s |upcount=%2d |uptime=%2d |upsize=%6.1f KB |%s\n",
					server->name, *node->up_count, upload_time, 
					node->up_size/1.0/KB, dstfile);
			else 
				UPLOG_TXT(node->monitor_id, 
					"%s |upcount=%2d |uptime=%2d |upsize=   0.0 B  |%s\n",
					server->name, *node->up_count, upload_time, dstfile);
		}
		UPLOG_LOG(node->monitor_id, "%s %s %d|upcount=%02d|uptime=%02d|%s|%s\n",
			server->name, (ret == 0? res[1]: res[0]),(int)pthread_self(),
			*node->up_count, upload_time, dstfile, curl_easy_strerror((CURLcode)ret));
	}

	return 0;
}
#if 0
int curl_set_sftp_postquotelist(struct curl_slist **postquotelist, char *subdir, char *filename, real_server *server)
{

	char cmd_buf[NEWIUP_PATH_LEN]={0};
	struct str_split split_primer={0, NULL, NULL};

	snprintf(cmd_buf, NEWIUP_PATH_LEN, "pwd");
	*postquotelist = curl_slist_append(*postquotelist, cmd_buf);

	if(server->file_suffix[0] == '\0'){
		snprintf(cmd_buf, sizeof(cmd_buf), "rename %s%s%s.tmp %s%s%s",
			server->dir,subdir,filename, server->dir,subdir,filename);
	}else{
		str_split_func(&split_primer, filename, '.');
		snprintf(cmd_buf, NEWIUP_PATH_LEN, "rename %s%s%s.tmp %s%s%s.%s",
				server->dir, subdir, filename, server->dir, subdir, split_primer.str_array[0], server->file_suffix ); 
	}
	
	*postquotelist = curl_slist_append(*postquotelist, cmd_buf);
	
	str_split_free(&split_primer);
	return 0;

}

int curl_set_ftp_postquotelist(struct curl_slist **postquotelist, char *subdir, char *filename, real_server *server)
{
	char cmd_buf[NEWIUP_PATH_LEN]={0};
	struct str_split split_primer={0, NULL, NULL};


	snprintf(cmd_buf, NEWIUP_PATH_LEN, "RNFR %s%s%s.tmp",
			server->dir, subdir, filename); 
	*postquotelist = curl_slist_append(*postquotelist, cmd_buf);

	if(server->file_suffix[0] == '\0'){
		snprintf(cmd_buf, NEWIUP_PATH_LEN, "RNTO %s%s%s",
				server->dir, subdir, filename ); 
	}else{
		str_split_func(&split_primer, filename, '.');
		snprintf(cmd_buf, NEWIUP_PATH_LEN, "RNTO %s%s%s.%s",
				server->dir, subdir, split_primer.str_array[0], server->file_suffix ); 
	}
	*postquotelist = curl_slist_append(*postquotelist, cmd_buf);
	
	str_split_free(&split_primer);
	return 0;
}

int curl_set_postquotelist(struct curl_slist *postquotelist, char *subdir, char *filename, real_server *server)
{

	int (*curl_rename)(struct curl_slist **postquotelist, char *subdir, char *filename, real_server *server);

	switch(server->protocol)
	{
		case UPLOAD_PROT_FTP:
			curl_rename = curl_set_ftp_postquotelist;
			break;
		case UPLOAD_PROT_SFTP:
			curl_rename = curl_set_sftp_postquotelist;
			break;
		case UPLOAD_PROT_SCP:
			curl_rename = NULL;
			postquotelist = NULL;
		default:
			return NEWIUP_OK;
	}

	if(curl_rename)
		curl_rename(&postquotelist, subdir, filename, server);

	return NEWIUP_OK;
}
static int curl_sftp_mkd2(CURL *curl, real_server *server, char *mkdir_ftp)
{
	int ret;
	struct curl_slist *headerlist_mkd = NULL;
	char buf_ftp[256]={0};
	char userpass[128] = {0};
	if(server == NULL ||mkdir_ftp == NULL)
	{
       return -1;
	}

	
	snprintf(buf_ftp, sizeof(buf_ftp), "pwd");
	headerlist_mkd = curl_slist_append(headerlist_mkd, buf_ftp);

	if(strcmp(server->dir, "/") == 0){
		snprintf(buf_ftp, sizeof(buf_ftp), "mkdir %s", mkdir_ftp);
	}else{
		snprintf(buf_ftp, sizeof(buf_ftp), "mkdir %s%s", server->dir, mkdir_ftp);
	}
	headerlist_mkd = curl_slist_append(headerlist_mkd, buf_ftp);
	snprintf(buf_ftp, sizeof(buf_ftp), "sftp://%s",
			server->addr); 

	snprintf(userpass, 128, "%s:%s", server->username, server->passwd);
	curl_easy_setopt(curl, CURLOPT_URL, buf_ftp);
	curl_easy_setopt(curl, CURLOPT_USERPWD, userpass);
	curl_easy_setopt(curl, CURLOPT_POSTQUOTE, headerlist_mkd);
	curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
	ret = curl_easy_perform(curl);
	curl_slist_free_all(headerlist_mkd);
	return ret;
}

static int curl_sftp_mkd_recursive(CURL *curl, real_server *server, char *mkdir_path)
{
	char subdir[256]={0};
	struct str_split split_primer={0, NULL, NULL};

	if(server == NULL ||mkdir_path == NULL)
	{
       return -1;
	}
	curl_easy_reset(curl);

	str_split_func(&split_primer, mkdir_path, '/');
	int i;
	for(i=0; i<split_primer.count-1; i++){
		strcat(subdir, split_primer.str_array[i]);
		curl_sftp_mkd2(curl, server, subdir);
		strcat(subdir,"/");
	}
	str_split_free(&split_primer);
	return 0;
}
#endif

static int curl_sftp_rm_file(CURL *curl, real_server *server, char *subdir,char *name)
{
	int ret;
	struct curl_slist *postquotelist = NULL;
	char buf_ftp[256]={0};
	char userpass[128] = {0};
	if(server == NULL || name == NULL)
	{
       return NEWIUP_FAIL;
	}

	snprintf(buf_ftp, sizeof(buf_ftp), "pwd");
	postquotelist = curl_slist_append(postquotelist, buf_ftp);
	snprintf(buf_ftp, sizeof(buf_ftp), "rm %s%s%s", server->dir,subdir,name);
	postquotelist = curl_slist_append(postquotelist, buf_ftp);

	snprintf(buf_ftp, sizeof(buf_ftp), "sftp://%s%s",
			server->addr, server->dir); 
	snprintf(userpass, 128, "%s:%s", server->username, server->passwd);
	curl_easy_setopt(curl, CURLOPT_URL, buf_ftp);
	curl_easy_setopt(curl, CURLOPT_USERPWD, userpass);
	curl_easy_setopt(curl, CURLOPT_POSTQUOTE, postquotelist);
	curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
	ret = curl_easy_perform(curl);
	curl_slist_free_all(postquotelist);
	//curl return 0 and rname_flag is ture
	return ret;
}


static int curl_ftp_rm_file(CURL *curl, real_server *server, char *subdir,char *name)
{
	int ret;
	struct curl_slist *postquotelist = NULL;
	char cmd_buf[256]={0};
	char userpass[128] = {0};
	if(server == NULL || name == NULL)
	{
       return NEWIUP_FAIL;
	}

	snprintf(cmd_buf, sizeof(cmd_buf), "DELE %s",name);
	postquotelist = curl_slist_append(postquotelist, cmd_buf);

	snprintf(cmd_buf, sizeof(cmd_buf), "ftp://%s%s%s",
			server->addr, server->dir, subdir); 
	snprintf(userpass, 128, "%s:%s", server->username, server->passwd);
	curl_easy_setopt(curl, CURLOPT_URL, cmd_buf);
	curl_easy_setopt(curl, CURLOPT_USERPWD, userpass);
	curl_easy_setopt(curl, CURLOPT_POSTQUOTE, postquotelist);
	curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
	//set initiative link mode 
	if(server->ftp_mode == FTP_PORT){
		curl_easy_setopt(curl, CURLOPT_FTP_USE_EPRT, 1);
		curl_easy_setopt(curl, CURLOPT_FTPPORT, "-");
	}else{
		curl_easy_setopt(curl, CURLOPT_FTP_USE_EPRT, 0);
	}
	ret = curl_easy_perform(curl);
	curl_slist_free_all(postquotelist);
	//curl return 0 and rname_flag is ture
	return ret;
}

static int curl_upload_rm_file(CURL *curl, char *subdir, char *filename, real_server *server)
{

	int (*curl_rm_file)(CURL *curl, real_server *server, char *subdir, char *filename);
	curl_easy_reset(curl);

	switch(server->protocol)
	{
		case UPLOAD_PROT_FTP:
			curl_rm_file = curl_ftp_rm_file;
			break;
		case UPLOAD_PROT_SFTP:
			curl_rm_file = curl_sftp_rm_file;
			break;
		case UPLOAD_PROT_SCP:
			curl_rm_file = NULL;
		default:
			return NEWIUP_OK;
	}
	if(curl_rm_file){
		curl_rm_file(curl, server, subdir, filename);
	}

	return NEWIUP_OK;
}

static int curl_sftp_rename(CURL *curl, char *subdir, char *oldname, char *newname, real_server *server)
{
	int ret;
	struct curl_slist *postquotelist = NULL;
	char cmd_buf[256]={0};
	char userpass[128] = {0};

	snprintf(cmd_buf, sizeof(cmd_buf), "pwd");
	postquotelist = curl_slist_append(postquotelist, cmd_buf);

	snprintf(cmd_buf, sizeof(cmd_buf), "rename %s%s%s %s%s%s",
		server->dir,subdir,oldname, server->dir,subdir,newname);

	postquotelist = curl_slist_append(postquotelist, cmd_buf);

	snprintf(cmd_buf, sizeof(cmd_buf), "sftp://%s%s",
			server->addr, server->dir); 
	snprintf(userpass, 128, "%s:%s", server->username, server->passwd);
	curl_easy_setopt(curl, CURLOPT_URL, cmd_buf);
	curl_easy_setopt(curl, CURLOPT_USERPWD, userpass);
	curl_easy_setopt(curl, CURLOPT_POSTQUOTE, postquotelist);
	curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
	ret = curl_easy_perform(curl);
	curl_slist_free_all(postquotelist);
	//curl return 0 and rname_flag is ture
	return ret;
}

static int curl_ftp_rename(CURL *curl, char *subdir, char *oldname, char *newname, real_server *server)
{

	char cmd_buf[NEWIUP_PATH_LEN]={0};
	struct curl_slist *postquotelist = NULL;
	char userpass[128] = {0};
	int ret;

	snprintf(cmd_buf, NEWIUP_PATH_LEN, "RNFR %s", oldname); 
	postquotelist = curl_slist_append(postquotelist, cmd_buf);

	snprintf(cmd_buf, NEWIUP_PATH_LEN, "RNTO %s", newname ); 
	postquotelist = curl_slist_append(postquotelist, cmd_buf);
	

	snprintf(cmd_buf, sizeof(cmd_buf), "ftp://%s%s%s",
			server->addr, server->dir,subdir); 
	snprintf(userpass, 128, "%s:%s", server->username, server->passwd);
	curl_easy_setopt(curl, CURLOPT_URL, cmd_buf);
	curl_easy_setopt(curl, CURLOPT_USERPWD, userpass);
	curl_easy_setopt(curl, CURLOPT_POSTQUOTE, postquotelist);
	curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
	//set initiative link mode 
	if(server->ftp_mode == FTP_PORT){
		curl_easy_setopt(curl, CURLOPT_FTP_USE_EPRT, 1);
		curl_easy_setopt(curl, CURLOPT_FTPPORT, "-");
	}else{
		curl_easy_setopt(curl, CURLOPT_FTP_USE_EPRT, 0);
	}
	ret = curl_easy_perform(curl);
	curl_slist_free_all(postquotelist);
	//curl return 0 and rname_flag is ture
	return ret;
}

int curl_upload_rename(CURL *curl, char *subdir, char *filename, real_server *server)
{
	int (*curl_rename)(CURL *curl, char *subdir, char *oldname, char *newname, real_server *server);
	curl_easy_reset(curl);

	switch(server->protocol)
	{
		case UPLOAD_PROT_FTP:
			curl_rename = curl_ftp_rename;
			break;
		case UPLOAD_PROT_SFTP:
			curl_rename = curl_sftp_rename;
			break;
		case UPLOAD_PROT_SCP:
			curl_rename = NULL;
		default:
			return NEWIUP_OK;
	}
	char oldname[128];
	char newname[128];

	snprintf(oldname, 128, "%s.tmp", filename);
	if(server->file_suffix[0] == '\0'){
		snprintf(newname, 128, "%s", filename); 
	}else{
		struct str_split split_primer={0, NULL, NULL};
		str_split_func(&split_primer, filename, '.');
		snprintf(newname, 128, "%s.%s",
				split_primer.str_array[0], server->file_suffix ); 
		str_split_free(&split_primer);
	}

	if(curl_rename){
		int ret = curl_rename(curl, subdir, oldname, newname, server);
		if(ret){
			curl_upload_rm_file(curl, subdir, newname, server);
		}
		curl_rename(curl, subdir, oldname, newname, server);
	}

	return NEWIUP_OK;
}

int curl_getinfo_deubg(CURL *curl)
{
	char *url=NULL;
	double time = 0;
	long new_counect = 0;
	long tot_counect = 0;
	char *path=NULL;
	

	//最后一次成功的 url
	curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_URL, &url);

	//dns解析时长
	curl_easy_getinfo(curl, CURLINFO_NAMELOOKUP_TIME, &time);

	//新连接个数
	curl_easy_getinfo(curl, CURLINFO_NUM_CONNECTS, &new_counect);

	//连接总数
	curl_easy_getinfo(curl, CURLINFO_REDIRECT_COUNT, &tot_counect);

	//最近一次ftp路径
	curl_easy_getinfo(curl, CURLINFO_FTP_ENTRY_PATH, &path);
	
	printf("lookup:%f  new_c:%ld tot_c:%ld url:%s path:%s\n", time, new_counect, tot_counect, url, path);
	
	return 0;
}

int curl_upload_scp(CURL *curl, real_server *server, FileQueue *node, char *subdir, char *dstfile)
{
	logging(LOG_ERROR, "scp protocol not support yet \n");
	return NEWIUP_OK;
}

/*Note that the node parameter can be null,
Mainly in order to obtain upload time and the number of retransmission
The return value: successful NEWIUP_OK;NEWIUP_FAIL failure or error code
*/
int curl_upload_sftp(CURL *curl, real_server *server, FileQueue *node, char *subdir, char *dstfile)
{
	int ret = 0;
	int upload_time = 0;
	char errbuf[512]={0};
	
	FILE *fd = fopen(dstfile, "r");
	if (fd == NULL)
	{
		logging(LOG_ERROR, "sftp upload file fopen error :%s\n", dstfile);
		return NEWIUP_FAIL;
	}
	
	char server_path[NEWIUP_PATH_LEN] = {0};
	char *filename = rindex(dstfile, '/')+1;
	char user_pass[128] = {0};
	snprintf(user_pass, sizeof(user_pass), "%s:%s", server->username, server->passwd);
	
	
	curl_easy_reset(curl);
	struct curl_slist *postquotelist = NULL;
	struct curl_slist *quotelist = NULL;
	curl_off_t fsize = (curl_off_t)get_file_size(dstfile);
	if(node != NULL){
		snprintf(server_path, sizeof(server_path), "sftp://%s%s%s%s.tmp",
				server->addr, server->dir, subdir, filename);
	}else{
		snprintf(server_path, sizeof(server_path), "sftp://%s%s%s%s",
				server->addr, server->dir, subdir, filename);
	}

	//set timeout
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, global_config.upload_timeout);
	curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, global_config.connect_timeout);
	curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
	//set initiative link mode 
	//curl_easy_setopt(curl, CURLOPT_FTPPORT, "-");

	if(server->archive != UPLOAD_ARCHIVE_MAX){
		curl_easy_setopt(curl, CURLOPT_FTP_CREATE_MISSING_DIRS,2L);
	}
	/*
	Configuration URL cache clearing time,	usually less than the best, the servers->fsleep_time.
	Otherwise possible curl will still access the old URL and does not take the initiative to update the status.
	*/
	curl_easy_setopt(curl, CURLOPT_DNS_CACHE_TIMEOUT, 30L);

	//0644 
	//curl_easy_setopt(curl, CURLOPT_NEW_FILE_PERMS, 0644L);
	curl_easy_setopt(curl, CURLOPT_USERPWD, user_pass); 	 
	curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
	curl_easy_setopt(curl, CURLOPT_URL, server_path);
	curl_easy_setopt(curl, CURLOPT_READDATA, fd);
	//Execute the command before uploading
	curl_easy_setopt(curl, CURLOPT_QUOTE, quotelist);
	//Execute the command after upload
	curl_easy_setopt(curl, CURLOPT_POSTQUOTE, postquotelist);
	curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);
	curl_easy_setopt(curl, CURLOPT_READFUNCTION, curl_read_callback_func);
	if(NEWIUP_OK == server->debug){
		curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, curl_debug_callback_func); 
	}
	curl_easy_setopt(curl, CURLOPT_VERBOSE, server->debug);
	curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, fsize);
	
	upload_time = time(NULL);
	ret = curl_easy_perform(curl);
	upload_time = time(NULL) - upload_time;
	curl_slist_free_all(quotelist);
	curl_slist_free_all(postquotelist);
	fclose(fd);

	if(node != NULL){
		if(ret == 0){
			//curl_getinfo_deubg(curl);
			curl_upload_rename(curl, subdir, filename, server);
			
			if( server->finish_confirm[0]!= '\0'){
				char confirm_file[NEWIUP_PATH_LEN] = {0};
			
				struct str_split split_primer={0, NULL, NULL};
				str_split_func(&split_primer, dstfile, '.');
				snprintf(confirm_file, NEWIUP_PATH_LEN, "%s.%s",
						split_primer.str_array[0], server->finish_confirm); 
				str_split_free(&split_primer);
			
				create_file(confirm_file);
				curl_upload_sftp(curl, server, NULL, subdir, confirm_file);
				delete_file(confirm_file);
			}
		}else{
			char filname_tmp[128];
			snprintf(filname_tmp, 128, "%s.tmp", filename);
			curl_upload_rm_file(curl, subdir, filname_tmp, server);
		}
		curl_upload_log(server, node, upload_time, dstfile, server_path, ret, errbuf);
	}
	
	return ret;
}

/*Note that the node parameter can be null,
Mainly in order to obtain upload time and the number of retransmission
The return value: successful NEWIUP_OK;NEWIUP_FAIL failure or error code
*/
int curl_upload_ftp(CURL *curl, real_server *server, FileQueue *node, char *subdir, char *dstfile)
{
	int ret = 0;
	int upload_time = 0;
	char errbuf[512]={0};
	
	FILE *fd = fopen(dstfile, "r");
	if (fd == NULL)
	{
		logging(LOG_ERROR, "ftp upload file fopen error :%s \n", dstfile);
		return NEWIUP_FAIL;
	}
	
	char server_path[NEWIUP_PATH_LEN] = {0};
	char *filename = rindex(dstfile, '/')+1;
	char user_pass[128] = {0};
	snprintf(user_pass, sizeof(user_pass), "%s:%s", server->username, server->passwd);
	
	
	curl_easy_reset(curl);
	struct curl_slist *postquotelist = NULL;
	struct curl_slist *quotelist = NULL;
	curl_off_t fsize = (curl_off_t)get_file_size(dstfile);
	if(node != NULL){
		snprintf(server_path, sizeof(server_path), "ftp://%s%s%s%s.tmp",
				server->addr, server->dir, subdir, filename);
	}else{
		snprintf(server_path, sizeof(server_path), "ftp://%s%s%s%s",
				server->addr, server->dir, subdir, filename);
	}

	//transfer type 0:binary 1:ascii
	curl_easy_setopt(curl, CURLOPT_TRANSFERTEXT, 0);
	//set timeout
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, global_config.upload_timeout);
	curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, global_config.connect_timeout);
	curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
	//set initiative link mode 
	if(server->ftp_mode == FTP_PORT){
		curl_easy_setopt(curl, CURLOPT_FTP_USE_EPRT, 1);
		curl_easy_setopt(curl, CURLOPT_FTPPORT, "-");
	}else{
		curl_easy_setopt(curl, CURLOPT_FTP_USE_EPRT, 0);
		//curl_easy_setopt(curl, CURLOPT_FTP_USE_EPSV, 0);
	}
	if(server->archive != UPLOAD_ARCHIVE_MAX){
		curl_easy_setopt(curl, CURLOPT_FTP_CREATE_MISSING_DIRS,2L);
	}
	/*
	Configuration URL cache clearing time,	usually less than the best, the servers->fsleep_time.
	Otherwise possible curl will still access the old URL and does not take the initiative to update the status.
	*/
	curl_easy_setopt(curl, CURLOPT_DNS_CACHE_TIMEOUT, 30L);

	//0644 
	//curl_easy_setopt(curl, CURLOPT_NEW_FILE_PERMS, 0644L);
	curl_easy_setopt(curl, CURLOPT_USERPWD, user_pass); 	 
	curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
	curl_easy_setopt(curl, CURLOPT_URL, server_path);
	curl_easy_setopt(curl, CURLOPT_READDATA, fd);
	//Execute the command before uploading
	curl_easy_setopt(curl, CURLOPT_QUOTE, quotelist);
	//Execute the command after upload
	curl_easy_setopt(curl, CURLOPT_POSTQUOTE, postquotelist);
	curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);
	curl_easy_setopt(curl, CURLOPT_READFUNCTION, curl_read_callback_func);
	if(NEWIUP_OK == server->debug){
		curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, curl_debug_callback_func); 
	}
	curl_easy_setopt(curl, CURLOPT_VERBOSE, server->debug);
	curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, fsize);
	
	upload_time = time(NULL);
	ret = curl_easy_perform(curl);
	upload_time = time(NULL) - upload_time;
	curl_slist_free_all(quotelist);
	curl_slist_free_all(postquotelist);
	fclose(fd);

	if(node != NULL){
		if(ret == 0){
			//curl_getinfo_deubg(curl);
			curl_upload_rename(curl, subdir, filename, server);

			if( server->finish_confirm[0]!= '\0'){
				char confirm_file[NEWIUP_PATH_LEN] = {0};
			
				struct str_split split_primer={0, NULL, NULL};
				str_split_func(&split_primer, dstfile, '.');
				snprintf(confirm_file, NEWIUP_PATH_LEN, "%s.%s",
						split_primer.str_array[0], server->finish_confirm); 
				str_split_free(&split_primer);
			
				create_file(confirm_file);
				curl_upload_ftp(curl, server, NULL, subdir, confirm_file);
				delete_file(confirm_file);
			}
		}else{
			char filname_tmp[128];
			snprintf(filname_tmp, 128, "%s.tmp", filename);
			curl_upload_rm_file(curl, subdir, filname_tmp, server);
		}
		curl_upload_log(server, node, upload_time, dstfile, server_path, ret, errbuf);
	}
	
	return ret;
}

int do_upload_file_checking(int index, FileQueue *node)
{
	
	if(get_server_available_state(index)==NEWIUP_FAIL){
		queue_node_free(node);
		return NEWIUP_FAIL;
	}
	node->dstfile = node->src;
	if(file_exists_state(node->dstfile) != 0){
		queue_node_free(node);
		return NEWIUP_FAIL;
	}
	if(g_server[index].compress == NEWIUP_OK){
		node->dstfile = node->src_com;
		
		char compress_file_k[NEWIUP_PATH_LEN]={0};
		get_compress_file_k_by_monitorfile(compress_file_k, node->monitorfile);
		if(file_exists_state(compress_file_k) != 0){
			queue_node_push(&g_server[index].queue, node);
			return NEWIUP_FAIL;
		}
		if(file_exists_state(node->dstfile) != 0){
			queue_node_free(node);
			return NEWIUP_FAIL;
		}
	}
	node->up_size = get_file_size(node->dstfile);
	
	return NEWIUP_OK;
}

int do_upload_file(CURL *curl, real_server *server, FileQueue *node)
{
	int ret;
//	int mkd_ret = NEWIUP_OK;
	char subdir[WDQUEUE_PATH_LEN]={0};
	if(server->archive != UPLOAD_ARCHIVE_MAX)
	{
		if(server->archive == UPLOAD_ARCHIVE_DATE){
			time_t tm = time(NULL);
			strftime(subdir, 512, "%Y%m%d/", localtime(&tm));
		}else{
			snprintf(subdir, 512, "%s/", node->up_subpath);
		}
	}

	switch(server->protocol)
	{
		case UPLOAD_PROT_FTP:
			ret = curl_upload_ftp(curl, server, node, subdir, node->dstfile);
			break;
		case UPLOAD_PROT_SFTP:
//sftp_mksubdir:
			ret = curl_upload_sftp(curl, server, node, subdir, node->dstfile);
			//windows msftpsrvr is not support  Recursive new directory
			/*
			if(0 && mkd_ret ==NEWIUP_OK){
				mkd_ret = ret;
				printf("mkdir %s\n", subdir);
				curl_sftp_mkd_recursive(curl, server, subdir);
				goto sftp_mksubdir;
			}
			*/
			break;
		case UPLOAD_PROT_SCP:
			ret = curl_upload_scp(curl, server, node, subdir, node->dstfile);
			break;
		default:
			logging(LOG_ERROR, "not support protocol(%s)\n", server->protocol);
			//Ok after returning to upload the node release
			ret = NEWIUP_OK;
			break;
	}
	return ret;
}



void *upload_handle(void *arg)
{
	int index = *(int *)arg;
	int ret;
	FileQueue *node = NULL;
	CURL *curl = NULL;
	curl = curl_easy_init();
	if (curl == NULL)
	{
		logging(LOG_ERROR, "%s curl init fail\n", g_server[index].name);
		exit(1);
	}
	
    logging(LOG_INFO, "UPLOAD [%s] thread start\n", g_server[index].name);
	while(get_upload_run_signal() == NEWIUP_OK){
		usleep(1);

        node = queue_node_pop(&g_server[index].queue);
        if (node == NULL){
            sleep (QUEUE_EMPTY_SLEEP);
            continue;
        }
		
		ret = do_upload_file_checking(index, node);
		if(NEWIUP_FAIL == ret)
			continue;

		ret = do_upload_file(curl, &g_server[index], node);
		if(ret == NEWIUP_OK)
		{
			do_upload_file_finish_single(g_server[index].name, node);
			g_server[index].cur_rc = 0;
		}else{
			if(g_server[index].cur_rc++ < g_server[index].retry_count){
				queue_node_push(&g_server[index].queue, node);
				continue;
			}
			__sync_lock_test_and_set(&g_server[index].cur_fst, time(NULL));
			g_server[index].cur_rc = 0;
		}

		queue_node_free(node);
	}

	curl_easy_cleanup(curl);
    logging(LOG_INFO, "UPLOAD [%s] thread exit\n", g_server[index].name);
	return NULL; 
}

void server_upload_handle_pthread_start(real_server *rserver)
{
	int i;
	
	if(rserver && rserver->make_citations == NEWIUP_FAIL){
		for(i=0; i<rserver->thread_num; i++){
			pthread_create(&rserver->pid[i], NULL, rserver->func, (void *)&rserver->index);
		}
		rserver->make_citations = NEWIUP_OK;
	}
}
void server_upload_handle_pthread_end(real_server *rserver)
{
	int i;
	if(rserver)
		for(i=0; i<rserver->thread_num; i++){
			pthread_join(rserver->pid[i], NULL);
		}
}
