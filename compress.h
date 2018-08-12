
#ifndef __COMPRESS_H__
#define __COMPRESS_H__

enum compress_type_e
{
	UPLOAD_COMPRESS_GZ,
	UPLOAD_COMPRESS_BZ,
//	UPLOAD_COMPRESS_BZ2,
	UPLOAD_COMPRESS_Z,
	UPLOAD_COMPRESS_ZIP,
	
	UPLOAD_COMPRESS_7Z,
	UPLOAD_COMPRESS_LZMA,
	UPLOAD_COMPRESS_TAR,
	UPLOAD_COMPRESS_TGZ,
	UPLOAD_COMPRESS_TAR_GZ,
	
	UPLOAD_COMPRESS_TAR_BZ,
	UPLOAD_COMPRESS_TAR_BZ2,
	UPLOAD_COMPRESS_TAR_Z,
	UPLOAD_COMPRESS_MAX
};



typedef struct compress_
{
	int index;                          //group id
	char *name;
	int compress_type;
	int compress_buff_size;
	int compress_pcount;
	pthread_t *pid;
	queue_s queue;				//need compress file queue
}compress_group;

void compress_handle_pthread_start(compress_group *compress);
void compress_handle_pthread_end(compress_group *compress);
void set_compress_run_signal(int signal);
void get_compress_file_k_by_monitorfile(char *compress_file_k, char *monitorfile);
int get_compress_type_id(char *str);
char * get_compress_suffix_by_id(int index);

#endif
