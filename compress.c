#include <sys/types.h>
#include <sys/stat.h>

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <zlib.h>
#include <bzlib.h>
#include <pthread.h>
#include <sys/mman.h>
#include "conf.h"
#include "./archive/Archive/archive.h"
#include "./archive/Archive/archive_entry.h"
#include "./archive/Lzma/LzmaUtil.h"

#include "public.h"
#include "logging.h"
#include "utils.h"
#include "compress.h"

static char *g_gzip_type[UPLOAD_COMPRESS_MAX]={
	"gz",
	"bz",
//	"bz2",
	"Z",
	"zip",
	
	"7z",
	"lzma",
	"tar",
	"tgz",
	"tar.gz",

	"tar.bz",
	"tar.bz2",
	"tar.Z"
};
#define COMPRESS_EMPTY_SLEEP 2



int get_compress_type_id(char *str)
{
    if (str == NULL)
    {
        return NEWIUP_FAIL;
    }
	int i;
	for(i=0; i<UPLOAD_COMPRESS_MAX; i++){
	    if (0==strcmp(str, g_gzip_type[i]))
	    {
	        return i; 
	    }
	}

    return NEWIUP_FAIL;
}



char * get_compress_suffix_by_id(int index)
{
	if(index >= UPLOAD_COMPRESS_MAX)
		return NULL;
    return g_gzip_type[index];
}
static int do_compress_bz2(char *buff, int buff_size, const char * DestName,const char *SrcName){    FILE * fp_in = NULL;int len = 0;    int re = NEWIUP_OK;        if( NULL == (fp_in = fopen(SrcName,"rb")))    {        return NEWIUP_FAIL;    }    BZFILE *out = BZ2_bzopen(DestName,"wb6f");    if(out == NULL)    {        return NEWIUP_FAIL;    }    for(;;)    {        len = fread(buff,1,buff_size,fp_in);        if(ferror(fp_in))        {            re = NEWIUP_FAIL;            break;        }        if(len == 0) break;        if(BZ2_bzwrite(out, buff, (unsigned)len) != len)        {            re = NEWIUP_FAIL;        }    }    BZ2_bzclose(out);    fclose(fp_in);    return re; }static int do_compress_gz(char *buff, int buff_size, const char * DestName,const char *SrcName){    FILE * fp_in = NULL;int len = 0;    int re = NEWIUP_OK;        if( NULL == (fp_in = fopen(SrcName,"rb")))    {        return NEWIUP_FAIL;    }    gzFile out = gzopen(DestName,"wb6f");    if(out == NULL)    {        return NEWIUP_FAIL;    }    for(;;)    {        len = fread(buff,1,buff_size,fp_in);        if(ferror(fp_in))        {            re = NEWIUP_FAIL;            break;        }        if(len == 0) break;        if(gzwrite(out, buff, (unsigned)len) != len)        {            re = NEWIUP_FAIL;        }    }    gzclose(out);    fclose(fp_in);    return re; }

static int g_compress_run_signal=NEWIUP_OK;
static int get_compress_run_signal(void)
{
	return g_compress_run_signal;
}
void set_compress_run_signal(int signal)
{
	g_compress_run_signal=signal;
	return ;
}

static int
do_compress_archive(int compress, char *buff, int buff_size,const char *outfile, const char *infile)
{
	struct archive *a = NULL;
	struct archive *disk = NULL;
	struct archive_entry *entry = NULL;
	ssize_t len;
	int fd;
	int ret = NEWIUP_FAIL;

	a = archive_write_new();
	switch (compress) {
//	case UPLOAD_COMPRESS_GZ:
	case UPLOAD_COMPRESS_TGZ:
	case UPLOAD_COMPRESS_TAR_GZ:
		archive_write_add_filter_gzip(a);
		archive_write_set_format_ustar(a);
		break;
//	case UPLOAD_COMPRESS_BZ:
	case UPLOAD_COMPRESS_TAR_BZ:
//	case UPLOAD_COMPRESS_BZ2:
	case UPLOAD_COMPRESS_TAR_BZ2:
		archive_write_add_filter_bzip2(a);
		archive_write_set_format_ustar(a);
		break;
	case UPLOAD_COMPRESS_Z:
	case UPLOAD_COMPRESS_TAR_Z:
		archive_write_add_filter_compress(a);
		archive_write_set_format_ustar(a);
		break;
	case UPLOAD_COMPRESS_ZIP:
		archive_write_add_filter_lzip(a);
		archive_write_set_format_zip(a);
		break;
	case UPLOAD_COMPRESS_7Z:
		archive_write_add_filter_lzip(a);
		archive_write_set_format_7zip(a);
		break;
	case UPLOAD_COMPRESS_TAR:
		archive_write_set_format_ustar(a);
		break;
	default:
		goto out;
		//archive_write_add_filter_none(a);
		break;
	}

	archive_write_open_filename(a, outfile);

	disk = archive_read_disk_new();
	int r;

	r = archive_read_disk_open(disk, infile);
	if (r != ARCHIVE_OK) {
		//logging(LOG_INFO,"do1 %s\n",archive_error_string(disk));
		goto out;
	}

	for (;;) {

		entry = archive_entry_new();
		r = archive_read_next_header2(disk, entry);
		if (r == ARCHIVE_EOF)
			break;
		if (r != ARCHIVE_OK) {
			//logging(LOG_INFO,"do2 %s\n",archive_error_string(disk));
			archive_entry_free(entry);
			goto out;
		}
		archive_read_disk_descend(disk);

		r = archive_write_header(a, entry);
		if (r < ARCHIVE_OK) {
			//logging(LOG_INFO,"%s\n",archive_error_string(disk));
		}
		if (r == ARCHIVE_FATAL){
			//logging(LOG_INFO,"%s\n",archive_error_string(disk));
			archive_entry_free(entry);
			goto out;
		}
		if (r > ARCHIVE_FAILED) {
			/* For now, we use a simpler loop to copy data
			 * into the target archive. */
			fd = open(archive_entry_sourcepath(entry), O_RDONLY);
			len = read(fd, buff, buff_size);
			while (len > 0) {
				archive_write_data(a, buff, len);
				len = read(fd, buff, buff_size);
			}
			close(fd);
		}
		archive_entry_free(entry);
	}
	ret = NEWIUP_OK;
out:
	archive_read_close(disk);
	archive_read_free(disk);

	archive_write_close(a);
	archive_write_free(a);
	return ret;
}

static int do_compress(int type, char *buff, int buff_size, char * DestName, char *SrcName)
{
	int ret = NEWIUP_FAIL;
	//lzma or 7z,but time is long
	if(type == UPLOAD_COMPRESS_GZ){
		ret = do_compress_gz( buff, buff_size,DestName, SrcName);
	}else if(type == UPLOAD_COMPRESS_BZ){
		ret = do_compress_bz2( buff, buff_size,DestName, SrcName);
	}else if(type == UPLOAD_COMPRESS_LZMA){
		ret = LzmaCompress(SrcName, DestName);
	}else if(type <UPLOAD_COMPRESS_MAX){
		ret = do_compress_archive(type,buff, buff_size, DestName, SrcName);
	}
	//ret = do_compress_gz(buff, buff_size, DestName, SrcName); //only compress gz
	//ret = do_compress_bz2(buff, buff_size, DestName, SrcName);//only compress bz2
	return ret;
}
void get_compress_file_k_by_monitorfile(char *compress_file_k, char *monitorfile)
{
	snprintf(compress_file_k, NEWIUP_PATH_LEN, "%s-compress-k", monitorfile);
	return;
}
char *get_compress_file_name(char *dstname, char *srcname, int compress_type)
{
	snprintf(dstname, sizeof(dstname), "%s.%s", srcname, g_gzip_type[compress_type]);
	return dstname;
}
void *compress_handle(void *arg)
{
	int ret;
	compress_group *compress = (compress_group *)arg;

	char *buff = malloc(compress->compress_buff_size);
	if(buff == NULL){
		logging(LOG_INFO, "%s compress malloc error (exit)\n", compress->name);
		exit(1);
	}
	char compress_file_k[NEWIUP_PATH_LEN]={0};
	
    logging(LOG_INFO, "COMPRESS [%s] thread start\n", compress->name);
    while (get_compress_run_signal() == NEWIUP_OK)
    {
    	usleep(1);
        FileQueue *node = queue_node_pop(&compress->queue);
        if (node == NULL)
        {
            sleep (COMPRESS_EMPTY_SLEEP);
            continue;
        }
		
		get_compress_file_k_by_monitorfile(compress_file_k, node->monitorfile);

		//src exists
        if (file_exists_state(node->src) == 0)
        {
        	if(file_exists_state(compress_file_k) == 0){
				//logging(LOG_INFO, "COMPRESS src and com k is exists\n");
				queue_node_free(node);
				continue;
			}else if(file_exists_state(node->src_com) == 0){
				//compress flag file is not exists ,need again compress.
				delete_file(node->src_com);
			}
        }else{
		//src not exists, free node
			queue_node_free(node);
			continue;
		}
		char cur_dir[512]={0};
		//char cur_dirs[512]={0};
		char src_file[512]=".";
		char dest_file[512]=".";
		char *p = rindex(node->src_com, '/');
		memcpy(cur_dir, node->src_com, (int)(p-node->src_com));
		strcat(dest_file, p);
		p = rindex(node->src, '/');
		strcat(src_file, p);
		chdir(cur_dir);
		//getcwd(cur_dirs,512);

		ret = do_compress(compress->compress_type,buff, compress->compress_buff_size, dest_file, src_file);
		if (ret == NEWIUP_OK){
			//create - compress-file-k 
			create_file(compress_file_k);
			queue_node_free(node);
		} else{
			queue_node_push(&compress->queue, node);
		}
    }

    logging(LOG_INFO, "COMPRESS [%s] thread exit\n",compress->name);
	free(buff);
	return NULL;
}

void compress_handle_pthread_start(compress_group *compress)
{
	int j;
	compress->compress_buff_size = global_config.compress_buff_size;
	for(j=0;j<compress->compress_pcount;j++){
		pthread_create(&compress->pid[j], NULL, compress_handle, (void *)compress);
	}


}
void compress_handle_pthread_end(compress_group *compress)
{
	int j;

	for(j=0;j<compress->compress_pcount;j++){
		pthread_join(compress->pid[j], NULL);
	}
	if(compress->pid){
		free(compress->pid);
	}
}

