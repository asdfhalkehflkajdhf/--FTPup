/*-
 * This file is in the public domain.
 * Do with it as you will.
 */
//  gcc minitar.c -larchive -lz -lbz2
//  ./a.out tar test.c test.tar
#include <sys/types.h>
#include <sys/stat.h>

#include "../Archive/archive.h"
#include "../Archive/archive_entry.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <zlib.h>
#include <bzlib.h>


#include <dirent.h>



#define NEWIUP_OK 0
#define NEWIUP_FAIL -1


enum compress_type_e
{
	UPLOAD_COMPRESS_GZ,
	UPLOAD_COMPRESS_BZ,
	//UPLOAD_COMPRESS_BZ2,
	UPLOAD_COMPRESS_Z,
	UPLOAD_COMPRESS_ZIP,
	UPLOAD_COMPRESS_7Z,
	UPLOAD_COMPRESS_TAR,
	UPLOAD_COMPRESS_TGZ,
	UPLOAD_COMPRESS_TAR_GZ,
	UPLOAD_COMPRESS_TAR_BZ,
	UPLOAD_COMPRESS_TAR_BZ2,
	UPLOAD_COMPRESS_TAR_Z,
	UPLOAD_COMPRESS_MAX
};

static char *g_gzip_type[UPLOAD_COMPRESS_MAX]={
	"gz",
	"bz",
//	"bz2",
	"Z",
	"zip",
	"7z",
	"tar",
	"tgz",
	"tar.gz",
	"tar.bz",
	"tar.bz2",
	"tar.Z",
};

static int do_compress_bz2(char *buff, int buff_size, const char * DestName,const char *SrcName)
{
    FILE * fp_in = NULL;int len = 0;

    int re = NEWIUP_OK;
    
    if( NULL == (fp_in = fopen(SrcName,"rb")))
    {
        return NEWIUP_FAIL;
    }
    BZFILE *out = BZ2_bzopen(DestName,"wb6f");
    if(out == NULL)
    {
        return NEWIUP_FAIL;
    }

    for(;;)
    {
        len = fread(buff,1,buff_size,fp_in);
        if(ferror(fp_in))
        {
            re = NEWIUP_FAIL;
            break;
        }
        if(len == 0) break;
        if(BZ2_bzwrite(out, buff, (unsigned)len) != len)
        {
            re = NEWIUP_FAIL;
        }
    }

    BZ2_bzclose(out);
    fclose(fp_in);
    return re;
 }

static int do_compress_gz(char *buff, int buff_size, const char * DestName,const char *SrcName)
{
    FILE * fp_in = NULL;int len = 0;

    int re = NEWIUP_OK;
    
    if( NULL == (fp_in = fopen(SrcName,"rb")))
    {
        return NEWIUP_FAIL;
    }
    gzFile out = gzopen(DestName,"wb6f");
    if(out == NULL)
    {
        return NEWIUP_FAIL;
    }

    for(;;)
    {
        len = fread(buff,1,buff_size,fp_in);
        if(ferror(fp_in))
        {
            re = NEWIUP_FAIL;
            break;
        }
        if(len == 0) break;
        if(gzwrite(out, buff, (unsigned)len) != len)
        {
            re = NEWIUP_FAIL;
        }
    }

    gzclose(out);
    fclose(fp_in);
    return re;
 }

#define ONE_M 1024*1024
static char *buff=NULL;
static int buff_size = ONE_M*100;
static char m_type[32]={0};
static char c_type[32]={0};
static char path[512]={0};
static char file[512]={0};
static int del_flag = 0;
static char mode = '*';
static int compress_type=-1;

static char *program_name=NULL;

static char dfile_name[128]={0};
static char sfile_name[128]={0};

char *l_opt_arg;
char* const short_options = "hdb:s:p:f:c:";
struct option long_options[] = {
{ "help", 0, NULL, 'h' },
{ 0, 0, 0, 0},
};

static int usage()
{
	int i;
	printf("usage: \n");
	
	printf("\t-h     show help\n\n");
	
	printf("mode:[f or p]\n");
	printf("\t-p     compress dir/file.                 example -p /home/ ,default is ./\n");
	printf("\t-f     compress file.                     example -f /home/abc.txt\n\n");
	
	printf("other\n");
	printf("\t-d     compress finish delete src file.\n");
	printf("\t-b     compress buff size,unit [M].       example -b 200 , default 100M\n");
	printf("\t-s     src file type.                     example -s txt \n");
	printf("\t-c     compress file type.                example -c gz support [");
		for(i = 0; i<UPLOAD_COMPRESS_MAX; i++)
		{
			printf("%s/", g_gzip_type[i]);
		}
		printf("]\n");
		
	printf("\n\n");
	printf("\t %s [-d [-b 100]] <-f ./abc.txt> <-c gz>\n", program_name);
	printf("\t %s [-d [-b 100]] <-p ./> <-s txt> <-c gz>\n", program_name);
	return 0;
}

static int get_compress_type_id(char *str)
{
    if (str == NULL)
    {
        return -1;
    }
	int i;
	for(i=0; i<UPLOAD_COMPRESS_MAX; i++){
	    if (0==strcmp(str, g_gzip_type[i]))
	    {
	        return i; 
	    }
	}

    return -1;
}


static int do_compress_archive(int compress_type, char *buff, int buff_size,const char *dfilename, const char *sfilename)
{
	struct archive *a;
	struct archive *disk;
	struct archive_entry *entry;
	ssize_t len;
	int fd;

	a = archive_write_new();
	switch (compress_type) {
	//case UPLOAD_COMPRESS_GZ:
	case UPLOAD_COMPRESS_TGZ:
	case UPLOAD_COMPRESS_TAR_GZ:
		archive_write_add_filter_gzip(a);
		archive_write_set_format_ustar(a);
		break;
	//case UPLOAD_COMPRESS_BZ:
	case UPLOAD_COMPRESS_TAR_BZ:
	//case UPLOAD_COMPRESS_BZ2:
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
		archive_write_add_filter_none(a);
		break;
	}

	archive_write_open_filename(a, dfilename);

		struct archive *disk2 = archive_read_disk_new();
		int r;

		r = archive_read_disk_open(disk2, sfilename);
		if (r != ARCHIVE_OK) {
			printf("do1 %s\n",archive_error_string(disk2));
			return NEWIUP_FAIL;
		}

		for (;;) {

			entry = archive_entry_new();
			r = archive_read_next_header2(disk2, entry);
			if (r == ARCHIVE_EOF){
				//printf("read eof\n");
				break;
			}
			if (r != ARCHIVE_OK) {
				printf("do2 %s\n",archive_error_string(disk2));
				return NEWIUP_FAIL;
			}
			archive_read_disk_descend(disk2);

			r = archive_write_header(a, entry);
			if (r < ARCHIVE_OK) {
				printf("%s\n",archive_error_string(disk2));
			}
			if (r == ARCHIVE_FATAL){
				printf("%s\n",archive_error_string(disk2));
				return NEWIUP_FAIL;
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
		archive_read_close(disk2);
		archive_read_free(disk2);

	archive_write_close(a);
	archive_write_free(a);
	return NEWIUP_OK;
}

static int argument_check_p()
{
	buff = malloc(buff_size);
	if(buff == NULL){
		return -1;
	}
	if(path[0] == 0){
		strcpy(path, "./");
	}
	if(m_type[0] == 0 || c_type[0] == 0 ){
		printf("-c and -m is need\n");
		return -1;
	}
	return 0;
}
static int compress_mode_p()
{
    DIR *pdir = NULL;
	int ret;

	pdir = opendir(path);
	if(pdir == NULL)
    {
        printf("opendir:%s error\n", path);
		return -1;
    }

	struct dirent *pdirent = NULL; 
    while ((pdirent = readdir(pdir)) != NULL)
    {
        if (pdirent->d_name[0] == '.')
		{
			continue;
		}

		if(is_monitor_type(pdirent->d_name, m_type)){
			continue;
		}
        snprintf(sfile_name, sizeof(sfile_name), "%s/%s", path, pdirent->d_name);
		if(is_file(sfile_name)){
			continue;
		}
		
		snprintf(dfile_name, sizeof(dfile_name), "%s/%s.%s", path, pdirent->d_name, c_type);
		if(file_exists_state(dfile_name) == 0){
			continue;
		}
		if(compress_type == UPLOAD_COMPRESS_GZ){
			ret = do_compress_gz( buff, buff_size,dfile_name, sfile_name);
		}else if(compress_type == UPLOAD_COMPRESS_BZ){
			ret = do_compress_bz2( buff, buff_size,dfile_name, sfile_name);
		}else{
			ret = do_compress_archive(compress_type, buff, buff_size,dfile_name, sfile_name);
		}
		if(ret == NEWIUP_OK && del_flag){
			delete_file(sfile_name);
		}
	}

    closedir(pdir);
	return 0;
}

static int argument_check_f()
{
	buff = malloc(buff_size);
	if(buff == NULL){
		return -1;
	}
	if(sfile_name[0] == 0){
		printf("-f is NULL\n");
		return -1;
	}
	if(c_type[0] == 0 ){
		printf("-c is need\n");
		return -1;
	}
	return 0;
}

static int compress_mode_f()
{
	int ret;

	if(is_file(sfile_name)){
		return -1;
	}
		
	snprintf(dfile_name, sizeof(dfile_name), "%s.%s", sfile_name, c_type);
	if(file_exists_state(dfile_name) == 0){
		return -1;
	}
	if(compress_type == UPLOAD_COMPRESS_GZ){
		ret = do_compress_gz( buff, buff_size,dfile_name, sfile_name);
	}else if(compress_type == UPLOAD_COMPRESS_BZ){
		ret = do_compress_bz2( buff, buff_size,dfile_name, sfile_name);
	}else{
		ret = do_compress_archive(compress_type, buff, buff_size,dfile_name, sfile_name);
	}
	if(ret == NEWIUP_OK && del_flag){
		delete_file(sfile_name);
	}
	

	return ret;
}


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
//is it a file 
//return 0:yes  -1:no
int is_file(char *filepath) 
{
    struct stat st;
    if (filepath != NULL && stat(filepath, &st) == 0)
    {
        if (S_ISREG(st.st_mode))
        {
            return 0;
        }
    }

    return -1; 
}
int file_exists_state(char *filename)
{
	//exists 0: is not exits -1
	return access(filename, 0);
}

int delete_file( char *path)
{
	remove(path);
    return 0;
}

int main(int argc, char * const*argv)
{
	int c;
	int ret;
	program_name = argv[0];
	
	while((c = getopt_long (argc, argv, short_options, long_options, NULL)) != -1)
	{
		switch (c)
		{
			case 'h':
				usage();
				return -1;
			case 'b':
				l_opt_arg = optarg;
				buff_size = ONE_M*atoi(l_opt_arg);
				break;
			case 's':
				l_opt_arg = optarg;
				strcpy(m_type, l_opt_arg);
				break;
			case 'p':
				if(mode != '*'){
					printf("mode is error [-f|-p] the only one\n");
					return -1;
				}
				mode = 'p';
				l_opt_arg = optarg;
				strcpy(path, l_opt_arg);
				break;
			case 'f':
				if(mode != '*'){
					printf("mode is error [-f|-p] the only one\n");
					return -1;
				}
				mode = 'f';
				l_opt_arg = optarg;
				strcpy(sfile_name, l_opt_arg);
				break;
			case 'c':
				l_opt_arg = optarg;
				strcpy(c_type, l_opt_arg);
				compress_type = get_compress_type_id(c_type);
				if(compress_type == -1){
					printf("-c %s compress type error\n", c_type);
					return -1;
				}
				break;
			case 'd':
				del_flag = 1;
				break;
			default:
				printf("%s usage error %c\n",program_name ,c);
				return -1;
		}
	}

	
	if(mode == 'f'){
		if(argument_check_f()){
			return -1;
		}
		compress_mode_f();
	}else if(mode == 'p'){
		if(argument_check_p()){
			return -1;
		}
		compress_mode_p();
	}else{
		printf("mode is error %c [-f|-p]\n",mode);
		usage();
	}
	
	if(buff)free(buff);
	return 0;
}


