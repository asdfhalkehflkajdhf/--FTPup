
#ifndef __CONF_YAML_LOADER_H__
#define __CONF_YAML_LOADER_H__

#ifdef __cplusplus
extern "C" {
#endif

int ConfYamlLoadFile(const char *);
int ConfYamlLoadString(const char *, size_t);

#ifdef __cplusplus
}
#endif

#endif /* !__CONF_YAML_LOADER_H__ */
