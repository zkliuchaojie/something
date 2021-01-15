#ifndef AEP_H_
#define AEP_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PM_AVAILABLE

#ifdef PM_AVAILABLE

#include <libvmem.h>

VMEM *vmp;

void init_vmp() {
	unsigned long pool_size =  200000000000ull;
	if ((vmp = vmem_create("/mnt/pmem0/pmdir", \
		pool_size)) == NULL) {
		perror("vmem_create");
		exit(1);
	}
}

void delete_vmp() {
	vmem_delete(vmp);
}

void* vmem_malloc(unsigned long size) {
	return (void*)vmem_malloc(vmp, size);
}

void vmem_free(void *ptr) {
	vmem_free(vmp, ptr);
}

#else
void init_vmp() {
	;
}

void delete_vmp() {
	;
}

void* vmem_malloc(unsigned long size) {
	return (void*)malloc(size);
}

void vmem_free(void *ptr) {
	free(ptr);
}
#endif

#endif