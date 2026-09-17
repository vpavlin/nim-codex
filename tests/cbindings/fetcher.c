// fetcher.c — a FRESH Logos Storage node that tries to fetch a CID purely via DISCOVERY
// (no direct dial to the holder). This is the public/shared model: given only a CID, find a
// provider through the DHT and pull it. If this works between two of our own nodes on the same
// network, discovery+advertise work and the mobile client will too. Usage: ./fetcher <CID>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include <unistd.h>
#include "../../library/libstorage.h"

typedef struct { pthread_mutex_t m; pthread_cond_t c; bool done; int ret; char *msg; } Resp;
static Resp *ra(void){ Resp*r=calloc(1,sizeof(Resp)); pthread_mutex_init(&r->m,0); pthread_cond_init(&r->c,0); r->ret=-1; return r; }
static void rf(Resp*r){ if(!r)return; if(r->msg)free(r->msg); pthread_cond_destroy(&r->c); pthread_mutex_destroy(&r->m); free(r); }
static void cb(int ret,const char*msg,size_t len,void*ud){
  Resp*r=(Resp*)ud; if(!r||ret==RET_PROGRESS)return;
  pthread_mutex_lock(&r->m);
  if(r->msg){free(r->msg);r->msg=0;}
  if(msg&&len>0){ r->msg=malloc(len+1); if(r->msg){memcpy(r->msg,msg,len);r->msg[len]=0;} }
  r->ret=ret; r->done=true; pthread_cond_signal(&r->c); pthread_mutex_unlock(&r->m);
}
static void waitr(Resp*r,int secs){
  struct timespec d; clock_gettime(CLOCK_REALTIME,&d); d.tv_sec+=secs;
  pthread_mutex_lock(&r->m);
  while(!r->done){ if(pthread_cond_timedwait(&r->c,&r->m,&d)==ETIMEDOUT)break; }
  pthread_mutex_unlock(&r->m);
}

int main(int argc,char**argv){
  const char *cid = argc>1 ? argv[1] : "zDvZRwzmAZ35ys1juAVEMsss158X5M3QfPMnwHdPbEMfiTdQkqWu";
  extern void libstorageNimMain(void); libstorageNimMain();
  const char *lvl = getenv("LOGLEVEL"); if(!lvl) lvl="INFO";
  char cfg[512];
  snprintf(cfg,sizeof(cfg),
    "{\"log-level\":\"%s\",\"data-dir\":\"%s/.storage-fetcher\",\"network\":\"logos.test\","
    "\"listen-ip\":\"0.0.0.0\",\"listen-port\":8071}", lvl, getenv("HOME"));
  Resp*r=ra();
  void*ctx=storage_new(cfg,(StorageCallback)cb,r);
  if(!ctx){ fprintf(stderr,"storage_new failed\n"); return 1; }
  waitr(r,30); rf(r);
  r=ra(); if(storage_start(ctx,(StorageCallback)cb,r)==RET_OK) waitr(r,30);
  if(r->ret!=RET_OK){ fprintf(stderr,"start failed: %s\n", r->msg?r->msg:""); return 1; } rf(r);

  fprintf(stderr,"fetcher up; giving discovery 20s to warm, then download_init(%s) via DISCOVERY only…\n", cid);
  sleep(20);

  // NO storage_connect — force the DHT discovery path (find providers for the CID).
  r=ra();
  time_t t0=time(0);
  int acc=storage_download_init(ctx,cid,65536,false,(StorageCallback)cb,r);
  if(acc==RET_OK) waitr(r,150);
  printf("\n==== FETCHER RESULT ====\n");
  printf("download_init ret=%d (%s) after %lds\n", r->ret, r->ret==RET_OK?"OK":"ERR", (long)(time(0)-t0));
  if(r->msg) printf("msg/err: %s\n", r->msg);
  printf("========================\n");
  return r->ret==RET_OK?0:2;
}
