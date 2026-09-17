// seeder.c — a long-lived Codex node that uploads a fixed blob and stays alive, so a phone can
// fetch the CID from it over the network. Reuses the proven FFI flow (storage.c). Fixed data-dir +
// payload → stable peerId + CID across restarts. Prints CID + peerId + the dialable mesh multiaddr.
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

#define BOX_MESH_IP "198.19.224.254"
#define LISTEN_PORT 8070

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
static void waitr(Resp*r){
  struct timespec d; clock_gettime(CLOCK_REALTIME,&d); d.tv_sec+=120;
  pthread_mutex_lock(&r->m);
  while(!r->done){ if(pthread_cond_timedwait(&r->c,&r->m,&d)==ETIMEDOUT)break; }
  pthread_mutex_unlock(&r->m);
}
// run an async call, wait, return strdup'd msg (caller frees); *ok set
static char* call1(int (*fn)(void*,StorageCallback,void*), void*ctx, int*ok){
  Resp*r=ra(); int a=fn(ctx,(StorageCallback)cb,r); if(a==RET_OK)waitr(r);
  *ok=(a==RET_OK&&r->ret==RET_OK); char*m=r->msg?strdup(r->msg):0; rf(r); return m;
}

int main(void){
  extern void libstorageNimMain(void); libstorageNimMain();
  char cfg[512];
  const char *lvl = getenv("LOGLEVEL"); if(!lvl) lvl = "INFO";  // set LOGLEVEL=DEBUG/TRACE to see blockexc
  snprintf(cfg,sizeof(cfg),
    "{\"log-level\":\"%s\",\"data-dir\":\"%s/.codex-seeder\",\"network\":\"logos.test\","
    "\"listen-ip\":\"0.0.0.0\",\"listen-port\":%d}", lvl, getenv("HOME"), LISTEN_PORT);
  Resp*r=ra();
  void*ctx=storage_new(cfg,(StorageCallback)cb,r);
  if(!ctx){ fprintf(stderr,"storage_new failed\n"); return 1; }
  waitr(r); rf(r);
  int ok=0; char*m;
  m=call1(storage_start,ctx,&ok); if(!ok){fprintf(stderr,"start failed: %s\n",m?m:"");return 1;} free(m);

  // upload a fixed payload → stable CID
  const char *payload="Hello from the box over Codex — fetched to the phone!";
  size_t plen=strlen(payload);
  r=ra();
  if(storage_upload_init(ctx,"hello_phone.txt",plen,(StorageCallback)cb,r)!=RET_OK){fprintf(stderr,"upload_init rejected\n");return 1;}
  waitr(r); char*sid=r->msg?strdup(r->msg):0; rf(r);
  if(!sid){fprintf(stderr,"no session id\n");return 1;}
  r=ra();
  storage_upload_chunk(ctx,sid,(uint8_t*)payload,plen,(StorageCallback)cb,r); waitr(r); rf(r);
  r=ra();
  storage_upload_finalize(ctx,sid,(StorageCallback)cb,r); waitr(r);
  char*cid=r->msg?strdup(r->msg):0; rf(r); free(sid);
  if(!cid){fprintf(stderr,"no CID\n");return 1;}

  // peerId via debug
  m=call1(storage_debug,ctx,&ok);
  char peerId[128]={0};
  if(m){ char*p=strstr(m,"\"peerId\":\""); if(p){ p+=10; char*e=strchr(p,'"'); if(e&&(size_t)(e-p)<sizeof(peerId)){memcpy(peerId,p,e-p);peerId[e-p]=0;} } }

  printf("\n================ CODEX SEEDER READY ================\n");
  printf("CID=%s\n",cid);
  printf("PEERID=%s\n",peerId);
  printf("MULTIADDR=/ip4/%s/tcp/%d/p2p/%s\n",BOX_MESH_IP,LISTEN_PORT,peerId);
  printf("DEBUG=%s\n", m?m:"(none)");
  printf("====================================================\n\n");
  fflush(stdout);
  if(m)free(m); free(cid);
  // stay alive so the phone can dial + fetch
  while(1) sleep(60);
  return 0;
}
