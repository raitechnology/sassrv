#ifndef __rai_sassrv__rvapi_h__
#define __rai_sassrv__rvapi_h__

#ifdef __cplusplus
extern "C" {
#endif

#define RV_OK 0
#define RV_NOT_PERMITTED 1
#define RV_BAD_HOST_IP 2
#define RV_BAD_GETHOSTBYNAME 3
#define RV_BAD_GETHOSTNAME 4
#define RV_BAD_LISTEN_ID 5
#define RV_NOT_CONNECTED 6
#define RV_INVALID_SUBJECT 7
#define RV_OUT_OF_MEMORY 8
#define RV_DAEMON_NOT_FOUND 9
#define RV_NO_NETWORK 10
#define RV_NO_SERVICE 11

typedef int rv_Status;
typedef void * rv_Session;
typedef void * rv_Listener;
typedef void * rv_Timer;
typedef void * rv_Signal;
typedef const char * rv_Name;
typedef int rvmsg_Type;

#define RVMSG_RVMSG  1 /* types for rv_Send and rv_SendWithReply */
#define RVMSG_OPAQUE 7
#define RV_TRUE 1
#define RV_FALSE 0

typedef unsigned long long rv_stats_t;
typedef unsigned int rv_ipaddr_t;

struct rv_Stats {
  rv_stats_t     bytes_recv,
                 msgs_recv,
                 lost_seqno,
                 repeat_seqno,
                 nak_count,
                 reorder_seqno;
  rv_ipaddr_t  * lost_src,
               * repeat_src,
               * nak_src,
               * reorder_src;
  unsigned int   lost_src_count,
                 repeat_src_count,
                 nak_src_count,
                 reorder_src_count;
};

typedef void (*rv_MessageCallback)( rv_Listener listener, rv_Name subject,
                                    rv_Name reply, rvmsg_Type type,
                                    size_t data_len, void *data, void * cl );
typedef void (*rv_TimerCallback)( rv_Timer timer, void * cl );
typedef void (*rv_SignalCallback)( rv_Signal shandle, void * cl );

rv_Status rv_Init( rv_Session * session, rv_Name service,
                   rv_Name network,  rv_Name daemon );
rv_Status rv_Term( rv_Session session );
rv_Status rv_GetStats( rv_Session session, struct rv_Stats * stats );
rv_Status rv_ListenSubject( rv_Session session, rv_Listener * listener,
                            rv_Name subject, rv_MessageCallback callback,
                            rv_Name reply,  void * closure );
rv_Status rv_ListenInbox( rv_Session session, rv_Listener * listener,
                          char *inbox_str,  size_t inbox_len,
                          rv_MessageCallback callback, void * closure );

rv_Status rv_Close( rv_Listener listener );
const char *rv_Subject( rv_Listener listener );
const char *rv_ErrorText( rv_Session session, rv_Status status );
rv_Status rv_Send( rv_Session session, rv_Name subject, rvmsg_Type type,
                   size_t data_len, void *data );
rv_Status rv_SendWithReply( rv_Session session, rv_Name subject, rv_Name reply,
                            rvmsg_Type type, size_t data_len, void *data );
rv_Status rv_MainLoop( rv_Session session );
rv_Status rv_CreateTimer( rv_Session session, rv_Timer * timer,
                          float interval,  rv_TimerCallback callback,
                          void * closure );
rv_Status rv_DestroyTimer( rv_Timer timer );
rv_Status rv_CreateSignal( rv_Session session,  rv_Signal * shandle, int signo,
                           rv_SignalCallback callback, void * closure );
rv_Status rv_DestroySignal( rv_Signal shandle );

#ifdef __cplusplus
}
#endif
#endif
