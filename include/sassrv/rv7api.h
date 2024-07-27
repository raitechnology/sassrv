#ifndef __rai_sassrv__rv7api_h__
#define __rai_sassrv__rv7api_h__

#ifdef __cplusplus
extern "C" {
#endif

typedef signed char        tibrv_i8;
typedef unsigned char      tibrv_u8;
typedef short              tibrv_i16;
typedef unsigned short     tibrv_u16;
typedef int                tibrv_i32;
typedef unsigned int       tibrv_u32;
typedef long long          tibrv_i64;
typedef unsigned long long tibrv_u64;
typedef float              tibrv_f32;
typedef double             tibrv_f64;
typedef unsigned short     tibrv_ipport16;
typedef unsigned int       tibrv_ipaddr32;
typedef void *             tibrvMsg;
typedef tibrv_u32          tibrvId;
typedef tibrvId            tibrvEvent;
typedef tibrvEvent         tibrvPollEvent;
typedef tibrvId            tibrvQueue;
typedef tibrvQueue         tibrvPollQueue;
typedef tibrvId            tibrvTransport;
typedef tibrvId            tibrvQueueGroup;
typedef tibrvId            tibrvDispatchable;
typedef tibrvId            tibrvDispatcher;
typedef tibrvId            tibrvContentDesc;
typedef tibrv_u32          tibrvEventType;

typedef enum {
  TIBRV_FALSE = 0,
  TIBRV_TRUE  = 1
} tibrv_bool;

typedef struct {
  tibrv_i64 sec;
  tibrv_u32 nsec;
} tibrvMsgDateTime;

typedef union {
    tibrvMsg         msg;
    const char *     str;
    const void *     buf;
    const void *     array;
    tibrv_bool       boolean;
    tibrv_i8         i8;
    tibrv_u8         u8;
    tibrv_i16        i16;
    tibrv_u16        u16;
    tibrv_i32        i32;
    tibrv_u32        u32;
    tibrv_i64        i64;
    tibrv_u64        u64;
    tibrv_f32        f32;
    tibrv_f64        f64;
    tibrv_ipport16   ipport16;
    tibrv_ipaddr32   ipaddr32;
    tibrvMsgDateTime date;
} tibrvLocalData;

typedef struct {
  const char   * name;
  tibrv_u32      size;
  tibrv_u32      count;
  tibrvLocalData data;
  tibrv_u16      id;
  tibrv_u8       type;
} tibrvMsgField;

typedef void (* tibrvEventCallback )( tibrvEvent , tibrvMsg , void * );
typedef void (* tibrvEventVectorCallback )( tibrvMsg * , tibrv_u32 );
typedef void (* tibrvEventOnComplete )( tibrvEvent , void * );
typedef void (* tibrvQueueOnComplete )( tibrvQueue, void * );

#define TIBRV_TIMER_EVENT       1
#define TIBRV_IO_EVENT          2
#define TIBRV_LISTEN_EVENT      3

#define TIBRV_INVALID_ID        0
#define TIBRV_DEFAULT_QUEUE     1
#define TIBRV_PROCESS_TRANSPORT 10

#define TIBRVMSG_MSG         1
#define TIBRVMSG_DATETIME    3
#define TIBRVMSG_OPAQUE      7
#define TIBRVMSG_STRING      8
#define TIBRVMSG_BOOL        9
#define TIBRVMSG_I8          14
#define TIBRVMSG_U8          15
#define TIBRVMSG_I16         16
#define TIBRVMSG_U16         17
#define TIBRVMSG_I32         18
#define TIBRVMSG_U32         19
#define TIBRVMSG_I64         20
#define TIBRVMSG_U64         21
#define TIBRVMSG_F32         24
#define TIBRVMSG_F64         25
#define TIBRVMSG_IPPORT16    26
#define TIBRVMSG_IPADDR32    27
#define TIBRVMSG_ENCRYPTED   32
#define TIBRVMSG_NONE        22
#define TIBRVMSG_I8ARRAY     34
#define TIBRVMSG_U8ARRAY     35
#define TIBRVMSG_I16ARRAY    36
#define TIBRVMSG_U16ARRAY    37
#define TIBRVMSG_I32ARRAY    38
#define TIBRVMSG_U32ARRAY    39
#define TIBRVMSG_I64ARRAY    40
#define TIBRVMSG_U64ARRAY    41
#define TIBRVMSG_F32ARRAY    44
#define TIBRVMSG_F64ARRAY    45
#define TIBRVMSG_XML         47
#define TIBRVMSG_STRINGARRAY 48
#define TIBRVMSG_MSGARRAY    49
#define TIBRVMSG_USER_FIRST  128
#define TIBRVMSG_USER_LAST   255
#define TIBRV_SUBJECT_MAX    255
#define TIBRVMSG_DATETIME_STRING_SIZE 32

typedef enum {
  TIBRV_OK                        = 0,
  TIBRV_INIT_FAILURE              = 1,
  TIBRV_INVALID_TRANSPORT         = 2,
  TIBRV_INVALID_ARG               = 3,
  TIBRV_NOT_INITIALIZED           = 4,
  TIBRV_ARG_CONFLICT              = 5,
  TIBRV_SERVICE_NOT_FOUND         = 16,
  TIBRV_NETWORK_NOT_FOUND         = 17,
  TIBRV_DAEMON_NOT_FOUND          = 18,
  TIBRV_NO_MEMORY                 = 19,
  TIBRV_INVALID_SUBJECT           = 20,
  TIBRV_DAEMON_NOT_CONNECTED      = 21,
  TIBRV_VERSION_MISMATCH          = 22,
  TIBRV_SUBJECT_COLLISION         = 23,
  TIBRV_VC_NOT_CONNECTED          = 24,
  TIBRV_NOT_PERMITTED             = 27,
  TIBRV_INVALID_NAME              = 30,
  TIBRV_INVALID_TYPE              = 31,
  TIBRV_INVALID_SIZE              = 32,
  TIBRV_INVALID_COUNT             = 33,
  TIBRV_NOT_FOUND                 = 35,
  TIBRV_ID_IN_USE                 = 36,
  TIBRV_ID_CONFLICT               = 37,
  TIBRV_CONVERSION_FAILED         = 38,
  TIBRV_RESERVED_HANDLER          = 39,
  TIBRV_ENCODER_FAILED            = 40,
  TIBRV_DECODER_FAILED            = 41,
  TIBRV_INVALID_MSG               = 42,
  TIBRV_INVALID_FIELD             = 43,
  TIBRV_INVALID_INSTANCE          = 44,
  TIBRV_CORRUPT_MSG               = 45,
  TIBRV_ENCODING_MISMATCH         = 46,
  TIBRV_TIMEOUT                   = 50,
  TIBRV_INTR                      = 51,
  TIBRV_INVALID_DISPATCHABLE      = 52,
  TIBRV_INVALID_DISPATCHER        = 53,
  TIBRV_INVALID_EVENT             = 60,
  TIBRV_INVALID_CALLBACK          = 61,
  TIBRV_INVALID_QUEUE             = 62,
  TIBRV_INVALID_QUEUE_GROUP       = 63,
  TIBRV_INVALID_TIME_INTERVAL     = 64,
  TIBRV_INVALID_IO_SOURCE         = 65,
  TIBRV_INVALID_IO_CONDITION      = 66,
  TIBRV_SOCKET_LIMIT              = 67,
  TIBRV_OS_ERROR                  = 68,
  TIBRV_INSUFFICIENT_BUFFER       = 70,
  TIBRV_EOF                       = 71,
  TIBRV_INVALID_FILE              = 72,
  TIBRV_FILE_NOT_FOUND            = 73,
  TIBRV_IO_FAILED                 = 74,
  TIBRV_NOT_FILE_OWNER            = 80,
  TIBRV_USERPASS_MISMATCH         = 81,
  TIBRV_TOO_MANY_NEIGHBORS        = 90,
  TIBRV_ALREADY_EXISTS            = 91,
  TIBRV_PORT_BUSY                 = 100,
  TIBRV_DELIVERY_FAILED           = 101,
  TIBRV_QUEUE_LIMIT               = 102,
  TIBRV_INVALID_CONTENT_DESC      = 110,
  TIBRV_INVALID_SERIALIZED_BUFFER = 111,
  TIBRV_DESCRIPTOR_NOT_FOUND      = 115,
  TIBRV_CORRUPT_SERIALIZED_BUFFER = 116,
  TIBRV_IPM_ONLY                  = 117
} tibrv_status;

const char * tibrvStatus_GetText( tibrv_status status );
const char * tibrv_Version( void );
tibrv_status tibrv_Open( void );
tibrv_status tibrv_Close( void );
tibrv_status tibrv_SetCodePages( char *host_codepage, char *net_codepage );
tibrv_status tibrv_SetRVParameters( tibrv_u32 argc, const char **argv );
tibrv_status tibrv_OpenEx( const char  *pathname );
tibrv_bool tibrv_IsIPM( void );

tibrv_status tibrvEvent_CreateListener( tibrvEvent * event,  tibrvQueue queue,  tibrvEventCallback cb,
                                        tibrvTransport tport,  const char * subj,  const void * closure );
tibrv_status tibrvEvent_CreateVectorListener( tibrvEvent * event,  tibrvQueue queue, tibrvEventVectorCallback cb,
                                              tibrvTransport tport, const char * subj, const void * closure );
tibrv_status tibrvEvent_CreateTimer( tibrvEvent * event,  tibrvQueue queue,  tibrvEventCallback cb,
                                     tibrv_f64 ival,  const void * closure );
#define tibrvEvent_Destroy( event ) tibrvEvent_DestroyEx( event, NULL )
tibrv_status tibrvEvent_DestroyEx( tibrvEvent event,  tibrvEventOnComplete cb );
tibrv_status tibrvEvent_GetType( tibrvEvent event,  tibrvEventType * type );
tibrv_status tibrvEvent_GetQueue( tibrvEvent event,  tibrvQueue * queue );
tibrv_status tibrvEvent_GetListenerSubject( tibrvEvent event,  const char ** subject );
tibrv_status tibrvEvent_GetListenerTransport( tibrvEvent event,  tibrvTransport * tport );
tibrv_status tibrvEvent_GetTimerInterval( tibrvEvent event,  tibrv_f64 * ival );
tibrv_status tibrvEvent_ResetTimerInterval( tibrvEvent event,  tibrv_f64 ival );

typedef enum {
  TIBRVQUEUE_DISCARD_NONE  = 0,
  TIBRVQUEUE_DISCARD_NEW   = 1,
  TIBRVQUEUE_DISCARD_FIRST = 2,
  TIBRVQUEUE_DISCARD_LAST  = 3
} tibrvQueueLimitPolicy;
typedef enum
{
  TIBRV_TRANSPORT_DEFAULT_BATCH = 0,
  TIBRV_TRANSPORT_TIMER_BATCH   = 1
} tibrvTransportBatchMode;

#define TIBRVQUEUE_DEFAULT_POLICY TIBRVQUEUE_DISCARD_NONE
#define TIBRVQUEUE_DEFAULT_PRIORITY 1
#define TIBRV_WAIT_FOREVER -1.0
#define TIBRV_NO_WAIT 0.0

tibrv_status tibrvQueue_Create( tibrvQueue * q );
tibrv_status tibrvQueue_TimedDispatch( tibrvQueue q, tibrv_f64 timeout );
tibrv_status tibrvQueue_TimedDispatchOneEvent( tibrvQueue q, tibrv_f64 time );
tibrv_status tibrvQueue_DestroyEx( tibrvQueue q, tibrvQueueOnComplete cb, const void * closure );
tibrv_status tibrvQueue_GetCount( tibrvQueue  q, tibrv_u32 * num );
tibrv_status tibrvQueue_GetPriority( tibrvQueue q, tibrv_u32 * priority );
tibrv_status tibrvQueue_SetPriority( tibrvQueue q, tibrv_u32 priority );
tibrv_status tibrvQueue_GetLimitPolicy( tibrvQueue q, tibrvQueueLimitPolicy * policy, tibrv_u32 * max_ev, tibrv_u32 * discard );
tibrv_status tibrvQueue_SetLimitPolicy( tibrvQueue q, tibrvQueueLimitPolicy policy, tibrv_u32 max_ev, tibrv_u32 discard );
tibrv_status tibrvQueue_SetName( tibrvQueue q, const char * name );
tibrv_status tibrvQueue_GetName( tibrvQueue q, const char ** name );
typedef void (* tibrvQueueHook )( tibrvQueue , void * );
tibrv_status tibrvQueue_SetHook( tibrvQueue q, tibrvQueueHook hook, void * closure );
tibrv_status tibrvQueue_GetHook( tibrvQueue q, tibrvQueueHook * hook );
#define tibrvQueue_Dispatch( q ) tibrvQueue_TimedDispatch( q, TIBRV_WAIT_FOREVER )
#define tibrvQueue_Poll( q ) tibrvQueue_TimedDispatch( q, TIBRV_NO_WAIT )
#define tibrvQueue_Destroy( q ) tibrvQueue_DestroyEx( q, NULL, NULL )

tibrv_status tibrvQueueGroup_Create( tibrvQueueGroup * grp );
#define tibrvQueueGroup_Dispatch(qg) tibrvQueueGroup_TimedDispatch( qg, TIBRV_WAIT_FOREVER )
#define tibrvQueueGroup_Poll(qg) tibrvQueueGroup_TimedDispatch( qg, TIBRV_NO_WAIT )
tibrv_status tibrvQueueGroup_TimedDispatch( tibrvQueueGroup grp, tibrv_f64 timeout );
tibrv_status tibrvQueueGroup_Destroy( tibrvQueueGroup grp );
tibrv_status tibrvQueueGroup_Add( tibrvQueueGroup grp, tibrvQueue q );
tibrv_status tibrvQueueGroup_Remove( tibrvQueueGroup grp, tibrvQueue q );

tibrv_status tibrvTransport_Create( tibrvTransport * tport, const char * service,
                                    const char * network, const char * daemon );
tibrv_status tibrvTransport_Send( tibrvTransport tport, tibrvMsg msg );
tibrv_status tibrvTransport_Sendv( tibrvTransport tport, tibrvMsg * vec, tibrv_u32 len );
tibrv_status tibrvTransport_SendRequest( tibrvTransport tport, tibrvMsg msg,
                                         tibrvMsg * reply, tibrv_f64 idle_timeout );
tibrv_status tibrvTransport_SendReply( tibrvTransport tport, tibrvMsg msg, tibrvMsg request_msg );
tibrv_status tibrvTransport_Destroy( tibrvTransport tport );
tibrv_status tibrvTransport_CreateInbox( tibrvTransport tport, char * inbox_str, tibrv_u32 inbox_len );
tibrv_status tibrvTransport_GetService( tibrvTransport tport, const char ** service_string );
tibrv_status tibrvTransport_GetNetwork( tibrvTransport tport, const char ** network_string );
tibrv_status tibrvTransport_GetDaemon( tibrvTransport tport, const char ** daemon_string );
tibrv_status tibrvTransport_SetDescription( tibrvTransport tport, const char * descr );
tibrv_status tibrvTransport_GetDescription( tibrvTransport tport, const char ** descr );
tibrv_status tibrvTransport_SetSendingWaitLimit( tibrvTransport tport, tibrv_u32 num_bytes );
tibrv_status tibrvTransport_GetSendingWaitLimit( tibrvTransport tport, tibrv_u32 * num_bytes );
tibrv_status tibrvTransport_SetBatchMode( tibrvTransport tport, tibrvTransportBatchMode mode );
tibrv_status tibrvTransport_SetBatchSize( tibrvTransport tport, tibrv_u32 num_bytes );
tibrv_status tibrvTransport_CreateLicensed( tibrvTransport * tport, const char * service,
                                            const char * network, const char * daemon, const char * );
tibrv_status tibrvTransport_RequestReliability( tibrvTransport tport, tibrv_f64 reliability );

#define tibrvDispatcher_Create( disp, able ) tibrvDispatcher_CreateEx( disp, able, TIBRV_WAIT_FOREVER )
tibrv_status tibrvDispatcher_CreateEx( tibrvDispatcher * disp, tibrvDispatchable able, tibrv_f64 idle_timeout );
tibrv_status tibrvDispatcher_Join( tibrvDispatcher disp );
tibrv_status tibrvDispatcher_Destroy( tibrvDispatcher disp );
tibrv_status tibrvDispatcher_SetName( tibrvDispatcher disp, const char * name );
tibrv_status tibrvDispatcher_GetName( tibrvDispatcher disp, const char ** name );

tibrv_status tibrvMsg_Create( tibrvMsg * msg );
tibrv_status tibrvMsg_CreateEx( tibrvMsg * msg,  tibrv_u32 initial );
tibrv_status tibrvMsg_Destroy( tibrvMsg msg );
tibrv_status tibrvMsg_Detach( tibrvMsg msg );
tibrv_status tibrvMsg_Reset( tibrvMsg msg );
tibrv_status tibrvMsg_Expand( tibrvMsg msg,  tibrv_i32 add );
tibrv_status tibrvMsg_SetSendSubject( tibrvMsg msg,  const char * subject );
tibrv_status tibrvMsg_GetSendSubject( tibrvMsg msg,  const char ** subject );
tibrv_status tibrvMsg_SetReplySubject( tibrvMsg msg,  const char * reply );
tibrv_status tibrvMsg_GetReplySubject( tibrvMsg msg,  const char ** reply );
tibrv_status tibrvMsg_GetEvent( tibrvMsg msg,  tibrvEvent * id );
tibrv_status tibrvMsg_GetClosure( tibrvMsg msg,  void ** closure );
tibrv_status tibrvMsg_GetNumFields( tibrvMsg msg,  tibrv_u32 * num_flds );
tibrv_status tibrvMsg_GetByteSize( tibrvMsg msg,  tibrv_u32 * size );
tibrv_status tibrvMsg_ConvertToString( tibrvMsg msg,  const char ** str );
tibrv_status tibrvMsg_AddField( tibrvMsg msg,  tibrvMsgField * field );
tibrv_status tibrvMsg_GetFieldInstance( tibrvMsg msg,  const char * name,  tibrvMsgField * field,  tibrv_u32 inst );
tibrv_status tibrvMsg_GetFieldByIndex( tibrvMsg msg,  tibrvMsgField * field,  tibrv_u32 idx );
tibrv_status tibrvMsg_RemoveFieldInstance( tibrvMsg msg,  const char * name,  tibrv_u32 inst );
tibrv_status tibrvMsg_UpdateField( tibrvMsg msg,  tibrvMsgField * fld );
tibrv_status tibrvMsg_CreateFromBytes( tibrvMsg * msg,   const void * bytes );
tibrv_status tibrvMsg_GetAsBytes( tibrvMsg msg,   const void ** ptr );
tibrv_status tibrvMsg_GetAsBytesCopy( tibrvMsg msg, void * ptr, tibrv_u32 size);
tibrv_status tibrvMsg_CreateCopy( const tibrvMsg msg,  tibrvMsg * copy );
tibrv_status tibrvMsg_MarkReferences( tibrvMsg msg );
tibrv_status tibrvMsg_ClearReferences( tibrvMsg msg );
tibrv_status tibrvMsg_GetCurrentTime( tibrvMsgDateTime * cur );
tibrv_status tibrvMsg_GetCurrentTimeString( char * local,  char * gmt );

tibrv_status tibrvMsg_AddMsgEx( tibrvMsg msg, const char * name, tibrvMsg value, tibrv_u16 id );
tibrv_status tibrvMsg_AddIPAddr32Ex( tibrvMsg msg,  const char * name,  tibrv_ipaddr32 value,  tibrv_u16 id );
tibrv_status tibrvMsg_AddIPPort16Ex( tibrvMsg msg, const char * name, tibrv_ipport16 value, tibrv_u16 id );
tibrv_status tibrvMsg_AddDateTimeEx( tibrvMsg msg, const char * name, const tibrvMsgDateTime * value, tibrv_u16 id );
tibrv_status tibrvMsg_AddBoolEx( tibrvMsg msg, const char * name, tibrv_bool value, tibrv_u16 id );
tibrv_status tibrvMsg_AddI8Ex( tibrvMsg msg, const char * name, tibrv_i8 value, tibrv_u16 id );
tibrv_status tibrvMsg_AddI8ArrayEx( tibrvMsg msg, const char * name, const tibrv_i8 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_AddU8Ex( tibrvMsg msg, const char * name, tibrv_u8 value, tibrv_u16 id );
tibrv_status tibrvMsg_AddU8ArrayEx( tibrvMsg msg, const char * name, const tibrv_u8 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_AddI16Ex( tibrvMsg msg, const char * name, tibrv_i16 value, tibrv_u16 id );
tibrv_status tibrvMsg_AddI16ArrayEx( tibrvMsg msg, const char * name, const tibrv_i16 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_AddU16Ex( tibrvMsg msg, const char * name, tibrv_u16 value, tibrv_u16 id );
tibrv_status tibrvMsg_AddU16ArrayEx( tibrvMsg msg, const char * name, const tibrv_u16 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_AddI32Ex( tibrvMsg msg, const char * name, tibrv_i32 value, tibrv_u16 id );
tibrv_status tibrvMsg_AddI32ArrayEx( tibrvMsg msg, const char * name, const tibrv_i32 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_AddU32Ex( tibrvMsg msg, const char * name, tibrv_u32 value, tibrv_u16 id );
tibrv_status tibrvMsg_AddU32ArrayEx( tibrvMsg msg, const char * name, const tibrv_u32 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_AddI64Ex( tibrvMsg msg, const char * name, tibrv_i64 value, tibrv_u16 id );
tibrv_status tibrvMsg_AddI64ArrayEx( tibrvMsg msg, const char * name, const tibrv_i64 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_AddU64Ex( tibrvMsg msg, const char * name, tibrv_u64 value, tibrv_u16 id );
tibrv_status tibrvMsg_AddU64ArrayEx( tibrvMsg msg, const char * name, const tibrv_u64 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_AddF32Ex( tibrvMsg msg, const char * name, tibrv_f32 value, tibrv_u16 id );
tibrv_status tibrvMsg_AddF32ArrayEx( tibrvMsg msg, const char * name, const tibrv_f32 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_AddF64Ex( tibrvMsg msg, const char * name, tibrv_f64 value, tibrv_u16 id );
tibrv_status tibrvMsg_AddF64ArrayEx( tibrvMsg msg, const char * name, const tibrv_f64 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_AddStringArrayEx( tibrvMsg msg, const char * name, const char ** value, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_AddMsgArrayEx( tibrvMsg msg, const char * name, const tibrvMsg * value, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_AddStringEx( tibrvMsg msg, const char * name, const char * value, tibrv_u16 id );
tibrv_status tibrvMsg_AddOpaqueEx( tibrvMsg msg, const char * name, const void * value, tibrv_u32 size, tibrv_u16 id );
tibrv_status tibrvMsg_AddXmlEx( tibrvMsg msg, const char * name, const void * value, tibrv_u32 size, tibrv_u16 id );

tibrv_status tibrvMsg_GetFieldEx( tibrvMsg msg, const char * name, tibrvMsgField * field, tibrv_u16 id );
tibrv_status tibrvMsg_GetMsgEx( tibrvMsg msg, const char * name, tibrvMsg * sub, tibrv_u16 id );
tibrv_status tibrvMsg_GetIPAddr32Ex( tibrvMsg msg, const char * name, tibrv_ipaddr32 * value, tibrv_u16 id );
tibrv_status tibrvMsg_GetIPPort16Ex( tibrvMsg msg, const char * name, tibrv_ipport16 * value, tibrv_u16 id );
tibrv_status tibrvMsg_GetDateTimeEx( tibrvMsg msg, const char * name, tibrvMsgDateTime * value, tibrv_u16 id );
tibrv_status tibrvMsg_GetBoolEx( tibrvMsg msg, const char * name, tibrv_bool * value, tibrv_u16 id );
tibrv_status tibrvMsg_GetI8Ex( tibrvMsg msg, const char * name, tibrv_i8 * value, tibrv_u16 id );
tibrv_status tibrvMsg_GetI8ArrayEx( tibrvMsg msg, const char * name, const tibrv_i8 ** array, tibrv_u32 * num, tibrv_u16 id );
tibrv_status tibrvMsg_GetU8Ex( tibrvMsg msg, const char * name, tibrv_u8 * value, tibrv_u16 id );
tibrv_status tibrvMsg_GetU8ArrayEx( tibrvMsg msg, const char * name, const tibrv_u8 ** array, tibrv_u32 * num, tibrv_u16 id );
tibrv_status tibrvMsg_GetI16Ex( tibrvMsg msg, const char * name, tibrv_i16 * value, tibrv_u16 id );
tibrv_status tibrvMsg_GetI16ArrayEx( tibrvMsg msg, const char * name, const tibrv_i16 ** array, tibrv_u32 * num, tibrv_u16 id );
tibrv_status tibrvMsg_GetU16Ex( tibrvMsg msg, const char * name, tibrv_u16 * value, tibrv_u16 id );
tibrv_status tibrvMsg_GetU16ArrayEx( tibrvMsg msg, const char * name, const tibrv_u16 ** array, tibrv_u32 * num, tibrv_u16 id );
tibrv_status tibrvMsg_GetI32Ex( tibrvMsg msg, const char * name, tibrv_i32 * value, tibrv_u16 id );
tibrv_status tibrvMsg_GetI32ArrayEx( tibrvMsg msg, const char * name, const tibrv_i32 ** array, tibrv_u32 * num, tibrv_u16 id );
tibrv_status tibrvMsg_GetU32Ex( tibrvMsg msg, const char * name, tibrv_u32 * value, tibrv_u16 id );
tibrv_status tibrvMsg_GetU32ArrayEx( tibrvMsg msg, const char * name, const tibrv_u32 ** array, tibrv_u32 * num, tibrv_u16 id );
tibrv_status tibrvMsg_GetI64Ex( tibrvMsg msg, const char * name, tibrv_i64 * value, tibrv_u16 id );
tibrv_status tibrvMsg_GetI64ArrayEx( tibrvMsg msg, const char * name, const tibrv_i64 ** array, tibrv_u32 * num, tibrv_u16 id );
tibrv_status tibrvMsg_GetU64Ex( tibrvMsg msg, const char * name, tibrv_u64 * value, tibrv_u16 id );
tibrv_status tibrvMsg_GetU64ArrayEx( tibrvMsg msg, const char * name, const tibrv_u64 ** array, tibrv_u32 * num, tibrv_u16 id );
tibrv_status tibrvMsg_GetF32Ex( tibrvMsg msg, const char * name, tibrv_f32 * value, tibrv_u16 id );
tibrv_status tibrvMsg_GetF32ArrayEx( tibrvMsg msg, const char * name, const tibrv_f32 ** array, tibrv_u32 * num, tibrv_u16 id );
tibrv_status tibrvMsg_GetF64Ex( tibrvMsg msg, const char * name, tibrv_f64 * value, tibrv_u16 id );
tibrv_status tibrvMsg_GetF64ArrayEx( tibrvMsg msg, const char * name, const tibrv_f64 ** array, tibrv_u32 * num, tibrv_u16 id );
tibrv_status tibrvMsg_GetStringArrayEx( tibrvMsg msg, const char * name, const char *** array, tibrv_u32 * num, tibrv_u16 id );
tibrv_status tibrvMsg_GetMsgArrayEx( tibrvMsg msg, const char * name, const tibrvMsg ** array, tibrv_u32 * num, tibrv_u16 id );
tibrv_status tibrvMsg_GetStringEx( tibrvMsg msg, const char * name, const char ** value, tibrv_u16 id );
tibrv_status tibrvMsg_GetOpaqueEx( tibrvMsg msg, const char * name, const void ** value, tibrv_u32 * size, tibrv_u16 id );
tibrv_status tibrvMsg_GetXmlEx( tibrvMsg msg, const char * name, const void ** value, tibrv_u32 * size, tibrv_u16 id );

tibrv_status tibrvMsg_RemoveFieldEx( tibrvMsg msg, const char * name, tibrv_u16 id);

tibrv_status tibrvMsg_UpdateMsgEx( tibrvMsg msg, const char * name, tibrvMsg value, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateIPAddr32Ex( tibrvMsg msg, const char * name, tibrv_ipaddr32 value, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateIPPort16Ex( tibrvMsg msg, const char * name, tibrv_ipport16 value, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateDateTimeEx( tibrvMsg msg, const char * name, const tibrvMsgDateTime * value, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateBoolEx( tibrvMsg msg, const char * name, tibrv_bool value, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateI8Ex( tibrvMsg msg, const char * name, tibrv_i8 value, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateI8ArrayEx( tibrvMsg msg, const char * name, const tibrv_i8 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateU8Ex( tibrvMsg msg, const char * name, tibrv_u8 value, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateU8ArrayEx( tibrvMsg msg, const char * name, const tibrv_u8 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateI16Ex( tibrvMsg msg, const char * name, tibrv_i16 value, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateI16ArrayEx( tibrvMsg msg, const char * name, const tibrv_i16 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateU16Ex( tibrvMsg msg, const char * name, tibrv_u16 value, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateU16ArrayEx( tibrvMsg msg, const char * name, const tibrv_u16 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateI32Ex( tibrvMsg msg, const char * name, tibrv_i32 value, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateI32ArrayEx( tibrvMsg msg, const char * name, const tibrv_i32 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateU32Ex( tibrvMsg msg, const char * name, tibrv_u32 value, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateU32ArrayEx( tibrvMsg msg, const char * name, const tibrv_u32 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateU64Ex( tibrvMsg msg, const char * name, tibrv_u64 value, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateU64ArrayEx( tibrvMsg msg, const char * name, const tibrv_u64 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateI64Ex( tibrvMsg msg, const char * name, tibrv_i64 value, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateI64ArrayEx( tibrvMsg msg, const char * name, const tibrv_i64 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateF32Ex( tibrvMsg msg, const char * name, tibrv_f32 value, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateF32ArrayEx( tibrvMsg msg, const char * name, const tibrv_f32 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateF64Ex( tibrvMsg msg, const char * name, tibrv_f64 value, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateF64ArrayEx( tibrvMsg msg, const char * name, const tibrv_f64 * array, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateStringArrayEx( tibrvMsg msg, const char * name, const char ** value, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateMsgArrayEx( tibrvMsg msg, const char * name, const tibrvMsg * value, tibrv_u32 num, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateStringEx( tibrvMsg msg, const char * name, const char * value, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateOpaqueEx( tibrvMsg msg, const char * name, const void * value, tibrv_u32 size, tibrv_u16 id );
tibrv_status tibrvMsg_UpdateXmlEx( tibrvMsg msg, const char * name, const void * value, tibrv_u32 size, tibrv_u16 id );

#define tibrvMsg_AddMsg( msg, name, value ) tibrvMsg_AddMsgEx( msg, name, value, 0 )
#define tibrvMsg_AddIPAddr32( msg, name, value ) tibrvMsg_AddIPAddr32Ex( msg, name, value, 0 )
#define tibrvMsg_AddIPPort16( msg, name, value ) tibrvMsg_AddIPPort16Ex( msg, name, value, 0 )
#define tibrvMsg_AddDateTime( msg, name, value ) tibrvMsg_AddDateTimeEx( msg, name, value, 0 )
#define tibrvMsg_AddBool( msg, name, value ) tibrvMsg_AddBoolEx( msg, name, value, 0 )
#define tibrvMsg_AddI8( msg, name, value ) tibrvMsg_AddI8Ex( msg, name, value, 0 )
#define tibrvMsg_AddU8( msg, name, value ) tibrvMsg_AddU8Ex( msg, name, value, 0 )
#define tibrvMsg_AddI16( msg, name, value ) tibrvMsg_AddI16Ex( msg, name, value, 0 )
#define tibrvMsg_AddU16( msg, name, value ) tibrvMsg_AddU16Ex( msg, name, value, 0 )
#define tibrvMsg_AddI32( msg, name, value ) tibrvMsg_AddI32Ex( msg, name, value, 0 )
#define tibrvMsg_AddU32( msg, name, value ) tibrvMsg_AddU32Ex( msg, name, value, 0 )
#define tibrvMsg_AddI64( msg, name, value ) tibrvMsg_AddI64Ex( msg, name, value, 0 )
#define tibrvMsg_AddU64( msg, name, value ) tibrvMsg_AddU64Ex( msg, name, value, 0 )
#define tibrvMsg_AddF32( msg, name, value ) tibrvMsg_AddF32Ex( msg, name, value, 0 )
#define tibrvMsg_AddF64( msg, name, value ) tibrvMsg_AddF64Ex( msg, name, value, 0 )
#define tibrvMsg_AddString( msg, name, value ) tibrvMsg_AddStringEx( msg, name, value, 0 )
#define tibrvMsg_AddOpaque( msg, name, value, length ) tibrvMsg_AddOpaqueEx( msg, name, value, length, 0 )
#define tibrvMsg_AddXml( msg, name, value, length ) tibrvMsg_AddXmlEx( msg, name, value, length, 0 )
#define tibrvMsg_AddI8Array( msg, name, array, num ) tibrvMsg_AddI8ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_AddU8Array( msg, name, array, num ) tibrvMsg_AddU8ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_AddI16Array( msg, name, array, num ) tibrvMsg_AddI16ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_AddU16Array( msg, name, array, num ) tibrvMsg_AddU16ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_AddI32Array( msg, name, array, num ) tibrvMsg_AddI32ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_AddU32Array( msg, name, array, num ) tibrvMsg_AddU32ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_AddI64Array( msg, name, array, num ) tibrvMsg_AddI64ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_AddU64Array( msg, name, array, num ) tibrvMsg_AddU64ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_AddF32Array( msg, name, array, num ) tibrvMsg_AddF32ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_AddF64Array( msg, name, array, num ) tibrvMsg_AddF64ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_AddStringArray( msg, name, array, num ) tibrvMsg_AddStringArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_AddMsgArray( msg, name, array, num ) tibrvMsg_AddMsgArrayEx( msg, name, array, num, 0 )

#define tibrvMsg_GetField( msg, name, field ) tibrvMsg_GetFieldEx( msg, name, field, 0 )
#define tibrvMsg_GetMsg( msg, name, value ) tibrvMsg_GetMsgEx( msg, name, value, 0 )
#define tibrvMsg_GetIPAddr32( msg, name, value ) tibrvMsg_GetIPAddr32Ex( msg, name, value, 0 )
#define tibrvMsg_GetIPPort16( msg, name, value ) tibrvMsg_GetIPPort16Ex( msg, name, value, 0 )
#define tibrvMsg_GetDateTime( msg, name, value ) tibrvMsg_GetDateTimeEx( msg, name, value, 0 )
#define tibrvMsg_GetBool( msg, name, value ) tibrvMsg_GetBoolEx( msg, name, value, 0 )
#define tibrvMsg_GetI8( msg, name, value ) tibrvMsg_GetI8Ex( msg, name, value, 0 )
#define tibrvMsg_GetU8( msg, name, value ) tibrvMsg_GetU8Ex( msg, name, value, 0 )
#define tibrvMsg_GetI16( msg, name, value ) tibrvMsg_GetI16Ex( msg, name, value, 0 )
#define tibrvMsg_GetU16( msg, name, value ) tibrvMsg_GetU16Ex( msg, name, value, 0 )
#define tibrvMsg_GetI32( msg, name, value ) tibrvMsg_GetI32Ex( msg, name, value, 0 )
#define tibrvMsg_GetU32( msg, name, value ) tibrvMsg_GetU32Ex( msg, name, value, 0 )
#define tibrvMsg_GetI64( msg, name, value ) tibrvMsg_GetI64Ex( msg, name, value, 0 )
#define tibrvMsg_GetU64( msg, name, value ) tibrvMsg_GetU64Ex( msg, name, value, 0 )
#define tibrvMsg_GetF32( msg, name, value ) tibrvMsg_GetF32Ex( msg, name, value, 0 )
#define tibrvMsg_GetF64( msg, name, value ) tibrvMsg_GetF64Ex( msg, name, value, 0 )
#define tibrvMsg_GetString( msg, name, value ) tibrvMsg_GetStringEx( msg, name, value, 0 )
#define tibrvMsg_GetOpaque( msg, name, value, length ) tibrvMsg_GetOpaqueEx( msg, name, value, length, 0 )
#define tibrvMsg_GetXml( msg, name, value, length ) tibrvMsg_GetXmlEx( msg, name, value, length, 0 )
#define tibrvMsg_GetI8Array( msg, name, array, num ) tibrvMsg_GetI8ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_GetU8Array( msg, name, array, num ) tibrvMsg_GetU8ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_GetI16Array( msg, name, array, num ) tibrvMsg_GetI16ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_GetU16Array( msg, name, array, num ) tibrvMsg_GetU16ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_GetI32Array( msg, name, array, num ) tibrvMsg_GetI32ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_GetU32Array( msg, name, array, num ) tibrvMsg_GetU32ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_GetI64Array( msg, name, array, num ) tibrvMsg_GetI64ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_GetU64Array( msg, name, array, num ) tibrvMsg_GetU64ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_GetF32Array( msg, name, array, num ) tibrvMsg_GetF32ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_GetF64Array( msg, name, array, num ) tibrvMsg_GetF64ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_GetStringArray( msg, name, array, num ) tibrvMsg_GetStringArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_GetMsgArray( msg, name, array, num ) tibrvMsg_GetMsgArrayEx( msg, name, array, num, 0 )

#define tibrvMsg_RemoveField( msg, name ) tibrvMsg_RemoveFieldEx( msg, name, 0 )

#define tibrvMsg_UpdateMsg( msg, name, value ) tibrvMsg_UpdateMsgEx( msg, name, value, 0 )
#define tibrvMsg_UpdateIPAddr32( msg, name, value ) tibrvMsg_UpdateIPAddr32Ex( msg, name, value, 0 )
#define tibrvMsg_UpdateIPPort16( msg, name, value ) tibrvMsg_UpdateIPPort16Ex( msg, name, value, 0 )
#define tibrvMsg_UpdateDateTime( msg, name, value ) tibrvMsg_UpdateDateTimeEx( msg, name, value, 0 )
#define tibrvMsg_UpdateBool( msg, name, value ) tibrvMsg_UpdateBoolEx( msg, name, value, 0 )
#define tibrvMsg_UpdateI8( msg, name, value ) tibrvMsg_UpdateI8Ex( msg, name, value, 0 )
#define tibrvMsg_UpdateU8( msg, name, value ) tibrvMsg_UpdateU8Ex( msg, name, value, 0 )
#define tibrvMsg_UpdateI16( msg, name, value ) tibrvMsg_UpdateI16Ex( msg, name, value, 0 )
#define tibrvMsg_UpdateU16( msg, name, value ) tibrvMsg_UpdateU16Ex( msg, name, value, 0 )
#define tibrvMsg_UpdateI32( msg, name, value ) tibrvMsg_UpdateI32Ex( msg, name, value, 0 )
#define tibrvMsg_UpdateU32( msg, name, value ) tibrvMsg_UpdateU32Ex( msg, name, value, 0 )
#define tibrvMsg_UpdateI64( msg, name, value ) tibrvMsg_UpdateI64Ex( msg, name, value, 0 )
#define tibrvMsg_UpdateU64( msg, name, value ) tibrvMsg_UpdateU64Ex( msg, name, value, 0 )
#define tibrvMsg_UpdateF32( msg, name, value ) tibrvMsg_UpdateF32Ex( msg, name, value, 0 )
#define tibrvMsg_UpdateF64( msg, name, value ) tibrvMsg_UpdateF64Ex( msg, name, value, 0 )
#define tibrvMsg_UpdateString( msg, name, value ) tibrvMsg_UpdateStringEx( msg, name, value, 0 )
#define tibrvMsg_UpdateOpaque( msg, name, value, length ) tibrvMsg_UpdateOpaqueEx( msg, name, value, length, 0 )
#define tibrvMsg_UpdateXml( msg, name, value, length ) tibrvMsg_UpdateXmlEx( msg, name, value, length, 0 )
#define tibrvMsg_UpdateI8Array( msg, name, array, num ) tibrvMsg_UpdateI8ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_UpdateU8Array( msg, name, array, num ) tibrvMsg_UpdateU8ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_UpdateI16Array( msg, name, array, num ) tibrvMsg_UpdateI16ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_UpdateU16Array( msg, name, array, num ) tibrvMsg_UpdateU16ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_UpdateI32Array( msg, name, array, num ) tibrvMsg_UpdateI32ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_UpdateU32Array( msg, name, array, num ) tibrvMsg_UpdateU32ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_UpdateI64Array( msg, name, array, num ) tibrvMsg_UpdateI64ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_UpdateU64Array( msg, name, array, num ) tibrvMsg_UpdateU64ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_UpdateF32Array( msg, name, array, num ) tibrvMsg_UpdateF32ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_UpdateF64Array( msg, name, array, num ) tibrvMsg_UpdateF64ArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_UpdateStringArray( msg, name, array, num ) tibrvMsg_UpdateStringArrayEx( msg, name, array, num, 0 )
#define tibrvMsg_UpdateMsgArray( msg, name, array, num ) tibrvMsg_UpdateMsgArrayEx( msg, name, array, num, 0 )

#ifdef __cplusplus
}
#endif
#endif
