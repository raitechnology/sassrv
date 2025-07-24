#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <time.h>

#include <sassrv/rv7api.h>
#include <raimd/md_msg.h>
#include <raimd/rv_msg.h>
#include <raimd/dict_load.h>

#define MIN_PARMS (2)

typedef struct {
  MDMsgMem_t * mem;
  MDOutput_t * mout;
  MDDict_t   * dict;
  const char * sub;
} rv_closure_t;

void
inbox_callback( tibrvEvent event, tibrvMsg message, void* closure )
{
  const char* send_subject  = NULL;
  tibrv_u8  * buffer        = NULL;
  tibrv_u32   size          = 0;
  rv_closure_t * cl         = (rv_closure_t *) closure;
  MDMsg_t   * rvmsg;
  char        localTime[ TIBRVMSG_DATETIME_STRING_SIZE ];
  char        gmtTime[ TIBRVMSG_DATETIME_STRING_SIZE ];

  tibrvMsg_GetSendSubject( message, &send_subject );

  /*tibrvMsg_ConvertToString( message, &theString );*/
  tibrvMsg_GetAsBytes( message, (const void **) &buffer );
  tibrvMsg_GetByteSize( message, &size );
  tibrvMsg_GetCurrentTimeString( localTime, gmtTime );
  md_msg_mem_reuse( cl->mem );
  rvmsg = rv_msg_unpack( buffer, 0, size, 0, cl->dict, cl->mem );

  printf( "%s (%s): subject=%s (%s)\n", localTime, gmtTime,
          cl->sub, send_subject );
  md_msg_print( rvmsg, cl->mout );

  fflush( stdout );
}

void
my_callback( tibrvEvent event, tibrvMsg message, void* closure )
{
  const char* send_subject  = NULL;
  const char* reply_subject = NULL;
  tibrv_u8  * buffer        = NULL;
  tibrv_u32   size          = 0;
  rv_closure_t * cl         = (rv_closure_t *) closure;
  MDMsg_t   * rvmsg;
  char        localTime[ TIBRVMSG_DATETIME_STRING_SIZE ];
  char        gmtTime[ TIBRVMSG_DATETIME_STRING_SIZE ];

  tibrvMsg_GetSendSubject( message, &send_subject );

  tibrvMsg_GetReplySubject( message, &reply_subject );

  /*tibrvMsg_ConvertToString( message, &theString );*/
  tibrvMsg_GetAsBytes( message, (const void **) &buffer );
  tibrvMsg_GetByteSize( message, &size );
  tibrvMsg_GetCurrentTimeString( localTime, gmtTime );
  md_msg_mem_reuse( cl->mem );
  rvmsg = rv_msg_unpack( buffer, 0, size, 0, cl->dict, cl->mem );

  if ( reply_subject )
    printf( "%s (%s): subject=%s, reply=%s\n", localTime, gmtTime,
            send_subject, reply_subject );
  else
    printf( "%s (%s): subject=%s\n", localTime, gmtTime,
            send_subject );
  md_msg_print( rvmsg, cl->mout );

  fflush( stdout );
}


void
dict_callback( tibrvEvent event, tibrvMsg message, void* closure )
{
  tibrv_u8  * buffer = NULL;
  tibrv_u32   size   = 0;
  rv_closure_t * cl  = (rv_closure_t *) closure;
  MDMsg_t   * rvmsg;

  /*tibrvMsg_ConvertToString( message, &theString );*/
  tibrvMsg_GetAsBytes( message, (const void **) &buffer );
  tibrvMsg_GetByteSize( message, &size );
  md_msg_mem_reuse( cl->mem );
  rvmsg = rv_msg_unpack( buffer, 0, size, 0, NULL, cl->mem );
  cl->dict = md_load_sass_dict( rvmsg );
  if ( cl->dict != NULL )
    printf( "Received dictionary\n" );
}

void
usage( void )
{
  fprintf( stderr, "tibrvlisten [-service service] [-network network] \n" );
  fprintf( stderr, "            [-daemon daemon] subject_list\n" );
  exit( 1 );
}

int
get_InitParms( int argc, char* argv[], int min_parms, char** serviceStr,
               char** networkStr, char** daemonStr )
{
  int i = 1;

  if ( argc < min_parms )
    usage();

  while ( i + 2 <= argc && *argv[ i ] == '-' ) {
    if ( strcmp( argv[ i ], "-service" ) == 0 ) {
      *serviceStr = argv[ i + 1 ];
      i += 2;
    }
    else if ( strcmp( argv[ i ], "-network" ) == 0 ) {
      *networkStr = argv[ i + 1 ];
      i += 2;
    }
    else if ( strcmp( argv[ i ], "-daemon" ) == 0 ) {
      *daemonStr = argv[ i + 1 ];
      i += 2;
    }
    else {
      usage();
    }
  }

  return ( i );
}

int
main( int argc, char** argv )
{
  tibrv_status   err;
  int            currentArg;
  tibrvEvent   * listenId,
               * inboxId,
                 dictId;
  tibrvTransport transport;
  rv_closure_t   closure,
               * inbox_closure;
  char           inbox[ 64 ];
  tibrvMsg       snapMsg;
  char           snapSubject[ 1024 ];
  int            i;

  char* serviceStr = NULL;
  char* networkStr = NULL;
  char* daemonStr  = NULL;

  char* progname = argv[ 0 ];

  currentArg = get_InitParms( argc, argv, MIN_PARMS, &serviceStr, &networkStr,
                              &daemonStr );
  err        = tibrv_Open();
  if ( err != TIBRV_OK ) {
    fprintf( stderr, "%s: Failed to open TIB/Rendezvous: %s\n", progname,
             tibrvStatus_GetText( err ) );
    exit( 1 );
  }

  err = tibrvTransport_Create( &transport, serviceStr, networkStr, daemonStr );
  if ( err != TIBRV_OK ) {
    fprintf( stderr, "%s: Failed to initialize transport: %s\n", progname,
             tibrvStatus_GetText( err ) );
    exit( 1 );
  }

  tibrvTransport_SetDescription( transport, progname );
  md_output_init( &closure.mout );
  md_msg_mem_create( &closure.mem );
  closure.sub  = NULL;
  closure.dict = NULL;

  i = ( argc - currentArg );
  if ( i == 0 ) {
    fprintf( stderr, "%s: No subscriptions\n", progname );
    exit( 1 );
  }
  listenId      = (tibrvEvent *) malloc( sizeof( tibrvEvent ) * i );
  inboxId       = (tibrvEvent *) malloc( sizeof( tibrvEvent ) * i );
  inbox_closure = (rv_closure_t *) malloc( sizeof( rv_closure_t ) * i );
  for ( i = 0; i + currentArg < argc; i++ ) {
    listenId[ i ] = 0;
    inboxId[ i ]  = 0;
    inbox_closure[ i ].mout = closure.mout;
    inbox_closure[ i ].mem  = closure.mem;
    inbox_closure[ i ].sub  = argv[ i + currentArg ];
    inbox_closure[ i ].dict = NULL;
  }

  err = tibrvTransport_CreateInbox( transport, inbox, sizeof( inbox ) );
  if ( err == TIBRV_OK )
    err = tibrvEvent_CreateListener( &dictId, TIBRV_DEFAULT_QUEUE,
                                     dict_callback, transport,
                                     inbox, &closure );
  if ( err == TIBRV_OK ) {
    int times = 0;
    for (;;) {
      tibrvMsg_Create( &snapMsg );
      tibrvMsg_SetSendSubject( snapMsg, "_TIC.REPLY.SASS.DATA.DICTIONARY" );
      tibrvMsg_SetReplySubject( snapMsg, inbox );
      err = tibrvTransport_Send( transport, snapMsg );
      tibrvMsg_Destroy( snapMsg );

      if ( err != TIBRV_OK ) {
        fprintf( stderr, "Dictionary failed\n" );
        exit( 1 );
      }
      while ( closure.dict == NULL ) {
        err = tibrvQueue_TimedDispatch( TIBRV_DEFAULT_QUEUE, 10.0 );
        if ( err != TIBRV_OK )
          break;
      }
      if ( closure.dict != NULL )
        break;
      if ( ++times == 3 ) {
        break;
      }
      else {
        fprintf( stderr, "Dictionary timeout, retrying\n" );
      }
    }
    if ( closure.dict == NULL ) {
      fprintf( stderr, "Dictionary timeout, tried 3 times\n" );
      exit( 1 );
    }
    for ( i = 0; i + currentArg < argc; i++ ) {
      inbox_closure[ i ].dict = closure.dict;
    }
  }

  for ( i = 0; i + currentArg < argc; i++ ) {

    printf( "tibrvlisten: Listening to subject %s\n", argv[ i + currentArg ] );

    err = tibrvTransport_CreateInbox( transport, inbox, sizeof( inbox ) );
    if ( err == TIBRV_OK ) {
      err = tibrvEvent_CreateListener( &inboxId[ i ], TIBRV_DEFAULT_QUEUE,
                                       inbox_callback, transport, inbox,
                                       &inbox_closure[ i ] );
    }
    if ( err == TIBRV_OK )
      err = tibrvEvent_CreateListener( &listenId[ i ], TIBRV_DEFAULT_QUEUE,
                                       my_callback, transport,
                                       argv[ i + currentArg ], &closure );
    if ( err == TIBRV_OK ) {
      tibrvMsg_Create( &snapMsg );
      snprintf( snapSubject, sizeof( snapSubject ), "_SNAP.%s", argv[ i + currentArg ] );
      tibrvMsg_SetSendSubject( snapMsg, snapSubject );
      tibrvMsg_SetReplySubject( snapMsg, inbox );
      tibrvMsg_AddU16( snapMsg, "flags", 6 );
      err = tibrvTransport_Send( transport, snapMsg );
      tibrvMsg_Destroy( snapMsg );
    }
    if ( err != TIBRV_OK ) {
      fprintf( stderr, "%s: Error %s listening to \"%s\"\n", progname,
               tibrvStatus_GetText( err ), argv[ i + currentArg ] );
      exit( 2 );
    }
  }

  while ( tibrvQueue_Dispatch( TIBRV_DEFAULT_QUEUE ) == TIBRV_OK )
    ;

  tibrv_Close();

  return 0;
}
