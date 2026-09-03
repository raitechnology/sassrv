#define _DEFAULT_SOURCE /* gethostname with -std=c11 */
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <time.h>
#if defined( _MSC_VER ) || defined( __MINGW32__ )
#include <raikv/win.h> /* winsock: gethostname, htonl, ... */
#else
#include <unistd.h>
#endif

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
  fprintf( stderr, "subrv7test [-service service] [-network network] \n" );
  fprintf( stderr, "            [-daemon daemon] [-x|-nodict] [-3|-sass3]\n" );
  fprintf( stderr, "            [-feed feed] subject_list\n" );
  fprintf( stderr, "  -x, -nodict : don't fetch a SASS dictionary at startup\n" );
  fprintf( stderr, "  -3, -sass3  : use the SASS3 protocol (_SASS.<feed>.SUB\n" );
  fprintf( stderr, "                SUBSCRIBE|INITIAL_VALUES) instead of rv7 _SNAP;\n" );
  fprintf( stderr, "                feed = first segment of the subject\n" );
  fprintf( stderr, "                (FEED.DOMAIN.INSTRUMENT.EXCHANGE)\n" );
  fprintf( stderr, "  -feed feed  : override the SASS3 feed name\n" );
  exit( 1 );
}

int
get_InitParms( int argc, char* argv[], int min_parms, char** serviceStr,
               char** networkStr, char** daemonStr, int* noDict,
               int* useSass3 )
{
  int i = 1;

  if ( argc < min_parms )
    usage();

  while ( i < argc && *argv[ i ] == '-' ) {
    if ( strcmp( argv[ i ], "-x" ) == 0 ||
         strcmp( argv[ i ], "-nodict" ) == 0 ) {
      *noDict = 1;
      i += 1;
    }
    else if ( strcmp( argv[ i ], "-3" ) == 0 ||
              strcmp( argv[ i ], "-sass3" ) == 0 ) {
      *useSass3 = 1;
      i += 1;
    }
    else if ( i + 2 > argc ) {
      usage();
    }
    else if ( strcmp( argv[ i ], "-service" ) == 0 ) {
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
  int   noDict     = 0;
  int   useSass3   = 0;

  char* progname = argv[ 0 ];

  currentArg = get_InitParms( argc, argv, MIN_PARMS, &serviceStr, &networkStr,
                              &daemonStr, &noDict, &useSass3 );
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

  err = TIBRV_OK;
  if ( ! noDict ) {
    err = tibrvTransport_CreateInbox( transport, inbox, sizeof( inbox ) );
    if ( err == TIBRV_OK )
      err = tibrvEvent_CreateListener( &dictId, TIBRV_DEFAULT_QUEUE,
                                       dict_callback, transport,
                                       inbox, &closure );
  }
  if ( ! noDict && err == TIBRV_OK ) {
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

    printf( "subrv7test: Listening to subject %s\n", argv[ i + currentArg ] );

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
      if ( useSass3 ) {
        /* SASS3: _SASS.<feed>.SUB, magic 23176, T = QueryFlags
         * (SUBSCRIBE|INITIAL_VALUES), A = accounting {U,H,A,P},
         * S = subjects as field NAMES; reply inbox = image/ack address.
         * The feed name is the FIRST SEGMENT of the subject: subjects are
         * FEED.DOMAIN.INSTRUMENT.EXCHANGE (DOMAIN usually REC for level 2
         * records, from MARKET_PRICE_DOMAIN in raimd omm_flags.h). */
        tibrvMsg subMsg, acctMsg, subjMsg;
        char     hostname[ 256 ];
        const char* user = getenv( "USER" );
        const char* subj = argv[ i + currentArg ];
        const char* dot  = strchr( subj, '.' );
        size_t      seg  = ( dot != NULL ? (size_t) ( dot - subj )
                                         : strlen( subj ) );
        if ( user == NULL )
          user = "subrv7test";
        if ( gethostname( hostname, sizeof( hostname ) ) != 0 )
          snprintf( hostname, sizeof( hostname ), "localhost" );

        if ( seg == 0 || seg > 255 ) {
          fprintf( stderr, "%s: bad feed segment in subject \"%s\"\n",
                   progname, subj );
          exit( 1 );
        }

        tibrvMsg_Create( &subMsg );
        tibrvMsg_Create( &acctMsg );
        tibrvMsg_Create( &subjMsg );
        snprintf( snapSubject, sizeof( snapSubject ), "_SASS.%.*s.SUB",
                  (int) seg, subj );
        tibrvMsg_SetSendSubject( subMsg, snapSubject );
        tibrvMsg_SetReplySubject( subMsg, inbox );
        tibrvMsg_AddU16( subMsg, "M", 23176 ); /* SASS3_SUB_MAGIC */
        tibrvMsg_AddU32( subMsg, "T", 6 );     /* SUBSCRIBE|INITIAL_VALUES */
        tibrvMsg_AddString( acctMsg, "U", user );
        tibrvMsg_AddString( acctMsg, "H", hostname );
        tibrvMsg_AddString( acctMsg, "A", "subrv7test" );
        tibrvMsg_AddU32( acctMsg, "P", (tibrv_u32) getpid() );
        tibrvMsg_AddMsg( subMsg, "A", acctMsg );
        tibrvMsg_AddU8( subjMsg, argv[ i + currentArg ], 1 );
        tibrvMsg_AddMsg( subMsg, "S", subjMsg );
        err = tibrvTransport_Send( transport, subMsg );
        tibrvMsg_Destroy( subjMsg );
        tibrvMsg_Destroy( acctMsg );
        tibrvMsg_Destroy( subMsg );
      }
      else {
        tibrvMsg_Create( &snapMsg );
        snprintf( snapSubject, sizeof( snapSubject ), "_SNAP.%s",
                  argv[ i + currentArg ] );
        tibrvMsg_SetSendSubject( snapMsg, snapSubject );
        tibrvMsg_SetReplySubject( snapMsg, inbox );
        tibrvMsg_AddU16( snapMsg, "flags", 6 );
        err = tibrvTransport_Send( transport, snapMsg );
        tibrvMsg_Destroy( snapMsg );
      }
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
