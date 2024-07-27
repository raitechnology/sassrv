#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <rv5cpp.h>

#pragma GCC diagnostic ignored "-Wunused-parameter"

struct OnMsg : public RvDataCallback {
  void onData( const char    * subject,
               RvSender      * replySender,
               const RvDatum & data,
               RvListener    * invoker ) {
    printf( "Send Subject: %s, Reply Subject: %s\n", subject,
            replySender ? replySender->name : "None" );
  }
} onMsg;

struct OnSignal : public RvSignalCallback {
  void onSignal( RvSignal *invoker ) {
    printf( "Signal callback\n" );
    rv_Term( invoker->session()->rv_session() );
  }
} onSignal;

struct OnTimer : public RvTimerCallback {
  void onTimer( RvTimer *invoker ) {
    printf( "Timer callback\n" );
  }
} onTimer;

int
main( int argc, const char *argv[] )
{
  const char  * example_subject = "example.subject";
  rv_Session    session;
  rv_Status     status;
  const char  * service = NULL, * network = NULL, * daemon = NULL,
             ** subject = NULL;
  int count = 0, i;

  for ( i = 1; i < argc && *argv[ i ] == '-'; ) {
    if ( strcmp( argv[ i ], "-service" ) == 0 ) {
      service = argv[ i + 1 ];
      i += 2;
    }
    else if ( strcmp( argv[ i ], "-network" ) == 0 ) {
      network = argv[ i + 1 ];
      i += 2;
    }
    else if ( strcmp( argv[ i ], "-daemon" ) == 0 ) {
      daemon = argv[ i + 1 ];
      i += 2;
    }
    else if ( strcmp( argv[ i ], "-subject" ) == 0 ) {
      subject = &argv[ i + 1 ];
      count = argc - ( i + 1 );
      break;
    }
    else {
      fprintf( stderr, "Unknown option: %s\n", argv[ i ] );
      i++;
    }
  }
  // Create a session
  status = rv_Init( &session, service, network, daemon );
  if ( status != RV_OK ) {
    fprintf( stderr, "Error creating session: %s\n", rv_ErrorText( NULL, status ) );
    exit( 1 );
  }

  if ( count == 0 ) {
    subject = &example_subject;
    count = 1;
  }
  RvSession *rv_session = (RvSession *) new RvSession( session );
  for ( i = 0; i < count; i++ )
    rv_session->newListener( subject[ i ], 0, &onMsg );
  rv_session->newSignal( SIGINT, &onSignal );
  rv_session->newTimer( 5 * 1000, 1, &onTimer );

  rv_MainLoop( session );
  rv_Term( session );

  return 0;
}

