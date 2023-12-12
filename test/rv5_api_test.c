#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <rvapi.h>

#pragma GCC diagnostic ignored "-Wunused-parameter"

// Callback function for received messages
void
onMessage( rv_Listener listener, rv_Name subject, rv_Name reply,
           rvmsg_Type type, size_t data_len, void* data, void *cl )
{
  printf( "Send Subject: %s, Reply Subject: %s\n", subject,
          reply ? reply : "None" );
}

void
timerCallback( rv_Timer timer, void* closure )
{
  printf( "Timer callback\n" );
}

void
signalCallback( rv_Signal sig, void* closure )
{
  printf( "Signal callback\n" );
  rv_Term( (rv_Session) closure );
}

int
main( int argc, const char *argv[] )
{
  const char  * example_subject = "example.subject";
  rv_Session    session;
  rv_Status     status;
  rv_Listener * listener;
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

  // Create a listener for a specific subject
  if ( count == 0 ) {
    subject = &example_subject;
    count = 1;
  }
  listener = (rv_Listener *) calloc( count, sizeof( rv_Listener ) );
  for ( i = 0; i < count; i++ ) {
    status = rv_ListenSubject( session, &listener[ i ], subject[ i ], onMessage,
                               NULL, NULL );
  }
  if ( status != RV_OK ) {
    fprintf( stderr, "Error creating listener: %s\n", rv_ErrorText( NULL, status ) );
    rv_Term( session );
    exit( 1 );
  }
  rv_Timer timer;
  rv_Signal shandle;
  rv_CreateTimer( session, &timer, 5 * 1000, timerCallback, NULL );
  rv_CreateSignal( session, &shandle, SIGINT, signalCallback, session );
  // Start the main event loop
  rv_MainLoop( session );

  // Cleanup
  for ( i = 0; i < count; i++ )
    rv_Close( listener[ i ] );
  rv_Term( session );

  return 0;
}

