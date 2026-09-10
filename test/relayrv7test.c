/* relay: subscribe IN, republish each message on OUT from the dispatch thread.
 * Batch mode + optional dispatch-flush.  Prints how many messages went out per
 * dispatch pass (= per flush) so the packing is visible.
 *   relay <mode 0|1|2> <dispatch_flush 0|1> <batch_size> <timer_secs> IN OUT */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sassrv/rv7api.h>

static tibrvTransport tp;
static const char * out_subj;
static unsigned long n_in = 0, n_pass = 0, in_this_pass = 0, hist[8];
static volatile int stop = 0;

static void
on_msg( tibrvEvent e, tibrvMsg m, void *c )
{
  (void) e;
  (void) c;
  tibrvMsg_SetSendSubject( m, out_subj );
  tibrvTransport_Send( tp, m );
  n_in++;
  in_this_pass++;
}
static void sig(int s) { (void)s; stop = 1; }

int
main( int argc, char **argv )
{
  if ( argc < 7 ) {
    fprintf( stderr, "usage: relay mode dflush batch_size timer IN OUT\n" );
    return 1;
  }
  int      mode = atoi( argv[ 1 ] ), dflush = atoi( argv[ 2 ] );
  unsigned bsz  = (unsigned) strtoul( argv[ 3 ], 0, 10 );
  double   ival = atof( argv[ 4 ] );
  out_subj      = argv[ 6 ];
  tibrvEvent ev;
  signal( SIGINT, sig );
  signal( SIGTERM, sig );
  tibrv_Open();
  tibrvTransport_Create( &tp, NULL, NULL, NULL );
  if ( mode ) {
    tibrvTransport_SetBatchMode( tp, mode == 2 ? TIBRV_TRANSPORT_SINGLE_BATCH
                                               : TIBRV_TRANSPORT_TIMER_BATCH );
    tibrvTransport_SetBatchSize( tp, bsz );
    if ( ival > 0 )
      tibrvTransport_SetBatchInterval( tp, ival );
    if ( dflush )
      tibrvTransport_SetBatchDispatchFlush( tp, TIBRV_TRUE );
  }
  tibrvEvent_CreateListener( &ev, TIBRV_DEFAULT_QUEUE, on_msg, tp, argv[ 5 ],
                             NULL );
  fprintf(
    stderr,
    "relay: mode=%d dispatch_flush=%d batch_size=%u timer=%g  %s -> %s\n", mode,
    dflush, bsz, ival, argv[ 5 ], out_subj );
  while ( !stop ) {
    in_this_pass    = 0;
    tibrv_status st = tibrvQueue_TimedDispatch( TIBRV_DEFAULT_QUEUE, 0.5 );
    if ( st == TIBRV_OK && in_this_pass > 0 ) {
      n_pass++;
      unsigned b = in_this_pass >= 128  ? 7
                   : in_this_pass >= 64 ? 6
                   : in_this_pass >= 32 ? 5
                   : in_this_pass >= 16 ? 4
                   : in_this_pass >= 8  ? 3
                   : in_this_pass >= 4  ? 2
                   : in_this_pass >= 2  ? 1
                                        : 0;
      hist[ b ]++;
    }
  }
  fprintf( stderr,
           "relay: in=%lu passes=%lu avg msgs/pass=%.1f  pass-size hist "
           "[1,2-3,4-7,8-15,16-31,32-63,64-127,128+]:",
           n_in, n_pass, n_pass ? (double) n_in / n_pass : 0 );
  for ( int i = 0; i < 8; i++ )
    fprintf( stderr, " %lu", hist[ i ] );
  fprintf( stderr, "\n" );
  if ( ival > 0 )
    tibrvTransport_SetBatchInterval( tp, 0.0 );
  tibrvTransport_Destroy( tp );
  tibrv_Close();
  return 0;
}
