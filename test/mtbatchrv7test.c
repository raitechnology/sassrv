/* N publisher threads Send() on one transport in batch mode while a flusher
 * thread hammers Flush(); a listener counts + checks per-thread seq order.
 * Exercises batch_mutex / c->lock / E-thread interplay. */
#define _POSIX_C_SOURCE 200809L   /* must precede every system header */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <raikv/util.h>
#include <sassrv/rv7api.h>

static double now_s(void) { return kv_current_monotonic_time_s(); }

#define NTHR 4
#define NMSG 20000
static tibrvTransport tp;
static volatile int done_pub = 0, pubs_finished = 0;
static pthread_mutex_t cnt_mutex = PTHREAD_MUTEX_INITIALIZER;
static unsigned long recv_cnt = 0, ooo = 0, dupes = 0;
static unsigned long last_seq[NTHR];

static void
cb( tibrvEvent e, tibrvMsg m, void *c )
{
  tibrv_u32 thr, seq;
  (void) e;
  (void) c;
  tibrvMsg_GetU32( m, "t", &thr );
  tibrvMsg_GetU32( m, "s", &seq );
  recv_cnt++;
  if ( seq == last_seq[ thr ] )
    dupes++;
  else if ( seq < last_seq[ thr ] )
    ooo++;
  last_seq[ thr ] = seq;
}

static void *
pub( void *a )
{
  long      id = (long) a;
  tibrvMsg  m;
  tibrv_u32 i;
  for ( i = 1; i <= NMSG; i++ ) {
    tibrvMsg_Create( &m );
    tibrvMsg_SetSendSubject( m, "MTB.X" );
    tibrvMsg_AddU32( m, "t", (tibrv_u32) id );
    tibrvMsg_AddU32( m, "s", i );
    if ( tibrvTransport_Send( tp, m ) != TIBRV_OK ) {
      fprintf( stderr, "send fail\n" );
      exit( 2 );
    }
    tibrvMsg_Destroy( m );
    if ( ( i & 1023 ) == 0 && ( id & 1 ) )
      tibrvTransport_Flush( tp ); /* odd threads also flush */
  }
  pthread_mutex_lock( &cnt_mutex );
  pubs_finished++;
  pthread_mutex_unlock( &cnt_mutex );
  return NULL;
}

static void
microsleep( long usec )
{
  struct timespec delay = { usec / 1000000L, ( usec % 1000000L ) * 1000L };
  nanosleep( &delay, NULL );
}

static void *
flusher( void *a )
{
  (void) a;
  while ( !done_pub ) {
    tibrvTransport_Flush( tp );
    microsleep( 50 );
  }
  return NULL;
}

int
main( int argc, char **argv )
{
  int mode = argc > 1 ? atoi( argv[ 1 ] ) : 1;
  int use_flusher =
    argc > 3 ? atoi( argv[ 3 ] ) : 1; /* 1=TIMER_BATCH 2=SINGLE_BATCH */
  double     ival = argc > 2 ? atof( argv[ 2 ] ) : 0.0;
  pthread_t  th[ NTHR ], fl;
  tibrvEvent ev;
  long       i;
  tibrv_Open();
  tibrvTransport_Create( &tp, NULL, NULL, NULL );
  tibrvTransport_SetBatchMode( tp, mode == 2 ? TIBRV_TRANSPORT_SINGLE_BATCH
                                             : TIBRV_TRANSPORT_TIMER_BATCH );
  tibrvTransport_SetBatchSize( tp, 8192 );
  if ( ival > 0 )
    tibrvTransport_SetBatchInterval( tp, ival );
  tibrvEvent_CreateListener( &ev, TIBRV_DEFAULT_QUEUE, cb, tp, "MTB.X", NULL );
  microsleep( 200000 );
  for ( i = 0; i < NTHR; i++ )
    pthread_create( &th[ i ], NULL, pub, (void *) i );
  if ( use_flusher )
    pthread_create( &fl, NULL, flusher, NULL );
  double t0 = now_s();
  /* phase 1: dispatch while publishers run (pubs_finished counts thread exits;
   * portable, unlike pthread_tryjoin_np) */
  for ( ;; ) {
    int finished;
    tibrvQueue_TimedDispatch( TIBRV_DEFAULT_QUEUE, 0.01 );
    pthread_mutex_lock( &cnt_mutex );
    finished = pubs_finished;
    pthread_mutex_unlock( &cnt_mutex );
    if ( finished == NTHR )
      break;
    if ( now_s() - t0 > 30 ) {
      fprintf( stderr, "publishers stuck after 30s (recv=%lu)\n", recv_cnt );
      break;
    }
  }
  double t_pub = now_s() - t0;
  done_pub     = 1;
  if ( use_flusher )
    pthread_join( fl, NULL );
  for ( i = 0; i < NTHR; i++ )
    pthread_join( th[ i ], NULL );
  /* phase 2: one explicit flush releases every tail; everything must then
   * arrive */
  tibrvTransport_Flush( tp );
  double t1 = now_s();
  while ( recv_cnt < (unsigned long) NTHR * NMSG && now_s() - t1 < 5.0 )
    tibrvQueue_TimedDispatch( TIBRV_DEFAULT_QUEUE, 0.01 );
  fprintf( stderr, "  pub %.2fs, tail drained in %.3fs\n", t_pub,
           now_s() - t1 );
  printf(
    "%.2fs mode=%d ival=%g  sent=%d recv=%lu dupes=%lu out-of-order=%lu  %s\n",
    now_s() - t0, mode, ival, NTHR * NMSG, recv_cnt, dupes, ooo,
    recv_cnt == (unsigned long) NTHR * NMSG && !dupes && !ooo ? "OK" : "FAIL" );
  if ( ival > 0 )
    tibrvTransport_SetBatchInterval( tp, 0.0 );
  tibrvTransport_Destroy( tp );
  tibrv_Close();
  return recv_cnt == (unsigned long) NTHR * NMSG && !dupes && !ooo ? 0 : 1;
}
