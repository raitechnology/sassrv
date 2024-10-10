#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <sassrv/ev_rv_client.h>
#include <sassrv/submgr.h>
#include <raimd/md_msg.h>
#include <raikv/ev_publish.h>

using namespace rai;
using namespace kv;
using namespace sassrv;
using namespace md;

/* rv client callback closure */
struct RvDataCallback : public EvConnectionNotify, public RvClientCB,
                        public EvTimerCallback,  public RvSubscriptionListener {
  EvPoll         & poll;            /* poll loop data */
  EvRvClient     & client;          /* connection to rv */
  RvSubscriptionDB sub_db;
  const char    ** sub;             /* subject strings */
  size_t           sub_count;       /* count of sub[] */
  bool             top;

  RvDataCallback( EvPoll &p,  EvRvClient &c,  const char **s,  size_t cnt,
                  bool t )
    : poll( p ), client( c ), sub_db( c, this ), sub( s ), sub_count( cnt ),
      top( t ) {}

  /* after CONNECTED message */
  virtual void on_connect( EvSocket &conn ) noexcept;
  /* start sub[] with inbox reply */
  void start_subscriptions( void ) noexcept;
  /* when signalled, unsubscribe */
  void on_unsubscribe( void ) noexcept;
  /* when disconnected */
  virtual void on_shutdown( EvSocket &conn,  const char *err,
                            size_t err_len ) noexcept;
  /* process events timer */
  virtual bool timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept;
  /* message from network */
  virtual bool on_rv_msg( EvPublish &pub ) noexcept;
  /* new listen start */
  virtual void on_listen_start( Start &add ) noexcept;
  virtual void on_listen_stop ( Stop  &rem ) noexcept;
  virtual void on_snapshot    ( Snap  &snp ) noexcept;
  const char * ts( uint32_t mono,  const char *str,  char *buf ) noexcept;
};

/* called after daemon responds with CONNECTED message */
void
RvDataCallback::on_connect( EvSocket &conn ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  printf( "Connected: %.*s\n", len, conn.peer_address.buf );
  fflush( stdout );
  for ( size_t i = 0; i < this->sub_count; i++ )
    this->sub_db.add_wildcard( this->sub[ i ] );
  this->start_subscriptions();
}

/* start subscriptions from command line, inbox number indexes the sub[] */
void
RvDataCallback::start_subscriptions( void ) noexcept
{
  this->sub_db.start_subscriptions( this->sub_count == 0 );
  this->poll.timer.add_timer_seconds( *this, 3, 1, 0 );
}

/* if ctrl-c, program signalled, unsubscribe the subs */
void
RvDataCallback::on_unsubscribe( void ) noexcept
{
  this->sub_db.stop_subscriptions();
}

const char *
RvDataCallback::ts( uint32_t mono,  const char *str,  char *buf ) noexcept
{
  if ( mono == 0 ) {
    buf[ 0 ] = '\0';
    return buf;
  }
  time_t t = this->client.poll.mono_to_utc_secs( mono );
  struct tm tm;
  localtime_r( &t, &tm );
  ::snprintf( buf, 32, "%02d:%02d:%02d",
    tm.tm_hour, tm.tm_min, tm.tm_sec );
  return str;
}

/* dict timer expired */
bool
RvDataCallback::timer_cb( uint64_t ,  uint64_t ) noexcept
{
  RvSubscriptionDB & db = this->sub_db;
  uint32_t i;
  db.process_events();

  if ( this->top ) {
    printf( "\033[H\033[J" );
    for ( i = 0; i < db.host_tab.count; i++ ) {
      RvHostEntry & host = db.host_tab.ptr[ i ];

      char b1[ 32 ], b2[ 32 ], b3[ 32 ], b4[ 32 ];
      const char * ref_str     = this->ts( host.ref_mono   , ", ref: "   , b1 ),
                 * status_str  = this->ts( host.status_mono, ", status: ", b2 ),
                 * start_str   = this->ts( host.start_mono , ", start: " , b3 ),
                 * stop_str    = this->ts( host.stop_mono  , ", stop: "  , b4 );

      printf( "host[%u] %08X  state:%s%s%s%s%s%s%s%s%s, age:%u\n", i,
            host.host_id, RvHostEntry::get_host_state_str( host.state ),
            ref_str, b1, status_str, b2, start_str, b3, stop_str, b4,
            db.cur_mono - host.ref_mono );

      if ( host.sess_ht != NULL ) {
        RvSessionEntry * entry;
        RvSubscription * script;
        size_t           pos, pos2;

        if ( (entry = db.first_session( host, pos )) != NULL ) {
          do {
            status_str  = this->ts( entry->ref_mono   , ", ref: "   , b1 );
            start_str   = this->ts( entry->start_mono , ", start: " , b2 );
            stop_str    = this->ts( entry->stop_mono  , ", stop: "  , b3 );
            printf( "  id %.*s  state:%s%s%s%s%s%s%s\n",
                entry->len, entry->value,
                RvSessionEntry::get_session_state_str( entry->state ),
                status_str, b1, start_str, b2, stop_str, b3 );
            if ( (script = db.first_subject( *entry, pos2 )) != NULL ) {
              do {
                printf( "    - %.*s\n", script->len, script->value );
              } while ( (script = db.next_subject( *entry, pos2 )) != NULL );
            }
          } while ( (entry = db.next_session( host, pos )) != NULL );
        }
      }
    }
    fflush( stdout );
  }
  return true; /* return false to disable recurrent timer */
}

void
RvDataCallback::on_listen_start( Start &add ) noexcept
{
  if ( add.reply_len == 0 ) {
    printf( "%sstart %.*s refs %u from %.*s\n",
      add.is_listen_start ? "listen_" : "assert_",
      add.sub.len, add.sub.value, add.sub.refcnt,
      add.session.len, add.session.value );
  }
  else {
    printf( "%sstart %.*s reply %.*s refs %u from %.*s\n",
      add.is_listen_start ? "listen_" : "assert_",
      add.sub.len, add.sub.value, add.reply_len, add.reply, add.sub.refcnt,
      add.session.len, add.session.value );
  }
}

void
RvDataCallback::on_listen_stop( Stop &rem ) noexcept
{
  printf( "%sstop %.*s refs %u from %.*s%s\n",
    rem.is_listen_stop ? "listen_" : "assert_",
    rem.sub.len, rem.sub.value, rem.sub.refcnt,
    rem.session.len, rem.session.value, rem.is_orphan ? " orphan" : "" );
}

void
RvDataCallback::on_snapshot( Snap &snp ) noexcept
{
  printf( "snap %.*s reply %.*s refs %u flags %u\n",
    snp.sub.len, snp.sub.value, snp.reply_len, snp.reply, snp.sub.refcnt,
    snp.flags );
}

/* when client connection stops */
void
RvDataCallback::on_shutdown( EvSocket &conn,  const char *err,
                             size_t errlen ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  printf( "Shutdown: %.*s %.*s\n",
          len, conn.peer_address.buf, (int) errlen, err );
  /* if disconnected by tcp, usually a reconnect protocol, but this just exits*/
  if ( this->poll.quit == 0 )
    this->poll.quit = 1; /* causes poll loop to exit */
}

bool
RvDataCallback::on_rv_msg( EvPublish &pub ) noexcept
{
  this->sub_db.process_pub( pub );
  return true;
}

static const char *
get_arg( int &x, int argc, const char *argv[], int b, const char *f,
         const char *g, const char *def ) noexcept
{
  for ( int i = 1; i < argc - b; i++ ) {
    if ( ::strcmp( f, argv[ i ] ) == 0 || ::strcmp( g, argv[ i ] ) == 0 ) {
      if ( x < i + b + 1 )
        x = i + b + 1;
      return argv[ i + b ];
    }
  }
  return def; /* default value */
}
int
main( int argc, const char *argv[] )
{
  SignalHandler sighndl;
  int x = 1;
  const char * daemon  = get_arg( x, argc, argv, 1, "-d", "-daemon", "tcp:7500" ),
             * network = get_arg( x, argc, argv, 1, "-n", "-network", ""),
             * service = get_arg( x, argc, argv, 1, "-s", "-service", "7500" ),
             * user    = get_arg( x, argc, argv, 1, "-u", "-user", "rv_subtop"),
             * log     = get_arg( x, argc, argv, 1, "-l", "-log", NULL ),
             * top     = get_arg( x, argc, argv, 0, "-t", "-top", NULL ),
             * help    = get_arg( x, argc, argv, 0, "-h", "-help", 0 );
  int first_sub = x, idle_count = 0;

  if ( help != NULL ) {
    fprintf( stderr,
 "%s [-d daemon] [-n network] [-s service] [-l log] [-t] subject ...\n"
             "  -d daemon  = daemon port to connect\n"
             "  -n network = network\n"
             "  -s service = service\n"
             "  -u user    = user name\n"
             "  -l log     = write to debug log\n"
             "  -t         = update with top format\n"
             "  subject    = subject wildcards to monitor\n", argv[ 0 ] );
    return 1;
  }
  EvPoll poll;
  poll.init( 5, false );

  EvRvClientParameters parm( daemon, network, service, user, 0 );
  EvRvClient           conn( poll );
  RvDataCallback       data( poll, conn, &argv[ first_sub ], argc - first_sub,
                             top != NULL );
  MDOutput             mout;

  if ( log != NULL ) {
    mout.open( log, "w" );
    data.sub_db.mout = &mout;
  }
  /* connect to daemon */
  if ( ! conn.rv_connect( parm, &data, &data ) ) {
    fprintf( stderr, "Failed to connect to daemon\n" );
    return 1;
  }
  /* handle ctrl-c */
  sighndl.install();
  for (;;) {
    /* loop 5 times before quiting, time to flush writes */
    if ( poll.quit >= 5 && idle_count > 0 )
      break;
    /* dispatch network events */
    int idle = poll.dispatch();
    if ( idle == EvPoll::DISPATCH_IDLE )
      idle_count++;
    else
      idle_count = 0;
    /* wait for network events */
    poll.wait( idle_count > 255 ? 100 : 0 );
    if ( sighndl.signaled ) {
      if ( poll.quit == 0 )
        data.on_unsubscribe();
      poll.quit++;
    }
  }
  return 0;
}

