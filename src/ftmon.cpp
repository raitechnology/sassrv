#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <sassrv/ev_rv_client.h>
#include <sassrv/ft.h>
#include <raikv/ev_publish.h>

using namespace rai;
using namespace kv;
using namespace sassrv;
using namespace md;

/* rv client callback closure */
struct RvDataCallback : public EvConnectionNotify, public RvClientCB,
                        public EvTimerCallback, public RvFtListener {
  EvPoll     & poll;            /* poll loop data */
  EvRvClient & client;          /* connection to rv */
  RvFt         ft;
  FtParameters param;
  uint32_t     ft_rank;
  bool         is_running,
               is_finishing,
               is_stopped,
               test_rejoin,
               top;

  RvDataCallback( EvPoll &p,  EvRvClient &c,  bool t )
    : poll( p ), client( c ), ft( c, this ), ft_rank( 0 ), is_running( false ),
      is_finishing( false ), is_stopped( false ), test_rejoin( false ), top( t ) {}

  /* after CONNECTED message */
  virtual void on_connect( EvSocket &conn ) noexcept;
  /* start sub[] with inbox reply */
  void start_ft( void ) noexcept;
  /* when signalled, unsubscribe */
  void on_stop( void ) noexcept;
  /* when disconnected */
  virtual void on_shutdown( EvSocket &conn,  const char *err,
                            size_t err_len ) noexcept;
  /* message from network */
  virtual bool on_rv_msg( EvPublish &pub ) noexcept;
  virtual bool timer_cb( uint64_t,  uint64_t ) noexcept;
  /* new listen start */
  virtual void on_ft_change( uint8_t action ) noexcept;
  virtual void on_ft_sync( EvPublish &pub ) noexcept;
  const char * ts( uint64_t mono,  const char *str,  char *buf ) noexcept;
};

/* called after daemon responds with CONNECTED message */
void
RvDataCallback::on_connect( EvSocket &conn ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  printf( "Connected: %.*s\n", len, conn.peer_address.buf );
  fflush( stdout );
  this->start_ft();
}

/* start subscriptions from command line, inbox number indexes the sub[] */
void
RvDataCallback::start_ft( void ) noexcept
{
  this->is_finishing = false;
  this->is_stopped = false;
  this->is_running = false;
  this->ft.start( this->param );
  this->poll.timer.add_timer_seconds( *this, 3, 1, 0 );
  if ( this->test_rejoin && this->param.join_ms != 0 )
    this->poll.timer.add_timer_seconds( *this, 20, 1, 1 );
}

const char *
RvDataCallback::ts( uint64_t mono,  const char *str,  char *buf ) noexcept
{
  if ( mono == 0 ) {
    buf[ 0 ] = '\0';
    return buf;
  }
  uint64_t ns = this->client.poll.mono_to_utc_ns( mono ),
           ms = ns / ( 1000 * 1000 );
  time_t   t  = ns / ( 1000 * 1000 * 1000 );
  struct   tm tm;
  localtime_r( &t, &tm );

  ::snprintf( buf, 32, "%02d:%02d:%02d.%d%d%d",
    tm.tm_hour, tm.tm_min, tm.tm_sec,
    (uint32_t) ( ( ms % 1000 ) / 100 ),
    (uint32_t) ( ( ms % 100 ) / 10 ),
    (uint32_t)   ( ms % 10 ) );
  return str;
}

bool
RvDataCallback::timer_cb( uint64_t , uint64_t event_id ) noexcept
{
  if ( event_id == 0 && this->top ) {
    printf( "\033[H\033[J" );
    printf(
    "%.*s hb:%u, act:%u, pre:%u, fin:%u, sta:%s, pri:%u, sec:%u, j:%u\n",
            (int) this->ft.ft_sub_len, this->ft.ft_sub,
            this->ft.heartbeat_ms, this->ft.activate_ms, this->ft.prepare_ms,
            this->ft.finish_ms, RvFt::state_str[ this->ft.me.state ],
            this->ft.state_count.primary, this->ft.state_count.secondary,
            this->ft.state_count.join );
    size_t maxlen = 0;
    for ( uint32_t i = 0; i < this->ft.ft_queue.count; i++ ) {
      FtPeer *p = this->ft.ft_queue.ptr[ i ];
      size_t len = ::strlen( p->user );
      if ( len > maxlen ) maxlen = len;
    }
    for ( uint32_t i = 0; i < this->ft.ft_queue.count; i++ ) {
      FtPeer *p = this->ft.ft_queue.ptr[ i ];

      char b1[ 32 ], b2[ 32 ];
      const char * start_str = this->ts( p->start_ns    , "start: " , b1 ),
                 * recv_str  = this->ts( p->last_rcv_ns , ", recv: "  , b2 );

      printf( "%s[%u]. %*s -- %s%s%s%s, weight:%u, state:%s, lat:%ld\n",
            p == &this->ft.me ? "->" : "  ", p->pos, (int) maxlen, p->user,
            start_str, b1, recv_str, b2, (uint32_t) p->weight,
            RvFt::state_str[ p->state ], p->latency() );
    }
    fflush( stdout );
  }
  else if ( this->is_running && event_id == 1 ) {
    if ( this->is_finishing )
      return false;
    this->ft.deactivate();
    this->poll.timer.add_timer_seconds( *this, 1, 1, 2 );
  }
  else if ( event_id == 2 ) {
    if ( ! this->is_finishing )
      this->ft.activate();
    return false;
  }
  return true;
}
/* if ctrl-c, program signalled, unsubscribe the subs */
void
RvDataCallback::on_stop( void ) noexcept
{
  this->is_stopped = true;
  if ( ! this->is_finishing ) {
    if ( this->ft.stop() == 0 ) {
      this->is_running = false;
      this->poll.quit++;
    }
    this->is_finishing = true; /* timer set for finish */
  }
}

void
RvDataCallback::on_ft_change( uint8_t action ) noexcept
{
  char b1[ 64 ];
  const char *str =
    this->ts( this->poll.mono_ns, RvFt::action_str[ action ], b1 );
  printf( "%s %s ", b1, str ); 
  this->ft.ft_queue.print();
  printf( "\n" );
  if ( action == RvFt::ACTION_FINISH ) {
    this->is_running   = false;
    this->is_finishing = false;
    this->poll.quit++;
  }
  else if ( action == RvFt::ACTION_ACTIVATE ||
            action == RvFt::ACTION_DEACTIVATE ) {
    this->is_running = true;
  }
  else if ( action == RvFt::ACTION_LISTEN ) {
    this->is_running = false;
  }
  if ( this->is_running || this->ft_rank > 0 ) {
    uint32_t new_rank = this->ft.me.pos;
    if ( this->ft_rank != new_rank ) {
      printf( "new_rank = %u -> %u (p:%u, s:%u, j:%u)\n", this->ft_rank,
              new_rank, this->ft.state_count.primary,
              this->ft.state_count.secondary, this->ft.state_count.join );
      this->ft_rank = new_rank;
    }
  }
}

void
RvDataCallback::on_ft_sync( EvPublish &pub ) noexcept
{
  printf( "sync %.*s\n", (int) pub.subject_len, pub.subject );
}

/* when client connection stops */
void
RvDataCallback::on_shutdown( EvSocket &conn,  const char *err,
                             size_t errlen ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  printf( "Shutdown: %.*s %.*s\n",
          len, conn.peer_address.buf, (int) errlen, err );
  this->ft.finish_ms = 0;
  if ( ! this->is_stopped )
    this->on_stop();
#if 0
  /* if disconnected by tcp, usually a reconnect protocol, but this just exits*/
  if ( this->poll.quit == 0 )
    this->poll.quit = 1; /* causes poll loop to exit */
#endif
}

bool
RvDataCallback::on_rv_msg( EvPublish &pub ) noexcept
{
  this->ft.process_pub( pub );
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
             * user    = get_arg( x, argc, argv, 1, "-u", "-user", "rv_ftmon" ),
             * weight  = get_arg( x, argc, argv, 1, "-w", "-weight", NULL ),
             * cluster = get_arg( x, argc, argv, 1, "-c", "-cluster", "GRP" ),
             * hb      = get_arg( x, argc, argv, 1, "-b", "-heartbeat", NULL ),
             * act     = get_arg( x, argc, argv, 1, "-a", "-activate", NULL ),
             * pre     = get_arg( x, argc, argv, 1, "-p", "-prepare", NULL ),
             * join    = get_arg( x, argc, argv, 0, "-j", "-join", NULL ),
             * rejoin  = get_arg( x, argc, argv, 0, "-r", "-rejoin", NULL ),
             * log     = get_arg( x, argc, argv, 1, "-l", "-log", NULL ),
             * top     = get_arg( x, argc, argv, 0, "-t", "-top", NULL ),
             * help    = get_arg( x, argc, argv, 0, "-h", "-help", 0 );
  int /*first_sub = x,*/ idle_count = 0;

  if ( help != NULL ) {
    fprintf( stderr,
 "%s [-d daemon] [-n network] [-s service] [-w weight] [-u user] [-c cluster] "
    "[-l log] [-b hb] [-a act] [-p prep]\n"
             "  -d daemon  = daemon port to connect\n"
             "  -n network = network\n"
             "  -s service = service\n"
             "  -u user    = user name\n"
             "  -w weight  = weight of ft member, more has priority\n"
             "  -c cluster = ft cluster to join (GRP)\n"
             "  -b hb      = heartbeat millisecs\n"
             "  -a act     = activate millisecs\n"
             "  -p prepare = prepare millisecs\n"
             "  -j         = join network, otherwise observe\n"
             "  -r         = return to listen and rejoin every 20s\n"
             "  -l log     = write to debug log\n"
             "  -t         = top, update display\n"
             "ft subject is _FT.<cluster>\n", argv[ 0 ] );
    return 1;
  }
  EvPoll poll;
  char   ft_sub[ 128 ], user_buf[ 128 ];
  poll.init( 5, false );

  EvRvClientParameters parm( daemon, network, service, user, 0 );
  EvRvClient           conn( poll );
  RvDataCallback       data( poll, conn, top != NULL );
  MDOutput             mout;

  if ( weight != NULL )
    data.param.weight = atoi( weight );
  ::snprintf( ft_sub, sizeof( ft_sub ), "_FT.%s", cluster );
  if ( user == NULL ) {
    if ( ::gethostname( user_buf, sizeof( user_buf ) ) != 0 )
      ::strcpy( user_buf, "localhost" );
    user = user_buf;
  }
  if ( join == NULL )
    data.param.join_ms = 0;
  if ( hb != NULL )
    data.param.heartbeat_ms = atoi( hb );
  if ( act != NULL )
    data.param.activate_ms = atoi( act );
  if ( pre != NULL )
    data.param.prepare_ms = atoi( pre );
  data.param.ft_sub     = ft_sub;
  data.param.ft_sub_len = ::strlen( ft_sub );
  data.param.user       = user;
  data.param.user_len   = ::strlen( user );
  if ( log != NULL ) {
    mout.open( log, "w" );
    data.ft.mout = &mout;
  }
  if ( rejoin != NULL )
    data.test_rejoin = true;
  /* connect to daemon */
  if ( ! conn.connect( parm, &data, &data ) ) {
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
      if ( poll.quit == 0 && ! data.is_finishing )
        data.on_stop();
      /*poll.quit++;*/
    }
  }
  data.ft.release();
  return 0;
}

