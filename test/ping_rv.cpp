#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <time.h>
#include <hdr_histogram.h>
#include <raikv/ev_net.h>
#include <raikv/ev_publish.h>
#include <sassrv/ev_rv_client.h>

using namespace rai;
using namespace sassrv;
using namespace kv;
using namespace md;

static const size_t PING_MSG_SIZE = 32; /* 8 * 4 */
struct PingMsg {
  char     ping_hdr[ 8 ];
  uint64_t ping_src,
           time_sent,
           seqno_sent;
};

static const char *
get_arg( int argc, char *argv[], int b, const char *f, const char *def )
{
  for ( int i = 1; i < argc - b; i++ )
    if ( ::strcmp( f, argv[ i ] ) == 0 )
      return argv[ i + b ];
  return def; /* default value */
}

uint64_t
current_time_nsecs( void )
{
  struct timespec ts;
  clock_gettime( CLOCK_MONOTONIC, &ts );
  return (uint64_t) ts.tv_sec * 1000000000 + ts.tv_nsec;
}

struct Endpoint : public EvSocket {
  PingMsg      last_msg;
  const char * sub,
             * rep;
  size_t       sublen,
               replen;
  uint32_t     sub_h,
               rep_h;

  Endpoint( EvPoll &p ) : EvSocket( p, p.register_type( "endpoint" ) ) {
    this->sock_opts = OPT_NO_POLL;
    this->last_msg.ping_src = 0;
  }
  int init_endpoint( const char *s,  const char *r ) {
    int pfd = this->poll.get_null_fd();
    this->PeerData::init_peer( pfd, NULL, "endpoint" );
    if ( this->poll.add_sock( this ) != 0 )
      return -1;

    this->sub    = s;
    this->sublen = ::strlen( s );
    this->sub_h  = kv_crc_c( s, this->sublen, 0 );
    this->rep    = r;
    this->replen = ::strlen( r );
    this->rep_h  = kv_crc_c( r, this->replen, 0 );
    return 0;
  }
  void start_sub( void ) {
    uint32_t rcnt = this->poll.sub_route.add_sub_route( this->rep_h, this->fd );
    this->poll.notify_sub( this->rep_h, this->rep, this->replen, this->fd,
                           rcnt, 'V', NULL, 0 );
  }
  virtual bool on_msg( EvPublish &pub ) noexcept {
    if ( pub.msg_len == sizeof( PingMsg ) ) {
      ::memcpy( &this->last_msg, pub.msg, sizeof( PingMsg ) );
      /*printf( "%.*s <- %lu %lu %lu\n", (int) pub.subject_len, pub.subject,
              this->last_msg.ping_src, this->last_msg.time_sent,
              this->last_msg.seqno_sent );*/
    }
    return true;
  }
  virtual void write( void ) noexcept {}
  virtual void read( void ) noexcept {}
  virtual void process( void ) noexcept {}
  virtual void release( void ) noexcept {}

  bool recv_ping( uint64_t &src, uint64_t &stamp, uint64_t &num ) {
    if ( this->last_msg.ping_hdr[ 0 ] == 'P' ) {
      src   = this->last_msg.ping_src;
      stamp = this->last_msg.time_sent;
      num   = this->last_msg.seqno_sent;
      this->last_msg.ping_hdr[ 0 ] = 0;
      return true;
    }
    return false;
  }

  bool send_ping( uint64_t src,  uint64_t stamp,  uint64_t num ) {
    PingMsg  m;
    ::memcpy( m.ping_hdr, "PING1234", 8 );
    m.ping_src   = src;
    m.time_sent  = stamp;
    m.seqno_sent = num;
    /*printf( "%s -> %lu %lu %lu\n", this->sub, src, stamp, num );*/
    EvPublish pub( this->sub, this->sublen, NULL, 0, &m, sizeof( m ), this->fd,
                   this->sub_h, NULL, 0, MD_OPAQUE, 'v' );
    return this->poll.forward_msg( pub );
  }
};

struct MyRvClient : public EvRvClient, public Endpoint,
                    public EvConnectionNotify, public EvTimerCallback {
  EvRvClientParameters & parameters;

  uint64_t timeout_usecs;
  double   reconnect_time;
  uint16_t reconnect_timeout_secs;
  bool     is_reconnecting,
           is_shutdown;

  MyRvClient( EvPoll &p,  EvRvClientParameters &parm )
    : EvRvClient( p ), Endpoint( p ), parameters( parm ),
      timeout_usecs( 0 ), reconnect_time( 0 ), reconnect_timeout_secs( 1 ),
      is_reconnecting( false ), is_shutdown( false ) {}

  bool do_connect( void ) {
    if ( ! this->EvRvClient::connect( this->parameters, this ) ) {
      fprintf( stderr, "create socket failed\n" );
      this->setup_reconnect();
      return false;
    }
    return true;
  }
  /* notifcation of ready, after authentication, etc */
  virtual void on_connect( EvConnection &conn ) noexcept {
    printf( "connected %s\n", conn.peer_address );
    this->start_sub();
  }
  /* notification of connection close or loss */
  virtual void on_shutdown( EvConnection &conn,  const char *,
                            size_t ) noexcept {
    printf( "disconnected %s\n", conn.peer_address );
    this->setup_reconnect();
  }
  void setup_reconnect( void ) {
    if ( ! this->is_reconnecting && ! this->is_shutdown ) {
      this->is_reconnecting = true;
      double now = current_monotonic_time_s();
      if ( this->reconnect_time != 0 && this->reconnect_time +
             (double) this->reconnect_timeout_secs * 2 > now ) {
          this->reconnect_timeout_secs *= 2;
          if ( this->reconnect_timeout_secs > 16 )
            this->reconnect_timeout_secs = 16;
      }
      else {
        this->reconnect_timeout_secs = 1;
      }
      this->reconnect_time = now;
      printf( "reconnect in %u seconds\n", this->reconnect_timeout_secs );
      this->Endpoint::poll.add_timer_seconds( *this,
                                           this->reconnect_timeout_secs, 0, 0 );
    }
  }
  virtual bool timer_cb( uint64_t, uint64_t ) noexcept {
    if ( this->is_reconnecting ) {
      this->is_reconnecting = false;
      if ( ! this->is_shutdown ) {
        if ( ! this->do_connect() )
          this->setup_reconnect();
      }
    }
    return false;
  }
};

int
main( int argc, char **argv )
{
  SignalHandler sighndl;
  const char * de = get_arg( argc, argv, 1, "-d", "7500" ),
             * ne = get_arg( argc, argv, 1, "-n", 0 ),
             * sv = get_arg( argc, argv, 1, "-s", "7500" ),
             * ct = get_arg( argc, argv, 1, "-c", 0 ),
             * re = get_arg( argc, argv, 0, "-r", 0 ),
             /** bu = get_arg( argc, argv, 0, "-b", 0 ),*/
             * he = get_arg( argc, argv, 0, "-h", 0 );
  uint64_t count = 0, warm = 0;
  
  if ( he != NULL ) {
    fprintf( stderr,
             "%s [-d daemon] [-n network] [-s service] [-r] [-b] [-c count]\n"
             "  -d daemon  = daemon port to connect\n"
             "  -n network = network to ping\n"
             "  -s service = service to ping\n"
             "  -r         = reflect pings\n"
             "  -b         = busy wait\n"
             "  -c count   = number of pings\n", argv[ 0 ] );
    return 1;
  }
  if ( ct != NULL ) {
    if ( atoll( ct ) <= 0 ) {
      fprintf( stderr, "count should be > 0\n" );
    }
    count = (uint64_t) atoll( ct );
    warm  = count;
    count = warm / 100;
    if ( count == 0 )
      count = 1;
  }

  EvPoll poll;
  poll.init( 5, false );

  struct hdr_histogram * histogram = NULL;
  uint64_t ping_ival = 1000000000,
           last      = current_time_nsecs(),
           next      = last + ping_ival,
           seqno     = 0,
           my_id     = getpid(),
           idle_cnt  = 0,
           src, stamp, num, delta, now;
  int      idle;
  bool     reflect  = ( re != NULL );
  const char * sub = ( reflect ? "_TIC.TEST" : "TEST" );
  const char * rep = ( reflect ? "TEST" : "_TIC.TEST" );
  EvRvClientParameters parm( "127.0.0.1", ne, sv, atoi( de ) );

  MyRvClient conn( poll, parm );
  conn.init_endpoint( sub, rep );
  conn.do_connect();
  sighndl.install();
  /*if ( bu != NULL )
    conn.EvRvClient::push( EV_BUSY_POLL );*/
  if ( ! reflect )
    hdr_init( 1, 1000000, 3, &histogram );
  for (;;) {
    if ( poll.quit >= 5 )
      break;
    idle = poll.dispatch();
    if ( idle == EvPoll::DISPATCH_IDLE )
      idle_cnt++;
    else
      idle_cnt = 0;
    poll.wait( idle_cnt > 100 ? 100 : 0 );

    if ( conn.recv_ping( src, stamp, num ) ) {
      /*spin_cnt = 0;*/
      if ( src == my_id ) {
        now  = current_time_nsecs();
        next = now; /* send the next immediately */
        hdr_record_value( histogram, now - stamp );
        if ( count > 0 && --count == 0 ) {
          if ( warm > 0 ) {
             hdr_reset( histogram );
             count = warm;
             warm  = 0;
          }
          else {
            conn.is_shutdown = true;
            poll.quit++;
          }
        }
      }
      else /* if ( reflect ) */ { /* not mine, reflect */
        conn.send_ping( src, stamp, num );
      }
    }
    /* activly send pings every second until response */
    else if ( ! reflect ) {
      now = current_time_nsecs();
      if ( now >= next ) {
        if ( conn.send_ping( my_id, now, seqno ) ) {
          seqno++;
          last = now;
          next = now + ping_ival;
          conn.timeout_usecs = 0;
          /*spin_cnt = 0;*/
        }
      }
      else {
        delta = next - now;
        if ( conn.timeout_usecs > delta / 1000 )
          conn.timeout_usecs = delta / 1000;
      }
    }
    if ( sighndl.signaled ) {
      conn.is_shutdown = true;
      poll.quit++;
    }
  }
  /*conn.close();*/
  if ( ! reflect )
    hdr_percentiles_print( histogram, stdout, 5, 1000.0, CLASSIC );
  return 0;
}
