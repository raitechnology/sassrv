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

static const size_t PING_MSG_SIZE = 32;
struct PingMsg {
  uint64_t ping_src,
           time_sent,
           seqno_sent;
  char     pad[ PING_MSG_SIZE - sizeof( uint64_t ) * 3 ];
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

    uint32_t rcnt = this->poll.sub_route.add_sub_route( this->rep_h, this->fd );
    this->poll.notify_sub( this->rep_h, this->rep, this->replen, this->fd,
                           rcnt, 'V', NULL, 0 );
    return 0;
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
    if ( this->last_msg.ping_src != 0 ) {
      src   = this->last_msg.ping_src;
      stamp = this->last_msg.time_sent;
      num   = this->last_msg.seqno_sent;
      this->last_msg.ping_src = 0;
      return true;
    }
    return false;
  }

  bool send_ping( uint64_t src,  uint64_t stamp,  uint64_t num ) {
    PingMsg  m;
    m.ping_src   = src;
    m.time_sent  = stamp;
    m.seqno_sent = num;
    /*printf( "%s -> %lu %lu %lu\n", this->sub, src, stamp, num );*/
    EvPublish pub( this->sub, this->sublen, NULL, 0, &m, sizeof( m ), this->fd,
                   this->sub_h, NULL, 0, MD_OPAQUE, 'v' );
    return this->poll.forward_msg( pub );
  }
};

struct MyRvClient : public EvRvClient, public Endpoint {
  uint64_t timeout_usecs;
  MyRvClient( EvPoll &p ) : EvRvClient( p ), Endpoint( p ),
                            timeout_usecs( 0 ) {}
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
             * bu = get_arg( argc, argv, 0, "-b", 0 ),
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
  MyRvClient conn( poll );
  if ( ! conn.connect( "127.0.0.1", atoi( de ), ne, sv, NULL ) ) {
    fprintf( stderr, "create RV socket to %s failed\n", de );
    return 1;
  }

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

  sighndl.install();
  while ( ! poll.quit ) {
    idle = poll.dispatch();
    poll.wait( idle == EvPoll::DISPATCH_IDLE ? 100 : 0 );
    if ( conn.rv_state == EvRvClient::DATA_RECV )
      break;
    if ( conn.rv_state == EvRvClient::ERR_CLOSE ) {
      fprintf( stderr, "connect failed\n" );
      return 1;
    }
    if ( sighndl.signaled )
      return 1;
  }
  conn.init_endpoint( sub, rep );
  if ( bu != NULL )
    conn.EvRvClient::push( EV_BUSY_POLL );
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
    if ( sighndl.signaled )
      poll.quit++;
  }
  /*conn.close();*/
  if ( ! reflect )
    hdr_percentiles_print( histogram, stdout, 5, 1000.0, CLASSIC );
  return 0;
}
