#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#ifndef _MSC_VER
#include <arpa/inet.h>
#else
#include <winsock.h>
#endif
#include <sassrv/ev_rv.h>
#include <raikv/key_hash.h>
#include <raikv/util.h>
#include <raikv/ev_publish.h>
#include <raikv/timer_queue.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#include <raikv/pattern_cvt.h>
#include <raimd/json_msg.h>
#include <raimd/tib_msg.h>
#include <raimd/tib_sass_msg.h>

using namespace rai;
using namespace sassrv;
using namespace kv;
using namespace md;

extern "C" {
const char *
sassrv_get_version( void )
{
  return kv_stringify( SASSRV_VER );
}
}
uint32_t rai::sassrv::rv_debug = 0;

/*
 * RV protocol (initial parts are int32 in big endian) (send: svr->client)
 *
 * 1. send ver-rec [0,4,0] {RV protocol version 4.0}
 *    recv ver-rec [0,4,0] 
 *
 *    These are likely legacy from the old CI protocol which had 1.X versions.
 *
 * 2. send info-rec [2, 2, 0, 1, 4<<24, 4<<24, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
 *    recv info-rec [3, 2, 0, 1, 4<<24, 4<<24, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
 *    send info-rec [1, 2, 0, 1, 4<<24, 4<<24, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
 *
 *    Negotiating options, not sure what it means, except that
 *      2 = request, 3 = response, 1 = agreement
 *
 * 3. recv init-rec { mtype: I, userid: user, service: port, network: ip;x;y,
 *                    vmaj: 5, vmin: 4, vupd: 2 }
 *    send init-rec { sub: RVD.INITRESP, mtype R,
 *                    data: { ipaddr: ip, ipport: port, vmaj: 8, vmin: 3,
 *                            vupd: 1, gob: utf*4096 } }
 *    recv init-rec { mtype: I, userid: user, session: <IP>.<PID><TIME><PTR>,
 *                    service: port, network: ip;x;y,
 *                    control: _INBOX.<session>.1, vmaj: 5, vmin: 4, vupd: 2 }
 *
 * 4. send connected { sub: _RV.INFO.SYSTEM.RVD.CONNECTED, mtype D
 *                     data: { ADV_CLASS: INFO, ADV_SOURCE: SYSTEM,
 *                             ADV_NAME: RVD.CONNECTED } }
 *
 * 5. recv inbox listen { mtype: L, sub: _INBOX.<session>.> }
 *
 * 6. recv sub listen { mtype: L, sub: TEST }
 *    send sub data { mtype: D, sub: TEST, data: msg-data }
 *    recv sub data { mtype: D, sub: TEST, data: msg-data }
 *    send sub cancel { mtype: C, sub: TEST }
 *
 * mtype:
 *   I = informational
 *   L = listen subject
 *   D = publish data on a subject
 *   C = cancel listen
 */
/* string constant + strlen() + 1 */

EvRvListen::EvRvListen( EvPoll &p ) noexcept
  : EvTcpListen( p, "rv_listen", "rv_sock" ), /*RvHost( *this ),*/
    sub_route( p.sub_route ), dummy_host( 0 ), host_timer_id( 0 ),
    ipport( 0 ), has_service_prefix( true )
{
  md_init_auto_unpack();
}

EvRvListen::EvRvListen( EvPoll &p,  RoutePublish &sr ) noexcept
  : EvTcpListen( p, "rv_listen", "rv_sock" ), /*RvHost( *this ),*/
    sub_route( sr ), dummy_host( 0 ), host_timer_id( 0 ),
    ipport( 0 ), has_service_prefix( true )
{
  md_init_auto_unpack();
}

int
EvRvListen::listen( const char *ip,  int port,  int opts ) noexcept
{
  int status;
  status = this->kv::EvTcpListen::listen2( ip, port, opts, "rv_listen",
                                           this->sub_route.route_id );
  if ( status == 0 )
    this->ipport = htons( port ); /* network order */
  return status;
}

EvSocket *
EvRvListen::accept( void ) noexcept
{
  EvRvService *c =
    this->poll.get_free_list<EvRvService, EvRvListen &>(
      this->accept_sock_type, *this );
  if ( c == NULL )
    return NULL;
  if ( ! this->accept2( *c, "rv" ) )
    return NULL;

  c->initialize_state( ++this->timer_id );
  uint32_t ver_rec[ 3 ] = { 0, 4, 0 };
  ver_rec[ 1 ] = get_u32<MD_BIG>( &ver_rec[ 1 ] ); /* flip */
  c->append( ver_rec, sizeof( ver_rec ) );
  c->idle_push( EV_WRITE_HI );

  return c;
}

RvHostError
EvRvListen::start_network( RvHost *&h, const RvMcast &mc,  const char *net,
                           size_t net_len,  const char *svc,
                           size_t svc_len ) noexcept
{
  RvHost    * host = NULL;
  RvHostError status = HOST_OK;
  size_t      i;

  for ( i = 0; i < this->host_tab.count; i++ ) {
    host = this->host_tab.ptr[ i ];
    if ( host->service_len == svc_len &&
         ::memcmp( host->service, svc, svc_len ) == 0 )
      break;
  }
  if ( i == this->host_tab.count ) {
    void * p = ::malloc( sizeof( RvHost ) );
    host = new ( p ) RvHost( *this, this->sub_route, this->has_service_prefix );
    this->host_tab[ i ] = host;
  }
  status = host->start_network( mc, net, net_len, svc, svc_len );
  if ( status != HOST_OK ) {
    if ( this->dummy_host == NULL ) {
      void * p = ::malloc( sizeof( RvHost ) );
      this->dummy_host = new ( p ) RvHost( *this, this->sub_route,
                                           this->has_service_prefix );
    }
    h = this->dummy_host;
    return status;
  }
  if ( host->rpc == NULL ) {
    DaemonInbox   ibx( *host );
    RvDaemonRpc * rpc;
    for ( i = 0; i < this->daemon_tab.count; i++ ) {
      rpc = this->daemon_tab.ptr[ i ];
      if ( rpc->ibx.equals( ibx ) )
        break;
    }
    if ( i == this->daemon_tab.count ) {
      void * p = ::malloc( sizeof( RvDaemonRpc ) );
      rpc = new ( p ) RvDaemonRpc( *host );
      this->daemon_tab[ i ] = rpc;
    }
    host->rpc = rpc;
  }
  h = host;
  if ( ! host->network_started ) {
    if ( ! host->start_in_process ) {
      host->start_in_process = true;
      if ( this->start_host( *host ) != 0 ) {
        host->start_in_process = false;
        return ERR_START_HOST_FAILED;
      }
    }
  }
  return HOST_OK;
}

void
EvRvService::read( void ) noexcept
{
  /* if host not started, wait for that event before reading pub/sub data */
  if ( this->host_started || this->svc_state <= DATA_RECV ) {
    this->EvConnection::read();
    return;
  }
  this->pop3( EV_READ, EV_READ_HI, EV_READ_LO );
}

int
EvRvListen::start_host2( RvHost &h,  uint32_t delay_secs ) noexcept
{
  return h.start_host2( delay_secs );
}

int
EvRvListen::start_host( RvHost &h) noexcept
{
  return h.start_host();
}

int
EvRvListen::stop_host( RvHost &h ) noexcept
{
  return h.stop_host();
}

void
EvRvService::send_info( bool agree ) noexcept
{
  static const uint32_t info_rec_prefix[] = { 2, 2, 0, 1, 0, 4 << 24, 4 << 24 };
  uint32_t info_rec[ 16 ];
  size_t i;

  ::memcpy( info_rec, info_rec_prefix, sizeof( info_rec_prefix ) );
  if ( agree )
    info_rec[ 0 ] = 1;
  for ( i = 0; i < sizeof( info_rec_prefix ) / 4; i++ )
    info_rec[ i ] = get_u32<MD_BIG>( &info_rec[ i ] ); /* flip */
  for ( ; i < sizeof( info_rec ) / 4; i++ )
    info_rec[ i ] = 0; /* zero rest */
  this->append( info_rec, sizeof( info_rec ) );
}

void
EvRvService::process( void ) noexcept
{
  uint32_t buflen, msglen;
  int      status = 0;

  /* state trasition from VERS_RECV -> INFO_RECV -> DATA_RECV */
  if ( this->svc_state >= DATA_RECV ) { /* main state */
  data_recv_loop:;
    do {
      buflen = this->len - this->off;
      if ( buflen < 8 )
        goto break_loop;
      msglen = get_u32<MD_BIG>( &this->recv[ this->off ] );
      if ( buflen < msglen )
        goto break_loop;
      status = this->dispatch_msg( &this->recv[ this->off ], msglen );
      this->off      += msglen;
      this->msgs_recv++;
      this->host->br += msglen;
      this->host->mr++;
    } while ( status == 0 );
  }
  else { /* initial connection states */
    for (;;) {
      buflen = this->len - this->off;
      if ( buflen < 8 )
        goto break_loop;
      if ( this->svc_state == VERS_RECV ) {
        if ( buflen < 3 * 4 )
          goto break_loop;
        this->off += 3 * 4;
        this->send_info( false );
        this->svc_state = INFO_RECV;
      }
      else { /* INFO_RECV */
        if ( buflen < 16 * 4 )
          goto break_loop;
        this->off += 16 * 4;
        this->send_info( true );
        this->svc_state = DATA_RECV;
        goto data_recv_loop;
      }
    }
  }
break_loop:;
  this->pop( EV_PROCESS );
  this->push_write();
  if ( status != 0 )
    this->push( EV_CLOSE );
}
#if 0
#include <unistd.h>
#include <fcntl.h>
#endif
/* dispatch a msg: 'C' - unsubscribe subject
 *                 'L' - subscribe subject
 *                 'I' - information for connection
 *                 'D' - data, forward to subscriptions */
int
EvRvService::dispatch_msg( void *msgbuf, size_t msglen ) noexcept
{
  static const char *rv_status_string[] = RV_STATUS_STRINGS;
  int status;
  if ( (status = this->msg_in.unpack( msgbuf, msglen )) != 0 ) {
    if ( msglen == 8 ) /* empty msg */
      return 0;
    if ( msglen != 0 ) {
      MDOutput mout;
      fprintf( stderr, "rv status %d: \"%s\" msglen=%u\n", status,
               rv_status_string[ status ], (uint32_t) msglen );
      mout.print_hex( msgbuf, msglen > 256 ? 256 : msglen );
    }
    return status;
  }
  /*this->msg_in.print();*/
  if ( this->msg_in.mtype == 'D' ) { /* publish */
#if 0
    static int xfd[ 100 ];
    if ( xfd[ this->fd ] == 0 ) {
      char fn[ 32 ];
      ::snprintf( fn, sizeof( fn ), "rv.%d", this->fd );
      xfd[ this->fd ] = ::open( fn, O_APPEND | O_WRONLY | O_CREAT, 0666 );
    }
    ::write( xfd[ this->fd ], "--------", 8 );
    ::write( xfd[ this->fd ], &this->msgs_recv, 8 );
    ::write( xfd[ this->fd ], msgbuf, msglen );
#endif
    this->fwd_pub();
    return 0;
  }
  switch ( this->msg_in.mtype ) { /* publish */
    case 'C': /* unsubscribe */
      this->rem_sub();
      return 0;
    case 'L': /* subscribe */
      this->add_sub();
      return 0;
    case 'I': /* info */
      if ( this->respond_info() == 0 )
        return 0;
      /* fall thru */
    default:
      return -1;
  }
}
/* match a field string in a message */
static bool
match_str( MDName &nm,  MDReference &mref,  const char *s,  char *buf,
           size_t buflen )
{
  if ( ::memcmp( nm.fname, s, nm.fnamelen ) != 0 )
    return false;
  if ( mref.ftype == MD_STRING && mref.fsize > 0 && mref.fsize < buflen ) {
    ::memcpy( buf, mref.fptr, mref.fsize );
    buf[ mref.fsize ] = '\0';
    return true;
  }
  return false;
}
/* match a field int in a message */
static bool
match_int( MDName &nm,  MDReference &mref,  const char *s,  uint16_t &ival )
{
  if ( ::memcmp( nm.fname, s, nm.fnamelen ) != 0 )
    return false;
  if ( mref.ftype == MD_INT || mref.ftype == MD_UINT ) {
    ival = get_uint<uint16_t>( mref );
    return true;
  }
  return false;
}
/* information msg informs user, session, service and network in two passes,
 * the first msg:
 *
 *  { mtype: I, userid: nobody, service: 7500, network: 127.0.0.1,
 *    vmaj: 5, vmin: 4, vupd: 2 }
 *
 * causes this process to try open or at least validate the network and
 * service, if successful, then the reply is:
 *
 * { sub: RVD.INITRESP, mtype: R, data : { ipaddr : 127.0.0.1, ipport : 7500,
 *   vmaj : 5, vmin : 4, vupd : 1, gob : unique-id } }
 *
 * then the client responds with the session id it wants to use, either by
 * creating one (older rv5 clients), or by using the gob param sent to it
 * (newer rv7+ clients)
 *
 * { mtype: I, userid: nobody, session: 7F000001.2202C25FE975070A48320,
 *   service: 7500, network: 127.0.0.1,
 *   control: _INBOX.7F000001.2202C25FE975070A48320.1,
 *   vmaj: 5, vmin: 4, vupd: 2 }
 *
 * the session parameter in this second message will start the session
 * and the host network if necessary
 */
int
EvRvService::respond_info( void ) noexcept
{
  RvFieldIter * iter = this->msg_in.iter;
  MDName        nm;
  MDReference   mref;
  char          net[ MAX_RV_NETWORK_LEN ],
                svc[ MAX_RV_SERVICE_LEN ];
  uint8_t       buf[ 8 * 1024 ];
  RvMsgWriter   rvmsg( buf, sizeof( buf ) ),
                submsg( NULL, 0 );
  size_t        size;
  uint32_t      net_len = 0, svc_len = 0;
  RvHostError   status = HOST_OK;

  if ( this->gob_len == 0 )
    this->gob_len = (uint16_t)
                    RvHost::time_to_str( this->active_ns, this->gob );
  net[ 0 ] = 0;
  svc[ 0 ] = 0;

  if ( iter->first() == 0 ) {
    do {
      if ( iter->get_name( nm ) != 0 || iter->get_reference( mref ) != 0 )
        return ERR_RV_REF;
      switch ( nm.fnamelen ) {
        case 7:
          if ( match_str( nm, mref, "userid", this->userid,
                          sizeof( this->userid ) ) )
            this->userid_len = (uint16_t) ( mref.fsize - 1 );
          break;
        case 8:
          if ( match_str( nm, mref, "session", this->session,
                          sizeof( this->session ) ) ) {
            this->session_len = (uint16_t) ( mref.fsize - 1 );
          }
          else if ( match_str( nm, mref, "control", this->control,
                               sizeof( this->control ) ) )
            this->control_len = (uint16_t) ( mref.fsize - 1 );
          else if ( match_str( nm, mref, "service", svc , sizeof( svc ) ) )
            svc_len = (uint32_t) ( mref.fsize - 1 );
          else if ( match_str( nm, mref, "network", net, sizeof( net ) ) )
            net_len = (uint32_t) ( mref.fsize - 1 );
          break;
        case 5:
          if ( ! match_int( nm, mref, "vmaj", this->vmaj ) )
            if ( ! match_int( nm, mref, "vmin", this->vmin ) )
              match_int( nm, mref, "vupd", this->vupd );
          break;
        default:
          break;
      }
    } while ( iter->next() == 0 );
  }

  RvMcast mc;
  if ( svc_len == 0 ) {
    ::strcpy( svc, "7500" );
    svc_len = 4;
  }
  status = mc.parse_network( net );
  /* check that the network is valid and start it */
  if ( status == HOST_OK ) {
    status = this->listener.start_network( this->host, mc, net, net_len, svc,
                                           svc_len );
    if ( this->host->network_started ) {
      if ( ! this->host_started ) {
        this->host_started = true;
        this->host->active_clients++;
      }
    }
    /*status = this->host->start_network( mc, net, net_len, svc, svc_len );*/
  }
  if ( status != HOST_OK ) {
    /* { sub: RVD.INITREFUSED, mtype: R,
     *   data: { error: 17 } } */
    rvmsg.append_subject( SARG( "sub" ), "RVD.INITREFUSED" );
    rvmsg.append_string( SARG( "mtype" ), SARG( "R" ) );
    rvmsg.append_msg( SARG( "data" ), submsg );
    submsg.append_int<int32_t>( SARG( "error" ), status );
    size = rvmsg.update_hdr( submsg );
    this->push( EV_SHUTDOWN );
  }
  else if ( ! this->sent_initresp ) {
    /* { sub: RVD.INITRESP, mtype: R,
     *   data: { ipaddr: a.b.c.d, ipport: 7500, vmaj: 5, vmin: 4, vupd: 2 } } */
    this->sent_initresp = true;
    rvmsg.append_subject( SARG( "sub" ), "RVD.INITRESP" );
    rvmsg.append_string( SARG( "mtype" ), SARG( "R" ) );
    rvmsg.append_msg( SARG( "data" ), submsg );
    submsg.append_ipdata( SARG( "ipaddr" ), this->host->mcast.host_ip );
    submsg.append_ipdata( SARG( "ipport" ), this->host->service_port );
    submsg.append_int<int32_t>( SARG( "vmaj" ), 5 );
    submsg.append_int<int32_t>( SARG( "vmin" ), 4 );
    submsg.append_int<int32_t>( SARG( "vupd" ), 2 );

    submsg.append_string( SARG( "gob" ), this->gob, this->gob_len + 1 );
    size = rvmsg.update_hdr( submsg );
  }
  else {
    /* { sub: _RV.INFO.SYSTEM.RVD.CONNECTED, mtype: D,
     *   data: { ADV_CLASS: INFO, ADV_SOURCE: SYSTEM, ADV_NAME: RVD.CONNECTED } } */
    this->sent_rvdconn = true;
    rvmsg.append_subject( SARG( "sub" ), "_RV.INFO.SYSTEM.RVD.CONNECTED" );
    rvmsg.append_string( SARG( "mtype" ), SARG( "D" ) );
    rvmsg.append_msg( SARG( "data" ), submsg );
    submsg.append_string( SARG( "ADV_CLASS" ), SARG( "INFO" ) );
    submsg.append_string( SARG( "ADV_SOURCE" ), SARG( "SYSTEM" ) );
    submsg.append_string( SARG( "ADV_NAME" ), SARG( "RVD.CONNECTED" ) );
    size = rvmsg.update_hdr( submsg );
  }
  /* send the result */
  char *m = this->append( buf, size );
  if ( is_rv_debug )
    this->print( m, size );

  if ( status == HOST_OK ) {
    if ( this->svc_state < DATA_RECV )
      this->svc_state = DATA_RECV; /* continue, need another info message */

    if ( this->sent_rvdconn ) {
      if ( this->session_len > 9 &&
          ::strcmp( this->gob, &this->session[ 9 ] ) == 0 ) {
        ::memcpy( this->session, this->host->daemon_id,
                  this->host->daemon_len + 1 );
        this->session_len = this->host->daemon_len;
        /* iphex.DAEMON.session */
        this->svc_state |= IS_RV_DAEMON;
      }
      else
        /* use the session as specified */
        this->svc_state |= IS_RV_SESSION;
    }
  }
  if ( this->host_started )
    this->send_start();
  return 0;
}

void
EvRvService::send_start( void ) noexcept
{
  if ( ( this->svc_state & ( IS_RV_DAEMON | IS_RV_SESSION ) ) != 0 ) {
    if ( this->host->active_clients == 1 ) {
      if ( ( this->svc_state & SENT_HOST_START ) == 0 )
        this->host->send_host_start( this );
    }
  }
  if ( ( this->svc_state & IS_RV_SESSION ) != 0 ) {
    if ( ( this->svc_state & SENT_SESSION_START ) == 0 )
      this->host->send_session_start( this );
  }
  if ( this->host->has_service_prefix )
    this->msg_in.set_prefix( this->host->service, this->host->service_len );
}

void
EvRvService::send_stop( void ) noexcept
{
  if ( ( this->svc_state & IS_RV_SESSION ) != 0 ) {
    if ( ( this->svc_state & SENT_SESSION_START ) != 0 &&
         ( this->svc_state & SENT_SESSION_STOP ) == 0 )
      this->host->send_session_stop( this );
  }
}

bool
EvRvService::timer_expire( uint64_t tid,  uint64_t ) noexcept
{
  if ( this->timer_id != tid )
    return false;
#if 0
  int err = 0;
  socklen_t errsz = sizeof( err );
  if ( ::getsockopt( this->fd, SOL_SOCKET, SO_ERROR, &err, &errsz ) != 0 )
    perror( "getsockopt" );
  else {
    printf( "err %s : %d, state %x sock_flags %x\n",
            this->peer_address, err, this->EvSocket::state, this->sock_flags );
    printf( "off %u len %u recv_size %u recv_highwater %u\n",
            this->off, this->len, this->recv_size, this->recv_highwater );
    printf( "bytes_recv %lu bytes_sent %lu poll_sent %ld\n", this->bytes_recv,
            this->bytes_sent, this->poll_bytes_sent );
    printf( "wr_pending %lu\n", this->pending() );
  }
#endif
  return true;
}

uint8_t
EvRvService::is_subscribed( const NotifySub &sub ) noexcept
{
  uint8_t v    = 0;
  bool    coll = false;
  if ( this->sub_tab.find( sub.subj_hash, sub.subject, sub.subject_len,
                           coll ) == RV_SUB_OK )
    v |= EV_SUBSCRIBED;
  else
    v |= EV_NOT_SUBSCRIBED;
  if ( coll )
    v |= EV_COLLISION;
  return v;
}

uint8_t
EvRvService::is_psubscribed( const NotifyPattern &pat ) noexcept
{
  uint8_t v    = 0;
  bool    coll = false;
  const PatternCvt & cvt = pat.cvt;
  RvPatternRoute   * rt;
  if ( this->pat_tab.find( pat.prefix_hash, pat.pattern, cvt.prefixlen,
                           rt, coll ) == RV_SUB_OK ) {
    RvWildMatch *m;
    for ( m = rt->list.hd; m != NULL; m = m->next ) {
      if ( m->len == pat.pattern_len &&
           ::memcmp( pat.pattern, m->value, m->len ) == 0 ) {
        v |= EV_SUBSCRIBED;
        break;
      }
    }
    if ( m == NULL )
      v |= EV_NOT_SUBSCRIBED | EV_COLLISION;
    else if ( rt->count > 1 )
      v |= EV_COLLISION;
  }
  else {
    v |= EV_NOT_SUBSCRIBED;
  }
  if ( coll )
    v |= EV_COLLISION;
  return v;
}

/* when a 'L' message is received from the client, remember the subject and
 * notify with by publishing a LISTEN.START */
void
EvRvService::add_sub( void ) noexcept
{
  char   * sub;
  size_t   len;
  uint32_t refcnt = 0, h;
  bool     coll   = false;

  this->msg_in.pre_subject( sub, len );
  if ( ! this->msg_in.is_wild ) {
    h = kv_crc_c( sub, len, 0 );
    if ( this->sub_tab.put( h, sub, len, refcnt, coll ) == RV_SUB_OK ) {
      NotifySub nsub( sub, len, this->msg_in.reply, this->msg_in.replylen,
                      h, this->fd, coll, 'V', this->host );
      this->sub_route.add_sub( nsub );
    }
  }
  else {
    RvPatternRoute * rt;
    PatternCvt cvt;

    if ( cvt.convert_rv( sub, len ) == 0 ) {
      h = kv_crc_c( sub, cvt.prefixlen,
                    this->sub_route.prefix_seed( cvt.prefixlen ) );
      if ( this->pat_tab.put( h, sub, cvt.prefixlen, rt,
                              coll ) != RV_SUB_NOT_FOUND ){
        RvWildMatch * m;
        for ( m = rt->list.hd; m != NULL; m = m->next ) {
          if ( m->len == len && ::memcmp( sub, m->value, len ) == 0 ) {
            refcnt = ++m->refcnt;
            break;
          }
        }
        if ( m == NULL ) {
          pcre2_real_code_8       * re = NULL;
          pcre2_real_match_data_8 * md = NULL;
          size_t erroff;
          int    error;
          bool   pattern_success = false;
          refcnt = 1;
          /* if prefix matches, no need for pcre2 */
          if ( cvt.prefixlen + 1 == len && sub[ cvt.prefixlen ] == '>' )
            pattern_success = true;
          else {
            re = pcre2_compile( (uint8_t *) cvt.out, cvt.off, 0, &error,
                                &erroff, 0 );
            if ( re == NULL ) {
              fprintf( stderr, "re failed\n" );
            }
            else {
              md = pcre2_match_data_create_from_pattern( re, NULL );
              if ( md == NULL )
                fprintf( stderr, "md failed\n" );
              else
                pattern_success = true;
            }
          }
          if ( pattern_success &&
               (m = RvWildMatch::create( len, sub, re, md )) != NULL ) {
            rt->list.push_hd( m );
            if ( rt->count++ > 0 )
              coll = true;
            this->pat_tab.sub_count++;
            NotifyPattern npat( cvt, sub, len,
                                this->msg_in.reply, this->msg_in.replylen,
                                h, this->fd, coll, 'V', this->host );
            this->sub_route.add_pat( npat );
          }
          else {
            fprintf( stderr, "wildcard failed\n" );
            if ( rt->count == 0 )
              this->pat_tab.tab.remove( h, sub, len );
            if ( md != NULL )
              pcre2_match_data_free( md );
            if ( re != NULL )
              pcre2_code_free( re );
          }
        }
      }
    }
  }
  /* if first ref & no listen starts on restricted subjects _RV. _INBOX. */
  sub = this->msg_in.sub;
  len = this->msg_in.sublen;
  if ( refcnt != 0 && ! is_restricted_subject( sub, len ) ) {
    static const char start[] = "_RV.INFO.SYSTEM.LISTEN.START."; /* 16+13=29 */
    uint8_t * buf;
    char    * listen;
    size_t    listenlen = 29 + len, /* start strlen + subject len */
              size      = 256 + len + listenlen + 10; /* session size max 64 */
    char    * rep       = NULL;
    size_t    replen    = 0;

    size   = align<size_t>( size, 8 );
    this->msg_in.mem.alloc( size, &buf );
    size  -= align<size_t>( listenlen + 1, 8 );
    listen = (char *) &buf[ size ];
    RvMsgWriter msg( buf, size );

    msg.append_string( SARG( "ADV_CLASS" ), SARG( "INFO" ) );
    msg.append_string( SARG( "ADV_SOURCE" ), SARG( "SYSTEM" ) );
    /* skip prefix: _RV.INFO.SYSTEM. */
    ::memcpy( listen, start, 29 );
    ::memcpy( &listen[ 29 ], sub, len );
    listen[ listenlen ] = '\0';
    /* ADV_NAME is LISTEN.START.subject */
    msg.append_string( SARG( "ADV_NAME" ), &listen[ 16 ], 13 + len + 1 );
    msg.append_string( SARG( "id" ), this->session, this->session_len + 1 );
    msg.append_string( SARG( "sub" ), sub, len + 1 );
    msg.append_uint( SARG( "refcnt" ), refcnt );
    size = msg.update_hdr();

    this->msg_in.make_pre_subject( sub, len, listen, listenlen );
    if ( this->msg_in.replylen > 0 )
      this->msg_in.make_pre_subject( rep, replen, this->msg_in.reply,
                                     this->msg_in.replylen );
    h = kv_crc_c( sub, len, 0 );
    EvPublish pub( sub, len, rep, replen, buf, size, this->sub_route, this->fd,
                   h, RVMSG_TYPE_ID, 'p');
    this->sub_route.forward_msg( pub );
  }
}
/* when a 'C' message is received from the client, remove the subject and
 * notify with by publishing a LISTEN.STOP */
void
EvRvService::rem_sub( void ) noexcept
{
  char   * sub;
  size_t   len;
  uint32_t refcnt = 0xffffffffU, h;
  bool     coll   = false;

  this->msg_in.pre_subject( sub, len );
  if ( ! this->msg_in.is_wild ) {
    h = kv_crc_c( sub, len, 0 );
    if ( this->sub_tab.rem( h, sub, len, refcnt, coll ) == RV_SUB_OK ) {
      if ( refcnt == 0 ) {
        NotifySub nsub( sub, len, h, this->fd, coll, 'V', this->host );
        this->sub_route.del_sub( nsub );
      }
    }
  }
  else {
    PatternCvt       cvt;
    RouteLoc         loc;
    RvPatternRoute * rt;

    if ( cvt.convert_rv( sub, len ) == 0 ) {
      h = kv_crc_c( sub, cvt.prefixlen,
                    this->sub_route.prefix_seed( cvt.prefixlen ) );
      if ( this->pat_tab.find( h, sub, cvt.prefixlen, loc, rt,
                               coll ) == RV_SUB_OK ) {
        for ( RvWildMatch *m = rt->list.hd; m != NULL; m = m->next ) {
          if ( m->len == len && ::memcmp( m->value, sub, len ) == 0 ) {
            refcnt = --m->refcnt;
            if ( refcnt == 0 ) {
              if ( m->md != NULL ) {
                pcre2_match_data_free( m->md );
                m->md = NULL;
              }
              if ( m->re != NULL ) {
                pcre2_code_free( m->re );
                m->re = NULL;
              }
              rt->list.pop( m );
              if ( --rt->count > 0 )
                coll = true;
              delete m;
              this->pat_tab.sub_count--;
              if ( rt->count == 0 )
                this->pat_tab.tab.remove( loc );

              NotifyPattern npat( cvt, sub, len, h, this->fd, coll, 'V',
                                  this->host );
              this->sub_route.del_pat( npat );
            }
            break;
          }
        }
      }
    }
  }
  /* if found ref, no listen stops on restricted subjects _RV. _INBOX. */
  sub = this->msg_in.sub;
  len = this->msg_in.sublen;
  if ( refcnt != 0xffffffffU && ! is_restricted_subject( sub, len ) ) {
    static const char stop[] = "_RV.INFO.SYSTEM.LISTEN.STOP."; /* 16+12=28 */
    uint8_t * buf;
    char    * listen;
    size_t    listenlen = 28 + len, /* stop strlen + subject len */
              size      = 256 + len + listenlen + 10; /* session size max 64 */

    size   = align<size_t>( size, 8 );
    this->msg_in.mem.alloc( size, &buf );
    size  -= align<size_t>( listenlen + 1, 8 );
    listen = (char *) &buf[ size ];
    RvMsgWriter msg( buf, size );

    msg.append_string( SARG( "ADV_CLASS" ), SARG( "INFO" ) );
    msg.append_string( SARG( "ADV_SOURCE" ), SARG( "SYSTEM" ) );
    /* skip prefix: _RV.INFO.SYSTEM. */
    ::memcpy( listen, stop, 28 );
    ::memcpy( &listen[ 28 ], sub, len );
    listen[ listenlen ] = '\0';
    /* ADV_NAME is LISTEN.START.subject */
    msg.append_string( SARG( "ADV_NAME" ), &listen[ 16 ], 12 + len + 1 );
    msg.append_string( SARG( "id" ), this->session, this->session_len + 1 );
    msg.append_string( SARG( "sub" ), sub, len + 1 );
    msg.append_uint( SARG( "refcnt" ), refcnt );
    size = msg.update_hdr();

    this->msg_in.make_pre_subject( sub, len, listen, listenlen );
    h = kv_crc_c( sub, len, 0 );
    EvPublish pub( sub, len, NULL, 0, buf, size, this->sub_route, this->fd,
                   h, RVMSG_TYPE_ID, 'p' );
    this->sub_route.forward_msg( pub );
  }
}
/* when client disconnects, unsubscribe everything */
void
EvRvService::rem_all_sub( void ) noexcept
{
  RvSubRoutePos     pos;
  RvPatternRoutePos ppos;

  if ( this->sub_tab.first( pos ) ) {
    do {
      bool coll = this->sub_tab.rem_collision( pos.rt );
      NotifySub nsub( pos.rt->value, pos.rt->len, pos.rt->hash,
                      this->fd, coll, 'V', this->host );
      this->sub_route.del_sub( nsub );
    } while ( this->sub_tab.next( pos ) );
  }
  if ( this->pat_tab.first( ppos ) ) {
    do {
      for ( RvWildMatch *m = ppos.rt->list.hd; m != NULL; m = m->next ) {
        PatternCvt cvt;
        if ( cvt.convert_rv( m->value, m->len ) == 0 ) {
          bool coll = this->pat_tab.rem_collision( ppos.rt, m );
          NotifyPattern npat( cvt, m->value, m->len, ppos.rt->hash,
                              this->fd, coll, 'V', this->host );
          this->sub_route.del_pat( npat );
        }
      }
    } while ( this->pat_tab.next( ppos ) );
  }
}
/* when a 'D' message is received from client, forward the message data,
 * this also decapsulates the opaque data field and forwards the message
 * with the correct message type attribute:
 * old style { sub: FD.SEC.INST.EX, mtype: 'D', data: <message> }
 * new style { sub: FD.SEC.INST.EX, mtype: 'D', data: { _data_ : <message> } }*/
bool
EvRvService::fwd_pub( void ) noexcept
{
  void   * msg     = this->msg_in.data.fptr;
  size_t   msg_len = this->msg_in.data.fsize;
  uint32_t ftype   = this->msg_in.data.ftype;

/*  printf( "fwd %.*s\n", (int) sublen, sub );*/
  if ( ftype == MD_MESSAGE || ftype == RVMSG_TYPE_ID ) {
    ftype = RVMSG_TYPE_ID;
    MDMsg * m = RvMsg::opaque_extract( (uint8_t *) msg, 8, msg_len, NULL,
                                       &this->msg_in.mem );
    if ( m != NULL ) {
      ftype   = m->get_type_id();
      msg     = &((uint8_t *) m->msg_buf)[ m->msg_off ];
      msg_len = m->msg_end - m->msg_off;
    }
  }
  else if ( ftype == MD_OPAQUE ) {
    uint32_t ft = MDMsg::is_msg_type( msg, 0, msg_len, 0 );
    if ( ft != 0 )
      ftype = ft;
  }
  char   * sub,
         * rep    = NULL;
  size_t   sublen,
           replen = 0;
  uint32_t h;
  this->msg_in.pre_subject( sub, sublen );
  h = kv_crc_c( sub, sublen, 0 );
  if ( this->msg_in.replylen > 0 )
    this->msg_in.make_pre_subject( rep, replen, this->msg_in.reply,
                                   this->msg_in.replylen );
  EvPublish pub( sub, sublen, rep, replen, msg, msg_len,
                 this->sub_route, this->fd, h, ftype, 'p' );
  return this->sub_route.forward_msg( pub );
}
/* a message from the network, forward if matched by a subscription only once
 * as it may match multiple wild subscriptions as well as a normal sub
 * each sub is looked up in order to increment the msg count */
bool
EvRvService::on_msg( EvPublish &pub ) noexcept
{
  for ( uint8_t cnt = 0; cnt < pub.prefix_cnt; cnt++ ) {
    RvSubStatus ret;
    if ( pub.subj_hash == pub.hash[ cnt ] ) {
      ret = this->sub_tab.updcnt( pub.subj_hash, pub.subject, pub.subject_len );
      if ( ret == RV_SUB_OK )
        return this->fwd_msg( pub );
    }
    else {
      RvPatternRoute * rt;
      ret = this->pat_tab.find( pub.hash[ cnt ], pub.subject, pub.prefix[ cnt ],
                                rt );
      if ( ret == RV_SUB_OK ) {
        for ( RvWildMatch *m = rt->list.hd; m != NULL; m = m->next ) {
          if ( m->re == NULL ||
               pcre2_match( m->re, (const uint8_t *) pub.subject,
                            pub.subject_len, 0, 0, m->md, 0 ) == 1 ) {
            /* don't match _INBOX with > */
            if ( rt->len != 0 || m->re != NULL ||
                 ! is_inbox_subject( pub.subject, pub.subject_len ) ) {
              m->msg_cnt++;
              return this->fwd_msg( pub );
            }
          }
        }
      }
    }
  }
  return true;
}
/* convert a hash into a subject, this does not process collisions,
 * it is only informational */
bool
EvRvService::hash_to_sub( uint32_t h,  char *key,  size_t &keylen ) noexcept
{
  RouteLoc     loc;
  RvSubRoute * rt = this->sub_tab.tab.find_by_hash( h, loc );
  if ( rt == NULL )
    return false;
  keylen = rt->len;
  ::memcpy( key, rt->value, keylen );
  return true;
}
/* message from network, encapsulate the message into the client format:
 * { mtype: 'D', sub: <subject>, data: <msg-data> }
 */
bool
EvRvService::fwd_msg( EvPublish &pub ) noexcept
{
  const char * sub     = pub.subject,
             * reply   = (const char *) pub.reply;
  size_t       sublen  = pub.subject_len,
               replen  = pub.reply_len,
               preflen = this->msg_in.prefix_len;

  uint8_t buf[ 2 * 1024 ], * b = buf;
  size_t  buf_len = sizeof( buf );

  if ( sublen < preflen ) {
    fprintf( stderr, "sub %.*s is less than prefix (%u)\n", (int) sublen, sub,
             (int) preflen );
    return true;
  }
  sub = &sub[ preflen ];
  sublen -= preflen;
  if ( replen > preflen ) {
    reply   = &reply[ preflen ];
    replen -= preflen;
  }

  if ( sublen + pub.reply_len > 2 * 1024 - 512 ) {
    buf_len = sublen + pub.reply_len + 512;
    b = (uint8_t *) this->alloc_temp( buf_len );
    if ( b == NULL )
      return true;
  }

  RvMsgWriter  rvmsg( b, buf_len ),
               submsg( NULL, 0 );
  size_t       off, msg_len;
  const void * msg;
  int          status;

  status = rvmsg.append_subject( SARG( "sub" ), sub, sublen );
  /* some subjects may not encode */
  if ( status == 0 ) {
    const char * mtype = "D"; /* data */
    if ( sublen > 16 && sub[ 0 ] == '_' ) {
      if ( ::memcmp( "_RV.INFO.SYSTEM.", sub, 16 ) == 0 ) {
        /* HOST.START, SESSION.START, LISTEN.START, UNREACHABLE.TRANSPORT */
        if ( ::memcmp( "HOST.", &sub[ 16 ], 5 ) == 0 ||
             ::memcmp( "SESSION.", &sub[ 16 ], 8 ) == 0 ||
             ::memcmp( "LISTEN.", &sub[ 16 ], 7 ) == 0 ||
             ::memcmp( "UNREACHABLE.", &sub[ 16 ], 12 ) == 0 )
          mtype = "A"; /* advisory */
      }
      else if ( ::memcmp( "_RV.ERROR.SYSTEM.", sub, 17 ) == 0 )
        mtype = "A"; /* advisory */
    }
    status = rvmsg.append_string( SARG( "mtype" ), mtype, 2 );
  }
  if ( status == 0 && replen > 0 ) {
    status = rvmsg.append_string( SARG( "return" ), reply, replen + 1 );
    b[ rvmsg.off - 1 ] = '\0';
  }
  if ( status == 0 ) {
    uint32_t msg_enc = pub.msg_enc;
    static const char data_hdr[] = "\005data";
    /* depending on message type, encode the hdr to send to the client */
    switch ( msg_enc ) {
      case RVMSG_TYPE_ID:
      do_rvmsg:;
        rvmsg.append_msg( SARG( "data" ), submsg );
        off         = rvmsg.off + submsg.off;
        submsg.off += pub.msg_len - 8;
        msg         = &((const uint8_t *) pub.msg)[ 8 ];
        msg_len     = pub.msg_len - 8;
        rvmsg.update_hdr( submsg );
        break;

      case MD_OPAQUE:
      case MD_STRING:
        if ( RvMsg::is_rvmsg( (void *) pub.msg, 0, pub.msg_len, 0 ) )
          goto do_rvmsg;
        if ( msg_enc == MD_STRING ) {
          if ( TibMsg::is_tibmsg( (void *) pub.msg, 0, pub.msg_len, 0 ) ||
             TibSassMsg::is_tibsassmsg( (void *) pub.msg, 0, pub.msg_len, 0 ) )
            msg_enc = MD_OPAQUE;
        }
        /* FALLTHRU */
      case RAIMSG_TYPE_ID:
      case TIB_SASS_TYPE_ID:
      case TIB_SASS_FORM_TYPE_ID:
      do_tibmsg:;
        msg     = pub.msg;
        msg_len = pub.msg_len;

        ::memcpy( &buf[ rvmsg.off ], data_hdr, sizeof( data_hdr ) );
        rvmsg.off += sizeof( data_hdr );
        buf[ rvmsg.off++ ] = ( msg_enc == MD_STRING ) ? 8 : 7/*RV_OPAQUE*/;
        if ( msg_len < 120 )
          buf[ rvmsg.off++ ] = (uint8_t) msg_len;
        else if ( msg_len + 2 < 30000 ) {
          buf[ rvmsg.off++ ] = 121;
          buf[ rvmsg.off++ ] = ( ( msg_len + 2 ) >> 8 ) & 0xff;
          buf[ rvmsg.off++ ] = ( msg_len + 2 ) & 0xff;
        }
        else {
          buf[ rvmsg.off++ ] = 122;
          buf[ rvmsg.off++ ] = ( ( msg_len + 4 ) >> 24 ) & 0xff;
          buf[ rvmsg.off++ ] = ( ( msg_len + 4 ) >> 16 ) & 0xff;
          buf[ rvmsg.off++ ] = ( ( msg_len + 4 ) >> 8 ) & 0xff;
          buf[ rvmsg.off++ ] = ( msg_len + 4 ) & 0xff;
        }
        off = rvmsg.off;
        rvmsg.off += msg_len;
        rvmsg.update_hdr();
        break;

      case MD_MESSAGE:
        if ( RvMsg::is_rvmsg( (void *) pub.msg, 0, pub.msg_len, 0 ) )
          goto do_rvmsg;
        if ( TibMsg::is_tibmsg( (void *) pub.msg, 0, pub.msg_len, 0 ) )
          goto do_tibmsg;
        if ( TibSassMsg::is_tibsassmsg( (void *) pub.msg, 0, pub.msg_len, 0 ) )
          goto do_tibmsg;
        /* FALLTHRU */
      default:
        if ( pub.msg_len == 0 ) {
          off = rvmsg.off;
          rvmsg.update_hdr();
        }
        else {
          off = 0;
        }
        msg     = NULL;
        msg_len = 0;
        break;
    }
    if ( off > 0 ) {
      char *m = this->append2( buf, off, msg, msg_len );
      if ( is_rv_debug ) {
        this->print( m, off + msg_len );
      }
      this->host->bs += off + msg_len;
      this->msgs_sent++;
      this->host->ms++;
      /*this->send( buf, off, msg, msg_len );*/
      return this->idle_push_write();
    }
    else {
      fprintf( stderr, "rv unknown msg_enc %u subject: %.*s %u\n",
               msg_enc, (int) sublen, sub, (uint32_t) off );
    }
  }
  return true;
}

void
RvPatternMap::release( void ) noexcept
{
  RvPatternRoutePos ppos;

  if ( this->first( ppos ) ) {
    do {
      RvWildMatch *next;
      for ( RvWildMatch *m = ppos.rt->list.hd; m != NULL; m = next ) {
        next = m->next;
        if ( m->md != NULL ) {
          pcre2_match_data_free( m->md );
          m->md = NULL;
        }
        if ( m->re != NULL ) {
          pcre2_code_free( m->re );
          m->re = NULL;
        }
        delete m;
      }
    } while ( this->next( ppos ) );
  }
  this->tab.release();
  this->sub_count = 0;
}

void
EvRvService::process_shutdown( void ) noexcept
{
  this->EvConnection::process_shutdown();
}

void
EvRvService::process_close( void ) noexcept
{
  if ( this->host_started ) {
    this->send_stop();
    this->host_started = false;
    if ( --this->host->active_clients == 0 )
      this->host->stop_network();
  }
  this->client_stats( this->sub_route.peer_stats );
  this->EvSocket::process_close();
}

void
EvRvService::release( void ) noexcept
{
  this->rem_all_sub();
  this->sub_tab.release();
  this->pat_tab.release();
  this->msg_in.release();
  this->EvConnection::release_buffers();
}

bool
RvMsgIn::subject_to_string( const uint8_t *buf,  size_t buflen ) noexcept
{
  uint8_t segs;
  size_t  i, j, k;

  this->is_wild = false;
  if ( buflen == 0 || buf[ 0 ] == 0 )
    goto bad_subject;

  j    = 1;
  segs = buf[ 0 ];
  k    = segs - 1;
  for (;;) {
    if ( j >= buflen || buf[ j ] < 2 ) {
      if ( segs != 0 || j != buflen )
        goto bad_subject;
      break;
    }
    i  = j + 1;
    j += buf[ j ];
    k += j - ( i + 1 );
    segs -= 1;
  }
  if ( k > 0xffffU )
    goto bad_subject;
  if ( k + 1 > sizeof( this->sub_buf ) ) {
    this->mem.alloc( k + 1 + this->prefix_len, &this->sub );
    if ( this->prefix_len > 0 ) {
      uint16_t n = sizeof( this->prefix ) - this->prefix_len;
      ::memcpy( this->sub, &this->prefix[ n ], this->prefix_len );
      this->sub = &this->sub[ this->prefix_len ];
    }
  }
  else {
    this->sub = this->sub_buf;
  }
  segs = buf[ 0 ];
  j = 1;
  k = 0;
  for (;;) {
    i  = j + 1;
    j += buf[ j ]; /* segment size is j - 1 - i */
    /* if segment size is 1 */
    if ( i + 2 == j ) {
      if ( buf[ i ] == '*' ||
           ( buf[ i ] == '>' && segs == 1 ) )
        this->is_wild = true;
    }
    /* copy segment */
    while ( i + 1 < j )
      this->sub[ k++ ] = (char) buf[ i++ ];
    /* terminate with . or \0 */
    if ( --segs > 0 )
      this->sub[ k++ ] = '.';
    else
      break;
  } 
  this->sub[ k ] = '\0';
  this->sublen = (uint16_t) k;
  return true;

bad_subject:;
  this->sub      = this->sub_buf;
  this->sub[ 0 ] = 0;
  this->sublen   = 0;
  return false;
}

void
EvRvService::print( void *m,  size_t len ) noexcept
{
  MDOutput mout;
  MDMsgMem mem;
  MDMsg *msg = MDMsg::unpack( m, 0, len, 0, NULL, &mem );
  printf( "---->\n" );
  if ( msg != NULL )
    msg->print( &mout, 1, "%12s : ", NULL );
  else
    mout.print_hex( m, len );
  printf( "---->\n" );
}


void
RvMsgIn::print( int status,  void *m,  size_t len ) noexcept
{
  MDOutput mout;
  printf( "<----\n" );
  if ( status != 0 ) {
    if ( len == 8 )
      printf( "ping\n" );
    else
      printf( "status %d\n", status );
  }
#if 0
  printf( "msg_in(%s)", this->sub );
  if ( this->reply != NULL )
    printf( " reply: %s\n", this->reply );
  else
    printf( "\n" );
#endif
  if ( this->msg != NULL )
    this->msg->print( &mout, 1, "%12s : ", NULL );
  else
    mout.print_hex( m, len );
  printf( "<----\n" );
}

int
RvMsgIn::unpack( void *msgbuf,  size_t msglen ) noexcept
{
  enum { HAS_SUB = 1, HAS_MTYPE = 2, HAS_RETURN = 4, HAS_DATA = 8 };
  MDFieldIter * it;
  MDName        nm;
  MDReference   mref;
  int           cnt = 0,
                status = 0;

  this->mem.reuse();
  this->msg = RvMsg::unpack_rv( msgbuf, 0, msglen, 0, NULL, &this->mem );
  if ( this->msg == NULL || this->msg->get_field_iter( it ) != 0 )
    status = ERR_RV_MSG;
  else {
    this->iter = (RvFieldIter *) it;
    if ( this->iter->first() == 0 ) {
      do {
        if ( this->iter->get_name( nm ) != 0 ||
             this->iter->get_reference( mref ) != 0 )
          return ERR_RV_REF;
        switch ( nm.fnamelen ) {
          case 4:
            if ( ::memcmp( nm.fname, SARG( "sub" ) ) == 0 ) {
              if ( this->subject_to_string( mref.fptr, mref.fsize ) )
                cnt |= HAS_SUB;
              else
                return ERR_RV_SUB;
            }
            break;
          case 5:
            if ( ::memcmp( nm.fname, SARG( "data" ) ) == 0 ) {
              this->data = mref;
              cnt |= HAS_DATA;
            }
            break;
          case 6:
            if ( ::memcmp( nm.fname, SARG( "mtype" ) ) == 0 ) {
              if ( mref.ftype == MD_STRING && mref.fsize == 2 ) {
                this->mtype = mref.fptr[ 0 ];
                if ( this->mtype >= 'A' && this->mtype <= 'R' ) {
                  #define B( c ) ( 1U << ( c - 'A' ) )
                  static const uint32_t valid =
                        B( 'A' ) /* advisorty */
                      | B( 'C' ) /* cancel */
                      | B( 'D' ) /* data */
                      | B( 'I' ) /* initialize */
                      | B( 'L' ) /* listen */
                      | B( 'R' );/* response */
                  if ( ( B( this->mtype ) & valid ) != 0 )
                    cnt |= HAS_MTYPE;
                  #undef B
                }
              }
            }
            break;
          case 7:
            if ( ::memcmp( nm.fname, SARG( "return" ) ) == 0 ) {
              this->reply    = (char *) mref.fptr;
              this->replylen = (uint16_t) mref.fsize;
              if ( this->replylen > 0 /*&& this->reply[ this->replylen - 1 ] == '\0'*/ )
                this->replylen--;
              cnt |= HAS_RETURN;
            }
            break;
          default:
            break;
        }
      } while ( cnt < 15 && this->iter->next() == 0 );
    }
    if ( ( cnt & HAS_MTYPE ) == 0 )
      status = ERR_RV_MTYPE; /* no mtype */
  }
  if ( ( cnt & HAS_SUB ) == 0 ) {
    this->sub      = this->sub_buf;
    this->sub[ 0 ] = '\0';
    this->sublen = 0; /* no subject */
  }
  if ( ( cnt & HAS_MTYPE ) == 0 )
    this->mtype = 0;
  if ( ( cnt & HAS_RETURN ) == 0 ) {
    this->reply    = NULL;
    this->replylen = 0;
  }
  if ( ( cnt & HAS_DATA ) == 0 )
    this->data.zero();
  if ( is_rv_debug )
    this->print( status, msgbuf, msglen );
  return status;
}

