#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
#include <arpa/inet.h>
#else
#include <raikv/win.h>
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
#include <raimd/rwf_msg.h>
#include <raimd/mf_msg.h>

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

EvRvListen::EvRvListen( EvPoll &p,  RvHostDB &d,  bool has_svc_pre ) noexcept
  : EvTcpListen( p, "rv_listen", "rv_sock" ), /*RvHost( *this ),*/
    sub_route( p.sub_route ), db( d ),
    ipport( 0 ), has_service_prefix( has_svc_pre )
{
  md_init_auto_unpack();
}

EvRvListen::EvRvListen( EvPoll &p,  RoutePublish &sr,  RvHostDB &d,
                        bool has_svc_pre ) noexcept
  : EvTcpListen( p, "rv_listen", "rv_sock" ), /*RvHost( *this ),*/
    sub_route( sr ), db( d ),
    ipport( 0 ), has_service_prefix( has_svc_pre )
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

int
EvRvListen::start_network( RvHost *&h, const RvHostNet &hn ) noexcept
{
  int status = this->db.start_service( h, this->poll, this->sub_route, hn );
  if ( status == 0 )
    status = this->start_host( *h, hn );
  if ( status != 0 ) {
    if ( status < 0 )
      return ERR_START_HOST_FAILED;
    return (RvHostError) status;
  }
  return HOST_OK;
}

void
EvRvService::read( void ) noexcept
{
  /* if host not started, wait for that event before reading pub/sub data */
  if ( ! this->bp_in_list() ) {
    if ( this->host_started || this->svc_state <= DATA_RECV ) {
      this->EvConnection::read();
      return;
    }
  }
  this->pop3( EV_READ, EV_READ_HI, EV_READ_LO );
}

int
EvRvListen::start_host2( RvHost &h,  uint32_t delay_secs ) noexcept
{
  return h.start_host2( delay_secs );
}

int
EvRvListen::start_host( RvHost &h, const RvHostNet &hn ) noexcept
{
  int status = HOST_OK;
  if ( ! h.network_started ) {
    if ( ! h.start_in_progress ) {
      status = h.check_network( hn );
      if ( status == HOST_OK ) {
        h.start_in_progress = true;
        status = h.start_host();
      }
      return status;
    }
  }
  if ( h.mcast.host_ip == 0 || ! h.is_same_network( hn ) )
    return ERR_SAME_SVC_TWO_NETS;
  return HOST_OK;
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
  void   * msgbuf;
  uint32_t buflen, msglen;
  int      status = -1;

  /* state trasition from VERS_RECV -> INFO_RECV -> DATA_RECV */
  if ( this->svc_state >= DATA_RECV ) { /* main state */
  data_recv_loop:;
    buflen = this->len - this->off;
    if ( buflen > this->recv_highwater )
      this->svc_state |= FWD_BUFFERSIZE;
    else
      this->svc_state &= ~FWD_BUFFERSIZE;
    for (;;) {
      if ( buflen < 8 )
        goto break_loop;
      msgbuf = &this->recv[ this->off ];
      msglen = get_u32<MD_BIG>( msgbuf );
      if ( buflen < msglen ) {
        uint32_t magic = get_u32<MD_BIG>( &this->recv[ off + 4 ] );
        if ( magic != 0x9955eeaaU ) {
          status = md::Err::BAD_MAGIC_NUMBER;
          this->print_rv_msg_err( msgbuf, msglen, status );
          break;
        }
        this->recv_need( msglen );
        goto break_loop;
      }
      status = this->dispatch_msg( msgbuf, msglen );

      if ( status != 0 )
        break;
      this->off += msglen;
      this->msgs_recv++;
      this->host->stat.br += msglen;
      this->host->stat.mr++;
      buflen = this->len - this->off;
    }
    if ( status != ERR_BACKPRESSURE )
      goto break_loop;
    this->pop( EV_PROCESS );
    this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
    if ( ! this->push_write_high() )
      this->clear_write_buffers();
    return;
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
  if ( ! this->push_write() )
    this->clear_write_buffers();

  if ( status > 0 )
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
  int status;
  status = this->msg_in.unpack( msgbuf, msglen );
  if ( is_rv_debug )
    this->print_in( status, msgbuf, msglen );
  if ( status != 0 ) {
    if ( msglen == 8 ) /* empty msg */
      return 0;
    if ( msglen != 0 )
      this->print_rv_msg_err( msgbuf, msglen, status );
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
    int flow = this->fwd_pub( msgbuf, msglen );
    if ( flow == RV_FLOW_GOOD )
      this->svc_state &= ~FWD_BACKPRESSURE;
    else
      this->svc_state |= FWD_BACKPRESSURE;
    if ( flow == RV_FLOW_STALLED ) /* no progress yet, hold msg */
      return ERR_BACKPRESSURE;
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

void
EvRvService::print_rv_msg_err( void *msgbuf,  size_t msglen,
                               int status ) noexcept
{
  static const char *rv_status_string[] = RV_STATUS_STRINGS;
  MDOutput mout;
  fprintf( stderr, "rv status %d: \"%s\" msglen=%u\n", status,
           rv_status_string[ status ], (uint32_t) msglen );
  if ( msglen > 0 )
    mout.print_hex( msgbuf, msglen > 256 ? 256 : msglen );
}

/* when a 'D' message is received from client, forward the message data,
 * this also decapsulates the opaque data field and forwards the message
 * with the correct message type attribute:
 * old style { sub: FD.SEC.INST.EX, mtype: 'D', data: <message> }
 * new style { sub: FD.SEC.INST.EX, mtype: 'D', data: { _data_ : <message> } }*/
int
EvRvService::fwd_pub( void *rvbuf,  size_t buflen ) noexcept
{
  void   * msg     = this->msg_in.data.fptr;
  size_t   msg_len = this->msg_in.data.fsize;
  uint32_t ftype   = this->msg_in.data.ftype;
  char     reply_buf[ 256 ];

/*  printf( "fwd %.*s\n", (int) sublen, sub );*/
  if ( ftype == MD_MESSAGE || ftype == RVMSG_TYPE_ID ) {
    ftype = RVMSG_TYPE_ID;
    MDMsg * m = RvMsg::opaque_extract( (uint8_t *) msg, 8, msg_len, NULL,
                                       this->msg_in.mem );
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
  if ( this->msg_in.replylen > 0 ) {
    size_t prelen = this->msg_in.prefix_len;
    char * buf    = reply_buf;
    rep    = this->msg_in.reply;
    replen = this->msg_in.replylen;
    if ( prelen > 0 ) {
      if ( prelen + replen >= sizeof( reply_buf ) )
        buf = this->msg_in.mem.str_make( prelen + replen + 1 );
      replen = this->msg_in.cat_pre_subject( buf, rep, replen );
      rep    = buf;
    }
  }
  EvPublish pub( sub, sublen, rep, replen, msg, msg_len,
                 this->sub_route, *this, h, ftype );
  if ( this->msg_in.suffix_len != 0 ) {
    uint32_t suf_len = this->msg_in.suffix_len;
    if ( &((uint8_t *) msg)[ msg_len + (size_t) suf_len ] ==
         &((uint8_t *) rvbuf)[ buflen ] ) {
      pub.msg_len += suf_len;
      pub.suf_len  = suf_len;
    }
  }
  BPData * data = NULL;
  if ( ( this->svc_state & ( FWD_BACKPRESSURE | FWD_BUFFERSIZE ) ) != 0 )
    data = this;
  if ( this->sub_route.forward_msg( pub, data ) )
    return RV_FLOW_GOOD;
  if ( ! this->bp_in_list() )
    return RV_FLOW_BACKPRESSURE;
  return RV_FLOW_STALLED;
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
  uint8_t       buf[ 2 * 1024 ];
  MDMsgMem      mem;
  RvMsgWriter   rvmsg( mem, buf, sizeof( buf ) ),
                submsg( mem, NULL, 0 );
  size_t        size;
  uint32_t      net_len = 0, svc_len = 0;
  int           status = HOST_OK;

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

  if ( svc_len == 0 ) {
    ::strcpy( svc, "7500" );
    svc_len = 4;
  }
  RvHostNet hn( svc, svc_len, net, net_len,
                this->listener.ipport, this->listener.has_service_prefix );
  /* check that the network is valid and start it */
  status = this->listener.start_network( this->host, hn );
  if ( this->host->network_started ) {
    if ( ! this->host_started ) {
      this->host_started = true;
      this->host->active_clients++;
    }
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
    submsg.append_ipdata( SARG( "ipaddr" ),
      this->host->mcast.fake_ip == 0 ? this->host->mcast.host_ip :
                                       this->host->mcast.fake_ip );
    submsg.append_ipdata( SARG( "ipport" ), this->host->service_port );
    submsg.append_int<int32_t>( SARG( "vmaj" ), 5 );
    submsg.append_int<int32_t>( SARG( "vmin" ), 4 );
    submsg.append_int<int32_t>( SARG( "vupd" ), 2 );

    submsg.append_string( SARG( "gob" ), this->gob, this->gob_len + 1 );
    size = rvmsg.update_hdr( submsg );
  }
  else {
    if ( this->notify != NULL )
      this->notify->on_connect( *this );
    /* { sub: _RV.INFO.SYSTEM.RVD.CONNECTED, mtype: D,
     *   data: { ADV_CLASS: INFO, ADV_SOURCE: SYSTEM, ADV_NAME: RVD.CONNECTED } } */
    this->sent_rvdconn = true;
    rvmsg.append_subject( SARG( "sub" ), _RV_INFO_SYSTEM ".RVD.CONNECTED" );
    rvmsg.append_string( SARG( "mtype" ), SARG( "D" ) );
    rvmsg.append_msg( SARG( "data" ), submsg );
    submsg.append_string( SARG( _ADV_CLASS ), SARG( _INFO ) );
    submsg.append_string( SARG( _ADV_SOURCE ), SARG( _SYSTEM ) );
    submsg.append_string( SARG( _ADV_NAME ), SARG( "RVD.CONNECTED" ) );
    size = rvmsg.update_hdr( submsg );
  }
  /* send the result */
  char *m = this->append( rvmsg.buf, size );
  if ( is_rv_debug )
    this->print_out( m, size );

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
    this->host->send_host_start( this );
  }
  if ( ( this->svc_state & IS_RV_SESSION ) != 0 ) {
    if ( ( this->svc_state & SENT_SESSION_START ) == 0 )
      this->host->send_session_start( *this );
  }
  if ( this->host->has_service_prefix )
    this->msg_in.set_prefix( this->host->service, this->host->service_len );
}

void
EvRvService::send_stop( void ) noexcept
{
  this->rem_all_sub();
  if ( ( this->svc_state & IS_RV_SESSION ) != 0 ) {
    if ( ( this->svc_state & SENT_SESSION_START ) != 0 &&
         ( this->svc_state & SENT_SESSION_STOP ) == 0 )
      this->host->send_session_stop( *this );
  }
  else if ( ( this->svc_state & IS_RV_DAEMON ) != 0 ) {
    if ( ( this->svc_state & SENT_SESSION_STOP ) == 0 )
      this->host->send_unreachable_tport( *this );
  }
}

bool
EvRvService::timer_expire( uint64_t tid,  uint64_t ) noexcept
{
  if ( tid == this->timer_id ) {
    this->svc_state &= ~TIMER_ACTIVE;
    this->push( EV_PROCESS );
    this->idle_push( EV_READ_LO );
  }
  return false;
}

void
EvRvService::on_write_ready( void ) noexcept
{
  this->push( EV_PROCESS );
  this->pop2( EV_READ, EV_READ_HI );
  this->idle_push( EV_READ_LO );
}

uint8_t
EvRvService::is_subscribed( const NotifySub &sub ) noexcept
{
  uint8_t v    = 0;
  bool    coll = false;
  if ( ! sub.is_notify_queue() &&
       this->sub_tab.find( sub.subj_hash, sub.subject, sub.subject_len,
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
  if ( ! pat.is_notify_queue() &&
       this->pat_tab.find( pat.prefix_hash, pat.pattern, cvt.prefixlen,
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
  char      * sub;
  size_t      len;
  uint32_t    refcnt = 0, h, sub_h = 0;
  RvSubStatus status;
  bool        coll   = false;

  this->msg_in.pre_subject( sub, len );
  if ( ! this->msg_in.is_wild ) {
    h = kv_crc_c( sub, len, 0 );
    sub_h = h;
    status = this->sub_tab.put( h, sub, len, refcnt, coll );
    NotifySub nsub( sub, len, this->msg_in.reply, this->msg_in.replylen,
                    h, coll, 'V', *this );
    if ( status == RV_SUB_OK ) {
      this->sub_route.add_sub( nsub );
    }
    else if ( status == RV_SUB_EXISTS ) {
      nsub.sub_count = refcnt;
      this->sub_route.notify_sub( nsub );
    }
  }
  else {
    RvPatternRoute * rt;
    PatternCvt cvt;

    if ( cvt.convert_rv( sub, len ) == 0 ) {
      h = kv_crc_c( sub, cvt.prefixlen,
                    this->sub_route.prefix_seed( cvt.prefixlen ) );
      if ( this->pat_tab.put( h, sub, cvt.prefixlen, rt,
                              coll ) != RV_SUB_NOT_FOUND ) {
        RvWildMatch * m;
        for ( m = rt->list.hd; m != NULL; m = m->next ) {
          if ( m->len == len && ::memcmp( sub, m->value, len ) == 0 ) {
            refcnt = ++m->refcnt;
            break;
          }
        }
        NotifyPattern npat( cvt, sub, len,
                            this->msg_in.reply, this->msg_in.replylen,
                            h, coll, 'V', *this );
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
              npat.hash_collision = true;
            this->pat_tab.sub_count++;
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
        else {
          npat.sub_count = refcnt;
          this->sub_route.notify_pat( npat );
        }
      }
    }
  }
  if ( refcnt != 0 && ( this->svc_state & IS_RV_DAEMON ) != 0 )
    refcnt = this->host->add_ref( sub, len, sub_h );
  /* if first ref & no listen starts on restricted subjects _RV. _INBOX. */
  if ( refcnt != 0 ) {
    sub = this->msg_in.sub;
    len = this->msg_in.sublen;
    this->host->send_listen_start( *this, sub, len, this->msg_in.reply,
                                   this->msg_in.replylen, refcnt );
  }
}
/* when a 'C' message is received from the client, remove the subject and
 * notify with by publishing a LISTEN.STOP */
void
EvRvService::rem_sub( void ) noexcept
{
  char   * sub;
  size_t   len;
  uint32_t refcnt = 0xffffffffU, h, sub_h = 0;
  bool     coll   = false;

  this->msg_in.pre_subject( sub, len );
  if ( ! this->msg_in.is_wild ) {
    h = kv_crc_c( sub, len, 0 );
    sub_h = h;
    if ( this->sub_tab.rem( h, sub, len, refcnt, coll ) == RV_SUB_OK ) {
      NotifySub nsub( sub, len, h, coll, 'V', *this );
      if ( refcnt == 0 ) {
        this->sub_route.del_sub( nsub );
      }
      else {
        nsub.sub_count = refcnt;
        this->sub_route.notify_unsub( nsub );
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
            NotifyPattern npat( cvt, sub, len, h, coll, 'V', *this );
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
                npat.hash_collision = true;
              delete m;
              this->pat_tab.sub_count--;
              if ( rt->count == 0 )
                this->pat_tab.tab.remove( loc );
              this->sub_route.del_pat( npat );
            }
            else {
              npat.sub_count = refcnt;
              this->sub_route.notify_unpat( npat );
            }
            break;
          }
        }
      }
    }
  }
  if ( refcnt != 0xffffffffU  && ( this->svc_state & IS_RV_DAEMON ) != 0 )
    refcnt = this->host->rem_ref( sub, len, sub_h, 1 );
  /* if found ref, no listen stops on restricted subjects _RV. _INBOX. */
  if ( refcnt == 0 ) {
    sub = this->msg_in.sub;
    len = this->msg_in.sublen;
    this->host->send_listen_stop( *this, sub, len, refcnt );
  }
}
/* when client disconnects, unsubscribe everything */
void
EvRvService::rem_all_sub( void ) noexcept
{
  RvSubRoutePos     pos;
  RvPatternRoutePos ppos;
  size_t            prelen = this->msg_in.prefix_len;
  bool              do_listen_stop = ! this->poll.quit && this->host != NULL;

  if ( do_listen_stop ) {
    if ( this->sub_tab.first( pos ) ) {
      do {
        if ( pos.rt->len > prelen ) {
          uint32_t     refcnt = pos.rt->refcnt;
          const char * sub;
          size_t       len;
          if ( ( this->svc_state & IS_RV_DAEMON ) != 0 ) {
            sub = pos.rt->value;
            len = pos.rt->len;
            refcnt = this->host->rem_ref( sub, len, pos.rt->hash, refcnt );
          }
          else
            refcnt = 0;
          if ( refcnt == 0 ) {
            sub = &pos.rt->value[ prelen ];
            len = pos.rt->len - prelen;
            this->host->send_listen_stop( *this, sub, len, 0 );
          }
        }
      } while ( this->sub_tab.next( pos ) );
    }
    if ( this->pat_tab.first( ppos ) ) {
      do {
        for ( RvWildMatch *m = ppos.rt->list.hd; m != NULL; m = m->next ) {
          if ( m->len > prelen ) {
            uint32_t     refcnt = m->refcnt;
            const char * sub;
            size_t       len;
            if ( ( this->svc_state & IS_RV_DAEMON ) != 0 ) {
              sub = m->value;
              len = m->len;
              refcnt = this->host->rem_ref( sub, len, 0, refcnt );
            }
            else
              refcnt = 0;
            if ( refcnt == 0 ) {
              sub = &m->value[ prelen ];
              len = m->len - prelen;
              this->host->send_listen_stop( *this, sub, len, 0 );
            }
          }
        }
      } while ( this->pat_tab.next( ppos ) );
    }
  }
  if ( this->sub_tab.first( pos ) ) {
    do {
      bool coll = this->sub_tab.rem_collision( pos.rt );
      NotifySub nsub( pos.rt->value, pos.rt->len, pos.rt->hash,
                      coll, 'V', *this );
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
                              coll, 'V', *this );
          this->sub_route.del_pat( npat );
        }
      }
    } while ( this->pat_tab.next( ppos ) );
  }
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
  if ( pub.subj_hash == this->host->dataloss_inbound_hash &&
       pub.subject_len == this->host->dataloss_inbound_len &&
       ::memcmp( pub.subject, this->host->dataloss_inbound_sub,
                 pub.subject_len ) == 0 )
    return this->fwd_msg( pub );
  if ( pub.subj_hash == this->host->dataloss_outbound_hash &&
       pub.subject_len == this->host->dataloss_outbound_len &&
       ::memcmp( pub.subject, this->host->dataloss_outbound_sub,
                 pub.subject_len ) == 0 )
    return this->fwd_msg( pub );
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
  const char * sub    = pub.subject,
             * reply  = (const char *) pub.reply;
  size_t       sublen = pub.subject_len,
               replen = pub.reply_len,
               prelen = this->msg_in.prefix_len;

  if ( sublen < prelen ) {
    fprintf( stderr, "sub %.*s is less than prefix (%u)\n", (int) sublen, sub,
             (int) prelen );
    return true;
  }
  sub = &sub[ prelen ];
  sublen -= prelen;
  if ( replen > prelen ) {
    reply   = &reply[ prelen ];
    replen -= prelen;
  }

  if ( pub.pub_status != EV_PUB_NORMAL ) {
    /* start and cycle are normal events */
    if ( pub.pub_status <= EV_MAX_LOSS || pub.pub_status == EV_PUB_RESTART ) {
      if ( this->notify != NULL )
        this->notify->on_data_loss( *this, pub );
      else
        this->host->inbound_data_loss( *this, pub, NULL );
    }
  }

  size_t buf_len = 1024;
  if ( sublen + pub.reply_len > 1024 - 512 )
    buf_len = sublen + pub.reply_len + 512;

  MDMsgMem    mem;
  RvMsgWriter rvmsg( mem, mem.make( buf_len ), buf_len );

  rvmsg.append_subject( SARG( "sub" ), sub, sublen );
  /* some subjects may not encode */
  if ( rvmsg.err == 0 ) {
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
    rvmsg.append_string( SARG( "mtype" ), mtype, 2 );
  }
  if ( rvmsg.err == 0 && replen > 0 ) {
    rvmsg.append_string( SARG( "return" ), reply, replen + 1 );
    rvmsg.buf[ rvmsg.off - 1 ] = '\0';
  }
  if ( rvmsg.err == 0 ) {
    RvMsgWriter submsg( rvmsg.mem, NULL, 0 );
    uint32_t msg_enc = pub.msg_enc,
             suf_len = pub.suf_len;
    size_t   msg_off = 0,
             msg_len = pub.msg_len - suf_len;
    void   * msg     = (void *) pub.msg;
    /* depending on message type, encode the hdr to send to the client */
    switch ( msg_enc ) {
      case RVMSG_TYPE_ID:
      do_rvmsg:;
        rvmsg.append_msg( SARG( "data" ), submsg );
        msg_off     = rvmsg.off + submsg.off;
        submsg.off += msg_len - 8;
        msg         = &((uint8_t *) msg)[ 8 ];
        msg_len     = msg_len - 8;
        rvmsg.update_hdr( submsg, suf_len );
        msg_len    += suf_len;
        break;

      case MD_OPAQUE:
      case MD_STRING:
        if ( RvMsg::is_rvmsg( msg, 0, msg_len, 0 ) )
          goto do_rvmsg;
        if ( msg_enc == MD_STRING ) {
          if ( TibMsg::is_tibmsg( msg, 0, msg_len, 0 ) ||
               TibSassMsg::is_tibsassmsg( msg, 0, msg_len, 0 ) )
            msg_enc = MD_OPAQUE;
          else {
            if ( MDMsg::is_msg_type( msg, 0, msg_len, 0 ) == JSON_TYPE_ID ) {
      case JSON_TYPE_ID:
              if ( EvRvService::convert_json( this->spc, msg, msg_len ) ) {
                suf_len = 0;
                goto do_rvmsg;
              }
            }
          }
        }
        /* FALLTHRU */
      case RAIMSG_TYPE_ID:
      case TIB_SASS_TYPE_ID:
      case TIB_SASS_FORM_TYPE_ID:
      case MARKETFEED_TYPE_ID:
      do_tibmsg:;
        if ( suf_len == 0 ) {
          if ( rvmsg.has_space( 20 ) ) {
            rvmsg.off += append_rv_field_hdr( &rvmsg.buf[ rvmsg.off ],
                                            SARG( "data" ), msg_len, msg_enc );
            msg_off    = rvmsg.off;
            rvmsg.off += msg_len;
            rvmsg.update_hdr();
          }
          break;
        }
        rvmsg.append_msg( SARG( "data" ), submsg );
        msg_off = rvmsg.off + submsg.off;
        if ( rvmsg.has_space( 20 ) ) {
          msg_off     = append_rv_field_hdr( &rvmsg.buf[ msg_off ],
                                           SARG( "_data_" ), msg_len, msg_enc );
          submsg.off += msg_off;
          msg_off     = rvmsg.off + submsg.off;
          submsg.off += msg_len;
          rvmsg.update_hdr( submsg, suf_len );
          msg_len    += suf_len;
        }
        break;

      case RWF_MSG_TYPE_ID:
        rvmsg.append_msg( SARG( "data" ), submsg );
        msg_off = rvmsg.off + submsg.off;
        if ( rvmsg.has_space( 20 ) ) {
          msg_off     = append_rv_field_hdr( &rvmsg.buf[ msg_off ],
                                             SARG( "_RWFMSG" ), msg_len,
                                             msg_enc );
          submsg.off += msg_off;
          msg_off     = rvmsg.off + submsg.off;
          submsg.off += msg_len;
          rvmsg.update_hdr( submsg );
        }
        break;

      case MD_MESSAGE:
        if ( RvMsg::is_rvmsg( msg, 0, msg_len, 0 ) )
          goto do_rvmsg;
        /* FALLTHRU */
      default:
        if ( msg_len == 0 ) {
          msg_off = rvmsg.off;
          rvmsg.update_hdr();
        }
        else {
          if ( MDMsg::is_msg_type( msg, 0, msg_len, 0 ) != 0 )
            goto do_tibmsg;
          msg_off = 0;
        }
        msg     = NULL;
        msg_len = 0;
        break;
    }
    if ( rvmsg.err == 0 && msg_off > 0 ) {
      uint32_t idx = 0;
      if ( msg_len > this->recv_highwater ) {
        idx = this->poll.zero_copy_ref( pub.src_route.fd, msg, msg_len );
        if ( idx != 0 )
          this->append_ref_iov( rvmsg.buf, msg_off, msg, msg_len, idx );
      }
      if ( idx == 0 ) {
        char *m = this->append2( rvmsg.buf, msg_off, msg, msg_len );
        if ( is_rv_debug )
          this->print_out( m, msg_off + msg_len );
      }
      this->host->stat.bs += msg_off + msg_len;
      this->msgs_sent++;
      this->host->stat.ms++;
      /*this->send( buf, off, msg, msg_len );*/
      return this->idle_push_write();
    }
    else {
      if ( rvmsg.err != 0 ) {
        fprintf( stderr, "rv msg error %d subject: %.*s %u\n",
                 rvmsg.err, (int) sublen, sub, (uint32_t) off );
      }
      else {
        fprintf( stderr, "rv unknown msg_enc %u subject: %.*s %u\n",
                 msg_enc, (int) sublen, sub, (uint32_t) msg_off );
      }
    }
  }
  return true;
}

bool
EvRvService::convert_json( MDMsgMem &spc,  void *&msg,
                           size_t &msg_len ) noexcept
{
  MDMsgMem   tmp_mem;
  JsonMsgCtx ctx;

  if ( ctx.parse( msg, 0, msg_len, NULL, tmp_mem, false ) != 0 )
    return false;
  spc.reuse();
  size_t max_len = ( ( msg_len | 15 ) + 1 ) * 16;
  RvMsgWriter rvmsg( spc, spc.reuse_make( max_len ), max_len );

  if ( rvmsg.convert_msg( *ctx.msg, false ) != 0 )
    return false;
  rvmsg.update_hdr();
  msg     = rvmsg.buf;
  msg_len = rvmsg.off;
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
    if ( this->host->stop_network() )
      this->listener.stop_host( *this->host );
  }
  this->client_stats( this->sub_route.peer_stats );
  this->EvSocket::process_close();
}

void
EvRvService::release( void ) noexcept
{
  if ( ( this->svc_state & TIMER_ACTIVE ) != 0 )
    this->poll.timer.remove_timer( this->fd, this->timer_id, 0 );
  if ( this->bp_in_list() )
    this->bp_retire( *this );
  this->sub_tab.release();
  this->pat_tab.release();
  this->msg_in.release();
  if ( this->notify != NULL )
    this->notify->on_shutdown( *this, NULL, 0 );
  this->EvConnection::release_buffers();
  this->spc.reuse();
  this->timer_id = 0;
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

struct RestrictOut : public MDOutput {
  RestrictOut( int hints = 0 ) : MDOutput( hints ) {}
  virtual int puts( const char *s ) noexcept {
    size_t len = ::strlen( s );
    if ( len > 80 )
      return this->printf( "%.76s...", s );
    return this->MDOutput::puts( s );
  }
};

void
EvRvService::print_out( void *m,  size_t len ) noexcept
{
  EvRvService::print( this->fd, m, len );
}

void
EvRvService::print( int fd,  void *m,  size_t len ) noexcept
{
  RestrictOut mout;
  MDMsgMem mem;
  MDMsg *msg = MDMsg::unpack( m, 0, len, 0, NULL, mem );
  mout.printf( "----> (%d)\n", fd );
  if ( msg != NULL )
    msg->print( &mout, 1, "%12s : ", NULL );
  else
    mout.print_hex( m, len );
  mout.printf( "---->\n" );
}


void
EvRvService::print_in( int status,  void *m,  size_t len ) noexcept
{
  RestrictOut mout;
  mout.printf( "<---- (%d)\n", this->fd );
  if ( status != 0 ) {
    if ( len == 8 )
      mout.printf( "ping\n" );
    else
      mout.printf( "status %d\n", status );
  }
#if 0
  printf( "msg_in(%s)", this->sub );
  if ( this->reply != NULL )
    printf( " reply: %s\n", this->reply );
  else
    printf( "\n" );
#endif
  if ( this->msg_in.msg != NULL )
    this->msg_in.msg->print( &mout, 1, "%12s : ", NULL );
  else
    mout.print_hex( m, len );
  mout.printf( "<----\n" );
}

int
RvMsgIn::unpack( void *msgbuf,  size_t msglen ) noexcept
{
  enum { HAS_SUB = 1, HAS_MTYPE = 2, HAS_RETURN = 4, HAS_DATA = 8 };
  MDFieldIter * it;
  MDName        nm;
  MDReference   mref;
  uint32_t      suf_len  = 0,
                pre_off  = 0,
                pre_len  = 0,
                data_off = 0,
                data_end = 0;
  int           cnt      = 0,
                status   = 0;

  this->mem.reuse();
  this->msg = RvMsg::unpack_rv( msgbuf, 0, msglen, 0, NULL, this->mem );
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
              goto matched_field;
            }
            break;
          case 5:
            if ( ::memcmp( nm.fname, SARG( "data" ) ) == 0 ) {
              data_off = this->iter->field_start;
              data_end = this->iter->field_end;
              this->data = mref;
              cnt |= HAS_DATA;
              goto matched_field;
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
              goto matched_field;
            }
            break;
          case 7:
            if ( ::memcmp( nm.fname, SARG( "return" ) ) == 0 ) {
              this->reply    = (char *) mref.fptr;
              this->replylen = (uint16_t) mref.fsize;
              if ( this->replylen > 0 /*&& this->reply[ this->replylen - 1 ] == '\0'*/ )
                this->replylen--;
              cnt |= HAS_RETURN;
              goto matched_field;
            }
            break;
          default:
            break;
        }
        if ( (cnt & HAS_DATA ) != 0 ) {
          suf_len += this->iter->field_end - this->iter->field_start;
        }
        else {
          if ( pre_off == 0 )
            pre_off = this->iter->field_start;
          pre_len += this->iter->field_end - this->iter->field_start;
        }
      matched_field:;
      } while ( this->iter->next() == 0 );
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
  this->suffix_len = 0;
  if ( ( cnt & HAS_DATA ) == 0 )
    this->data.zero();
  else if ( ( suf_len | pre_len ) != 0 ) {
    uint8_t * b = (uint8_t *) msgbuf;
    if ( pre_off + pre_len == data_off ) {
      uint32_t data_len = data_end - data_off;
      void   * tmp = this->mem.make( pre_len );
      ::memcpy( tmp, &b[ pre_off ], pre_len );
      ::memmove( &b[ pre_off ], &b[ data_off ], data_len );
      ::memcpy( &b[ pre_off + data_len ], tmp, pre_len );
      this->data.fptr -= pre_len;
      suf_len += pre_len;
    }
    if ( suf_len != 0 ) {
      if ( &this->data.fptr[ this->data.fsize + suf_len ] == &b[ msglen ] )
        this->suffix_len = suf_len;
    }
  }
  return status;
}

bool
EvRvService::get_service( void *host,  uint16_t &svc ) const noexcept
{
  if ( host != NULL )
    *(void **) host = (void *) &this->host;
  if ( this->host != NULL ) {
    svc = this->host->service_num;
    return true;
  }
  svc = 0;
  return false;
}

bool
EvRvService::set_session( const char session[ MAX_SESSION_LEN ] ) noexcept
{
  (void) session;
  return false;
}

/* get session name */
size_t
EvRvService::get_userid( char userid[ MAX_USERID_LEN ] ) const noexcept
{
  ::memcpy( userid, this->userid, this->userid_len );
  userid[ this->userid_len ] = '\0';
  return this->userid_len;
}
/* get session name */
size_t
EvRvService::get_session( uint16_t svc,
                          char session[ MAX_SESSION_LEN ] ) const noexcept
{
  if ( this->host != NULL && this->host->service_num == svc ) {
    ::memcpy( session, this->session, this->session_len );
    session[ this->session_len ] = '\0';
    return this->session_len;
  }
  session[ 0 ] = '\0';
  return 0;
}
/* get session name */
size_t
EvRvService::get_subscriptions( uint16_t svc,  SubRouteDB &subs ) noexcept
{
  RvSubRoutePos pos;
  RouteLoc      loc;
  size_t        cnt    = 0,
                prelen = this->msg_in.prefix_len;

  if ( this->host == NULL || this->host->service_num != svc )
    return 0;
  if ( this->sub_tab.first( pos ) ) {
    do {
      if ( pos.rt->len > prelen ) {
        const char * val = &pos.rt->value[ prelen ];
        size_t       len = pos.rt->len - prelen;
        /* rv host will filter these */
        /*if ( ! is_restricted_subject( val, len ) )*/ {
          uint32_t h = kv_crc_c( val, len, 0 );
          subs.upsert( h, val, len, loc );
          if ( loc.is_new )
            cnt++;
        }
      }
    } while ( this->sub_tab.next( pos ) );
  }
  return cnt;
}

size_t
EvRvService::get_patterns( uint16_t svc,  int pat_fmt,
                           SubRouteDB &pats ) noexcept
{
  RvPatternRoutePos ppos;
  RouteLoc          loc;
  size_t            cnt    = 0,
                    prelen = this->msg_in.prefix_len;

  if ( this->host == NULL || this->host->service_num != svc )
    return 0;
  if ( pat_fmt != RV_PATTERN_FMT )
    return 0;
  if ( this->pat_tab.first( ppos ) ) {
    do {
      for ( RvWildMatch *m = ppos.rt->list.hd; m != NULL; m = m->next ) {
        if ( m->len > prelen ) {
          const char * val = &m->value[ prelen ];
          size_t       len = m->len - prelen;
          /*if ( ! is_restricted_subject( val, len ) )*/ {
            uint32_t h = kv_crc_c( val, len, 0 );
            pats.upsert( h, val, len, loc );
            if ( loc.is_new )
              cnt++;
          }
        }
      }
    } while ( this->pat_tab.next( ppos ) );
  }
  return cnt;
}

