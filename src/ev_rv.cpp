#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <arpa/inet.h>
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
  : EvTcpListen( p, "rv_listen", "rv_sock" ), RvHost( *this )
{
  md_init_auto_unpack();
}

int
EvRvListen::listen( const char *ip,  int port,  int opts )
{
  int status;
  status = this->kv::EvTcpListen::listen( ip, port, opts, "rv_listen" );
  if ( status == 0 ) {
    this->ipport = htons( port ); /* network order */
#if 0
    this->poll.add_timer_seconds( this->fd, 1, this->timer_id, 0 );
    this->idle_push( EV_PROCESS );
#endif
  }
  return status;
}

bool
EvRvListen::accept( void ) noexcept
{
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof( addr );
  int sock = ::accept( this->fd, (struct sockaddr *) &addr, &addrlen );
  if ( sock < 0 ) {
    if ( errno != EINTR ) {
      if ( errno != EAGAIN )
        perror( "accept" );
      this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
    }
    return false;
  }
  EvRvService *c =
    this->poll.get_free_list2<EvRvService, RvHost>(
      this->accept_sock_type, *this );
  if ( c == NULL ) {
    perror( "accept: no memory" );
    ::close( sock );
    return false;
  }
  EvTcpListen::set_sock_opts( this->poll, sock, this->sock_opts );
  ::fcntl( sock, F_SETFL, O_NONBLOCK | ::fcntl( sock, F_GETFL ) );

  c->PeerData::init_peer( sock, (struct sockaddr *) &addr, "rv" );
  c->initialize_state( ++this->timer_id );
  if ( this->poll.add_sock( c ) < 0 ) {
    fprintf( stderr, "failed to add sock %d\n", sock );
    ::close( sock );
    this->poll.push_free_list( c );
    return false;
  }
  this->active_clients++;
  uint32_t ver_rec[ 3 ] = { 0, 4, 0 };
  ver_rec[ 1 ] = get_u32<MD_BIG>( &ver_rec[ 1 ] ); /* flip */
  c->append( ver_rec, sizeof( ver_rec ) );
  c->idle_push( EV_WRITE_HI );
  if ( this->host_status_timer != 0 )
    c->host_started = true;
  /*this->poll.add_timer_seconds( c->fd, 5, c->timer_id, 0 );*/
  return true;
}

bool
EvRvListen::timer_expire( uint64_t, uint64_t eid ) noexcept
{
  /* stop timer when host stops */
  if ( eid != (uint64_t) this->host_status_timer )
    return false;
  this->host_status(); /* still going */
  return true;
}

void
EvRvListen::host_status( void ) noexcept
{
  static const char status[] = "_RV.INFO.SYSTEM.HOST.STATUS.";
  uint8_t     buf[ 8192 ];
  char        subj[ 64 ];
  size_t      sublen,
              size;
  RvMsgWriter msg( buf, sizeof( buf ) );

  sublen = this->pack_advisory( msg, status, subj, ADV_HOST_STATUS, NULL );
  size   = msg.update_hdr();
  EvPublish pub( subj, sublen, NULL, 0, buf, size, this->fd,
                 kv_crc_c( subj, sublen, 0 ), NULL, 0,
                 (uint8_t) RVMSG_TYPE_ID, 'p' );
  this->poll.forward_msg( pub );
}

void
EvRvService::read( void ) noexcept
{
  /* if host not started, wait for that event before reading pub/sub data */
  if ( this->host_started || this->state <= DATA_RECV ) {
    this->EvConnection::read();
    return;
  }
  this->pop3( EV_READ, EV_READ_HI, EV_READ_LO );
}

int
EvRvListen::start_host( void ) noexcept
{
  /* subscribe _INBOX.DAEMON.iphex */
  this->subscribe_daemon_inbox();
  /* start timer to send the status every 90 seconds */
  this->host_status_timer = ++this->host_network_start;
  this->poll.add_timer_seconds( this->fd, RV_STATUS_IVAL, 0,
                                this->host_status_timer );
  PeerMatchArgs ka;
  PeerMatchIter iter( *this, ka );
  ka.type     = "rv";
  ka.type_len = 2;
  bool do_h = true;
  for ( PeerData *p = iter.first(); p != NULL; p = iter.next() ) {
    EvRvService *svc = (EvRvService *) p;
    svc->host_started = true;
    /* advise host + session start, then restart reading from clients */
    this->send_start( do_h, svc->state == EvRvService::DATA_RECV_SESSION, svc );
    do_h = false; /* only start host once */
    svc->idle_push( EV_READ );
  }
  return 0;
}

int
EvRvListen::stop_host( void ) noexcept
{
  /* unsubscribe _INBOX.DAEMON.iphex */
  this->unsubscribe_daemon_inbox();
  /* advise host stop */
  this->send_stop( true, false, NULL );
  /* stop the timer, the timer_expire() function tests this */
  this->host_status_timer = 0;
  return 0;
}

void
EvRvListen::subscribe_daemon_inbox( void ) noexcept
{
  char daemon_inbox[ 24 ];
  uint32_t h, rcnt;
  ::memcpy( daemon_inbox, "_INBOX.", 7 );
  ::memcpy( &daemon_inbox[ 7 ], this->session_ip, 8 );
  ::memcpy( &daemon_inbox[ 15 ], ".DAEMON", 8 );
  const size_t len = 22;
  h = kv_crc_c( daemon_inbox, len, 0 );
  if ( ! this->poll.sub_route.is_sub_member( h, this->fd ) ) {
    rcnt = this->poll.sub_route.add_sub_route( h, this->fd );
    this->poll.notify_sub( h, daemon_inbox, len, this->fd, rcnt, 'V', NULL, 0 );
  }
}

void
EvRvListen::unsubscribe_daemon_inbox( void ) noexcept
{
  char daemon_inbox[ 24 ];
  uint32_t h, rcnt;
  ::memcpy( daemon_inbox, "_INBOX.", 7 );
  ::memcpy( &daemon_inbox[ 7 ], this->session_ip, 8 );
  ::memcpy( &daemon_inbox[ 15 ], ".DAEMON", 8 );
  const size_t len = 22;
  h = kv_crc_c( daemon_inbox, len, 0 );
  if ( this->poll.sub_route.is_sub_member( h, this->fd ) ) {
    rcnt = this->poll.sub_route.del_sub_route( h, this->fd );
    this->poll.notify_unsub( h, daemon_inbox, len, this->fd, rcnt, 'V' );
  }
}

void
EvRvListen::process( void ) noexcept
{
  this->pop( EV_PROCESS );
}

void
EvRvListen::process_close( void ) noexcept
{
  this->unsubscribe_daemon_inbox();
}

static bool
match_string( const char *s,  size_t len,  const MDReference &mref )
{
  return len == mref.fsize && mref.ftype == MD_STRING &&
         ::memcmp( s, mref.fptr, len ) == 0;
}

void
EvRvListen::send_sessions( const void *reply,  size_t reply_len ) noexcept
{
  PeerMatchArgs ka;
  PeerMatchIter iter( *this, ka );
  PeerData    * p;
  MDMsgMem      mem;
  size_t        buflen = 8; /* header of rvmsg */
  uint8_t     * buf = NULL;
  bool          have_daemon;

  ka.type     = "rv";
  ka.type_len = 2;
  have_daemon = false; /* include daemon onlly once */
  for ( p = iter.first(); p != NULL; p = iter.next() ) {
    EvRvService * svc = (EvRvService *) p;
    if ( svc->state == EvRvService::DATA_RECV_DAEMON && have_daemon )
      continue;
    buflen += svc->session_len + 8; /* field + type + session str */
    if ( svc->state == EvRvService::DATA_RECV_DAEMON )
      have_daemon = true;
  }
  mem.alloc( buflen, &buf );
  RvMsgWriter msg( buf, buflen );
  have_daemon = false;
  for ( p = iter.first(); p != NULL; p = iter.next() ) {
    EvRvService * svc = (EvRvService *) p;
    if ( svc->state == EvRvService::DATA_RECV_DAEMON && have_daemon )
      continue;
    msg.append_string( NULL, 0, svc->session, svc->session_len + 1 );
    if ( svc->state == EvRvService::DATA_RECV_DAEMON )
      have_daemon = true;
  }
  buflen = msg.update_hdr();
  EvPublish pub( (const char *) reply, reply_len, NULL, 0, buf, buflen,
                 this->fd, kv_crc_c( reply, reply_len, 0 ), NULL, 0,
                 (uint8_t) RVMSG_TYPE_ID, 'p' );
  this->poll.forward_msg( pub );
}

void
EvRvListen::send_subscriptions( const char *session,  size_t session_len,
                                const void *reply,  size_t reply_len ) noexcept
{
  DLinkList<RvServiceLink> list;
  PeerMatchArgs     ka;
  PeerMatchIter     iter( *this, ka );
  RvSubRoutePos     pos;
  RvPatternRoutePos ppos;
  MDMsgMem          mem;
  uint8_t         * buf = NULL;
  size_t            buflen,
                    subcnt;
  ka.type     = "rv";
  ka.type_len = 2;
  subcnt      = 0;
  for ( PeerData *p = iter.first(); p != NULL; p = iter.next() ) {
    EvRvService *svc = (EvRvService *) p;
    if ( (size_t) svc->session_len + 1 == session_len &&
         ::memcmp( session, svc->session, svc->session_len ) == 0 ) {
      void * p;
      mem.alloc( sizeof( RvServiceLink ), &p );
      RvServiceLink *link = new ( p ) RvServiceLink( svc );
      size_t n = svc->sub_tab.sub_count() + svc->pat_tab.sub_count;
      if ( n > subcnt ) {
        list.push_hd( link );
        subcnt = n;
      }
      else {
        list.push_tl( link );
      }
      if ( svc->state != EvRvService::DATA_RECV_DAEMON )
        break;
    }
  }
  if ( list.hd != NULL ) {
    /* hdr + "user" : "name", "end" : 1 */
    buflen = 8 + list.hd->svc.userid_len + 12 + 12;
    for ( RvServiceLink *link = list.hd; link != NULL; link = link->next ) {
      EvRvService & s = link->svc;
      ::memset( link->bits, 0, sizeof( link->bits ) );
      if ( s.sub_tab.first( pos ) ) {
        do {
          if ( link->check_subject( pos.rt->value, pos.rt->len, pos.rt->hash ) )
            buflen += pos.rt->len + 8 + pos.rt->segments() * 2;
        } while ( s.sub_tab.next( pos ) );
      }
      if ( s.pat_tab.first( ppos ) ) {
        do {
          for ( RvWildMatch *m = ppos.rt->list.hd; m != NULL; m = m->next ) {
            if ( link->check_pattern( ppos.rt->value, ppos.rt->len,
                                      ppos.rt->hash, m ) )
              buflen += m->len + 8 + m->segments() * 2;
          }
        } while ( s.pat_tab.next( ppos ) );
      }
    }
  }
  else {
    buflen = 8 + 8 + 12 + 12;
  }
  mem.alloc( buflen, &buf );
  RvMsgWriter msg( buf, buflen );

  if ( list.hd != NULL ) {
    msg.append_string( SARG( "user" ), list.hd->svc.userid,
                                       list.hd->svc.userid_len + 1 );
    for ( RvServiceLink *link = list.hd; link != NULL; link = link->next ) {
      EvRvService & s = link->svc;
      ::memset( link->bits, 0, sizeof( link->bits ) );
      if ( s.sub_tab.first( pos ) ) {
        do {
          if ( link->check_subject( pos.rt->value, pos.rt->len, pos.rt->hash ) )
            msg.append_subject( NULL, 0, pos.rt->value, pos.rt->len );
        } while ( s.sub_tab.next( pos ) );
      }
      if ( s.pat_tab.first( ppos ) ) {
        do {
          for ( RvWildMatch *m = ppos.rt->list.hd; m != NULL; m = m->next ) {
            if ( link->check_pattern( ppos.rt->value, ppos.rt->len,
                                      ppos.rt->hash, m ) )
              msg.append_subject( NULL, 0, m->value, m->len );
          }
        } while ( s.pat_tab.next( ppos ) );
      }
    }
  }
  else {
    msg.append_string( SARG( "user" ), SARG( "nobody" ) );
  }
  msg.append_int<int32_t>( SARG( "end" ), 1 );
  buflen = msg.update_hdr();
  EvPublish pub( (const char *) reply, reply_len, NULL, 0, buf, buflen,
                 this->fd, kv_crc_c( reply, reply_len, 0 ), NULL, 0,
                 (uint8_t) RVMSG_TYPE_ID, 'p' );
  this->poll.forward_msg( pub );
}

bool
EvRvListen::on_msg( EvPublish &pub ) noexcept
{
  if ( pub.reply_len == 0 /*|| pub.msg_enc != (uint8_t) RVMSG_TYPE_ID*/ )
    return true;
  MDMsgMem     mem;
  /*MDOutput     mout;*/
  RvMsg       * m;
  MDFieldIter * it;
  MDReference   mref;

  m = RvMsg::unpack_rv( (void *) pub.msg, 0, pub.msg_len, 0, NULL, &mem );
  if ( m != NULL ) {
    /*m->print( &mout );*/
    /* "op":"get", "what":"sessions"
     * "op":"get", "what":"subscriptions", "session":"0A040416.5FCC7B705B5" */
    if ( m->get_field_iter( it ) == 0 ) {
      if ( it->find( SARG( "op" ), mref ) == 0 &&
           match_string( SARG( "get" ), mref ) ) {
        if ( it->find( SARG( "what" ), mref ) == 0 ) {
          if ( match_string( SARG( "sessions" ), mref ) ) {
            this->send_sessions( pub.reply, pub.reply_len );
          }
          else if ( match_string( SARG( "subscriptions" ), mref ) ) {
            if ( it->find( SARG( "session" ), mref ) == 0 ) {
              if ( mref.ftype == MD_STRING ) {
                this->send_subscriptions( (const char *) mref.fptr,
                                         mref.fsize, pub.reply, pub.reply_len );
              }
            }
          }
        }
      }
    }
  }
  return true;
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
  size_t  buflen, msglen;
  int32_t status = 0;

  /* state trasition from VERS_RECV -> INFO_RECV -> DATA_RECV */
  if ( this->state >= DATA_RECV ) { /* main state */
  data_recv_loop:;
    do {
      buflen = this->len - this->off;
      if ( buflen < 8 )
        goto break_loop;
      msglen = get_u32<MD_BIG>( &this->recv[ this->off ] );
      if ( buflen < msglen )
        goto break_loop;
      status = this->dispatch_msg( &this->recv[ this->off ], msglen );
      this->off     += msglen;
      this->stat.br += msglen;
      this->stat.mr++;
    } while ( status == 0 );
  }
  else { /* initial connection states */
    for (;;) {
      buflen = this->len - this->off;
      if ( buflen < 8 )
        goto break_loop;
      if ( this->state == VERS_RECV ) {
        if ( buflen < 3 * 4 )
          goto break_loop;
        this->off += 3 * 4;
        this->send_info( false );
        this->state = INFO_RECV;
      }
      else { /* INFO_RECV */
        if ( buflen < 16 * 4 )
          goto break_loop;
        this->off += 16 * 4;
        this->send_info( true );
        this->state = DATA_RECV;
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
      fprintf( stderr, "rv status %d: \"%s\"\n", status,
               rv_status_string[ status ] );
      mout.print_hex( msgbuf, msglen );
    }
    return status;
  }
  /*this->msg_in.print();*/
  if ( this->msg_in.mtype == 'D' ) { /* publish */
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
      return this->respond_info();
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
  char          net[ RvHost::MAX_NETWORK_LEN ],
                svc[ RvHost::MAX_SERVICE_LEN ];
  uint8_t       buf[ 8 * 1024 ];
  RvMsgWriter   rvmsg( buf, sizeof( buf ) ),
                submsg( NULL, 0 );
  size_t        size;
  uint32_t      net_len = 0, svc_len = 0;
  bool          has_session = false;
  RvHostError   status = HOST_OK;

  if ( this->gob_len == 0 )
    this->gob_len = RvHost::time_to_str( this->active_ns, this->gob );
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
            this->userid_len = mref.fsize - 1;
          break;
        case 8:
          if ( match_str( nm, mref, "session", this->session,
                          sizeof( this->session ) ) ) {
            has_session = true;
            this->session_len = mref.fsize - 1;
          }
          else if ( match_str( nm, mref, "control", this->control,
                               sizeof( this->control ) ) )
            this->control_len = mref.fsize - 1;
          else if ( match_str( nm, mref, "service", svc , sizeof( svc ) ) )
            svc_len = mref.fsize - 1;
          else if ( match_str( nm, mref, "network", net, sizeof( net ) ) )
            net_len = mref.fsize - 1;
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
  if ( status == HOST_OK )
    status = this->stat.start_network( mc, net, net_len, svc, svc_len );
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
  else if ( ! has_session ) {
    /* { sub: RVD.INITRESP, mtype: R,
     *   data: { ipaddr: a.b.c.d, ipport: 7500, vmaj: 5, vmin: 4, vupd: 2 } } */
    rvmsg.append_subject( SARG( "sub" ), "RVD.INITRESP" );
    rvmsg.append_string( SARG( "mtype" ), SARG( "R" ) );
    rvmsg.append_msg( SARG( "data" ), submsg );
    submsg.append_ipdata( SARG( "ipaddr" ), this->stat.mcast.host_ip );
    submsg.append_ipdata( SARG( "ipport" ), this->stat.service_port );
    submsg.append_int<int32_t>( SARG( "vmaj" ), 5 );
    submsg.append_int<int32_t>( SARG( "vmin" ), 4 );
    submsg.append_int<int32_t>( SARG( "vupd" ), 2 );

    submsg.append_string( SARG( "gob" ), this->gob, this->gob_len + 1 );
    size = rvmsg.update_hdr( submsg );
  }
  else {
    /* { sub: _RV.INFO.SYSTEM.RVD.CONNECTED, mtype: D,
     *   data: { ADV_CLASS: INFO, ADV_SOURCE: SYSTEM, ADV_NAME: RVD.CONNECTED } } */
    rvmsg.append_subject( SARG( "sub" ), "_RV.INFO.SYSTEM.RVD.CONNECTED" );
    rvmsg.append_string( SARG( "mtype" ), SARG( "D" ) );
    rvmsg.append_msg( SARG( "data" ), submsg );
    submsg.append_string( SARG( "ADV_CLASS" ), SARG( "INFO" ) );
    submsg.append_string( SARG( "ADV_SOURCE" ), SARG( "SYSTEM" ) );
    submsg.append_string( SARG( "ADV_NAME" ), SARG( "RVD.CONNECTED" ) );
    size = rvmsg.update_hdr( submsg );
  }
  /* send the result */
  this->append( buf, size );

  if ( status == HOST_OK ) {
    if ( has_session ) {
      if ( this->session_len > 9 &&
          ::strcmp( this->gob, &this->session[ 9 ] ) == 0 ) {
        ::memcpy( this->session, this->stat.daemon_id,
                  this->stat.daemon_len + 1 );
        this->session_len = this->stat.daemon_len;
        this->state = DATA_RECV_DAEMON;   /* use the iphex.DAEMON.session */
      }
      else
        this->state = DATA_RECV_SESSION; /* use the session as specified */
    }
    else {
      this->state = DATA_RECV; /* continue, need another info message */
    }
  }
  /* if host started already, send session start if rv5 */
  if ( this->host_started && this->state == DATA_RECV_SESSION )
    this->stat.send_start( false, true, this );
  return 0;
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
/* when a 'L' message is received from the client, remember the subject and
 * notify with by publishing a LISTEN.START */
void
EvRvService::add_sub( void ) noexcept
{
  const char   * sub = this->msg_in.sub;
  const uint32_t len = this->msg_in.sublen;
  uint32_t refcnt = 0;

  if ( ! this->msg_in.is_wild ) {
    uint32_t h = kv_crc_c( sub, len, 0 ),
             rcnt;
    if ( this->sub_tab.put( h, sub, len, refcnt ) == RV_SUB_OK ) {
      rcnt = this->poll.sub_route.add_sub_route( h, this->fd );
      this->poll.notify_sub( h, sub, len, this->fd, rcnt, 'V',
                             this->msg_in.reply, this->msg_in.replylen );
    }
  }
  else {
    RvPatternRoute * rt;
    PatternCvt cvt;
    uint32_t   h, rcnt;

    if ( cvt.convert_rv( sub, len ) == 0 ) {
      h = kv_crc_c( sub, cvt.prefixlen,
                    this->poll.sub_route.prefix_seed( cvt.prefixlen ) );
      if ( this->pat_tab.put( h, sub, cvt.prefixlen, rt ) != RV_SUB_NOT_FOUND ){
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
            rcnt = this->poll.sub_route.add_pattern_route( h, this->fd,
                                                           cvt.prefixlen );
            this->poll.notify_psub( h, cvt.out, cvt.off, sub, cvt.prefixlen,
                                    this->fd, rt->count + rcnt, 'V' );
            rt->count++;
            this->pat_tab.sub_count++;
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
  if ( refcnt != 0 && ! is_restricted_subject( sub, len ) ) {
    static const char start[] = "_RV.INFO.SYSTEM.LISTEN.START."; /* 16+13=29 */
    uint8_t * buf;
    char    * listen;
    size_t    listenlen = 29 + len, /* start strlen + subject len */
              size      = 256 + len + listenlen + 10; /* session size max 64 */

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

    EvPublish pub( listen, listenlen, this->msg_in.reply, this->msg_in.replylen,
                   buf, size, this->fd,
                   kv_crc_c( listen, listenlen, 0 ), NULL, 0,
                   (uint8_t) RVMSG_TYPE_ID, 'p' );
    this->poll.forward_msg( pub );
  }
}
/* when a 'C' message is received from the client, remove the subject and
 * notify with by publishing a LISTEN.STOP */
void
EvRvService::rem_sub( void ) noexcept
{
  const char   * sub = this->msg_in.sub;
  const uint32_t len = this->msg_in.sublen;
  uint32_t refcnt = 0xffffffffU;

  if ( ! this->msg_in.is_wild ) {
    uint32_t h = kv_crc_c( sub, len, 0 ),
             rcnt;
    if ( this->sub_tab.rem( h, sub, len, refcnt ) == RV_SUB_OK ) {
      if ( refcnt == 0 ) {
        if ( this->sub_tab.tab.find_by_hash( h ) == NULL )
          rcnt = this->poll.sub_route.del_sub_route( h, this->fd );
        else
          rcnt = this->poll.sub_route.get_sub_route_count( h );
        this->poll.notify_unsub( h, sub, len, this->fd, rcnt, 'V' );
      }
    }
  }
  else {
    PatternCvt       cvt;
    RouteLoc         loc;
    RvPatternRoute * rt;
    uint32_t         h, rcnt;

    if ( cvt.convert_rv( sub, len ) == 0 ) {
      h = kv_crc_c( sub, cvt.prefixlen,
                    this->poll.sub_route.prefix_seed( cvt.prefixlen ) );
      if ( (rt = this->pat_tab.tab.find( h, sub, cvt.prefixlen,
                                         loc )) != NULL ) {
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
              rt->count -= 1;
              rcnt = this->poll.sub_route.del_pattern_route( h, this->fd,
                                                             cvt.prefixlen );
              this->poll.notify_punsub( h, cvt.out, cvt.off, sub, cvt.prefixlen,
                                        this->fd, rcnt + rt->count, 'V' );
              delete m;
              this->pat_tab.sub_count--;
              if ( rt->count == 0 )
                this->pat_tab.tab.remove( loc );
            }
            break;
          }
        }
      }
    }
  }
  /* if last ref & no listen stops on restricted subjects _RV. _INBOX. */
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

    EvPublish pub( listen, listenlen, NULL, 0, buf, size, this->fd,
                   kv_crc_c( listen, listenlen, 0 ), NULL, 0,
                   (uint8_t) RVMSG_TYPE_ID, 'p' );
    this->poll.forward_msg( pub );
  }
}
/* when client disconnects, unsubscribe everything */
void
EvRvService::rem_all_sub( void ) noexcept
{
  RvSubRoutePos     pos;
  RvPatternRoutePos ppos;
  uint32_t          rcnt;

  if ( this->sub_tab.first( pos ) ) {
    do {
      rcnt = this->poll.sub_route.del_sub_route( pos.rt->hash, this->fd );
      this->poll.notify_unsub( pos.rt->hash, pos.rt->value, pos.rt->len,
                               this->fd, rcnt, 'V' );
    } while ( this->sub_tab.next( pos ) );
  }
  if ( this->pat_tab.first( ppos ) ) {
    do {
      rcnt = this->poll.sub_route.del_pattern_route( ppos.rt->hash, this->fd,
                                                     ppos.rt->len );
      rcnt += ppos.rt->count;
      for ( RvWildMatch *m = ppos.rt->list.hd; m != NULL; m = m->next ) {
        PatternCvt cvt;
        if ( cvt.convert_rv( m->value, m->len ) == 0 ) {
          this->poll.notify_punsub( ppos.rt->hash, cvt.out, cvt.off,
                                    m->value, cvt.prefixlen,
                                    this->fd, --rcnt, 'V' );
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
  char   * sub     = this->msg_in.sub,
         * rep     = this->msg_in.reply;
  size_t   sublen  = this->msg_in.sublen,
           replen  = this->msg_in.replylen;
  uint32_t h       = kv_crc_c( sub, sublen, 0 );
  void   * msg     = this->msg_in.data.fptr;
  uint32_t msg_len = this->msg_in.data.fsize;
  uint8_t  ftype   = this->msg_in.data.ftype;

/*  printf( "fwd %.*s\n", (int) sublen, sub );*/
  if ( ftype == MD_MESSAGE || ftype == (uint8_t) RVMSG_TYPE_ID ) {
    ftype = (uint8_t) RVMSG_TYPE_ID;
    MDMsg * m = RvMsg::opaque_extract( (uint8_t *) msg, 8, msg_len, NULL,
                                       &this->msg_in.mem );
    if ( m != NULL ) {
      ftype   = (uint8_t) m->get_type_id();
      msg     = &((uint8_t *) m->msg_buf)[ m->msg_off ];
      msg_len = m->msg_end - m->msg_off;
    }
  }
  else if ( ftype == MD_OPAQUE ) {
    MDMsg * m = MDMsg::unpack( msg, 0, msg_len, 0, NULL, &this->msg_in.mem );
    if ( m != NULL )
      ftype = (uint8_t) m->get_type_id();
  }
  EvPublish pub( sub, sublen, rep, replen, msg, msg_len,
                 this->fd, h, NULL, 0, ftype, 'p' );
  return this->poll.forward_msg( pub );
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
          bool matches = false;
          if ( m->re == NULL ) {
            /* > trails */
            if ( ::memcmp( m->value, pub.subject, m->len - 1 ) == 0 )
              matches = true;
          }
          else if ( pcre2_match( m->re, (const uint8_t *) pub.subject,
                                 pub.subject_len, 0, 0, m->md, 0 ) == 1 )
            matches = true;
          if ( matches ) {
            m->msg_cnt++;
            return this->fwd_msg( pub );
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
  uint8_t buf[ 2 * 1024 ], * b = buf;
  size_t  buf_len = sizeof( buf );

  if ( pub.subject_len + pub.reply_len > 2 * 1024 - 512 ) {
    buf_len = pub.subject_len + pub.reply_len + 512;
    b = (uint8_t *) this->alloc_temp( buf_len );
    if ( b == NULL )
      return true;
  }

  RvMsgWriter  rvmsg( b, buf_len ),
               submsg( NULL, 0 );
  size_t       off, msg_len;
  const void * msg;
  int          status;

  status = rvmsg.append_subject( SARG( "sub" ), pub.subject,
                                 pub.subject_len );
  /* some subjects may not encode */
  if ( status == 0 ) {
    const char * mtype = "D"; /* data */
    if ( pub.subject_len > 16 && pub.subject[ 0 ] == '_' ) {
      if ( ::memcmp( "_RV.INFO.SYSTEM.", pub.subject, 16 ) == 0 ) {
        /* HOST.START, SESSION.START, LISTEN.START, UNREACHABLE.TRANSPORT */
        if ( ::memcmp( "HOST.", &pub.subject[ 16 ], 5 ) == 0 ||
             ::memcmp( "SESSION.", &pub.subject[ 16 ], 8 ) == 0 ||
             ::memcmp( "LISTEN.", &pub.subject[ 16 ], 7 ) == 0 ||
             ::memcmp( "UNREACHABLE.", &pub.subject[ 16 ], 12 ) == 0 )
          mtype = "A"; /* advisory */
      }
    }
    status = rvmsg.append_string( SARG( "mtype" ), mtype, 2 );
  }
  if ( status == 0 && pub.reply_len > 0 ) {
    status = rvmsg.append_string( SARG( "return" ), (const char *) pub.reply,
                                  pub.reply_len + 1 );
    buf[ rvmsg.off - 1 ] = '\0';
  }
  if ( status == 0 ) {
    uint8_t msg_enc = pub.msg_enc;
    /* depending on message type, encode the hdr to send to the client */
    switch ( msg_enc ) {
      case (uint8_t) RVMSG_TYPE_ID:
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
      case (uint8_t) RAIMSG_TYPE_ID:
      case (uint8_t) TIB_SASS_TYPE_ID:
      case (uint8_t) TIB_SASS_FORM_TYPE_ID:
      do_tibmsg:;
        msg     = pub.msg;
        msg_len = pub.msg_len;

        static const char data_hdr[] = "\005data";
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
      this->append2( buf, off, msg, msg_len );
      this->stat.bs += off + msg_len;
      this->stat.ms++;
      /*this->send( buf, off, msg, msg_len );*/
      bool flow_good = ( this->pending() <= this->send_highwater );
      this->idle_push( flow_good ? EV_WRITE : EV_WRITE_HI );
      return flow_good;
    }
    else {
      fprintf( stderr, "rv unknown msg_enc %u subject: %.*s %lu\n",
               msg_enc, (int) pub.subject_len, pub.subject, off );
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
  if ( this->state == DATA_RECV_SESSION )
    this->stat.send_stop( false, true, this );
  if ( --this->stat.active_clients == 0 )
    this->stat.stop_network();
}

void
EvRvService::release( void ) noexcept
{
  this->rem_all_sub();
  this->sub_tab.release();
  this->pat_tab.release();
  this->msg_in.release();
  this->EvConnection::release_buffers();
  this->poll.push_free_list( this );
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
  if ( k + 1 > sizeof( this->sub_buf ) )
    this->mem.alloc( k + 1, &this->sub );
  else
    this->sub = this->sub_buf;
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
  this->sublen = k;
  return true;

bad_subject:;
  this->sub      = this->sub_buf;
  this->sub[ 0 ] = 0;
  this->sublen   = 0;
  return false;
}

void
RvMsgIn::print( void ) noexcept
{
  MDOutput mout;
  printf( "----\n" );
  printf( "msg_in(%s)", this->sub );
  if ( this->reply != NULL )
    printf( " reply: %s\n", this->reply );
  else
    printf( "\n" );
  this->msg->print( &mout );
  printf( "----\n" );
}

int
RvMsgIn::unpack( void *msgbuf,  size_t msglen ) noexcept
{
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
#if 0
    MDOutput out;
    printf( "<< %lu >>\n", msglen );
    this->msg->print( &out );
#endif
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
                cnt |= 1;
              else
                return ERR_RV_SUB;
            }
            break;
          case 5:
            if ( ::memcmp( nm.fname, SARG( "data" ) ) == 0 ) {
              this->data = mref;
              cnt |= 8;
            }
            break;
          case 6:
            if ( ::memcmp( nm.fname, SARG( "mtype" ) ) == 0 ) {
              if ( mref.ftype == MD_STRING && mref.fsize == 2 ) {
                this->mtype = mref.fptr[ 0 ];
                if ( this->mtype >= 'C' && this->mtype <= 'L' ) {
                  static const uint32_t valid =
                    1 /* C */ | 2 /* D */ | 64 /* I */ | 512 /* L */;
                  if ( ( ( 1 << ( this->mtype - 'C' ) ) & valid ) != 0 )
                    cnt |= 2;
                }
              }
            }
            break;
          case 7:
            if ( ::memcmp( nm.fname, SARG( "return" ) ) == 0 ) {
              this->reply    = (char *) mref.fptr;
              this->replylen = mref.fsize;
              if ( this->replylen > 0 /*&& this->reply[ this->replylen - 1 ] == '\0'*/ )
                this->replylen--;
              cnt |= 4;
            }
            break;
          default:
            break;
        }
      } while ( cnt < 15 && this->iter->next() == 0 );
    }
    if ( ( cnt & 2 ) == 0 )
      status = ERR_RV_MTYPE; /* no mtype */
  }
  if ( ( cnt & 1 ) == 0 ) {
    this->sub      = this->sub_buf;
    this->sub[ 0 ] = '\0';
    this->sublen = 0; /* no subject */
  }
  if ( ( cnt & 2 ) == 0 )
    this->mtype = 0;
  if ( ( cnt & 4 ) == 0 ) {
    this->reply    = NULL;
    this->replylen = 0;
  }
  if ( ( cnt & 8 ) == 0 )
    this->data.zero();
  return status;
}

