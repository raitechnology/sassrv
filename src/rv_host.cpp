#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include <time.h>
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
#include <sys/ioctl.h>
#include <linux/if.h>
#else
#include <raikv/win.h>
#endif
#include <raikv/util.h>
#include <raikv/ev_publish.h>
#include <raikv/array_space.h>
#include <sassrv/ev_rv.h>

using namespace rai;
using namespace sassrv;
using namespace kv;
using namespace md;
uint32_t rai::sassrv::rv_host_debug = 0;

RvHost::RvHost( RvHostDB &d,  EvPoll &poll,  RoutePublish &sr,
                const char *svc,  size_t svc_len,  uint16_t port,
                bool has_svc_pre ) noexcept
      : EvSocket( poll, poll.register_type( "rv_host" ) ),
        db( d ), sub_route( sr )
{
                             /*_RV.ERROR.SYSTEM.DATALOSS.OUTBOUND.BCAST */
  const char   outbound_sub[] = _RV_ERROR_DATALOSS_OUT_BCAST,
                             /*_RV.ERROR.SYSTEM.DATALOSS.INBOUND.BCAST */
               inbound_sub[]  = _RV_ERROR_DATALOSS_INB_BCAST;
  const char * osub = outbound_sub,
             * isub = inbound_sub;
  size_t       olen = sizeof( outbound_sub ) - 1,
               ilen = sizeof( inbound_sub ) - 1;
  const char * v    = sassrv_get_version();
  size_t       vlen = ::strlen( v );

  this->sock_opts = OPT_NO_POLL;
  ::memset( this->host, 0, (char *) (void *) &this[ 1 ] - this->host );
  ::memcpy( this->service, svc, svc_len );
  if ( vlen > MAX_RV_VER_LEN - 1 )
    vlen = MAX_RV_VER_LEN - 1;
  ::memcpy( this->ver, v, vlen );
  this->ver[ vlen ] = '\0';
  this->ver_len     = vlen + 1;
  this->service_len = svc_len;
  this->ipport      = port;
  this->has_service_prefix = has_svc_pre;
  this->service_num = atoi( this->service );

  if ( has_svc_pre ) {
    char * o = (char *) ::malloc( olen + svc_len + 2 + 1 );
    char * i = (char *) ::malloc( olen + svc_len + 2 + 1 );
    olen = 0; ilen = 0;
    o[ olen++ ] = '_'; i[ ilen++ ] = '_';
    ::memcpy( &o[ olen ], this->service, this->service_len );
    ::memcpy( &i[ ilen ], this->service, this->service_len );
    olen += this->service_len; ilen += this->service_len;
    o[ olen++ ] = '.'; i[ ilen++ ] = '.';
    ::memcpy( &o[ olen ], outbound_sub, sizeof( outbound_sub ) );
    ::memcpy( &i[ ilen ], inbound_sub, sizeof( inbound_sub ) );
    olen += sizeof( outbound_sub ) - 1;
    ilen += sizeof( inbound_sub ) - 1;
    osub  = o;
    isub  = i;
  }
  this->dataloss_outbound_sub  = osub;
  this->dataloss_inbound_sub   = isub;
  this->dataloss_outbound_len  = olen;
  this->dataloss_inbound_len   = ilen;
  this->dataloss_outbound_hash = kv_crc_c( osub, olen, 0 );
  this->dataloss_inbound_hash  = kv_crc_c( isub, ilen, 0 );
}

int
RvHost::init_host( void ) noexcept
{
  this->PeerData::init_peer( this->poll.get_next_id(), this->poll.get_null_fd(),
                             this->sub_route.route_id, NULL, "rv_host" );
  return this->poll.add_sock( this );
}

bool
RvHostDB::get_service( RvHost *&h,  const RvHostNet &hn ) noexcept
{
  h = NULL;
  if ( this->host_tab != NULL ) {
    for ( size_t i = 0; i < this->host_tab->count; i++ ) {
      RvHost * host = this->host_tab->ptr[ i ];
      if ( host->service_len == hn.service_len &&
           ::memcmp( host->service, hn.service, hn.service_len ) == 0 ) {
        h = host;
        return true;
      }
    }
  }
  return false;
}

bool
RvHostDB::get_service( RvHost *&h,  uint16_t svc ) noexcept
{
  h = NULL;
  if ( this->host_tab != NULL ) {
    uint16_t digits = uint16_digits( svc );
    char name[ 8 ];
    uint16_to_string( svc, name, digits );
    for ( size_t i = 0; i < this->host_tab->count; i++ ) {
      RvHost * host = this->host_tab->ptr[ i ];
      if ( host->service_len == digits &&
           ::memcmp( host->service, name, digits ) == 0 ) {
        h = host;
        return true;
      }
    }
  }
  return false;
}

int
RvHostDB::start_service( RvHost *&h,  EvPoll &poll,  RoutePublish &sr,
                         const RvHostNet &hn ) noexcept
{
  if ( ! this->get_service( h, hn ) ) {
    if ( this->host_tab == NULL )
      this->host_tab = new ( ::malloc( sizeof( RvHostTab ) ) ) RvHostTab();
    void * p = ::malloc( sizeof( RvHost ) );
    h = new ( p ) RvHost( *this, poll, sr, hn.service, hn.service_len,
                          hn.ipport, hn.has_service_prefix );
    this->host_tab->push( h );
  }
  else {
    if ( hn.ipport != 0 && h->ipport == 0 )
      h->ipport = hn.ipport;
    if ( hn.has_service_prefix != h->has_service_prefix )
      return ERR_BAD_SERVICE_NUM;
  }
  return HOST_OK;
}

void
RvHost::start_daemon( void ) noexcept
{
  DaemonInbox ibx( *this );
  size_t i;

  if ( this->db.daemon_tab == NULL )
    this->db.daemon_tab =
      new ( ::malloc( sizeof( RvDaemonTab ) ) ) RvDaemonTab();
  for ( i = 0; i < this->db.daemon_tab->count; i++ ) {
    this->rpc = this->db.daemon_tab->ptr[ i ];
    if ( this->rpc->ibx.equals( ibx ) )
      return;
  }
  void * p = ::malloc( sizeof( RvDaemonRpc ) );
  this->rpc = new ( p ) RvDaemonRpc( *this );
  this->db.daemon_tab->push( this->rpc );
}

bool
RvHost::timer_expire( uint64_t tid,  uint64_t ) noexcept
{
  /* stop timer when host stops */
  if ( tid != this->host_status_timer ) {
    if ( tid == this->host_delay_timer ) {
      /*printf( "delay start %s expire\n", this->service );*/
      this->start_host2( 0 );
    }
    else if ( tid == (uint64_t) this->host_loss_timer ) {
      if ( this->pub_inbound_data_loss() )
        return true;
      this->host_loss_timer = 0;
    }
    return false;
  }
  this->send_host_status(); /* still going */
  return true;
}

int
RvHost::start_host2( uint32_t delay_secs ) noexcept
{
  if ( ! this->in_list( IN_ACTIVE_LIST ) )
    this->init_host();

  if ( delay_secs != 0 ) {
    if ( this->host_delay_timer == 0 )
      this->host_delay_timer = ++this->timer_id;
    this->poll.timer.add_timer_seconds( this->fd, delay_secs,
                                        this->host_delay_timer, 0 );
    return 0;
  }
  /* subscribe _INBOX.DAEMON.iphex */
  if ( this->rpc != NULL && ! this->daemon_subscribed ) {
    this->rpc->subscribe_daemon_inbox();
    this->daemon_subscribed = true;
  }
  if ( this->host_status_timer == 0 ) {
    this->host_status_timer = ++this->timer_id;
    /* start timer to send the status every 90 seconds */
    this->poll.timer.add_timer_seconds( this->fd, RV_STATUS_IVAL,
                                        this->host_status_timer, 0 );
  }
  PeerMatchArgs ka( "rv", 2 );
  PeerMatchIter iter( *this, ka );
  for ( EvSocket *p = iter.first(); p != NULL; p = iter.next() ) {
    EvRvService *svc = (EvRvService *) p;
    if ( svc->host == this ) {
      if ( ! svc->host_started ) {
        svc->host_started = true;
        this->active_clients++;
      }
      if ( svc->svc_state >= EvRvService::DATA_RECV )
        svc->send_start();
      svc->idle_push( EV_READ );
    }
  }
  this->network_started   = true;
  this->start_in_progress = false;
  return 0;
}

int
RvHost::start_host( void ) noexcept
{
  if ( this->host_status_timer != 0 ) /* host already started */
    /* reassert all subs open */
    this->reassert_subs();
  else
    this->start_host2( 0 );
  return 0;
}

int
RvHost::stop_host( void ) noexcept
{
  if ( this->host_status_timer != 0 ) {
    /* stop the timer, the timer_expire() function tests this */
    this->host_status_timer = 0;
    this->host_delay_timer  = 0;
    this->host_loss_timer   = 0;
    /* unsubscribe _INBOX.DAEMON.iphex */
    /*this->unsubscribe_daemon_inbox();*/
    if ( this->rpc != NULL && this->daemon_subscribed ) {
      this->rpc->unsubscribe_daemon_inbox();
      this->daemon_subscribed = false;
    }
    if ( this->host_started )
      this->send_host_stop( NULL );
  }
  return 0;
}

void
RvHost::data_loss_error( uint64_t bytes_lost,  const char *err,
                         size_t errlen ) noexcept
{
  const char * subj   = this->dataloss_outbound_sub;
  const size_t sublen = this->dataloss_outbound_len;
  MDMsgMem    mem;
  char        str[ 256 ];
  size_t      size;
  RvMsgWriter msg( mem, mem.make( 1024 ), 1024 );

  msg.append_string( SARG( _ADV_CLASS ), SARG( _ERROR ) );
  msg.append_string( SARG( _ADV_SOURCE ), SARG( _SYSTEM ) );
  msg.append_string( SARG( _ADV_NAME ), SARG( _DATALOSS_OUTBOUND_BCAST ) );
  if ( errlen > 0 ) {
    if ( errlen > sizeof( str ) - 1 )
      errlen = sizeof( str ) - 1;
    ::memcpy( str, err, errlen );
    str[ errlen ] = '\0';
  }
  else {
    ::strcpy( str, "nats-server connection lost" );
    errlen = ::strlen( str );
  }
  msg.append_string( SARG( _ADV_DESC ), str, errlen + 1 );
  msg.append_uint( SARG( "lost" ), bytes_lost );
  if ( this->ipport != 0 )
    msg.append_ipdata( SARG( "scid" ), this->ipport );
  size = msg.update_hdr();

  EvPublish pub( subj, sublen, NULL, 0, msg.buf, size, this->sub_route, *this,
                 this->dataloss_outbound_hash, RVMSG_TYPE_ID );
  PeerMatchArgs ka( "rv", 2 );
  PeerMatchIter iter( *this, ka );
  for ( PeerData *p = iter.first(); p != NULL; p = iter.next() ) {
    EvRvService *svc = (EvRvService *) p;
    if ( svc->host == this )
      svc->fwd_msg( pub );
  }
}

void
RvHost::send_outbound_data_loss( uint32_t msg_loss,  bool is_restart,
                          uint32_t pub_host,  const char *pub_host_id ) noexcept
{
  const char * subj   = this->dataloss_outbound_sub;
  const size_t sublen = this->dataloss_outbound_len;
  MDMsgMem     mem;
  char         pub_host_ip[ 32 ];
  size_t       pub_host_ip_len = 0;
  size_t       pub_host_id_len = 0;
  RvMsgWriter  rvmsg( mem, mem.make( 1024 ), 1024 );

  if ( msg_loss <= EV_MAX_LOSS )
    this->stat.odl += msg_loss;

  pub_host_id_len = ( pub_host_id == NULL ? 0 : ::strlen( pub_host_id ) );
  pub_host_ip_len = RvMcast::ip4_string( pub_host, pub_host_ip );

  rvmsg.append_string( SARG( _ADV_CLASS ), SARG( _ERROR ) );
  rvmsg.append_string( SARG( _ADV_SOURCE ), SARG( _SYSTEM ) );
  rvmsg.append_string( SARG( _ADV_NAME ), SARG( _DATALOSS_OUTBOUND_BCAST ) );
  if ( is_restart )
    rvmsg.append_string( SARG( _ADV_DESC ), SARG( "lost seqno tracking" ) );
  else
    rvmsg.append_string( SARG( _ADV_DESC ), SARG( "lost msgs" ) );
  rvmsg.append_uint( SARG( "lost" ), msg_loss );
  rvmsg.append_string( SARG( "host" ), pub_host_ip, pub_host_ip_len + 1 );
  if ( pub_host_id_len != 0 )
    rvmsg.append_string( SARG( "hostid" ), pub_host_id, pub_host_id_len + 1 );
  if ( this->ipport != 0 )
    rvmsg.append_ipdata( SARG( "scid" ), this->ipport );
  size_t size = rvmsg.update_hdr();

  EvPublish pub( subj, sublen, NULL, 0, rvmsg.buf, size, this->sub_route, *this,
                 this->dataloss_outbound_hash, RVMSG_TYPE_ID );
  PeerMatchArgs ka( "tcp", 3 );
  PeerMatchIter iter( *this, ka );
  for ( EvSocket *p = iter.first(); p != NULL; p = iter.next() ) {
    char sess[ MAX_SESSION_LEN ];
    if ( p->get_session( this->service_num, sess ) > 0 )
      this->sub_route.forward_to( pub, p->fd );
  }
}

void
RvHost::inbound_data_loss( kv::EvSocket &dest,  EvPublish &pub,
                           const char *pub_host_id ) noexcept
{
  RvPubLoss * loss = this->loss_array[ dest.fd ];
  if ( loss == NULL ) {
    loss = new ( ::malloc( sizeof( RvPubLoss ) ) ) RvPubLoss( &dest );
    this->loss_array[ dest.fd ] = loss;
    this->loss_array.refs++;
  }
  loss->data_loss( *this, pub, pub_host_id );
  if ( this->host_loss_timer == 0 ) {
    this->host_loss_timer = ++this->timer_id;
    this->poll.timer.add_timer_seconds( this->fd, 1, this->host_loss_timer, 0 );
  }
}

void
RvLossArray::remove_loss_entry( RvHost &host,  uint32_t fd ) noexcept
{
  if ( fd < this->count ) {
    RvPubLoss * loss = this->ptr[ fd ];
    if ( loss != NULL ) {
      this->ptr[ fd ] = NULL;
      delete loss;
      if ( --this->refs == 0 ) {
        this->print_events( host );
        this->clear();
      }
    }
  }
}

void
RvLossArray::print_events( RvHost &host ) noexcept
{
  if ( this->pub_events != 0 ) {
    static const char *event[ 4 ] =
      { "pub_start", "pub_cycle", "pub_restart", "msg_loss" };
    fprintf( stderr, "rv svc %s loss events: %u\n", host.service,
             this->pub_events );
    for ( int i = 0; i < 4; i++ ) {
      if ( this->pub_status[ i ] != 0 ) {
        fprintf( stderr, " event %s: %u\n", event[ i ], this->pub_status[ i ] );
        this->pub_status[ i ] = 0;
      }
    }
    this->pub_events = 0;
  }
}

void
RvPubLoss::data_loss( RvHost &host,  EvPublish &pub,
                      const char *pub_host_id ) noexcept
{
  if ( pub.pub_status <= EV_MAX_LOSS ) /* if message loss */
    host.loss_array.pub_status[ 3 ] += pub.pub_status;
  else
    host.loss_array.pub_status[ pub.pub_status & EV_MAX_LOSS ]++; /* other ev */

  uint32_t msg_loss = pub.pub_status,
           restart  = 0;
  if ( msg_loss == EV_PUB_RESTART ) {
    msg_loss = 1; /* not sure how many lost */
    restart  = 1;
  }
  if ( msg_loss <= EV_MAX_LOSS ) { /* if message loss */
    host.stat.idl += msg_loss;

    if ( this->loss_queue == NULL )
      this->loss_queue =
        new ( ::malloc( sizeof( RvDataLossQueue ) ) ) RvDataLossQueue();

    const char * sub;
    size_t       sublen;
    RouteLoc     loc;
    RvDataLossElem * el;

    sub    = pub.subject;
    sublen = pub.subject_len;

    if ( host.has_service_prefix ) {
      sublen -= host.service_len + 2;
      sub    += host.service_len + 2;
    }
    el = this->loss_queue->tab.upsert( pub.subj_hash, sub, sublen, loc );
    if ( loc.is_new ) {
      el->msg_loss     = msg_loss;
      el->restart      = restart;
      el->pub_host     = pub.pub_host;
      el->pub_host_id  = pub_host_id;
      el->pub_msg_loss = 0;
      el->pub_restart  = 0;
    }
    else {
      el->msg_loss   += msg_loss;
      el->restart    += restart;
      el->pub_host    = pub.pub_host;
      el->pub_host_id = pub_host_id;
    }

    if ( host.loss_array.pub_events++ == 0 )
      host.send_inbound_data_loss( *this );
  }
}

bool
RvHost::pub_inbound_data_loss( void ) noexcept
{
  bool one = false;
  for ( size_t i = 0; i < this->loss_array.count; i++ ) {
    RvPubLoss * loss = this->loss_array.ptr[ i ];
    if ( loss != NULL ) {
      if ( this->send_inbound_data_loss( *loss ) )
        one = true;
      else
        this->loss_array.remove_loss_entry( *this, i );
    }
  }
  return one;
}

bool
RvHost::send_inbound_data_loss( RvPubLoss &loss ) noexcept
{
  RvDataLossElem * el;
  RouteLoc loc;
  if ( loss.loss_queue == NULL )
    return false;

  const char * sub[ 9 ];
  size_t       sublen[ 9 ];
  uint32_t     msg_loss[ 9 ],
               sub_cnt       = 0,
               extra_sublen  = 0,
               pub_host      = 0;
  uint64_t     total_loss    = 0,
               total_restart = 0;
  char         pub_host_ip[ 32 ];
  size_t       pub_host_ip_len = 0;
  const char * pub_host_id  = NULL;
  size_t       pub_host_id_len = 0;

  sub[ 0 ]      = NULL;
  sublen[ 0 ]   = 0;
  msg_loss[ 0 ] = 0;
  for ( el = loss.loss_queue->tab.first( loc ); el != NULL;
        el = loss.loss_queue->tab.next( loc ) ) {
    if ( el->pub_msg_loss != el->msg_loss &&
         ( el->pub_host == pub_host || pub_host == 0 ) ) {
      uint32_t loss    = el->msg_loss - el->pub_msg_loss,
               restart = el->restart  - el->pub_restart;
      if ( sub_cnt < 9 ) {
        sub[ sub_cnt ]      = el->value;
        sublen[ sub_cnt ]   = el->len;
        msg_loss[ sub_cnt ] = loss;
        extra_sublen       += el->len;
      }
      sub_cnt++;
      total_loss      += loss;
      total_restart   += restart;
      el->pub_msg_loss = el->msg_loss;
      el->pub_restart  = el->restart;
      pub_host         = el->pub_host;
      pub_host_id      = el->pub_host_id;
      pub_host_id_len  = ( pub_host_id == NULL ? 0 : ::strlen( pub_host_id ) );
    }
  }
  if ( total_loss == 0 ) {
    delete loss.loss_queue;
    loss.loss_queue = NULL;
    return false;
  }
  pub_host_ip_len = RvMcast::ip4_string( pub_host, pub_host_ip );

  const char * sys_sub    = this->dataloss_inbound_sub;
  const size_t sys_sublen = this->dataloss_inbound_len;
  MDMsgMem  mem;
  size_t    blen;
  uint32_t  extra_sub = ( sub_cnt < 9 ? sub_cnt : 9 );

  blen = extra_sublen + extra_sub * 32 + pub_host_id_len + 512;
  RvMsgWriter rvmsg( mem, mem.make( blen ), blen );

  rvmsg.append_string( SARG( _ADV_CLASS ), SARG( _ERROR ) );
  rvmsg.append_string( SARG( _ADV_SOURCE ), SARG( _SYSTEM ) );
  rvmsg.append_string( SARG( _ADV_NAME ), SARG( _DATALOSS_INBOUND_BCAST ) );
  if ( total_loss == total_restart )
    rvmsg.append_string( SARG( _ADV_DESC ), SARG( "lost seqno tracking" ) );
  else
    rvmsg.append_string( SARG( _ADV_DESC ), SARG( "lost msgs" ) );
  rvmsg.append_string( SARG( "host" ), pub_host_ip, pub_host_ip_len + 1 );
  if ( pub_host_id_len != 0 )
    rvmsg.append_string( SARG( "hostid" ), pub_host_id, pub_host_id_len + 1 );
  rvmsg.append_uint( SARG( "lost" ), (uint32_t) ( total_loss < 0x7fffffffU ?
    (uint32_t) total_loss : 0x7fffffffU ) );
  rvmsg.append_uint( SARG( "sub_cnt" ), sub_cnt );
  for ( uint32_t i = 0; i < extra_sub; i++ ) {
    static const char *sub1[ 9 ] = { "sub1", "sub2", "sub3", "sub4",
                                     "sub5", "sub6", "sub7", "sub8", "sub9" };
    static const char *lost1[ 9 ] = { "lost1", "lost2", "lost3", "lost4",
                                  "lost5", "lost6", "lost7", "lost8", "lost9" };
    rvmsg.append_string( sub1[ i ], 5, sub[ i ], sublen[ i ] );
    rvmsg.append_uint( lost1[ i ], 6, msg_loss[ i ] );
  }
  if ( this->ipport != 0 )
    rvmsg.append_ipdata( SARG( "scid" ), this->ipport );
  blen = rvmsg.update_hdr();

  EvPublish pub( sys_sub, sys_sublen, NULL, 0, rvmsg.buf, blen,
                 this->sub_route, *this, this->dataloss_inbound_hash,
                 RVMSG_TYPE_ID );
  this->sub_route.forward_to( pub, loss.sock->fd );
  return true;
}

void
RvHost::reassert_subs( void ) noexcept
{
  PeerMatchArgs      ka( "rv", 2 );
  PeerMatchIter      iter( *this, ka );
  RvSubRoutePos      pos;
  RvPatternRoutePos  ppos;
  RouteVec<RouteSub> sub_db, pat_db;
  DaemonInbox        ibx( *this );

  sub_db.insert( ibx.h, ibx.buf, ibx.len );
  for ( PeerData *p = iter.first(); p != NULL; p = iter.next() ) {
    EvRvService &s = *(EvRvService *) p;
    if ( s.host == this ) {
      if ( s.sub_tab.first( pos ) ) {
        do {
          sub_db.upsert( pos.rt->hash, pos.rt->value, pos.rt->len );
        } while ( s.sub_tab.next( pos ) );
      }
      if ( s.pat_tab.first( ppos ) ) {
        do {
          pat_db.upsert( ppos.rt->hash, ppos.rt->value, ppos.rt->len );
        } while ( s.pat_tab.next( ppos ) );
      }
    }
  }
  this->sub_route.notify_reassert( this->fd, sub_db, pat_db );
}

size_t
RvHost::utime_to_str( uint64_t us,  char *str ) noexcept
{
  uint8_t j, k;
  for ( j = 0; ( us >> j ) != 0; j += 4 )
    ;
  for ( k = 0; j > 0; ) {
    j -= 4;
    str[ k++ ] = hexchar2( ( us >> j ) & 0xf );
  }
  str[ k ] = '\0';
  return k;
}

size_t
RvHost::make_session( uint64_t ns,  char session[ MAX_SESSION_LEN ] ) noexcept
{
  size_t i = this->session_ip_len;
  ::memcpy( session, this->session_ip, i );
  session[ i++ ] = '.';
  uint64_t us = ns / 1000;
  if ( us <= this->last_session_us )
    us = ++this->last_session_us;
  else
    this->last_session_us = us;
  return i + RvHost::utime_to_str( us, &session[ i ] );
}

const char *
rai::sassrv::get_rv_host_error( int status ) noexcept
{
  switch ( status ) {
    case HOST_OK:
      return "host ok";
    case ERR_GETHOSTNAME_FAILED:
      return "gethostname failed";
    case ERR_NO_INTERFACE_FOUND:
      return "the hostname addr does not match interface";
    case ERR_HOSTNAME_NOT_FOUND:
      return "the hostname does not resolve";
    case ERR_SAME_SVC_TWO_NETS:
      return "the same service used with two different networks";
    case ERR_NETWORK_NOT_FOUND:
      return "network is not found";
    case ERR_BAD_SERVICE_NUM:
      return "the service number invalid";
    case ERR_BAD_PARAMETERS:
      return "the length of network overflows MAX_RV_NETWORK";
    case ERR_START_HOST_FAILED:
      return "host start failed";
    default:
      return "host error";
  }
}

int
RvHost::check_network( const RvHostNet &hn ) noexcept
{
  int status = HOST_OK;
  if ( this->mcast.host_ip == 0 || ! this->is_same_network( hn ) ) {
    RvMcast mc;
    status = mc.parse_network( hn.network, hn.network_len );
    if ( status == HOST_OK )
      status = this->start_network( mc, hn );
    if ( status != HOST_OK )
      return status;
  }
  return status;
}

uint16_t
RvMcast::ip4_string( uint32_t ip,  char *buf ) noexcept
{
  const uint8_t * q = (const uint8_t *) &ip;
  size_t sz = 0;
  for ( int i = 0; ; i++ ) {
    sz += uint32_to_string( q[ i ], &buf[ sz ] );
    if ( i == 3 ) break;
    buf[ sz++ ] = '.';
  }
  buf[ sz ] = '\0';
  return (uint16_t) sz;
}

uint16_t
RvMcast::ip4_hex_string( uint32_t ip,  char *buf ) noexcept
{
  const uint8_t * q = (const uint8_t *) &ip;
  for ( int k = 0; k < 8; k += 2 ) {
    buf[ k ] = hexchar2( ( q[ k/2 ] >> 4 ) & 0xf );
    buf[ k+1 ] = hexchar2( q[ k/2 ] & 0xf );
  }
  buf[ 8 ] = '\0';
  return 8;
}

int
RvHost::start_network( const RvMcast &mc,  const RvHostNet &hn ) noexcept
{
  if ( this->network_started ) {
    if ( this->is_same_network( hn ) )
      return HOST_OK;
    return ERR_SAME_SVC_TWO_NETS;
  }
  this->zero_stats( kv_current_realtime_ns() );
  return this->copy_network( mc, hn );
}

int
RvHost::copy_network( const RvMcast &mc,  const RvHostNet &hn ) noexcept
{
  if ( ! this->copy( hn ) )
    return ERR_BAD_PARAMETERS;
  if ( rv_debug )
    printf( "start svc=%.*s net=%.*s\n",
            hn.service_len, hn.service, hn.network_len, hn.network );

  int n = atoi( this->service );
  if ( n <= 0 || n > 0xffff )
    return ERR_BAD_SERVICE_NUM;
  this->service_num  = n;
  this->service_port = htons( n );

  this->host_ip = mc.host_ip;
  if ( mc.fake_ip != 0 )
    this->host_ip = mc.fake_ip;
  this->mcast.copy( mc );
  RvMcast::ip4_hex_string( this->host_ip, this->session_ip );
  this->sess_ip_len = RvMcast::ip4_string( this->host_ip, this->sess_ip );
  /*this->sess_ip_len = ::snprintf( this->sess_ip, sizeof( this->sess_ip ),
                              "%u.%u.%u.%u", q[ 0 ], q[ 1 ], q[ 2 ], q[ 3 ] );*/
  /*q = (const uint8_t *) (const void *) &mc.host_ip;
  this->host_ip_len = ::snprintf( this->host_ip, sizeof( this->host_ip ),
                                "%u.%u.%u.%u", q[ 0 ], q[ 1 ], q[ 2 ], q[ 3 ] );*/
  ::memcpy( this->daemon_id, this->session_ip, this->session_ip_len );
  ::memcpy( &this->daemon_id[ this->session_ip_len ], ".DAEMON.", 8 );
  this->daemon_len = this->session_ip_len + 8;
  this->daemon_len += (uint16_t) time_to_str( this->start_stamp,
                                   &this->daemon_id[ this->daemon_len ] );
  this->start_daemon();
  return HOST_OK;
}

bool
RvHost::stop_network( void ) noexcept
{
  if ( this->network_started ) {
    if ( --this->active_clients == 0 ) {
      this->network_started   = false;
      this->start_in_progress = false;
      return true;
    }
  }
  return false;
}

RvFwdAdv::RvFwdAdv( RvHost &host,  EvRvService *svc,  const char *prefix,
                    size_t prefix_len,  int flags ) noexcept
        : subj_prefix( prefix ), fix_len( prefix_len ),
          subj_suffix( 0 ), suf_len( 0 ), reply( 0 ), reply_len( 0 ),
          refcnt( 0 )
{
  if ( svc != NULL ) {
    this->userid  = svc->userid;  this->userid_len  = svc->userid_len,
    this->session = svc->session; this->session_len = svc->session_len;
  }
  else {
    this->userid  = NULL; this->userid_len  = 0;
    this->session = NULL; this->session_len = 0;
  }
  this->fwd( host, flags );
}

RvFwdAdv::RvFwdAdv( RvHost &host,  const char *uid,  size_t uid_len,
                    const char *sess,  size_t sess_len,
                    const char *prefix,  size_t prefix_len, int flags,
                    uint32_t ref,  const char *suffix,  size_t suffix_len,
                    const char *rep,  size_t rep_len ) noexcept
        : userid( uid ), userid_len( uid_len ),
          session( sess ), session_len( sess_len ),
          subj_prefix( prefix ), fix_len( prefix_len ),
          subj_suffix( suffix ), suf_len( suffix_len ),
          reply( rep ), reply_len( rep_len ),
          refcnt( ref )
{
  this->fwd( host, flags );
}

void
RvFwdAdv::fwd( RvHost &host,  int flags ) noexcept
{
  MDMsgMem mem;
  char   * subj     = NULL,
         * rsubj    = NULL,
         * tport    = NULL;
  size_t   sublen   = 0,
           prelen   = 0,
           rsublen  = 0,
           tportlen = 0;
  uint64_t now      = 0;

  if ( host.has_service_prefix )
    sublen += host.service_len + 2;
  sublen += this->fix_len;
  if ( ( flags & ADV_SESS_SUF ) != 0 )
    sublen += host.session_ip_len + 1 + this->suf_len;
  else if ( this->suf_len > 0 )
    sublen += this->suf_len;
  else
    sublen += host.session_ip_len;
  subj = mem.str_make( sublen + 1 );
  sublen = 0;

  if ( this->reply_len != 0 ) {
    if ( host.has_service_prefix )
      rsublen += host.service_len + 2;
    rsublen += this->reply_len;
    rsubj = mem.str_make( rsublen + 1 );
    rsublen = 0;
  }

  if ( host.has_service_prefix ) {
    subj[ sublen++ ] = '_';
    ::memcpy( &subj[ sublen ], host.service, host.service_len );
    sublen += host.service_len;
    subj[ sublen++ ] = '.';
    prelen = sublen;

    if ( rsubj != NULL ) {
      rsubj[ rsublen++ ] = '_';
      ::memcpy( &rsubj[ rsublen ], host.service, host.service_len );
      rsublen += host.service_len;
      rsubj[ rsublen++ ] = '.';
    }
  }

  ::memcpy( &subj[ sublen ], this->subj_prefix, this->fix_len );
  sublen += this->fix_len;

  if ( ( flags & ADV_SESS_SUF ) != 0 ) {
    tport = &subj[ sublen ];
    ::memcpy( &subj[ sublen ], host.session_ip, host.session_ip_len );
    sublen += host.session_ip_len;
    subj[ sublen++ ] = '.';
    ::memcpy( &subj[ sublen ], this->subj_suffix, this->suf_len );
    sublen += this->suf_len;
    tportlen = &subj[ sublen + 1 ] - tport;
  }
  else if ( this->suf_len > 0 ) {
    ::memcpy( &subj[ sublen ], this->subj_suffix, this->suf_len );
    sublen += this->suf_len;
  }
  else {
    ::memcpy( &subj[ sublen ], host.session_ip, host.session_ip_len );
    sublen += host.session_ip_len;
  }
  subj[ sublen ] = '\0';
  if ( rsubj != NULL ) {
    ::memcpy( &rsubj[ rsublen ], this->reply, this->reply_len );
    rsublen += this->reply_len;
    rsubj[ rsublen ] = '\0';
  }

  /* _RV.<class>.SYSTEM.name */
  prelen += 4; /* skip _RV. */

  const char * sys = ::strchr( &subj[ prelen ], '.' ), /* .SYSTEM */
             * nam = ::strchr( sys + 1, '.' ) + 1;     /* name */
  size_t       class_len = (size_t) ( sys - &subj[ prelen ] );
  char         class_str[ 8 ]; /* INFO, ERROR */

  if ( class_len < 8 ) {
    ::memcpy( class_str, &subj[ prelen ], class_len );
    class_str[ class_len ] = '\0';
  }
  else {
    ::strcpy( class_str, "UNKN" );
    class_len = 4;
  }

  RvMsgWriter msg( mem, mem.make( sublen + 1024 ), sublen + 1024 );

  msg.append_string( SARG( _ADV_CLASS ), class_str, class_len + 1 );
  msg.append_string( SARG( _ADV_SOURCE ), SARG( _SYSTEM ) );
  /* skip prefix: _RV.INFO.SYSTEM. */
  msg.append_string( SARG( _ADV_NAME ), nam, &subj[ sublen + 1 ] - nam );

  if ( ( flags & ADV_HOST_COMMON ) != 0 ) {
    now = kv_current_realtime_ns();

    if ( ( flags & ADV_HOSTADDR ) != 0 ) {
      msg.append_ipdata( SARG( "hostaddr" ), host.host_ip );
      if ( host.host_id_len > 0 )
        msg.append_string( SARG( "hostid" ), host.host_id,
                           host.host_id_len + 1 );
    }
    if ( ( flags & ADV_SN ) != 0 )
      msg.append_uint( SARG( "sn" ), (uint32_t) 1 );
    if ( ( flags & ADV_OS ) != 0 )
      msg.append_uint( SARG( "os" ), (uint8_t) 1 );
    if ( ( flags & ADV_VER ) != 0 )
      msg.append_string( SARG( "ver" ), host.ver, host.ver_len );
    if ( ( flags & ADV_HTTPADDR ) != 0 ) {
      if ( host.http_addr != 0 || host.http_port != 0 )
        msg.append_ipdata( SARG( "httpaddr" ), host.http_addr );
    }
    if ( ( flags & ADV_HTTPPORT ) != 0 ) {
      if ( host.http_addr != 0 || host.http_port != 0 )
        msg.append_ipdata( SARG( "httpport" ), host.http_port );
    }
    if ( ( flags & ADV_TIME ) != 0 ) {
      uint64_t s = now / 1000000000,
               m = ( now / 1000000 ) % 1000;
      s  = ( s << 32 ) | m;
      msg.append_type( SARG( "time" ), s, MD_DATETIME );
    }
  }
  if ( ( flags & ADV_ID ) != 0 && this->session_len != 0 )
    msg.append_string( SARG( "id" ), this->session, this->session_len + 1 );
  if ( ( flags & ADV_USERID ) != 0 && this->userid_len != 0 )
    msg.append_string( SARG( "userid" ), this->userid, this->userid_len + 1 );
  if ( ( flags & ADV_TPORT ) != 0 )
    msg.append_string( SARG( "tport" ), tport, tportlen + 1 );

  if ( ( flags & ADV_SUB ) != 0 && this->suf_len != 0 ) {
    char * sub = mem.str_make( this->suf_len + 1 );
    ::memcpy( sub, this->subj_suffix, this->suf_len );
    sub[ this->suf_len ] = '\0';
    msg.append_string( SARG( "sub" ), sub, this->suf_len + 1 );
  }

  if ( ( flags & ADV_REFCNT ) != 0 )
    msg.append_uint( SARG( "refcnt" ), this->refcnt );

  if ( ( flags & ADV_HOST_COMMON ) != 0 ) {
    if ( ( flags & ADV_UP ) != 0 ) {
      uint64_t s = ( now - host.start_stamp ) / 1000000000;
      msg.append_uint( SARG( "up" ), (uint32_t) s );
    }
    if ( ( flags & ADV_STATS ) != 0 ) {
      host.stat.time_ns = now;
      host.previous_stat[ 1 ].copy( host.previous_stat[ 0 ] );
      host.previous_stat[ 0 ].copy( host.stat );
      if ( ( flags & ADV_MS ) != 0 )
        msg.append_uint( SARG( "ms" ), host.stat.ms );
      if ( ( flags & ADV_BS ) != 0 )
        msg.append_uint( SARG( "bs" ), host.stat.bs );
      if ( ( flags & ADV_MR ) != 0 )
        msg.append_uint( SARG( "mr" ), host.stat.mr );
      if ( ( flags & ADV_BR ) != 0 )
        msg.append_uint( SARG( "br" ), host.stat.br );
      if ( ( flags & ADV_PS ) != 0 )
        msg.append_uint( SARG( "ps" ), host.stat.ps );
      if ( ( flags & ADV_PR ) != 0 )
        msg.append_uint( SARG( "pr" ), host.stat.pr );
      if ( ( flags & ADV_RX ) != 0 )
        msg.append_uint( SARG( "rx" ), host.stat.rx );
      if ( ( flags & ADV_PM ) != 0 )
        msg.append_uint( SARG( "pm" ), host.stat.pm );
      if ( ( flags & ADV_IDL ) != 0 )
        msg.append_uint( SARG( "idl" ), host.stat.idl );
      if ( ( flags & ADV_ODL ) != 0 )
        msg.append_uint( SARG( "odl" ), host.stat.odl );
    }
    if ( ( flags & ADV_IPPORT ) != 0 && host.ipport != 0 )
      msg.append_ipdata( SARG( "ipport" ), host.ipport );
    if ( ( flags & ADV_SERVICE ) != 0 )
      msg.append_string( SARG( "service" ), host.service,
                         host.service_len + 1 );
    if ( ( flags & ADV_NETWORK ) != 0 )
      msg.append_string( SARG( "network" ), host.network,
                         host.network_len + 1 );
  }
  size_t msg_size = msg.update_hdr();

  uint32_t h = kv_crc_c( subj, sublen, 0 );
  if ( is_rv_debug )
    printf( "fwd %.*s\n", (int) sublen, subj );
  EvPublish pub( subj, sublen, rsubj, rsublen, msg.buf, msg_size,
                 host.sub_route, host, h, RVMSG_TYPE_ID,
                 PUB_TYPE_SERIAL );
  if ( is_rv_debug )
    EvRvService::print( host.fd, msg.buf, msg_size );
  host.sub_route.forward_msg( pub );
}

void
RvHost::send_host_status( void ) noexcept
{
                                /*_RV.INFO.SYSTEM.HOST.STATUS.*/
  static const char   status[]   = _RV_INFO_HOST_STATUS ".";
  static const size_t status_len = sizeof( status ) - 1;

  PeerMatchArgs ka( "tcp", 3 );
  PeerMatchIter iter( *this, ka );
  PeerStats     cur;
  for ( EvSocket *p = iter.first(); p != NULL; p = iter.next() ) {
    char sess[ MAX_SESSION_LEN ];
    if ( ::strcmp( p->kind, "rv" ) == 0 )
      continue;
    if ( p->get_session( this->service_num, sess ) > 0 )
      p->client_stats( cur );
  }
  PeerStats & last = this->other_stat;
  if ( last.bytes_recv < cur.bytes_recv )
    this->stat.br += cur.bytes_recv - last.bytes_recv;
  if ( last.bytes_sent < cur.bytes_sent )
    this->stat.bs += cur.bytes_sent - last.bytes_sent;
  if ( last.msgs_recv < cur.msgs_recv )
    this->stat.mr += cur.msgs_recv - last.msgs_recv;
  if ( last.msgs_sent < cur.msgs_sent )
    this->stat.ms += cur.msgs_sent - last.msgs_sent;
  last = cur;
  RvFwdAdv fwd( *this, NULL, status, status_len, ADV_HOST_STATUS );
}

void
RvHost::send_host_start( EvRvService *svc ) noexcept
{
                               /*_RV.INFO.SYSTEM.HOST.START.*/
  static const char   start[]   = _RV_INFO_HOST_START ".";
  static const size_t start_len = sizeof( start ) - 1;
  if ( ! this->host_started ) {
    RvFwdAdv fwd( *this, svc, start, start_len, ADV_HOST_START );
    this->host_started = true;
  }
}

void
RvHost::send_session_start( EvRvService &svc ) noexcept
{
  this->send_session_start( svc.userid, svc.userid_len,
                            svc.session, svc.session_len );
  svc.svc_state |= EvRvService::SENT_SESSION_START;
}

void
RvHost::send_session_start( EvSocket &sock ) noexcept
{
  char   userid[ MAX_USERID_LEN ],
         session[ MAX_SESSION_LEN ];
  size_t userid_len  = sock.get_userid( userid ),
         session_len = sock.get_session( this->service_num, session );
  this->send_session_start( userid, userid_len, session, session_len );
}

void
RvHost::send_session_start( const char *user,  size_t user_len,
                            const char *session,  size_t session_len ) noexcept
{
                               /*_RV.INFO.SYSTEM.SESSION.START.*/
  static const char   start[]   = _RV_INFO_SESSION_START ".";
  static const size_t start_len = sizeof( start ) - 1;
  RvFwdAdv fwd( *this, user, user_len, session, session_len, start,
                start_len, ADV_SESSION );
}

void
RvHost::send_host_stop( EvRvService *svc ) noexcept
{
                              /*_RV.INFO.SYSTEM.HOST.STOP.*/
  static const char   stop[]   = _RV_INFO_HOST_STOP ".";
  static const size_t stop_len = sizeof( stop ) - 1;
  if ( this->host_started ) {
    RvFwdAdv fwd( *this, svc, stop, stop_len, ADV_HOST_STOP );
    this->host_started = false;
  }
}

void
RvHost::send_session_stop( EvRvService &svc ) noexcept
{
  this->send_session_stop( svc.userid, svc.userid_len,
                           svc.session, svc.session_len );
  svc.svc_state |= EvRvService::SENT_SESSION_STOP;
  this->loss_array.remove_loss_entry( *this, svc.fd );
}

void
RvHost::send_unreachable_tport( EvRvService &svc ) noexcept
{
                              /*_RV.INFO.SYSTEM.UNREACHABLE.TRANSPORT.*/
  static const char   unre[]   = _RV_INFO_UNREACHABLE_TPORT ".";
  static const size_t unre_len = sizeof( unre ) - 1;
  RvFwdAdv fwd( *this, svc.userid, svc.userid_len, svc.session,
                svc.session_len, unre, unre_len, ADV_UNREACHABLE, 0,
                svc.gob, svc.gob_len );
  svc.svc_state |= EvRvService::SENT_SESSION_STOP;
  this->loss_array.remove_loss_entry( *this, svc.fd );
}

void
RvHost::send_session_stop( EvSocket &sock ) noexcept
{
  char   userid[ MAX_USERID_LEN ],
         session[ MAX_SESSION_LEN ];
  size_t userid_len  = sock.get_userid( userid ),
         session_len = sock.get_session( this->service_num, session );
  this->send_session_stop( userid, userid_len, session, session_len );
  this->loss_array.remove_loss_entry( *this, sock.fd );
}

void
RvHost::send_session_stop( const char *user,  size_t user_len,
                           const char *session,  size_t session_len ) noexcept
{
                              /*_RV.INFO.SYSTEM.SESSION.STOP.*/
  static const char   stop[]   = _RV_INFO_SESSION_STOP ".";
  static const size_t stop_len = sizeof( stop ) - 1;
  RvFwdAdv fwd( *this, user, user_len, session, session_len, stop,
                stop_len, ADV_SESSION );
}

void
RvHost::send_listen_start( EvRvService &svc,  const char *sub,  size_t sublen,
                           const char *rep,  size_t replen,
                           uint32_t refcnt ) noexcept
{
  this->send_listen_start( svc.session, svc.session_len, sub, sublen,
                           rep, replen, refcnt );
}

void
RvHost::send_listen_start( const char *session,  size_t session_len,
                           const char *sub,  size_t sublen,
                           const char *rep,  size_t replen,
                           uint32_t refcnt ) noexcept
{
  if ( ! is_restricted_subject( sub, sublen ) ) {
                                 /*_RV.INFO.SYSTEM.LISTEN.START.*/
    static const char   start[]   = _RV_INFO_LISTEN_START ".";
    static const size_t start_len = sizeof( start ) - 1;
    RvFwdAdv fwd( *this, NULL, 0, session, session_len, start,
                  start_len, ADV_LISTEN, refcnt, sub, sublen, rep, replen );
  }
}

void
RvHost::send_listen_stop( EvRvService &svc,  const char *sub,  size_t sublen,
                           uint32_t refcnt ) noexcept
{
  this->send_listen_stop( svc.session, svc.session_len, sub, sublen,
                          refcnt );
}

void
RvHost::send_listen_stop( const char *session,  size_t session_len,
                           const char *sub,  size_t sublen,
                           uint32_t refcnt ) noexcept
{
  if ( ! is_restricted_subject( sub, sublen ) ) {
                                /*_RV.INFO.SYSTEM.LISTEN.STOP.*/
    static const char   stop[]   = _RV_INFO_LISTEN_STOP ".";
    static const size_t stop_len = sizeof( stop ) - 1;
    RvFwdAdv fwd( *this, NULL, 0, session, session_len, stop,
                  stop_len, ADV_LISTEN, refcnt, sub, sublen, NULL, 0 );
  }
}

bool
RvMcast::is_empty_string( const char *s ) noexcept
{
  if ( s != NULL )
    for ( ; *s != '\0' && isspace( *s ); s++ )
      ;
  return ( s == NULL || s[ 0 ] == '\0' );
}

uint32_t
RvMcast::lookup_host_ip4( const char *host ) noexcept
{
  struct addrinfo * h = NULL,
                  * res,
                    hints;
  uint32_t ipaddr = 0;

  ::memset( &hints, 0, sizeof( hints ) );
  hints.ai_family   = AF_INET;
  hints.ai_socktype = SOCK_DGRAM;
  hints.ai_protocol = IPPROTO_UDP;

  if ( ::getaddrinfo( host, NULL, &hints, &h ) == 0 ) {
    for ( res = h; res != NULL; res = res->ai_next ) {
      if ( res->ai_family == AF_INET &&
           res->ai_addrlen >= sizeof( struct sockaddr_in ) ) {
        struct sockaddr_in *in = (struct sockaddr_in *) res->ai_addr;
        ::memcpy( &ipaddr, &in->sin_addr.s_addr, 4 );
        break;
      }
    }
    ::freeaddrinfo( h );
  }
  return ipaddr;
}

uint32_t
RvMcast::lookup_host_ip4( const char *host,  uint32_t &netmask ) noexcept
{
  uint32_t ipaddr;
  netmask = 0;
  if ( (ipaddr = lookup_host_ip4( host )) == 0 )
    return 0;

#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
  ifconf conf;
  ifreq  ifbuf[ 256 ],
       * ifp, ifa, ifm;
  int    s  = ::socket( PF_INET, SOCK_DGRAM, IPPROTO_UDP );

  ::memset( ifbuf, 0, sizeof( ifbuf ) );
  ::memset( &conf, 0, sizeof( conf ) );

  conf.ifc_len = sizeof( ifbuf );
  conf.ifc_buf = (char *) ifbuf;

  if ( ::ioctl( s, SIOCGIFCONF, &conf ) != -1 ) {
    ifp = ifbuf;
    /* for each interface */
    for ( ; (uint8_t *) ifp < &((uint8_t *) ifbuf)[ conf.ifc_len ]; ifp++ ) {
      ::strcpy( ifa.ifr_name, ifp->ifr_name );
      ::strcpy( ifm.ifr_name, ifp->ifr_name );

      /* fetch flags check if multicast exists, get address and netmask */
      if ( ::ioctl( s, SIOCGIFADDR, &ifa )    >= 0 &&
           ifa.ifr_addr.sa_family             == AF_INET &&
           ::ioctl( s, SIOCGIFNETMASK, &ifm ) >= 0 ) {
        uint32_t mask, addr;
        mask = ((struct sockaddr_in &) ifm.ifr_netmask).sin_addr.s_addr;
        addr = ((struct sockaddr_in &) ifa.ifr_addr).sin_addr.s_addr;

        if ( ( addr & mask ) == ( ipaddr & mask ) ) {
          netmask = mask;
          ipaddr  = addr;
          ::close( s );
          goto found_address;
        }
      }
    }
  }
  ::close( s );
  return 0;
#else
  SOCKET s = socket( AF_INET, SOCK_DGRAM, IPPROTO_UDP );
  DWORD  nbytes;
  char   buf_out[ 64 * 1024 ];
  char   buf_in[ 64 * 1024 ];

  if ( ::WSAIoctl( s, SIO_GET_INTERFACE_LIST,
                   buf_in, sizeof( buf_in ),
                   buf_out, sizeof( buf_out ),
                   &nbytes, NULL, NULL ) != SOCKET_ERROR ) {
    LPINTERFACE_INFO info;

    if ( nbytes != 0 ) {
      for ( info = (INTERFACE_INFO *) buf_out;
            info < (INTERFACE_INFO *) &buf_out[ nbytes ];
            info++ ) {
        if ( ( info->iiFlags & IFF_UP ) != 0 &&
             /*( info->iiFlags & IFF_MULTICAST ) != 0 &&*/
             ((struct sockaddr_in &) info->iiAddress).sin_family == AF_INET ) {
          uint32_t mask, addr;
          mask = ((struct sockaddr_in &) info->iiNetmask).sin_addr.s_addr;
          addr = ((struct sockaddr_in &) info->iiAddress).sin_addr.s_addr;

          if ( ( addr & mask ) == ( ipaddr & mask ) ) {
            netmask = mask;
            ipaddr  = addr;
            ::closesocket( s );
            goto found_address;
          }
        }
      }
    }
  }
  ::closesocket( s );
  return 0;
#endif
found_address:;
  return ipaddr;
}

uint32_t
RvMcast::lookup_dev_ip4( const char *dev,  uint32_t &netmask ) noexcept
{
  uint32_t ipaddr = 0;
  netmask = 0;
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
  ifreq    ifa, ifm;
  int      s  = ::socket( PF_INET, SOCK_DGRAM, IPPROTO_UDP );
  if ( ::strlen( dev ) < sizeof( ifa.ifr_name ) ) {
    ::strcpy( ifa.ifr_name, dev );
    ::strcpy( ifm.ifr_name, dev );
    if ( ::ioctl( s, SIOCGIFADDR, &ifa ) >= 0 &&
         ifa.ifr_addr.sa_family          == AF_INET ) {
      ipaddr = ((struct sockaddr_in &) ifa.ifr_addr).sin_addr.s_addr;
      if ( ::ioctl( s, SIOCGIFNETMASK, &ifm ) >= 0 )
        netmask = ((struct sockaddr_in &) ifm.ifr_addr).sin_addr.s_addr;
    }
  }
  ::close( s );
#else
  (void) dev; (void) netmask;
#endif
  return ipaddr;
}

RvHostError
RvMcast::parse_network( const char *network,  size_t net_len ) noexcept
{
  char tmp_buf[ 4 * 1024 ],
       recv_host[ 16 ],
       host[ 256 ],
     * recv_part[ MAX_RECV_MCAST ],
     * send_part,
     * net_part,
     * ptr;
  RvHostError status = HOST_OK;
  ::memset( this, 0, sizeof( *this ) );

  /* network is format: network;mcast,mcast;sendmcast:port,port */
  send_part = NULL;
  if ( net_len > sizeof( tmp_buf ) - 1 )
    net_len = sizeof( tmp_buf ) - 1;
  ::memcpy( tmp_buf, network, net_len );
  tmp_buf[ net_len ] = '\0';

  net_part = tmp_buf;
  while ( isspace( *net_part ) )
    net_part++;

  /* find the recv mcast addresses, after the semicolon on network spec */
  if ( (ptr = ::strchr( net_part, ';' )) != NULL ) {
    *ptr++ = '\0';
    recv_part[ 0 ] = ptr;
    this->recv_cnt = 1;
    /* find the send mcast address, after the semicolon on the recv spec */
    if ( (ptr = ::strchr( ptr, ';' )) != NULL ) {
      *ptr++ = '\0';
      send_part = ptr;
    }
    /* parse list of mcast addrs */
    for ( ptr = recv_part[ 0 ]; ; ) {
      if ( (ptr = ::strchr( ptr, ',' )) == NULL )
        break;
      *ptr++ = '\0';
      recv_part[ this->recv_cnt++ ] = ptr;
      if ( this->recv_cnt == MAX_RECV_MCAST )
        break;
    }
  }
  /* if no recv address, use zeros so that net parser doesn't error */
  if ( this->recv_cnt == 0 || ( this->recv_cnt == 1 &&
                                is_empty_string( recv_part[ 0 ] ) ) ) {
    ::strncpy( recv_host, "0.0.0.0", sizeof( recv_host ) );
    recv_part[ 0 ] = recv_host;
    this->recv_cnt = 1;
  }
  if ( is_empty_string( send_part ) )
    send_part = recv_part[ 0 ];

  /* lookup the hosts */
  this->send_ip = this->lookup_host_ip4( send_part );
  if ( this->send_ip == 0 && ::strcmp( send_part, "0.0.0.0" ) != 0 )
    status = ERR_NETWORK_NOT_FOUND;
  for ( uint32_t i = 0; i < this->recv_cnt; i++ ) {
    this->recv_ip[ i ] = this->lookup_host_ip4( recv_part[ i ] );
    if ( this->recv_ip[ i ] == 0 && ::strcmp( recv_part[ i ], "0.0.0.0" ) != 0 )
      status = ERR_NETWORK_NOT_FOUND;
  }
  if ( is_empty_string( net_part ) ) {
    if ( ::gethostname( host, sizeof( host ) ) != 0 ) {
      host[ 0 ] = '\0';
      status = ERR_GETHOSTNAME_FAILED;
    }
    net_part = host;
  }
  if ( ! is_empty_string( net_part ) ) {
    this->host_ip = this->lookup_dev_ip4( net_part, this->netmask );
    if ( this->host_ip == 0 )
      this->host_ip = this->lookup_host_ip4( net_part, this->netmask );
    if ( this->host_ip == 0 )
      status = ERR_NO_INTERFACE_FOUND;
  }
  return status;
}

void
RvMcast::print( void ) noexcept
{
  uint8_t *q;
  if ( this->host_ip != 0 ) {
    q = (uint8_t *) &this->host_ip;
    printf( "%u.%u.%u.%u", q[ 0 ], q[ 1 ], q[ 2 ], q[ 3 ] );
  }
  if ( this->recv_cnt != 1 || this->recv_ip[ 0 ] != this->send_ip ) {
    q = (uint8_t *) &this->recv_ip[ 0 ];
    printf( ";%u.%u.%u.%u", q[ 0 ], q[ 1 ], q[ 2 ], q[ 3 ] );
    for ( uint32_t i = 1; i < this->recv_cnt; i++ ) {
      q = (uint8_t *) &this->recv_ip[ i ];
      printf( ",%u.%u.%u.%u", q[ 0 ], q[ 1 ], q[ 2 ], q[ 3 ] );
    }
  }
  if ( this->send_ip != 0 ) {
    q = (uint8_t *) &this->send_ip;
    printf( ";%u.%u.%u.%u\n", q[ 0 ], q[ 1 ], q[ 2 ], q[ 3 ] );
  }
  else {
    printf( "\n" );
  }
}

RvDaemonRpc::RvDaemonRpc( RvHost &h ) noexcept
      : EvSocket( h.poll, h.poll.register_type( "rv_daemon_rpc" ) ),
        sub_route( h.sub_route ),
        ibx( h ), host_refs( 0 ), svc( h.service_num )
{
  this->sock_opts = OPT_NO_POLL;
}

int
RvDaemonRpc::init_rpc( void ) noexcept
{
  this->PeerData::init_peer( this->poll.get_next_id(), this->poll.get_null_fd(),
                             this->sub_route.route_id, NULL, "rv_daemon_rpc" );
  return this->poll.add_sock( this );
}

void
RvDaemonRpc::subscribe_daemon_inbox( void ) noexcept
{
  if ( ! this->in_list( IN_ACTIVE_LIST ) )
    this->init_rpc();
  if ( this->host_refs++ == 0 ) {
    if ( is_rv_debug )
      printf( "subscribe daemon %.*s\n", (int) this->ibx.len, this->ibx.buf );
    NotifySub nsub( this->ibx.buf, this->ibx.len, this->ibx.h,
                    false, 'V', *this );
    this->sub_route.add_sub( nsub );
  }
}

void
RvDaemonRpc::unsubscribe_daemon_inbox( void ) noexcept
{
  if ( --this->host_refs == 0 ) {
    if ( is_rv_debug )
      printf( "unsubscribe daemon %.*s\n", (int) this->ibx.len, this->ibx.buf );
    NotifySub nsub( this->ibx.buf, this->ibx.len, this->ibx.h,
                    false, 'V', *this );
    this->sub_route.del_sub( nsub );
  }
}

uint8_t
RvDaemonRpc::is_subscribed( const NotifySub &sub ) noexcept
{
  uint8_t v = 0;
  if ( this->host_refs > 0 && sub.subj_hash == this->ibx.h ) {
    if ( sub.subject_len == this->ibx.len &&
         ::memcmp( sub.subject, this->ibx.buf, this->ibx.len ) == 0 )
      v = EV_SUBSCRIBED;
    else
      v = EV_NOT_SUBSCRIBED | EV_COLLISION;
  }
  else {
    v = EV_NOT_SUBSCRIBED;
  }
  return v;
}

void
RvDaemonRpc::process_close( void ) noexcept
{
  this->client_stats( this->sub_route.peer_stats );
  this->EvSocket::process_close();
}

static bool
match_string( const char *s,  size_t len,  const MDReference &mref )
{
  if ( mref.ftype != MD_STRING )
    return false;
  size_t len2 = mref.fsize;
  if ( s[ len - 1 ] == '\0' )
    len--;
  if ( len2 > 0 && mref.fptr[ len2 - 1 ] == '\0' )
    len2--;
  return len == len2 && ::memcmp( s, mref.fptr, len ) == 0;
}

void
RvDaemonRpc::send_sessions( const void *reply,  size_t reply_len ) noexcept
{
  SubRouteDB    sess_db;
  PeerMatchArgs ka;
  PeerMatchIter iter( *this, ka );
  EvSocket    * p;
  MDMsgMem      mem;
  size_t        buflen = 8; /* header of rvmsg */
  RouteLoc      loc;

  ka.skipme = true;
  for ( p = iter.first(); p != NULL; p = iter.next() ) {
    char sess[ MAX_SESSION_LEN ];
    size_t n;
    if ( (n = p->get_session( this->svc, sess )) > 0 ) {
      uint32_t h = kv_crc_c( sess, n + 1, 0 );
      sess_db.upsert( h, sess, n + 1, loc );
      if ( loc.is_new )
        buflen += n + 8; /* field + type + session str */
    }
  }
  RvMsgWriter msg( mem, mem.make( buflen ), buflen );

  for ( RouteSub *s = sess_db.first( loc ); s != NULL;
        s = sess_db.next( loc ) ) {
    msg.append_string( NULL, 0, s->value, s->len );
  }
  buflen = msg.update_hdr();
  if ( is_rv_debug )
    printf( "pub sessions reply %.*s\n", (int) reply_len, (char *) reply );
  EvPublish pub( (const char *) reply, reply_len, NULL, 0, msg.buf, buflen,
                 this->sub_route, *this, kv_crc_c( reply, reply_len, 0 ),
                 RVMSG_TYPE_ID );
  this->sub_route.forward_msg( pub );
}

void
RvDaemonRpc::send_subscriptions( const char *session,  size_t session_len,
                                 const void *reply,  size_t reply_len ) noexcept
{
  PeerMatchArgs ka;
  PeerMatchIter iter( *this, ka );
  MDMsgMem      mem;
  SubRouteDB    subs, pats;
  char          user[ MAX_USERID_LEN ];
  size_t        buflen  = 0,
                subcnt  = 0,
                patcnt  = 0,
                userlen = 0;
  RouteSub    * s;
  RouteLoc      loc;

  ka.skipme = true;
  if ( session_len > 0 && session[ session_len - 1 ] == '\0' )
    session_len--;
  for ( EvSocket *p = iter.first(); p != NULL; p = iter.next() ) {
    char sess[ MAX_SESSION_LEN ];
    size_t n;
    if ( (n = p->get_session( this->svc, sess )) > 0 ) {
      if ( n == session_len && ::memcmp( sess, session, n ) == 0 ) {
        size_t i = p->get_subscriptions( this->svc, subs ),
               j = p->get_patterns( this->svc, RV_PATTERN_FMT, pats );
        if ( i + j != 0 ) {
          if ( is_rv_host_debug )
            printf( "rv session [%.*s] subcnt %u patcnt %u\n",
                    (int) session_len, session, (uint32_t) i, (uint32_t) j );
          subcnt += i;
          patcnt += j;
          if ( userlen == 0 )
            userlen = p->get_userid( user );
        }
      }
    }
  }
  /* calculate the max length of the message */
  /* hdr + "user" : "name", "end" : 1 */
  buflen = 8 + 8 + 12 + 12 + userlen + 8;
  if ( subcnt > 0 ) {
    for ( s = subs.first( loc ); s != NULL; s = subs.next( loc ) ) {
      if ( ! is_restricted_subject( s->value, s->len ) )
        buflen += (size_t) s->len + 8 + count_segments( s->value, s->len ) * 2;
    }
  }
  if ( patcnt > 0 ) {
    for ( s = pats.first( loc ); s != NULL; s = pats.next( loc ) ) {
      if ( ! is_restricted_subject( s->value, s->len ) )
        buflen += (size_t) s->len + 8 + count_segments( s->value, s->len ) * 2;
    }
  }
  /* create the message with unique subjects */
  RvMsgWriter msg( mem, mem.make( buflen ), buflen );

  if ( userlen == 0 )
    msg.append_string( SARG( "user" ), SARG( "nobody" ) );
  else
    msg.append_string( SARG( "user" ), user, userlen + 1 );

  if ( subcnt > 0 ) {
    for ( s = subs.first( loc ); s != NULL; s = subs.next( loc ) ) {
      if ( ! is_restricted_subject( s->value, s->len ) )
        msg.append_subject( NULL, 0, s->value, s->len );
    }
  }
  if ( patcnt > 0 ) {
    for ( s = pats.first( loc ); s != NULL; s = pats.next( loc ) ) {
      if ( ! is_restricted_subject( s->value, s->len ) )
        msg.append_subject( NULL, 0, s->value, s->len );
    }
  }
  msg.append_int<int32_t>( SARG( "end" ), 1 );
  buflen = msg.update_hdr();
  if ( is_rv_debug )
    printf( "pub subs reply %.*s\n", (int) reply_len, (char *) reply );
  EvPublish pub( (const char *) reply, reply_len, NULL, 0, msg.buf, buflen,
                 this->sub_route, *this, kv_crc_c( reply, reply_len, 0 ),
                 RVMSG_TYPE_ID );
  this->sub_route.forward_msg( pub );
}

bool
RvDaemonRpc::on_msg( EvPublish &pub ) noexcept
{
  if ( pub.reply_len == 0 /*|| pub.msg_enc != (uint8_t) RVMSG_TYPE_ID*/ )
    return true;
  MDMsgMem     mem;
  /*MDOutput     mout;*/
  RvMsg       * m;
  MDFieldIter * it;
  MDReference   mref;

  if ( is_rv_debug )
    printf( "daemon rpc %.*s\n", (int) pub.subject_len, pub.subject );
  m = RvMsg::unpack_rv( (void *) pub.msg, 0, pub.msg_len, 0, NULL, mem );
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

uint32_t
RvHost::add_ref( const char *sub,  size_t sublen,  uint32_t h ) noexcept
{
  if ( h == 0 )
    h = kv_crc_c( sub, sublen, 0 );
  RouteLoc      loc;
  RvDaemonSub * rt = this->sub_map.upsert( h, sub, sublen, loc );
  if ( loc.is_new )
    rt->refcnt = 0;
  return ++rt->refcnt;
}

uint32_t
RvHost::rem_ref( const char *sub,  size_t sublen,  uint32_t h,
                 uint32_t cnt ) noexcept
{
  if ( h == 0 )
    h = kv_crc_c( sub, sublen, 0 );
  RouteLoc      loc;
  RvDaemonSub * rt = this->sub_map.find( h, sub, sublen, loc );
  if ( rt == NULL )
    return 0;
  if ( rt->refcnt <= cnt ) {
    this->sub_map.remove( loc );
    if ( this->sub_map.is_empty() )
      this->sub_map.release();
    return 0;
  }
  rt->refcnt -= cnt;
  return rt->refcnt;
}

void RvHost::write( void ) noexcept {}
void RvHost::read( void ) noexcept {}
void RvHost::process( void ) noexcept {}
void RvHost::release( void ) noexcept {}
void RvDaemonRpc::write( void ) noexcept {}
void RvDaemonRpc::read( void ) noexcept {}
void RvDaemonRpc::process( void ) noexcept {}
void RvDaemonRpc::release( void ) noexcept {}
