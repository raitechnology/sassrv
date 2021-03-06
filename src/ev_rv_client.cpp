#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <ctype.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include <linux/types.h>
#include <linux/ip.h>
#include <linux/if.h>
#include <raikv/util.h>
#include <raikv/ev_publish.h>
#include <sassrv/ev_rv_client.h>
#include <raimd/tib_msg.h>
#include <raimd/tib_sass_msg.h>

using namespace rai;
using namespace sassrv;
using namespace kv;
using namespace md;

int rv_client_pub_verbose,
    rv_client_msg_verbose,
    rv_client_sub_verbose,
    rv_client_init;

static int
getenv_bool( const char *var )
{
  const char *val = ::getenv( var );
  return val != NULL && val[ 0 ] != 'f' && val[ 0 ] != '0';
}

EvRvClient::EvRvClient( EvPoll &p ) noexcept
  : EvConnection( p, p.register_type( "rvclient" ) ), rv_state( VERS_RECV ),
    fwd_all_msgs( 1 ), fwd_all_subs( 1 )
{
  if ( ! rv_client_init ) {
    rv_client_init = 1;
    rv_client_pub_verbose   = getenv_bool( "RV_CLIENT_PUB_VERBOSE" );
    rv_client_msg_verbose   = getenv_bool( "RV_CLIENT_MSG_VERBOSE" );
    rv_client_sub_verbose   = getenv_bool( "RV_CLIENT_SUB_VERBOSE" );
    int all_verbose         = getenv_bool( "RV_CLIENT_VERBOSE" );
    rv_client_pub_verbose  |= all_verbose;
    rv_client_msg_verbose  |= all_verbose;
    rv_client_sub_verbose  |= all_verbose;
  }
  md_init_auto_unpack();
}


bool
EvRvClient::connect( EvRvClientParameters &p,
                     EvConnectionNotify *n ) noexcept
{
  if ( this->fd != -1 )
    return false;
  this->initialize_state();
  if ( EvTcpConnection::connect( *this, p.daemon, p.port, p.opts ) != 0 )
    return false;
  this->network = p.network;
  this->service = p.service;
  this->notify  = n;
  return true;
}

void
EvRvClient::process( void ) noexcept
{
  size_t  buflen, msglen;
  int32_t status = 0;

  /* state from VERS_RECV->INFO_RECV->INIT_RECV->CONN_RECV->DATA_RECV */
  if ( this->rv_state >= INIT_RECV ) { /* main state, rv msg envelope */
  data_recv_loop:;
    do {
      buflen = this->len - this->off;
      if ( buflen < 8 )
        goto break_loop;
      msglen = get_u32<MD_BIG>( &this->recv[ this->off ] );
      if ( buflen < msglen )
        goto break_loop;
      if ( rv_client_msg_verbose )
        this->trace_msg( '<', &this->recv[ this->off ], msglen );
      status = this->dispatch_msg( &this->recv[ this->off ], msglen );
      this->off += msglen;
    } while ( status == 0 );
  }
  else { /* initial connection states */
    for (;;) {
      buflen = this->len - this->off;
      if ( buflen < 8 )
        goto break_loop;
      switch ( this->rv_state ) {
        case VERS_RECV:
          if ( buflen < 3 * 4 )
            goto break_loop;
          if ( rv_client_msg_verbose )
            this->trace_msg( '<', &this->recv[ this->off ], buflen );
          this->off += 3 * 4;
          this->send_vers();          /* recv 12 bytes, send 12 bytes */
          this->rv_state = INFO_RECV;
          break;
        case INFO_RECV:
          if ( buflen < 16 * 4 )
            goto break_loop;
          if ( rv_client_msg_verbose )
            this->trace_msg( '<', &this->recv[ this->off ], buflen );
          if ( get_u32<MD_BIG>( &this->recv[ this->off ] ) != 1 )
            this->send_info();       /* recv 64, send 64, recv 64 */
          else {
            this->send_init_rec();   /* send network, service parms */
            this->rv_state = INIT_RECV;
          }
          this->off += 16 * 4;
          break;
        default: /* rvmsg encapaulation */
          goto data_recv_loop;
      }
    }
  }
break_loop:;
  this->pop( EV_PROCESS );
  this->push_write();
  if ( status != 0 ) {
    this->rv_state = ERR_CLOSE;
    this->push( EV_CLOSE );
  }
}

void
EvRvClient::trace_msg( char dir,  void *msg,  size_t msglen ) noexcept
{
  MDOutput out;
  MDMsgMem mem;
  RvMsg  * m;
  printf( " === %s\n", dir == '>' ? "send ->" : "recv <-" );

  m = RvMsg::unpack_rv( msg, 0, msglen, 0, NULL, &mem );
  if ( m != NULL )
    m->print( &out );
  else {
    out.print_hex( msg, msglen );
  }
}

void
EvRvClient::send_vers( void ) noexcept
{
  uint32_t ver_rec[ 3 ] = { 0, 4, 0 };
  ver_rec[ 1 ] = get_u32<MD_BIG>( &ver_rec[ 1 ] );
  if ( rv_client_pub_verbose )
    this->trace_msg( '>', ver_rec, sizeof( ver_rec ) );
  this->append( ver_rec, sizeof( ver_rec ) );
}

void
EvRvClient::send_info( void ) noexcept
{
  static const uint32_t info_rec_prefix[] = { 3, 2, 0, 1, 0, 4 << 24, 4 << 24 };
  uint32_t info_rec[ 16 ];
  size_t i;

  ::memcpy( info_rec, info_rec_prefix, sizeof( info_rec_prefix ) );
  for ( i = 0; i < sizeof( info_rec_prefix ) / 4; i++ )
    info_rec[ i ] = get_u32<MD_BIG>( &info_rec[ i ] ); /* flip */
  for ( ; i < sizeof( info_rec ) / 4; i++ )
    info_rec[ i ] = 0; /* zero rest */
  if ( rv_client_pub_verbose )
    this->trace_msg( '>', info_rec, sizeof( info_rec ) );
  this->append( info_rec, sizeof( info_rec ) );
}

void
EvRvClient::send_init_rec( void ) noexcept
{
  const char * svc = ( this->service ? this->service : "" ),
             * net = ( this->network ? this->network : "" );
  size_t       svclen = ::strlen( svc ) + 1,
               netlen = ::strlen( net ) + 1,
               size;
  uint8_t      buf[ 8 * 1024 ];
  RvMsgWriter  rvmsg( buf, sizeof( buf ) );

  rvmsg.append_string( SARG( "mtype" ), SARG( "I" ) );
  rvmsg.append_string( SARG( "userid" ), SARG( "nobody" ) );
  if ( this->session_len != 0 )
    rvmsg.append_string( SARG( "session" ), this->session,
                                            this->session_len + 1 );
  rvmsg.append_string( SARG( "service" ), svc, svclen );
  rvmsg.append_string( SARG( "network" ), net, netlen );
  if ( this->control_len != 0 )
    rvmsg.append_string( SARG( "control" ), this->control,
                                            this->control_len + 1 );
  rvmsg.append_int<int32_t>( SARG( "vmaj" ), 5 );
  rvmsg.append_int<int32_t>( SARG( "vmin" ), 4 );
  rvmsg.append_int<int32_t>( SARG( "vupd" ), 2 );
  size = rvmsg.update_hdr();
  if ( rv_client_pub_verbose )
    this->trace_msg( '>', buf, size );
  this->append( buf, size );
}

static bool
match_field( MDFieldIter *it,  const char *fname,  size_t fname_size,
             void *fdata,  size_t &fsize,  MDType t )
{
  MDReference mref;
  if ( it->find( fname, fname_size, mref ) == 0 ) {
    if ( mref.ftype == t && mref.fsize <= fsize ) {
      ::memcpy( fdata, mref.fptr, mref.fsize );
      fsize = mref.fsize;
      return true;
    }
  }
  fsize = 0;
  return false;
}

int
EvRvClient::recv_info( void ) noexcept
{
  static const char init[] = "RVD.INITRESP",
                    refu[] = "RVD.INITREFUSED";
  MDMsgMem      mem;
  RvMsg       * m;
  MDFieldIter * it;

  m = RvMsg::unpack_rv( this->msg_in.data.fptr, 0, this->msg_in.data.fsize, 0,
                        NULL, &mem );
  if ( this->msg_in.sublen != sizeof( init ) - 1 ||
       ::memcmp( this->msg_in.sub, init, sizeof( init ) - 1 ) != 0 ) {
    if ( this->msg_in.sublen == sizeof( refu ) - 1 &&
         ::memcmp( this->msg_in.sub, refu, sizeof( refu ) - 1 ) == 0 ) {
      if ( m != NULL && m->get_field_iter( it ) == 0 ) {
        int32_t err;
        size_t  elen = sizeof( err );
        if ( match_field( it, SARG( "error" ), &err, elen, MD_INT ) ) {
          if ( elen == sizeof( err ) )
            return get_i32<MD_BIG>( &err );
        }
      }
    }
    return ERR_START_HOST_FAILED;
  }

  if ( m != NULL && m->get_field_iter( it ) == 0 ) {
    uint8_t * ip = (uint8_t *) (void *) &this->ipaddr,
            * pt = (uint8_t *) (void *) &this->ipport;
    size_t    alen = sizeof( this->ipaddr ),
              plen = sizeof( this->ipport ),
              glen = sizeof( this->gob );
    if ( match_field( it, SARG( "ipaddr" ), ip,        alen, MD_IPDATA ) &&
         match_field( it, SARG( "ipport" ), pt,        plen, MD_IPDATA ) &&
         match_field( it, SARG( "gob" ),    this->gob, glen, MD_STRING ) ) {
      size_t i;
      char * ptr = this->session;
      this->gob_len = glen - 1;
      /* <ipaddr>.<pid><time><ptr> */
      for ( i = 0; i < 8; i += 2 ) {
        *ptr++ = hexchar2( ( ip[ i/2 ] >> 4 ) & 0xf );
        *ptr++ = hexchar2( ip[ i/2 ] & 0xf );
      }
      *ptr++ = '.';
      this->start_stamp = kv_current_realtime_ns();
      ptr += RvHost::time_to_str( this->start_stamp, ptr );
      this->session_len = (size_t) ( ptr - this->session );
      this->control_len = ::snprintf( this->control, sizeof( this->control ),
                                      "_INBOX.%s.1", this->session );
      this->send_init_rec(); /* resend with session */
      this->rv_state = CONN_RECV;
      return 0;
    }
  }
  return ERR_START_HOST_FAILED;
}

int
EvRvClient::recv_conn( void ) noexcept
{
  static const char conn[] = "_RV.INFO.SYSTEM.RVD.CONNECTED";

  if ( this->msg_in.sublen != sizeof( conn ) - 1 ||
       ::memcmp( this->msg_in.sub, conn, sizeof( conn ) - 1 ) != 0 )
    return ERR_START_HOST_FAILED;

  uint8_t      buf[ 1024 ];
  char         inbox[ 64 ];
  RvMsgWriter  rvmsg( buf, sizeof( buf ) );
  size_t       size;

  rvmsg.append_string( SARG( "mtype" ), SARG( "L" ) );
  /* control: _INBOX.<iphex>.<time>.1 */
  ::memcpy( inbox, this->control, this->control_len + 1 );
  inbox[ this->control_len - 1 ] = '>';
  rvmsg.append_subject( SARG( "sub" ), inbox, this->control_len );
  size = rvmsg.update_hdr();
  if ( rv_client_pub_verbose )
    this->trace_msg( '>', buf, size );
  this->append( buf, size );
  this->rv_state = DATA_RECV;

  /* if all subs are forwarded to RV */
  if ( this->fwd_all_subs )
    this->poll.add_route_notify( *this );
  /* if all msgs are forwarded to RV */
  if ( this->fwd_all_msgs ) {
    uint32_t h = this->poll.sub_route.prefix_seed( 0 );
    this->poll.sub_route.add_pattern_route( h, this->fd, 0 );
  }
  if ( this->notify != NULL )
    this->notify->on_connect( *this );
  return 0;
}

/* dispatch a msg: 'D' - data, forward to subscriptions */
int
EvRvClient::dispatch_msg( void *msgbuf, size_t msglen ) noexcept
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
  if ( this->rv_state < DATA_RECV ) {
    if ( this->rv_state == INIT_RECV ) /* initresp */
      status = this->recv_info();
    else
      status = this->recv_conn();
    if ( status != 0 ) {
      fprintf( stderr, "RV host status: %d/%s\n", status,
               get_rv_host_error( status ) );
    }
    return status;
  }
  /*this->msg_in.print();*/
  if ( this->msg_in.mtype == 'D' ||  /* publish */
       this->msg_in.mtype == 'A' ) { /* advisory */
    this->fwd_pub();
    return 0;
  }
  return -1;
}
/* when a 'D' message is received from client, forward the message data,
 * this also decapsulates the opaque data field and forwards the message
 * with the correct message type attribute:
 * old style { sub: FD.SEC.INST.EX, mtype: 'D', data: <message> }
 * new style { sub: FD.SEC.INST.EX, mtype: 'D', data: { _data_ : <message> } }*/
bool
EvRvClient::fwd_pub( void ) noexcept
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
    uint32_t ft = MDMsg::is_msg_type( msg, 0, msg_len, 0 );
    if ( ft != 0 )
      ftype = (uint8_t) ft;
  }
  EvPublish pub( sub, sublen, rep, replen, msg, msg_len,
                 this->fd, h, NULL, 0, ftype, 'p' );
  return this->poll.forward_msg( pub );
}
/* a message from the network, forward if matched by a subscription only once
 * as it may match multiple wild subscriptions as well as a normal sub
 * each sub is looked up in order to increment the msg count */
bool
EvRvClient::on_msg( EvPublish &pub ) noexcept
{
  if ( pub.src_route == (uint32_t) this->fd )
    return true;

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
  if ( status == 0 )
    status = rvmsg.append_string( SARG( "mtype" ), SARG( "D" ) );
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
EvRvClient::release( void ) noexcept
{
  if ( this->fwd_all_msgs ) {
    uint32_t h = this->poll.sub_route.prefix_seed( 0 );
    this->poll.sub_route.del_pattern_route( h, this->fd, 0 );
  }
  if ( this->fwd_all_subs )
    this->poll.remove_route_notify( *this );
  if ( this->notify != NULL )
    this->notify->on_shutdown( *this, NULL, 0 );
  this->EvConnection::release_buffers();
}
/* a new subscription */
void
EvRvClient::do_sub( const char *sub,  size_t sublen,
                    const char *rep,  size_t replen ) noexcept
{
  uint8_t buf[ 2 * 1024 ], * b = buf;
  size_t  len    = sublen + replen,
          buflen = sizeof( buf );

  if ( buflen < len * 2 + 32 ) {
    b = (uint8_t *) this->alloc_temp( len * 2 + 32 );
    buflen = sublen * 2 + 32;
    if ( b == NULL )
      return;
  }
  RvMsgWriter msg( b, buflen );
  msg.append_string( SARG( "mtype" ), SARG( "L" ) );
  msg.append_subject( SARG( "sub" ), sub, sublen );
  if ( replen > 0 ) {
    msg.append_string( SARG( "return" ), rep, replen + 1 );
    b[ msg.off - 1 ] = '\0';
  }
  size_t size = msg.update_hdr();
  if ( rv_client_sub_verbose )
    this->trace_msg( '>', buf, size );
  this->append( buf, size );
}

void
EvRvClient::on_sub( uint32_t /*h*/,  const char *sub,  size_t sublen,
                    uint32_t /*src_fd*/,  uint32_t /*rcnt*/,  char /*st*/,
                    const char *rep,  size_t rlen ) noexcept
{
  this->do_sub( sub, sublen, rep, rlen );
  this->idle_push( EV_WRITE );
}
/* an unsubscribed sub */
void
EvRvClient::on_unsub( uint32_t /*h*/,  const char *sub,  size_t sublen,
                      uint32_t /*src_fd*/,  uint32_t rcnt,
                      char /*src_type*/ ) noexcept
{
  uint8_t buf[ 2 * 1024 ], * b = buf;
  size_t  buflen = sizeof( buf );

  if ( rcnt != 0 ) /* if no routes left */
    return;
  
  if ( buflen < sublen * 2 + 32 ) {
    b = (uint8_t *) this->alloc_temp( sublen * 2 + 32 );
    buflen = sublen * 2 + 32;
    if ( b == NULL )
      return;
  }
  RvMsgWriter msg( b, buflen );
  msg.append_string( SARG( "mtype" ), SARG( "C" ) );
  msg.append_subject( SARG( "sub" ), sub, sublen );
  size_t size = msg.update_hdr();
  if ( rv_client_sub_verbose )
    this->trace_msg( '>', buf, size );
  this->append( buf, size );
}
/* a new pattern subscription */
void
EvRvClient::do_psub( const char *prefix,  uint8_t prefix_len ) noexcept
{
  uint8_t buf[ 2 * 1024 ];
  char    sub[ 1024 ];
  size_t  sublen = prefix_len;

  ::memcpy( sub, prefix, sublen );
  if ( sublen != 0 )
    sub[ sublen++ ] = '.';
  sub[ sublen++ ] = '>';
  sub[ sublen ] = '\0';

  RvMsgWriter msg( buf, sizeof( buf ) );
  msg.append_string( SARG( "mtype" ), SARG( "L" ) );
  msg.append_subject( SARG( "sub" ), sub, sublen );
  size_t size = msg.update_hdr();
  if ( rv_client_sub_verbose )
    this->trace_msg( '>', buf, size );
  this->append( buf, size );
}

void
EvRvClient::on_psub( uint32_t/*h*/,  const char * /*pat*/,  size_t /*patlen*/,
                     const char *prefix,  uint8_t prefix_len,
                     uint32_t /*src_fd*/,  uint32_t /*rcnt*/,
                     char /*src_type*/ ) noexcept
{
  this->do_psub( prefix, prefix_len );
  this->idle_push( EV_WRITE );
}
/* an unsubscribed pattern sub */
void
EvRvClient::on_punsub( uint32_t/*h*/,  const char * /*pat*/,  size_t /*patlen*/,
                       const char *prefix,  uint8_t prefix_len,
                       uint32_t /*src_fd*/,  uint32_t rcnt,
                       char /*src_type*/ ) noexcept
{
  bool fwd = false;
  if ( rcnt == 0 ) {
    if ( prefix_len > 0 && prefix[ prefix_len - 1 ] == '.' )
      fwd = true;
  }
  else if ( rcnt == 1 ) {
    if ( prefix_len == 0 ) /* EvRvClient is subscribed to > */
      fwd = true;
  }
  if ( ! fwd )
    return;

  uint8_t buf[ 2 * 1024 ];
  char    sub[ 1024 ];
  size_t  sublen = prefix_len;

  ::memcpy( sub, prefix, sublen );
  if ( sublen != 0 ) {
    sub[ sublen++ ] = '.';
  }
  sub[ sublen++ ] = '>';
  sub[ sublen ] = '\0';

  RvMsgWriter msg( buf, sizeof( buf ) );
  msg.append_string( SARG( "mtype" ), SARG( "C" ) );
  msg.append_subject( SARG( "sub" ), sub, sublen );
  size_t size = msg.update_hdr();
  if ( rv_client_sub_verbose )
    this->trace_msg( '>', buf, size );
  this->append( buf, size );
  this->idle_push( EV_WRITE );
}
/* reassert subs after reconnect */
void
EvRvClient::on_reassert( uint32_t /*fd*/,  kv::RouteVec<kv::RouteSub> &sub_db,
                          kv::RouteVec<kv::RouteSub> &pat_db ) noexcept
{
  RouteLoc   loc;
  RouteSub * sub;

  for ( sub = sub_db.first( loc ); sub != NULL; sub = sub_db.next( loc ) ) {
    this->do_sub( sub->value, sub->len, NULL, 0 );
  }
  for ( sub = pat_db.first( loc ); sub != NULL; sub = pat_db.next( loc ) ) {
    this->do_psub( sub->value, sub->len );
  }
  this->idle_push( EV_WRITE );
}
