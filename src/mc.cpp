#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
#include <sys/socket.h>
#include <arpa/inet.h>
#else
#include <raikv/win.h>
#endif
#include <sassrv/mc.h>
#include <sassrv/rv_host.h>
#include <raimd/md_types.h>

using namespace rai;
using namespace kv;
using namespace md;
using namespace sassrv;
using namespace trdp;

bool
TrdpHdr::copy_in( const void *msg_buf,  size_t msg_len ) noexcept
{
  const uint8_t * msg = (const uint8_t *) msg_buf;
  if ( msg_len < TRDP_HDR_SIZE )
    return false;
  this->magic = get_u32<MD_BIG>( msg ); msg += 4;
  if ( this->magic != 0x88fa6923U )
    return false;

  this->src_addr  = get_u32<MD_BIG>( msg ); msg += 4;
  this->src_id    = get_u32<MD_BIG>( msg ); msg += 4;
  this->dest_addr = get_u32<MD_BIG>( msg ); msg += 4;
  this->pkt_type  = get_u16<MD_BIG>( msg ); msg += 2;
  this->status    = get_u16<MD_BIG>( msg ); msg += 2;

  switch ( this->type() ) {
    case DATA:
      if ( msg_len < TRDP_DATA_HDR_SIZE )
        return false;
      this->data.seqno     = get_u32<MD_BIG>( msg ); msg += 4;
      this->data.total_len = get_u32<MD_BIG>( msg ); msg += 4;
      this->data.data_len  = get_u16<MD_BIG>( msg ); msg += 2;
      this->data.frag_num  = get_u16<MD_BIG>( msg ); msg += 2;
      break;
    case NAK:
      if ( msg_len < TRDP_HDR_SIZE + 4 )
        return false;
      this->nak.seqno_len = get_u32<MD_BIG>( msg ); msg += 4;
      break;
    case SPM:
    case NAK_ERR:
    case P2P_DATA:
    case P2P_SYN:
    case P2P_ACK:
    case P2P_NAK:
      if ( msg_len < TRDP_HDR_SIZE + 4 )
        return false;
      this->seqno = get_u32<MD_BIG>( msg ); msg += 4;
      break;
  }
  return true;
}

TrdpWindowPkt *
TrdpWindow::merge( const TrdpHdr &hdr,  const void *data,
                   uint64_t cur_mono_time ) noexcept
{
  static const uint64_t reliability_ns = 1000000000;
  TrdpWindowPkt *p = this->list.hd, *x, *y;

  while ( p != NULL ) {
    if ( p->seqno == hdr.seqno ) {
      this->repeat_seqno++;
      return NULL;
    }
    if ( p->seqno > hdr.seqno )
      break;
    p = p->next;
  }
  x = new ( ::malloc( sizeof( TrdpWindowPkt ) + hdr.data.data_len ) )
    TrdpWindowPkt( hdr, data, cur_mono_time );
  this->save_size += hdr.data.data_len;
  if ( p == NULL ) {
    this->list.push_tl( x );
    if ( x == this->list.hd )
      return NULL;
  }
  else {
    this->list.insert_before( x, p );
  }

  x = this->list.hd;
  int32_t sdiff = seqno_diff( x->seqno, this->last_seqno + 1 );
  if ( sdiff != 0 ) {
    if ( cur_mono_time - this->last_mono_time < reliability_ns )
      return NULL;
    this->lost_seqno += sdiff > 0 ? sdiff : -sdiff;
    this->last_seqno = x->seqno - 1;
    this->output_len = 0;
  }
  TrdpWindowPktList ready;
  do {
    uint32_t total_len = x->total_len;
    total_len -= x->data_len;
    y = x;
    if ( total_len > 0 ) {
      for ( y = x->next; ; ) {
        if ( y == NULL || y->seqno != x->seqno + 1 )
          break;
        total_len -= y->data_len;
        if ( total_len == 0 )
          break;
        x = y;
        y = y->next;
      }
    }
    if ( total_len == 0 ) {
      do {
        x = this->list.pop_hd();
        ready.push_tl( x );
        this->save_size -= x->data_len;
      } while ( x != y );
      this->last_seqno = y->seqno;
      x = this->list.hd;
      if ( x == NULL )
        break;
    }
  } while ( x->seqno == this->last_seqno + 1 );

  if ( ready.hd != NULL ) {
    this->last_mono_time = ready.tl->mono_time;
    return ready.hd;
  }
  return NULL;
}

TrdpWindow &
TrdpWindowDB::get_window( TrdpTsid &tsid ) noexcept
{
  uint32_t k;
  size_t pos;
  if ( this->hash == NULL )
    this->hash = TrdpHashTab::resize( NULL );
  if ( ! this->hash->find( tsid, pos, k ) ) {
    this->hash->set_rsz( this->hash, tsid, pos, this->windows.count );
    TrdpWindow &w = this->windows.push();
    w.tsid = tsid;
    return w;
  }
  return this->windows[ k ];
}

void
TrdpWindowDB::process_msg( void *msg,  size_t msg_len,
                           uint64_t cur_mono_time ) noexcept
{
  TrdpHdr hdr;
  if ( ! hdr.copy_in( msg, msg_len ) )
    return;
  TrdpTsid tsid( hdr.tsid() );
  int t = hdr.type();
  if ( t != DATA ) {
    if ( t == NAK ) {
      TrdpWindow &w = this->get_window( tsid );
      w.nak_count++;
    }
    return;
  }
  char * payload = (char *) msg + TRDP_DATA_HDR_SIZE;
  TrdpWindow &w = this->get_window( tsid );
  if ( w.last_mono_time == 0 ) {
    if ( hdr.data.frag_num != 0 )
      return;
    w.last_seqno     = hdr.seqno;
    w.last_mono_time = cur_mono_time;
    this->output( w, hdr.data.data_len, payload );
  }
  else {
    int32_t diff = seqno_diff( hdr.seqno, w.last_seqno );
    if ( diff <= 0 )
      return;
    if ( diff > 1 )
      w.reorder_seqno += diff - 1;
    if ( diff > 1 ||
         ( diff == 1 && hdr.data.total_len != hdr.data.data_len ) ) {
      TrdpWindowPkt *p = w.merge( hdr, payload, cur_mono_time );
      while ( p != NULL ) {
        this->output( w, p->data_len, p->data );
        TrdpWindowPkt *n = p->next;
        delete p;
        p = n;
      }
    }
    else {
      this->output( w, hdr.data.data_len, payload );
      w.last_seqno++;
      w.last_mono_time = cur_mono_time;
    }
  }
}

void
TrdpWindowDB::output( TrdpWindow &w,  size_t len,  const void *data ) noexcept
{
  this->output_bytes += len;
  if ( w.output_len != 0 ) {
    if ( w.output_off + len <= w.output_len ) {
      ::memcpy( &((char *) w.output_buf)[ w.output_off ], data, len );
      w.output_off += len;
      if ( w.output_off == w.output_len ) {
        this->recv_msg( w.output_buf, w.output_len );
        w.output_off = 0;
        w.output_len = 0;
      }
    }
  }
  else if ( len >= 8 ) {
    for (;;) {
      uint32_t magic    = get_u32<MD_BIG>( &((uint8_t *) data)[ 4 ] );
      size_t   msg_size = get_u32<MD_BIG>( &((uint8_t *) data)[ 0 ] );
      if ( magic == 0x9955eeaaU ) {
        if ( msg_size <= len ) {
          this->recv_msg( data, msg_size );
          len -= msg_size;
          data = &((uint8_t *) data)[ msg_size ];
          if ( len < 8 )
            break;
        }
        else {
          if ( w.output_len < msg_size )
            w.output_buf = ::realloc( w.output_buf, msg_size );
          w.output_off = len;
          w.output_len = msg_size;
          ::memcpy( w.output_buf, data, len );
          break;
        }
      }
    }
  }
}

void
TrdpWindowDB::recv_msg( const void *msg,  size_t msg_len ) noexcept
{
  this->msgs_recv++;
  if ( this->conn != NULL ) {
    if ( this->conn->off > 0 )
      this->conn->adjust_recv();
    if ( this->conn->len + msg_len > this->conn->recv_size )
      this->conn->resize_recv_buf( msg_len );
    ::memcpy( &this->conn->recv[ this->conn->len ], msg, msg_len );
    this->conn->len += msg_len;
    this->conn->bytes_recv += msg_len;
    this->conn->recv_count++;
    this->conn->read_ns = this->conn->poll.now_ns;
    this->conn->idle_push( EV_PROCESS );
  }
}

void
UdpSvc::print_addr( const char *what,  const void *sa ) noexcept
{
  struct sockaddr_in * p = (struct sockaddr_in *) sa;

  if ( p != NULL && p->sin_family == AF_INET ) {
    char buf[ 256 ];
    inet_ntop( AF_INET, &p->sin_addr, buf, sizeof( buf ) );
    printf( "%s: %s:%u\n", what, buf, ntohs( p->sin_port ) );
  }
}

void
UdpSvc::process( void ) noexcept
{
  if ( this->in_moff >= this->in_nmsgs ) {
    this->pop( EV_PROCESS );
    return;
  }
  void * msg;
  size_t msg_len;

  uint32_t cnt = this->in_nmsgs - this->in_moff;
  for ( uint32_t i = 0; i < cnt; i++ ) {
    uint32_t  j  = this->in_moff++;
    mmsghdr & ih = this->in_mhdr[ j ];
    if ( ih.msg_hdr.msg_iovlen != 1 )
      continue;

    msg     = ih.msg_hdr.msg_iov[ 0 ].iov_base;
    msg_len = ih.msg_len;

    this->db.process_msg( msg, msg_len, this->poll.mono_ns );
    this->msgs_recv++;
  }
  this->pop( EV_PROCESS );
  this->clear_buffers();
}

void
UdpSvc::release( void ) noexcept
{
  printf( "release %.*s\n",
          (int) this->get_peer_address_strlen(), this->peer_address.buf );
  this->EvUdp::release_buffers();
}

void
UdpSvc::process_shutdown( void ) noexcept
{
  printf( "shutdown %.*s\n",
          (int) this->get_peer_address_strlen(), this->peer_address.buf );
  this->pushpop( EV_CLOSE, EV_SHUTDOWN );
}

void
UdpSvc::process_close( void ) noexcept
{
  printf( "close %.*s\n",
          (int) this->get_peer_address_strlen(), this->peer_address.buf );
  this->EvSocket::process_close();
}
#if 0
bool
TrdpSvc::timer_cb( uint64_t /*timer_id*/,  uint64_t /*event_id*/ ) noexcept
{
  TrdpWindowDB &db = this->db;
  uint64_t delta = db.msgs_recv - db.last_msgs_recv,
           delta_b = db.output_bytes - db.last_output_bytes,
           delta_l = 0, delta_n,
           save_size = 0, lost_seqno = 0, nak_count = 0;
  for ( size_t k = 0; k < db.windows.count; k++ ) {
    TrdpWindow &w = db.windows[ k ];
    save_size  += w.save_size;
    lost_seqno += w.lost_seqno;
    nak_count  += w.nak_count;
  }
  delta_l = lost_seqno - db.last_lost;
  delta_n = nak_count - db.last_nak;
  db.last_msgs_recv    = db.msgs_recv;
  db.last_output_bytes = db.output_bytes;
  db.last_lost         = lost_seqno;
  db.last_nak          = nak_count;

  printf( "msgs %u bytes %u save %u lost %u nak %u\n",
          (uint32_t) delta, (uint32_t) delta_b,
          (uint32_t) save_size, (uint32_t) delta_l,
          (uint32_t) delta_n );
  return true;
}
#endif

uint64_t
TrdpWindowDB::lost_seqno( void ) noexcept
{
  uint64_t lost_seqno = 0;
  this->lost_seqno_list.count = 0;
  for ( size_t k = 0; k < this->windows.count; k++ ) {
    TrdpWindow &w = this->windows[ k ];
    lost_seqno += w.lost_seqno;
    if ( w.last_lost < w.lost_seqno ) {
      w.last_lost = w.lost_seqno;
      this->lost_seqno_list.push( w.tsid.src_addr );
    }
  }
  return lost_seqno;
}

uint64_t
TrdpWindowDB::repeat_seqno( void ) noexcept
{
  uint64_t repeat_seqno = 0;
  this->repeat_seqno_list.count = 0;
  for ( size_t k = 0; k < this->windows.count; k++ ) {
    TrdpWindow &w = this->windows[ k ];
    repeat_seqno += w.repeat_seqno;
    if ( w.last_repeat < w.repeat_seqno ) {
      w.last_repeat = w.repeat_seqno;
      this->repeat_seqno_list.push( w.tsid.src_addr );
    }
  }
  return repeat_seqno;
}

uint64_t
TrdpWindowDB::reorder_seqno( void ) noexcept
{
  uint64_t reorder_seqno = 0;
  this->reorder_seqno_list.count = 0;
  for ( size_t k = 0; k < this->windows.count; k++ ) {
    TrdpWindow &w = this->windows[ k ];
    reorder_seqno += w.reorder_seqno;
    if ( w.last_reorder < w.reorder_seqno ) {
      w.last_reorder = w.reorder_seqno;
      this->reorder_seqno_list.push( w.tsid.src_addr );
    }
  }
  return reorder_seqno;
}

uint64_t
TrdpWindowDB::nak_count( void ) noexcept
{
  uint64_t nak_count = 0;
  this->nak_count_list.count = 0;
  for ( size_t k = 0; k < this->windows.count; k++ ) {
    TrdpWindow &w = this->windows[ k ];
    nak_count += w.nak_count;
    if ( w.last_nak < w.nak_count ) {
      w.last_nak = w.nak_count;
      this->nak_count_list.push( w.tsid.src_addr );
    }
  }
  return nak_count;
}

TrdpSvc *
TrdpSvc::create( EvPoll &poll,  const char *network,
                 const char *service ) noexcept
{
  int port = ( service == NULL ? 0 : atoi( service ) );
  if ( port == 0 )
    port = 7500;

  RvMcast mcast;
  int status;
  if ( (status = mcast.parse_network( network, ::strlen( network ) )) != 0 ) {
    fprintf( stderr, "invalid network %s status %d\n", network, status );
    return NULL;
  }
  if ( mcast.recv_cnt == 0 ) {
    fprintf( stderr, "no multicast addresses\n" );
    return NULL;
  }
  TrdpSvc * svc = new ( aligned_malloc( sizeof( TrdpSvc ) ) ) TrdpSvc();
  for ( uint32_t i = 0; i < mcast.recv_cnt; i++ ) {
    UdpSvc  * udp = new ( aligned_malloc( sizeof( UdpSvc ) ) )
                    UdpSvc( poll, &svc->db );
    svc->udp.push( udp );
    char buf[ 64 ];
    uint16_t j = RvMcast::ip4_string( mcast.host_ip, buf );
    buf[ j++ ] = ';';
    RvMcast::ip4_string( mcast.recv_ip[ i ], &buf[ j ] );
    udp->listen2( buf, port, DEFAULT_UDP_LISTEN_OPTS |
                             OPT_REUSEADDR | OPT_REUSEPORT, "trdp_svc", -1 );
#if 0
    TimerQueue timer_q = poll.timer;
    timer_q.add_timer_seconds( *svc, 1, poll.now_ns, 0 );
#endif
  }
  return svc;
}

