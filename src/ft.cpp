#include <stdio.h>
#include <stdlib.h>
#include <string.h> 
#include <stdint.h>
#include <stdarg.h>
#include <time.h>
#include <netinet/in.h>
#include <sassrv/ev_rv_client.h>
#include <sassrv/ft.h>
#include <raimd/md_msg.h>
#include <raimd/sass.h>
#include <raikv/ev_publish.h>

using namespace rai;
using namespace kv;
using namespace sassrv;
using namespace md;

static const uint64_t NS = 1000 * 1000 * 1000;

const char * RvFt::action_str[ RVFT_ACTION_COUNT ]     = RVFT_ACTION_STR;
const char * RvFt::state_str[ RVFT_STATE_COUNT ]       = RVFT_STATE_STR;
const char * RvFt::msg_type_str[ RVFT_MSG_TYPE_COUNT ] = RVFT_MSG_TYPE_STR;

RvFt::RvFt( EvRvClient &c,  RvFtListener *ftl ) noexcept
    : poll( c.poll ), client( c ), peer_ht( 0 ), cb( ftl )
{
  this->me.zero();
  this->me.update_state( STATE_LISTEN, this->state_count );
  this->peer_ht  = FtPeerHT::resize( NULL );
  uint8_t * x = (uint8_t *) (void *) &this->tid,
          * y = (uint8_t *) (void *) &this[ 1 ];
  ::memset( x, 0, y - x );
}

void RvFtListener::on_ft_change( uint8_t ) noexcept {}
void RvFtListener::on_ft_sync( EvPublish & ) noexcept {}

static char * hms( uint64_t now,  char *buf ) noexcept {
  struct tm tm;
  time_t t = now / NS;
  uint32_t ms = ( now % NS ) / 1000000;
  localtime_r( &t, &tm );
  ::snprintf( buf, 32, "%02d:%02d:%02d.%u%u%u",
              tm.tm_hour, tm.tm_min, tm.tm_sec,
              ms / 100, ( ms / 10 ) % 10, ms % 10);
  return buf;
}

void
RvFt::notify_change( uint8_t action ) noexcept
{
  this->cb->on_ft_change( action );
  this->last_seqno = this->ft_queue.seqno;
}

bool
RvFt::notify_update( void ) noexcept
{
  if ( this->last_seqno != this->ft_queue.seqno ) {
    this->cb->on_ft_change( ACTION_UPDATE );
    this->last_seqno = this->ft_queue.seqno;
    return true;
  }
  return false;
}

void
RvFt::start( FtParameters &param ) noexcept
{
  this->poll.update_time_ns();
  this->me.start_ns    = this->poll.now_ns;
  this->me.last_rcv_ns = 0;
  this->me.weight      = param.weight;
  this->me.update_state( STATE_LISTEN, this->state_count );

  this->tid            = this->poll.now_ns;
  this->state_ns       = this->poll.mono_ns;
  this->start_mono     = this->poll.mono_ns;
  this->join_ms        = param.join_ms;
  this->heartbeat_ms   = param.heartbeat_ms;
  this->activate_ms    = param.activate_ms;
  this->prepare_ms     = param.prepare_ms;
  this->finish_ms      = param.finish_ms;
  this->inbox_num      = param.inbox_num;
  this->sync_inbox_num = param.sync_inbox_num;
  this->client.make_inbox( this->sync_inbox, param.sync_inbox_num );
  this->ft_sub         = param.ft_sub;
  this->ft_sub_len     = param.ft_sub_len;
  this->accuracy_warn  = 100;

  size_t len = param.user_len;
  if ( len >= sizeof( this->me.user ) )
    len = sizeof( this->me.user ) - 1;
  ::memcpy( this->me.user, param.user, len );
  this->me.user[ len ] = '\0';
  this->client.subscribe( this->ft_sub, this->ft_sub_len );

  if ( this->join_ms != 0 ) {
    this->timer_active( ACTION_JOIN );
    this->poll.timer.add_timer_millis( *this, this->join_ms, this->tid,
                                       ACTION_JOIN );
  }
  this->start_hb( param.heartbeat_ms, param.activate_ms, param.prepare_ms );
  this->set_state( STATE_LISTEN );
  this->notify_change( ACTION_LISTEN );
}

uint32_t
RvFt::stop( void ) noexcept
{
  if ( this->finish_ms == 0 ) {
    this->set_state( STATE_FINISH );
    return 0;
  }
  if ( ! this->timer_test_set( ACTION_FINISH ) ) {
    this->send_msg( FINISH );
    this->poll.timer.add_timer_millis( *this, this->finish_ms, this->tid,
                                       ACTION_FINISH );
  }
  return this->finish_ms;
}

void
RvFt::activate( void ) noexcept
{
  if ( this->me.state == STATE_LISTEN ) {
    this->start_mono = this->poll.mono_ns;
    if ( this->timer_clear( ACTION_JOIN ) )
      this->poll.timer.remove_timer_cb( *this, this->tid, ACTION_JOIN );
    this->set_state( STATE_JOIN );
  }
}

void
RvFt::deactivate( void ) noexcept
{
  if ( this->me.state != STATE_LISTEN ) {
    this->ft_queue.remove( &this->me );
    this->stop_timers();
    this->set_state( STATE_LISTEN );
    this->notify_change( ACTION_LISTEN );
  }
}

void
RvFt::finish( void ) noexcept
{
  if ( this->me.state != STATE_LISTEN )
    this->send_msg( EXITING );
  if ( this->timer_clear( ACTION_HEARTBEAT ) )
    this->poll.timer.remove_timer_cb( *this, this->tid, ACTION_HEARTBEAT );
  this->stop_timers();
  this->client.unsubscribe( this->ft_sub, this->ft_sub_len );
  this->notify_change( ACTION_FINISH );
}

void
RvFt::stop_timers( void ) noexcept
{
  TimerQueue & timer = this->poll.timer;
  if ( this->timer_clear( ACTION_JOIN ) )
    timer.remove_timer_cb( *this, this->tid, ACTION_JOIN );
  if ( this->timer_clear( ACTION_ACTIVATE ) )
    timer.remove_timer_cb( *this, this->tid, ACTION_ACTIVATE );
  if ( this->timer_clear( ACTION_PREPARE ) )
    timer.remove_timer_cb( *this, this->tid, ACTION_PREPARE );
  if ( this->timer_clear( ACTION_FINISH ) )
    timer.remove_timer_cb( *this, this->tid, ACTION_FINISH );
}

void
FtStateCount::update( uint8_t old_state,  uint8_t new_state ) noexcept
{
  switch ( old_state ) {
    default: break;
    case RvFt::STATE_JOIN:      this->join--;      break;
    case RvFt::STATE_PRIMARY:   this->primary--;   break;
    case RvFt::STATE_SECONDARY: this->secondary--; break;
  }
  switch ( new_state ) {
    default: break;
    case RvFt::STATE_JOIN:      this->join++;      break;
    case RvFt::STATE_PRIMARY:   this->primary++;   break;
    case RvFt::STATE_SECONDARY: this->secondary++; break;
  }
}

void
RvFt::set_state( FtState state ) noexcept
{
  uint8_t old_state = this->me.state;
  if ( state != this->me.state ) {
    this->me.update_state( state, this->state_count );
    this->state_ns = this->poll.mono_ns;

    if ( old_state == STATE_JOIN && state > STATE_JOIN &&
         state < STATE_FINISH ) {
      this->ft_queue.update( &this->me );
    }
  }
  switch ( state ) {
    case STATE_LISTEN:
      this->send_msg( ANNOUNCE );
      break;

    case STATE_JOIN:
      this->ft_queue.insert( &this->me );
      this->send_msg( ANNOUNCE );
      this->prepare_takeover( ACTION_JOIN, 0 );
      this->notify_change( ACTION_JOIN );
      break;

    case STATE_PRIMARY:
      this->activate_ns = this->state_ns;
      this->notify_change( ACTION_ACTIVATE );
      this->send_msg( ACTIVATE );
      break;

    case STATE_SECONDARY:
      if ( old_state != STATE_SECONDARY ) {
        this->deactivate_ns = this->state_ns;
        this->send_msg( DEACTIVATE );
        this->notify_change( ACTION_DEACTIVATE );
      }
      this->prepare_takeover( ACTION_DEACTIVATE, 0 );
      break;

    case STATE_FINISH:
    default:
      this->finish();
      break;
  }
}

void
RvFt::set_timer( FtAction action,  uint64_t delta_ms,
                 uint64_t &ori_ns,  uint64_t &exp_ns ) noexcept
{
  TimerQueue & timer     = this->poll.timer;
  FtPeer     * p         = this->ft_queue.ptr[ 0 ];
  uint64_t     now       = this->poll.mono_ns,
               origin_ns = this->start_mono,
               expire_ns;

  if ( this->me.state != STATE_JOIN && p != &this->me )
    origin_ns = p->last_rcv_ns;

  expire_ns = ( origin_ns + delta_ms * 1000 * 1000 );
  if ( expire_ns > now )
    expire_ns -= now;
  else
    expire_ns = 0;

  if ( this->timer_is_set( action ) ) {
    if ( ori_ns + exp_ns == origin_ns + expire_ns )
      return;
    timer.remove_timer_cb( *this, this->tid, action );
  }
  this->timer_active( action );

  ori_ns = origin_ns;
  exp_ns = expire_ns;
  uint32_t expire_us = ( expire_ns + 999 ) / 1000 + 1000;
  if ( expire_us <= (uint64_t) MAX_TIMER_RANGE ) {
    timer.add_timer_micros( *this, expire_us, this->tid, action );
  }
  else {
    uint32_t expire_ms = ( expire_ns + 999999 ) / ( 1000 * 1000 ) + 1;
    timer.add_timer_millis( *this, expire_ms, this->tid, action );
  }
}

void
RvFt::set_prepare_timer( void ) noexcept
{
  this->set_timer( ACTION_PREPARE, this->prepare_ms,
                   this->prepare_origin, this->prepare_expires );
}

void
RvFt::set_activate_timer( void ) noexcept
{
  this->set_timer( ACTION_ACTIVATE, this->activate_ms,
                   this->activate_origin, this->activate_expires );
}

int64_t
RvFt::expired_delta_ms( uint64_t stamp_ns,  uint64_t delta_ms ) noexcept
{
  uint64_t now   = this->poll.mono_ns,
           delta = ( stamp_ns + delta_ms * 1000 * 1000 );
  if ( delta < now )
    return -(int64_t) ( ( now - delta ) / 1000 / 1000 );
  return ( delta - now ) / 1000 / 1000;
}

void
RvFt::trim_ft_queue( void ) noexcept
{
  size_t i = this->ft_queue.count;
  while ( i > 0 ) {
    FtPeer * p = this->ft_queue.ptr[ --i ];
    if ( p != &this->me &&
         this->expired_delta_ms( p->last_rcv_ns, this->activate_ms ) <= 0 ) {
      size_t pos;
      fprintf( stderr, "FT Peer %s is missing\n", p->user );
      p->update_state( STATE_FINISH, this->state_count );
      this->ft_queue.remove( p );
      if ( this->peer_ht->find( p->start_ns, pos ) )
        this->peer_ht->remove( pos );
      this->release_peer( p );
    }
  }
}

void
RvFt::prepare_takeover( uint8_t action,  uint64_t deadline ) noexcept
{
  FtPeer * p = this->ft_queue.ptr[ 0 ];
  uint64_t origin_ns;
  if ( p == &this->me || this->me.state == STATE_JOIN ) {
    origin_ns = this->start_mono;
    if ( this->expired_delta_ms( origin_ns, this->activate_ms ) <= 0 ) {
      if ( this->me.state == STATE_JOIN ) {
        /*this->me.state = STATE_SECONDARY;
        this->ft_queue.update( &this->me );
        this->me.state = STATE_JOIN;*/
        if ( p != &this->me && FtPeer::is_greater_weight( this->me, *p ) )
          p = &this->me;
        /*p = this->ft_queue.ptr[ 0 ];*/
      }
      if ( p == &this->me )
        this->set_state( STATE_PRIMARY );
      else
        this->set_state( STATE_SECONDARY );
      this->notify_update();
      return;
    }
  }
  else {
    if ( this->ft_queue.ptr[ 1 ] != &this->me )
      return;
    if ( deadline != 0 ) {
      TimerQueue & timer = this->poll.timer;
      if ( this->timer_clear( ACTION_PREPARE ) )
        timer.remove_timer_cb( *this, this->tid, ACTION_PREPARE );
      if ( this->timer_clear( ACTION_ACTIVATE ) )
        timer.remove_timer_cb( *this, this->tid, ACTION_ACTIVATE );
      if ( this->prepare_ms > 0 )
        this->notify_change( ACTION_PREPARE );
      return;
    }
    origin_ns = p->last_rcv_ns;
  }
  if ( this->prepare_ms > 0 &&
       this->expired_delta_ms( origin_ns, this->prepare_ms ) > 0 ) {
    this->set_prepare_timer();
    return;
  }
  if ( action == ACTION_PREPARE ) {
    bool notify = true;
    if ( this->me.state == STATE_JOIN ) {
      if ( p != &this->me ) {
        if ( FtPeer::is_greater_weight( *p, this->me ) )
          notify = false;
      }
    }
    if ( notify )
      this->notify_change( ACTION_PREPARE );
  }
  this->set_activate_timer();
}

bool
RvFt::timer_cb( uint64_t id,  uint64_t action ) noexcept
{
  if ( id != this->tid )
    return false;
  switch ( action ) {
    case ACTION_ACTIVATE:
    case ACTION_PREPARE: {
      int64_t ival;
      if ( action == ACTION_ACTIVATE )
        ival = this->expired_delta_ms( this->activate_origin,
                                       this->activate_ms );
      else
        ival = this->expired_delta_ms( this->prepare_origin,
                                       this->prepare_ms );
      bool is_accurate = ( ival > -100 && ival < 100 );
      this->timer_clear( (FtAction) action );
      /* if delayed, wait for accurate timer */
      if ( is_accurate ) {
        if ( this->me.state == STATE_SECONDARY ||
             this->me.state == STATE_JOIN ) {
          this->trim_ft_queue();
          this->prepare_takeover( action, 0 );
        }
      }
      this->notify_update();
      return false;
    }

    case ACTION_HEARTBEAT: {
      int64_t ival = this->expired_delta_ms( this->last_hb_ns,
                                             this->heartbeat_ms );
      bool is_accurate = ( ival > -100 && ival < 100 );
      /* if not accurate, could throw away all peers because they have not
       * been updated */
      if ( ! is_accurate ) {
        if ( ival >= this->accuracy_warn || ival <= -this->accuracy_warn ) {
          const char *fmt = "heartbeat timer accuracy+-%ldms %ld\n";
          this->warn( fmt, this->accuracy_warn, ival );
          if ( this->mout != NULL )
            this->mout->printe( fmt, this->accuracy_warn, ival );
          this->accuracy_warn += 100;
        }
      }
      if ( is_accurate )
        this->trim_ft_queue();

      if ( this->me.state == STATE_PRIMARY )
        this->send_msg( HEARTBEAT );
      else if ( this->me.state != STATE_LISTEN ) {
        if ( is_accurate )
          this->prepare_takeover( action, 0 );
        this->send_msg( KEEPALIVE );
      }
      this->last_hb_ns = this->poll.mono_ns;
      this->notify_update();
      return true;
    }
    case ACTION_JOIN:
      this->timer_clear( ACTION_JOIN );
      this->set_state( STATE_JOIN );
      return false;

    case ACTION_FINISH:
      /* this maybe should be tied to prepare */
      this->timer_clear( ACTION_FINISH );
      this->set_state( STATE_FINISH );
      return false;

    default:
      return false;
  }
}

void
RvFt::start_hb( uint32_t hb_ival,  uint32_t act_ival,
                uint32_t prep_ival ) noexcept
{
  bool changed = false;
  if ( hb_ival != 0 && this->heartbeat_ms != hb_ival ) {
    this->warn( "changing heartbeat %u -> %u\n", this->heartbeat_ms,
                hb_ival );
    this->heartbeat_ms = hb_ival;
    changed = true;
  }
  if ( act_ival != 0 && this->activate_ms != act_ival ) {
    this->warn( "changing activate %u -> %u\n", this->activate_ms,
                act_ival );
    this->activate_ms = act_ival;
    changed = true;
  }
  if ( prep_ival != 0 && this->prepare_ms != prep_ival ) {
    this->warn( "changing prepare %u -> %u\n", this->prepare_ms,
                prep_ival );
    this->prepare_ms = prep_ival;
    changed = true;
  }
  if ( this->activate_ms <= this->heartbeat_ms ) {
    uint32_t new_act = this->heartbeat_ms * 2 + this->heartbeat_ms / 10;
    this->warn( "adjust activate %u -> %u\n", this->activate_ms, new_act );
    this->activate_ms = new_act;
    changed = true;
  }
  if ( this->prepare_ms != 0 ) {
    if ( this->prepare_ms < this->heartbeat_ms ||
         this->prepare_ms >= this->activate_ms ) {
      uint32_t new_prep = ( this->heartbeat_ms + this->activate_ms ) / 2;
      this->warn( "adjust prepare %u -> %u\n", this->prepare_ms, new_prep );
      this->prepare_ms = new_prep;
      changed = true;
    }
  }
  TimerQueue & timer = this->poll.timer;
  if ( changed ) {
    if ( this->timer_clear( ACTION_ACTIVATE ) )
      timer.remove_timer_cb( *this, this->tid, ACTION_ACTIVATE );
    if ( this->timer_clear( ACTION_PREPARE ) )
      timer.remove_timer_cb( *this, this->tid, ACTION_PREPARE );
    if ( this->timer_clear( ACTION_HEARTBEAT ) )
      timer.remove_timer_cb( *this, this->tid, ACTION_HEARTBEAT );

  }
  if ( ! this->timer_test_set( ACTION_HEARTBEAT ) ) {
    this->last_hb_ns = this->poll.mono_ns;
    timer.add_timer_millis( *this, this->heartbeat_ms, this->tid,
                            ACTION_HEARTBEAT );
  }
}

bool
RvFt::check_latency( FtPeer &p,  int64_t ns ) noexcept
{
  int64_t skew = p.latency();
  uint64_t warn_time;
  if ( skew != 0 ) { /* if have samples */
    int64_t ms      = ns / ( 1000 * 1000 ),
            act_ms  = (int64_t) this->activate_ms,
            hb_ms   = (int64_t) this->heartbeat_ms;
    skew /= ( 1000 * 1000 );
    if ( ms < -( act_ms - skew ) || ms > ( act_ms + skew ) ) {
      const char *fmt = "discarding old msg from %s, outside activation %ld, "
                        "lat %ld, skew = %ld\n";
      warn_time = this->poll.mono_ns / ( 1000 * 1000 * 1000 );
      if ( ( warn_time - this->last_warn ) > 0 ) {
        this->last_warn = warn_time;
        this->warn( fmt, p.user, act_ms, ms, skew );
        if ( this->mout != NULL )
          this->mout->printe( fmt, p.user, act_ms, ms, skew );
      }
      return false;
    }
    if ( ms < -( hb_ms - skew ) || ms > ( hb_ms + skew ) ) {
      const char *fmt = "msg from %s, outside heartbeat %ld, "
                        "lat %ld, skew = %ld\n";
      warn_time = this->poll.mono_ns / ( 1000 * 1000 * 1000 );
      if ( ( warn_time - this->last_warn ) > 0 ) {
        this->last_warn = warn_time;
        this->warn( fmt, p.user, hb_ms, ms, skew );
        if ( this->mout != NULL )
          this->mout->printe( fmt, p.user, hb_ms, ms, skew );
      }
      return true;
    }
  }
  p.accum_latency( ns );
  return true;
}

void
RvFt::on_peer_msg( const FtPeerMsg &peer ) noexcept
{
  FtPeer * p;
  size_t   pos;
  uint64_t cur_time = this->poll.now_ns;
  this->last_rcv_ns = this->poll.mono_ns;

  /*if ( debug_ft )
    if ( peer.type != HEARTBEAT )
      peer.print();*/
  if ( this->me.state == STATE_LISTEN ) {
    if ( peer.start_ns < this->me.start_ns ) {
      if ( this->oldest_peer == 0 || peer.start_ns < this->oldest_peer ) {
        this->oldest_peer = peer.start_ns;
        /* check that my heartbeat matches peer */
        this->start_hb( peer.hb_ival, peer.act_ival, peer.prep_ival );
      }
    }
  }
  if ( peer.state == STATE_LISTEN ) {
    /* went back to listen state */
    if ( this->peer_ht->find( peer.start_ns, pos, p ) ) {
      p->update_state( STATE_LISTEN, this->state_count );
      this->ft_queue.remove( p );
      if ( this->peer_ht->find( p->start_ns, pos ) )
        this->peer_ht->remove( pos );
      this->release_peer( p );
    }
    this->notify_update();
  }
  else {
    if ( ! this->peer_ht->find( peer.start_ns, pos, p ) ) {
      p = this->make_peer( &peer );
      p->accum_latency( cur_time - peer.cur_time );
      this->peer_ht->set_rsz( this->peer_ht, peer.start_ns, pos, p );
    }
    else {
      int64_t ns = cur_time - peer.cur_time;
      if ( ! this->check_latency( *p, ns ) )
        return;
      p->set( peer, this->state_count );
    }
    p->last_rcv_ns = this->last_rcv_ns;
    if ( peer.type != EXITING ) {
      this->ft_queue.update( p );
      if ( this->me.state == STATE_LISTEN ) {
        this->notify_update();
        return;
      }
      if ( peer.type == FINISH ) {
        if ( p == this->ft_queue.ptr[ 0 ] )
          this->prepare_takeover( FINISH, peer.fin_time );
        this->notify_update();
        return;
      }
    }
    else { /* exited */
      p->update_state( STATE_FINISH, this->state_count );
      this->ft_queue.remove( p );
      if ( this->peer_ht->find( p->start_ns, pos ) )
        this->peer_ht->remove( pos );
      this->release_peer( p );
    }
    /* check if I am primary or secondary */
    if ( this->me.state == STATE_SECONDARY ) {
      if ( this->ft_queue.ptr[ 0 ] != &this->me )
        this->set_state( STATE_SECONDARY );
      else
        this->set_state( STATE_PRIMARY );
    }
    else if ( this->me.state == STATE_PRIMARY ) {
      if ( this->ft_queue.ptr[ 0 ] != &this->me )
        this->set_state( STATE_SECONDARY );
    }
    this->notify_update();
  }
  /* ack the announcements */
  if ( peer.type == ANNOUNCE && this->me.state != STATE_LISTEN )
    this->send_msg( ACK, peer.reply, peer.reply_len );
}

bool
FtPeer::is_greater( const FtPeer &p1,  const FtPeer &p2 ) noexcept
{
  uint8_t s1 = ( p1.state > RvFt::STATE_JOIN ? 1 : 0 ),
          s2 = ( p2.state > RvFt::STATE_JOIN ? 1 : 0 );
  if ( s1 > s2 )
    return true;
  if ( s1 == s2 )
    return is_greater_weight( p1, p2 );
  return false;
}

bool
FtPeer::is_greater_weight( const FtPeer &p1,  const FtPeer &p2 ) noexcept
{
  if ( p1.weight > p2.weight )
    return true;
  if ( p1.weight == p2.weight )
    if ( p1.start_ns < p2.start_ns )
      return true;
  return false;
}

void
FtQueue::insert( FtPeer *p ) noexcept
{
  this->push( p );
  this->reorder();
  this->seqno++;
}

void
FtQueue::reorder( void ) noexcept
{
  uint32_t j = this->count - 1;
  FtPeer * p = this->ptr[ j ];
  while ( j > 0 ) {
    if ( FtPeer::is_greater( *this->ptr[ j-1 ], *p ) )
      break;
    this->ptr[ j ] = this->ptr[ j-1 ];
    this->ptr[ --j ] = p;
  }
  for ( ; j < this->count; j++ )
    this->ptr[ j ]->pos = j + 1;
}

uint32_t
FtQueue::get_pos( FtPeer *p ) noexcept
{
  uint32_t j = p->pos;
  if ( j > 0 && this->ptr[ --j ] == p )
    return j;
  fprintf( stderr, "*** get %s pos %u not equal %u\n", p->user, p->pos, j );
  for ( j = 0; j < this->count; j++ )
    if ( this->ptr[ j ] == p )
      break;
  return j;
}

void
FtQueue::remove( FtPeer *p ) noexcept
{
  uint32_t j;
  if ( p->pos == 0 || (j = this->get_pos( p )) == this->count )
    return;
  for ( ; j + 1 < this->count; j++ ) {
    this->ptr[ j ] = this->ptr[ j + 1 ];
    this->ptr[ j ]->pos = j + 1;
  }
  p->pos = 0;
  this->count -= 1;
  this->seqno++;
}

bool
FtQueue::in_order2( FtPeer *p ) const noexcept
{
  uint32_t j = p->pos;
  if ( j == 0 || this->ptr[ --j ] != p )
    return false;
  if ( ( j   == 0           || FtPeer::is_greater( *this->ptr[ j-1 ], *p ) ) &&
       ( j+1 == this->count || FtPeer::is_greater( *p, *this->ptr[ j+1 ] ) ) )
    return true;
  return false;
}

void
FtQueue::update( FtPeer *p ) noexcept
{
  uint32_t j;
  if ( p->pos == 0 || (j = this->get_pos( p )) == this->count ) {
    this->insert( p );
    return;
  }
  if ( this->in_order2( p ) )
    return;

  for ( ; j + 1 < this->count; j++ ) {
    this->ptr[ j ] = this->ptr[ j + 1 ];
    this->ptr[ j ]->pos = j + 1;
  }
  this->ptr[ j ] = p;
  this->reorder();
  this->seqno++;
}

void
FtQueue::print( void ) noexcept
{
  if ( this->count > 0 ) {
    printf( "{%s.%u[%s]", this->ptr[ 0 ]->user, this->ptr[ 0 ]->pos,
                          RvFt::state_str[ this->ptr[ 0 ]->state ] );
    for ( uint32_t j = 1; j < this->count; j++ ) {
      printf( ", %s.%u[%s]", this->ptr[ j ]->user, this->ptr[ j ]->pos,
                             RvFt::state_str[ this->ptr[ j ]->state ] );
    }
    printf( "}" );
  }
}

bool
RvFt::process_pub( EvPublish &pub ) noexcept
{
  const char * subject     = pub.subject;
  size_t       subject_len = pub.subject_len;

  if ( subject_len != this->ft_sub_len ||
       ::memcmp( subject, this->ft_sub, subject_len ) != 0 ) {
    uint32_t i = this->client.is_inbox( subject, subject_len );
    if ( i != this->inbox_num ) {
      if ( i == this->sync_inbox_num ) {
        this->cb->on_ft_sync( pub );
        return true;
      }
      return false;
    }
  }
  MDMsgMem mem;
  RvMsg  * m = NULL;
  uint64_t cur_time = this->poll.now_ns;

  if ( pub.msg_len > 0 ) {
    m = RvMsg::unpack_rv( (void *) pub.msg, 0, pub.msg_len, 0, NULL, mem );
    if ( m != NULL ) {
      FtPeerMsg peer( (const char *) pub.reply, pub.reply_len );
      MDIterMap map[ FT_MAX_FIELDS ];
      if ( MDIterMap::get_map( *m, map, peer.iter_map( map ) ) > 0 &&
           peer.type < RVFT_MSG_TYPE_COUNT && peer.state < RVFT_STATE_COUNT &&
           peer.start_ns != 0 ) {

        if ( peer.start_ns == this->me.start_ns )
          return true;

        if ( this->mout != NULL ) {
          char buf[ 32 ];
          this->mout->printf( "%d: %s -- recv %s(%u)\n",
                              this->me.state, hms( cur_time, buf ),
                              msg_type_str[ peer.type ], peer.type );
        }
        this->on_peer_msg( peer );
        return true;
      }
      else {
        this->warn( "not parsed %.*s\n", (int) subject_len, subject );
        return false;
      }
    }
    else {
      this->warn( "unpack_rv error %.*s\n", (int) subject_len, subject );
      return false;
    }
  }
  return false;
}

void
RvFt::send_msg( FtMsgType type,  const char *dest,  size_t dest_len ) noexcept
{
  MDMsgMem    mem;
  RvMsgWriter msg( mem, mem.make( 1024 ), 1024 );
  uint64_t    cur_time = this->poll.now_ns;
  char        inbox[ MAX_RV_INBOX_LEN ];
  size_t      inbox_len;

  if ( dest_len == 0 ) {
    dest     = this->ft_sub;
    dest_len = this->ft_sub_len;
  }

  msg.append_uint  ( FT_MSG_TYPE   , (uint8_t) type )
     .append_uint  ( FT_STATE      , this->me.state )
     .append_uint  ( FT_START_NS   , this->me.start_ns )
     .append_uint  ( FT_WEIGHT     , this->me.weight )
     .append_uint  ( FT_CUR_TIME   , cur_time )
     .append_uint  ( FT_HB_IVAL    , this->heartbeat_ms )
     .append_uint  ( FT_ACT_IVAL   , this->activate_ms )
     .append_uint  ( FT_PREP_IVAL  , this->prepare_ms );

  if ( type == FINISH )
    msg.append_uint( FT_FIN_TIME   , cur_time + this->finish_ms * 1000000 );
  msg.append_string( FT_SYNC_INBOX , this->sync_inbox )
     .append_uint  ( FT_MEMBER_CNT , (uint32_t) this->ft_queue.count )
     .append_string( FT_USER       , this->me.user )
     .update_hdr();

  if ( this->mout != NULL ) {
    char buf[ 32 ];
    this->mout->printf( "%d: %s -- send %s(%u)\n",
                        this->me.state, hms( cur_time, buf ),
                        msg_type_str[ type ], type );
    this->mout->flush();
  }
  if ( type == ANNOUNCE )
    inbox_len = this->client.make_inbox( inbox, this->inbox_num );
  else
    inbox_len = 0;
  EvPublish pub( dest, dest_len, inbox, inbox_len, msg.buf, msg.off,
                 this->client.sub_route, this->client, 0, RVMSG_TYPE_ID );
  this->client.publish( pub );
  this->client.idle_push_write();
}

FtPeer *
RvFt::make_peer( const FtPeerMsg *msg ) noexcept
{
  if ( this->ft_free == NULL || this->ft_free->is_all_used() ) {
    void * m = ::malloc( sizeof( FtPeerFree ) );
    this->ft_free = new ( m ) FtPeerFree( this->ft_free );
  }
  return new ( this->ft_free->make_peer() ) FtPeer( msg, this->state_count );
}

void
RvFt::release_peer( FtPeer *p ) noexcept
{
  this->ft_free->release_peer( p );
}

bool
FtPeerFree::is_all_used( void ) noexcept
{
  for ( FtPeerFree *f = this; f != NULL; f = f->next ) {
    if ( f->used_mask != 0xffff )
      return false;
  }
  return true;
}

void *
FtPeerFree::make_peer( void ) noexcept
{
  for ( FtPeerFree *f = this; ; f = f->next ) {
    if ( f->used_mask != 0xffff ) {
      size_t off = 0;
      for ( uint16_t m = 1; ; m <<= 1 ) {
        if ( ( f->used_mask & m ) == 0 ) {
          f->used_mask |= m;
          return &f->block[ off / sizeof( uint64_t ) ];
        }
        off += sizeof( FtPeer );
      }
    }
  }
}

void
FtPeerFree::release_peer( void *p ) noexcept
{
  for ( FtPeerFree *f = this; ; f = f->next ) {
    if ( (uint64_t *) p < f->block ||
         (uint64_t *) p >= &f->block[ sizeof( f->block )/sizeof( uint64_t ) ] )
      continue;
    size_t off = (uint8_t *) p - (uint8_t *) f->block;
    uint16_t m = 1 << ( off / sizeof( FtPeer ) );
    f->used_mask &= ~m;
    return;
  }
}

void
RvFt::warn( const char *fmt, ... ) noexcept
{
  fprintf( stderr, "%.*s warning: ", (int) this->ft_sub_len, this->ft_sub );
  va_list args;
  va_start( args, fmt );
  vfprintf( stderr, fmt, args );
  va_end( args );
}

void
FtPeerMsg::print( void ) const noexcept
{
  printf( "{ %s, %s, user=%s }\n", RvFt::msg_type_str[ this->type ],
          RvFt::state_str[ this->state ], this->user );
}
