#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <pthread.h>

#include <sassrv/ev_rv_client.h>
#include <raikv/ev_publish.h>
#include <sassrv/rv7api.h>
#include <sassrv/rv7cpp.h>
#include <sassrv/rv7ftcpp.h>

namespace rv7 {

/* L _RVFT.START.TIBRVFT_TIME_EXAMPLE
 *   _RVFT.SYNC.TIBRVFT_TIME_EXAMPLE
 *   _RVFT.STOP.TIBRVFT_TIME_EXAMPLE
 *   _RVFT.STATUS.TIBRVFT_TIME_EXAMPLE
 *   _RVFT.ACTIVE_HB.TIBRVFT_TIME_EXAMPLE
 *   _RVFT.ACTIVE_START.TIBRVFT_TIME_EXAMPLE
 *   _RVFT.ACTIVE_STOP.TIBRVFT_TIME_EXAMPLE
 * D _RVFT.START.TIBRVFT_TIME_EXAMPLE 77B.1 { WEIGHT : 50, MAX_ACTIVES : 1, HB_INTERVAL : 1500, PEND_INTERVAL : 0, ACT_INTERVAL : 4800 }
 * L _RV.*.RVFT.*.TIBRVFT_TIME_EXAMPLE
 * D _RVFT.ACTIVE_START.TIBRVFT_TIME_EXAMPLE 77B.1 { WEIGHT : 50 }
 * D _RVFT.ACTIVE_HB.TIBRVFT_TIME_EXAMPLE 77B.1 { WEIGHT : 50 }
 */
void tibrv_ft_inbox_cb         ( tibrvEvent ,  tibrvMsg m,  void *cl ) noexcept { ((api_FtMember *) cl)->inbox_cb( m ); }
void tibrv_ft_start_cb         ( tibrvEvent ,  tibrvMsg m,  void *cl ) noexcept { ((api_FtMember *) cl)->start_cb( m ); }
void tibrv_ft_sync_cb          ( tibrvEvent ,  tibrvMsg m,  void *cl ) noexcept { ((api_FtMember *) cl)->sync_cb( m ); }
void tibrv_ft_stop_cb          ( tibrvEvent ,  tibrvMsg m,  void *cl ) noexcept { ((api_FtMember *) cl)->stop_cb( m ); }
void tibrv_ft_status_cb        ( tibrvEvent ,  tibrvMsg m,  void *cl ) noexcept { ((api_FtMember *) cl)->status_cb( m ); }
void tibrv_ft_active_hb_cb     ( tibrvEvent ,  tibrvMsg m,  void *cl ) noexcept { ((api_FtMember *) cl)->active_hb_cb( m ); }
void tibrv_ft_active_start_cb  ( tibrvEvent ,  tibrvMsg m,  void *cl ) noexcept { ((api_FtMember *) cl)->active_start_cb( m ); }
void tibrv_ft_active_stop_cb   ( tibrvEvent ,  tibrvMsg m,  void *cl ) noexcept { ((api_FtMember *) cl)->active_stop_cb( m ); }
void tibrv_ft_activate_timer_cb( tibrvEvent ,  tibrvMsg  ,  void *cl ) noexcept { ((api_FtMember *) cl)->activate_timer_cb(); }
void tibrv_ft_prepare_timer_cb ( tibrvEvent ,  tibrvMsg  ,  void *cl ) noexcept { ((api_FtMember *) cl)->prepare_timer_cb(); }
void tibrv_ft_hb_timer_cb      ( tibrvEvent ,  tibrvMsg  ,  void *cl ) noexcept { ((api_FtMember *) cl)->hb_timer_cb(); }
void tibrv_ft_unreachable_cb   ( tibrvEvent ,  tibrvMsg m,  void *cl ) noexcept { ((TibrvFtPeer *) cl)->unreachable_cb( m ); }
void tibrv_ft_session_stop_cb  ( tibrvEvent ,  tibrvMsg m,  void *cl ) noexcept { ((TibrvFtPeer *) cl)->session_stop_cb( m ); }
void tibrv_ft_host_stop_cb     ( tibrvEvent ,  tibrvMsg m,  void *cl ) noexcept { ((TibrvFtPeer *) cl)->host_stop_cb( m ); }

int debug_rvft;
#define MAX_FT_SUBJECT_LEN 256
static const double NS_DBL = ( 1000.0 * 1000.0 * 1000.0 );

tibrv_status
Tibrv_API::CreateFtMember( tibrvftMember * memb, tibrvQueue queue,
                           tibrvftMemberCallback cb, tibrvTransport tport,
                           const char * name, tibrv_u16 weight,
                           tibrv_u16 active_goal, tibrv_f64 hb_ival,
                           tibrv_f64 prepare_ival, tibrv_f64 activate_ival,
                           const void * closure ) noexcept
{
#define mod1000( d ) (tibrv_f64) ( (uint32_t) ( d * 1000.0 ) ) / 1000.0
#define FT_SUBJECT_EXTRA "_RVFT.ACTIVE_START."
  *memb = TIBRV_INVALID_ID;
  api_Queue     * q   = this->get<api_Queue>( queue, TIBRV_QUEUE );
  api_Transport * t   = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  size_t          len = ( name == NULL ? 0 : ::strlen( name ) + 1 );
  if ( q == NULL ) return TIBRV_INVALID_QUEUE;
  if ( t == NULL ) return TIBRV_INVALID_TRANSPORT;
  /* should check valid subject */
  if ( len <= 1 || len >= MAX_FT_SUBJECT_LEN - sizeof( FT_SUBJECT_EXTRA ) )
    return TIBRV_INVALID_NAME;
  if ( prepare_ival != 0 && prepare_ival <= hb_ival )
    return TIBRV_INVALID_ARG;
  if ( prepare_ival != 0 && prepare_ival >= activate_ival )
    return TIBRV_INVALID_ARG;
  if ( activate_ival <= hb_ival )
    return TIBRV_INVALID_ARG;
  api_FtMember * ft = this->make<api_FtMember>( TIBRV_FTMEMBER, len + MAX_RV_INBOX_LEN );
  char         * group_name = (char *) (void *) &ft[ 1 ],
               * inbox      = &group_name[ len ];
  char           subject[ MAX_FT_SUBJECT_LEN ];
  CatPtr         p( subject );
  uint32_t       inbox_num;
  tibrv_status   status;

  ft->queue            = queue;
  ft->cb               = cb;
  ft->cl               = closure;
  ft->tport            = tport;
  ft->name             = group_name;
  ft->me.inbox         = inbox;
  ft->me.weight        = weight;
  ft->me.active_goal   = active_goal;
  ft->me.hb_ival       = mod1000( hb_ival );
  ft->me.hb_ns         = (tibrv_u64) ( ft->me.hb_ival * NS_DBL );
  ft->me.prepare_ival  = mod1000( prepare_ival );
  ft->me.activate_ival = mod1000( activate_ival );
  ft->me.activate_ns   = (tibrv_u64) ( ft->me.activate_ival * NS_DBL );
  ::memcpy( group_name, name, len );

  pthread_mutex_lock( &t->mutex );
  inbox_num = t->inbox_count++;
  p.s( "_INBOX." )
   .b( t->x.session, t->x.session_len )
   .c( '.' )
   .u( inbox_num )
   .end();
  pthread_mutex_unlock( &t->mutex );

  ::memcpy( inbox, subject, p.len() + 1 );

  static struct { const char *s;  tibrvEventCallback cb; } ar[] = {
  { NULL,           tibrv_ft_inbox_cb },
  { "START",        tibrv_ft_start_cb },
  { "SYNC",         tibrv_ft_sync_cb },
  { "STOP",         tibrv_ft_stop_cb },
  { "STATUS",       tibrv_ft_status_cb },
  { "ACTIVE_HB",    tibrv_ft_active_hb_cb },
  { "ACTIVE_START", tibrv_ft_active_start_cb },
  { "ACTIVE_STOP",  tibrv_ft_active_stop_cb }
  };
  status = this->CreateListener( &ft->cb_id[ 0 ], queue, tport, ar[ 0 ].cb,
                                 NULL, subject, ft );
  if ( status != TIBRV_OK )
    return status;

  for ( size_t i = 1; i < sizeof( ar ) / sizeof( ar[ 0 ] ); i++ ) {
    p.begin().s( "_RVFT." ).s( ar[ i ].s ).c( '.' ).b( name, len - 1 ).end();
    status = this->CreateListener( &ft->cb_id[ i ], queue, tport, ar[ i ].cb,
                                   NULL, subject, ft );
    if ( status != TIBRV_OK )
      return status;
  }
  ft->peers.push_hd( &ft->me );
  ft->me.is_running = false;
  ft->me.last_seen = current_monotonic_time_ns();
  ft->start_time = ft->me.last_seen;
  ft->publish( t, "START", 1 );

  status = ft->prepare();
  if ( status != TIBRV_OK )
    return status;

#if 0
  EvPipeRec rec( OP_CREATE_FTMEMBER, ft, &q->mutex, &q->cond );
  pthread_mutex_lock( &q->mutex );
  this->ev_read->exec( rec );
  pthread_mutex_unlock( &q->mutex );
#endif
  *memb = ft->id;
  return TIBRV_OK;
}

static const char _RVFT_WEIGHT[]        = "WEIGHT";
static const char _RVFT_MAX_ACTIVES[]   = "MAX_ACTIVES";
static const char _RVFT_HB_INTERVAL[]   = "HB_INTERVAL";
static const char _RVFT_PEND_INTERVAL[] = "PEND_INTERVAL";
static const char _RVFT_ACT_INTERVAL[]  = "ACT_INTERVAL";

void
api_FtMember::publish( api_Transport *t,  const char *sub,  uint8_t flds ) noexcept
{
  char subject[ MAX_FT_SUBJECT_LEN ];
  
  if ( sub[ 0 ] != '_' ) {
    CatPtr p( subject );
    p.s( "_RVFT." ).s( sub ).c( '.' ).s( this->name ).end();
    sub = subject;
  }

  if ( t == NULL )
    t = this->api.get<api_Transport>( this->tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return;

  MDMsgMem    mem;
  RvMsgWriter msg( mem, mem.make( 1024 ), 1024 );

  msg.append_uint( SARG( _RVFT_WEIGHT ), (uint32_t) this->me.weight );
  if ( flds > 0 ) {
    msg.append_uint( SARG( _RVFT_MAX_ACTIVES ),
                     (uint32_t) this->me.active_goal );
    msg.append_uint( SARG( _RVFT_HB_INTERVAL ),
                     (uint32_t) ( this->me.hb_ival * 1000 ) );
    msg.append_uint( SARG( _RVFT_PEND_INTERVAL ),
                     (uint32_t) ( this->me.prepare_ival * 1000 ) );
    msg.append_uint( SARG( _RVFT_ACT_INTERVAL ),
                     (uint32_t) ( this->me.activate_ival * 1000 ) );
  }
  EvPublish pub( sub, ::strlen( sub ),
                 this->me.inbox, ::strlen( this->me.inbox ),
                 msg.buf, msg.update_hdr(), t->client.sub_route, *t->me,
                 0, RVMSG_TYPE_ID );
  EvPipeRec rec( OP_TPORT_SEND, t, &pub, 1, &t->mutex, &t->cond );
  pthread_mutex_lock( &t->mutex );
  this->api.ev_read->exec( rec );
  pthread_mutex_unlock( &t->mutex );
}

bool
api_FtMember::publish_rvftsub( const char *cl,  const char *nm,
                               const char *de ) noexcept
{
  tibrv_u64 now = current_monotonic_time_ns();
  if ( this->fterr_time + this->me.hb_ns > now )
    return false;
  this->fterr_time = now;
  api_Transport *t = this->api.get<api_Transport>( this->tport,
                                                   TIBRV_TRANSPORT );
  if ( t == NULL )
    return false;

  char subject[ MAX_FT_SUBJECT_LEN ];
  CatPtr p( subject );
  /* "_RV.WARN.RVFT.TOO_MANY_ACTIVE.group_name"
   *  cl = "WARN", nm= "TOO_MANY_ACTIVE" */
  p.s( "_RV." ).s( cl ).s( ".RVFT." );
  const char * suf = &subject[ p.len() ];
  p.s( nm ).c( '.' ).s( this->name ).end();

  MDMsgMem    mem;
  RvMsgWriter msg( mem, mem.make( 1024 ), 1024 );

  msg.append_string( SARG( _ADV_CLASS ), cl, ::strlen( cl ) + 1 );
  msg.append_string( SARG( _ADV_SOURCE ), SARG( "RVFT" ) );
  msg.append_string( SARG( _ADV_NAME ), suf, ::strlen( suf ) + 1 );
  if ( de != NULL )
    msg.append_string( SARG( _ADV_DESC ), de, ::strlen( de ) + 1 );

  uint32_t h = kv_crc_c( subject, p.len(), 0 );
  EvPublish pub( subject, p.len(), NULL, 0,
                 msg.buf, msg.update_hdr(), t->client.sub_route, *t->me,
                 h, RVMSG_TYPE_ID );
  t->on_rv_msg( pub );
  return true;
}

static void pt( const char *s,  tibrvMsg m = NULL ) noexcept {
  struct timespec ts;
  clock_gettime( CLOCK_REALTIME, &ts );
  printf( "%02u:%02u.%03u %s",
                       (unsigned int) ( ( ts.tv_sec / 60 ) % 60 ),
                       (unsigned int) ( ts.tv_sec % 60 ),
                       (unsigned int) ( ts.tv_nsec / 1000000ULL ), s );
  if ( m != NULL )
    tibrvMsg_Print( m );
}

void
api_FtMember::stop_timers( void ) noexcept
{
  if ( this->prepare_id != 0 ) {
    this->api.DestroyEvent( this->prepare_id, NULL );
    this->prepare_id = 0;
  }
  if ( this->activate_id != 0 ) {
    this->api.DestroyEvent( this->activate_id, NULL );
    this->activate_id = 0;
  }
  if ( this->hb_id != 0 ) {
    this->api.DestroyEvent( this->hb_id, NULL );
    this->hb_id = 0;
  }
}

tibrv_status
api_FtMember::prepare( void ) noexcept
{
  tibrv_status status;
  this->stop_timers();
  if ( this->me.prepare_ival != 0 &&
       this->me.prepare_ival < this->me.activate_ival )
    status = this->api.CreateTimer( &this->prepare_id, this->queue,
                                    tibrv_ft_prepare_timer_cb,
                                    this->me.prepare_ival, this );
  else
    status = this->api.CreateTimer( &this->activate_id, this->queue,
                                    tibrv_ft_activate_timer_cb,
                                    this->me.activate_ival, this );

  if ( status == TIBRV_OK ) {
    TibrvFtPeer * p = this->peers.hd;
    if ( p != &this->me && ! p->is_running ) {
      uint64_t now = current_monotonic_time_ns();
      if ( p->sync_time == 0 && p->last_sync + p->activate_ns < now ) {
        p->sync_time = now;
        p->last_sync = p->sync_time;
        this->publish( NULL, "SYNC", 1 );
      }
    }
  }
  return status;
}

static int
compare_peer( const TibrvFtPeer &p1, const TibrvFtPeer &p2 ) noexcept
{
  if ( p1.is_stopped != p2.is_stopped ) {
    if ( p1.is_stopped )
      return 1;
    return -1;
  }
  if ( p1.is_unresponsive != p2.is_unresponsive ) {
    if ( p1.is_unresponsive )
      return 1;
    return -1;
  }
  if ( p1.weight > p2.weight )
    return -1;
  if ( p1.weight < p2.weight )
    return 1;
  if ( p1.is_running != p2.is_running ) {
    if ( p1.is_running )
      return -1;
    return 1;
  }
  /*if ( oldest_first )
    return ::strcmp( &p1.inbox[ 16 ], &p2.inbox[ 16 ] );*/
  return ::strcmp( &p2.inbox[ 16 ], &p1.inbox[ 16 ] );
}

void
TibrvFtPeer::print( void ) noexcept
{
  bool is_me = ( this->me != NULL && &this->me->me == this ? "*" : " " );
  printf( "%s %u. w=%u is_run=%s is_dwn=%s is_unr=%s ibx=%s\n",
          is_me ? "*" : " ",
          this->rank, this->weight,
          this->is_running ? "yes" : "no",
          this->is_stopped ? "yes" : "no",
          this->is_unresponsive ? "yes" : "no",
          this->inbox );
}

void
TibrvFtPeer::start_listeners( Tibrv_API &api,
                              tibrvQueue queue,
                              tibrvTransport tport,
                              tibrvEventCallback unreachable_cb,
                              tibrvEventCallback session_stop_cb,
                              tibrvEventCallback host_stop_cb ) noexcept
{
  const char * end = ::strrchr( &this->inbox[ 16 ], '.' );
  if ( end == NULL )
    return;

  char subject[ MAX_FT_SUBJECT_LEN ];
  CatPtr l( subject );
  size_t len = end - &inbox[ 7 ];

  l.s( _RV_INFO_UNREACHABLE_TPORT "." )
   .b( &inbox[ 7 ], len ).end();
  api.CreateListener( &this->unreachable_id, queue, tport,
                      unreachable_cb, NULL, subject, this );

  l.begin().s( _RV_INFO_SESSION_STOP "." )
           .b( &inbox[ 7 ], 8 ).end();
  api.CreateListener( &this->session_stop_id, queue, tport,
                      session_stop_cb, NULL, subject, this );

  l.begin().s( _RV_INFO_HOST_STOP "." )
           .b( &inbox[ 7 ], 8 ).end();
  api.CreateListener( &this->host_stop_id, queue, tport,
                      host_stop_cb, NULL, subject, this );
}

void
TibrvFtPeer::stop_events( Tibrv_API &api ) noexcept
{
  this->is_stopped = true;
  this->is_running = false;
  if ( this->unreachable_id != 0 ) {
    api.DestroyEvent( this->unreachable_id, NULL );
    this->unreachable_id = 0;
  }
  if ( this->session_stop_id != 0 ) {
    api.DestroyEvent( this->session_stop_id, NULL );
    this->session_stop_id = 0;
  }
  if ( this->host_stop_id != 0 ) {
    api.DestroyEvent( this->host_stop_id, NULL );
    this->host_stop_id = 0;
  }
}

void
api_FtMember::update_peer( TibrvFtPeer * p,  tibrvMsg msg,
                           FtState state ) noexcept
{
  struct _Excl { pthread_mutex_t &mut;
         _Excl( pthread_mutex_t &m ) : mut( m ) { pthread_mutex_lock( &m ); }
        ~_Excl() { pthread_mutex_unlock( &this->mut ); } };
  _Excl mutate_db( this->mutex );

  TibrvFtPeer * x;
  api_Msg     * m      = (api_Msg *) msg;
  uint32_t      weight = this->me.weight;
  tibrv_u64     now    = current_monotonic_time_ns();

  if ( msg != NULL && tibrvMsg_GetU32( msg, _RVFT_WEIGHT, &weight ) != TIBRV_OK )
    return;

  if ( p == NULL ) {
    if ( m->reply_len < 16 )
      return;

    this->me.last_seen = now;
    for ( p = this->peers.hd; p != NULL; p = p->next ) {
      if ( ::strcmp( m->reply, p->inbox ) == 0 ) {
        if ( p == &this->me )
          return;
        p->last_seen = now;
        p->sync_time = 0;
        p->weight    = weight;
        p->is_unresponsive = false;
        break;
      }
    }
  }
  if ( p != NULL ) {
    if ( p->is_stopped || state == RVFT_STOP )
      p->stop_events( this->api );
  }
  uint32_t active_goal   = 0,
           hb_ival       = 0,
           prepare_ival  = 0,
           activate_ival = 0;
   bool    set_values    = true;

  if ( msg == NULL ||
       tibrvMsg_GetU32( msg, _RVFT_MAX_ACTIVES, &active_goal ) != TIBRV_OK ||
       tibrvMsg_GetU32( msg, _RVFT_HB_INTERVAL, &hb_ival ) != TIBRV_OK ||
       tibrvMsg_GetU32( msg, _RVFT_PEND_INTERVAL, &prepare_ival ) != TIBRV_OK ||
       tibrvMsg_GetU32( msg, _RVFT_ACT_INTERVAL, &activate_ival ) != TIBRV_OK ){
    active_goal   = this->me.active_goal;
    hb_ival       = (uint32_t) ( this->me.hb_ival * 1000.0 );
    prepare_ival  = (uint32_t) ( this->me.prepare_ival * 1000.0 );
    activate_ival = (uint32_t) ( this->me.activate_ival * 1000.0 );
    set_values    = false;
  }

  if ( p == NULL || state == RVFT_SYNC )
    this->publish( NULL, m->reply, 1 );
  if ( p == NULL ) {
    void * mem = ::malloc( sizeof( TibrvFtPeer ) + m->reply_len + 1 );
    p = new ( mem ) TibrvFtPeer( this );
    char * inbox = (char *) (void *) &p[ 1 ];
    ::memcpy( inbox, m->reply, m->reply_len + 1 );
    p->inbox  = inbox;
    p->weight = weight;
    
    p->start_listeners( this->api, this->queue, this->tport,
                        tibrv_ft_unreachable_cb,
                        tibrv_ft_session_stop_cb,
                        tibrv_ft_host_stop_cb );

    this->peers.push_tl( p );
    set_values = true;
  }
  if ( set_values ) {
    p->active_goal   = active_goal;
    p->hb_ival       = (double) hb_ival / 1000.0;
    p->hb_ns         = (tibrv_u64) ( p->hb_ival * NS_DBL );
    p->prepare_ival  = (double) prepare_ival / 1000.0;
    p->activate_ival = (double) activate_ival / 1000.0;
    p->activate_ns   = (tibrv_u64) ( p->activate_ival * NS_DBL );

    static const char PARAM_MISMATCH[] = "PARAM_MISMATCH";
    if ( active_goal != this->me.active_goal && ( p->pub_err & 1 ) == 0 &&
         this->publish_rvftsub( _ERROR, PARAM_MISMATCH, "Active goal" ) )
      p->pub_err |= 1;
    else if ( p->hb_ival != this->me.hb_ival && ( p->pub_err & 2 ) == 0 &&
         this->publish_rvftsub( _WARN, PARAM_MISMATCH, "Heartbeat interval" ) )
      p->pub_err |= 2;
    else if ( p->activate_ival != this->me.activate_ival && ( p->pub_err & 4 ) == 0 &&
         this->publish_rvftsub( _ERROR, PARAM_MISMATCH, "Activate interval" ) )
      p->pub_err |= 4;
  }
  p->last_seen = now;
  p->sync_time = 0;
  p->is_unresponsive = false;
  if ( state == RVFT_ACTIVE )
    p->is_running = true;
  else if ( state == RVFT_ACTIVE_STOP || state == RVFT_STOP || state == RVFT_START )
    p->is_running = false;

  for ( x = this->peers.hd; x != NULL; x = x->next ) {
    if ( now > x->last_seen && now - x->last_seen > p->activate_ns )
      x->is_running = false;
  }
  this->peers.sort<compare_peer>();

  uint16_t i = 0;
  for ( x = this->peers.hd; x != NULL; x = x->next )
    x->rank = i++;
  /*if ( (uint32_t) p->rank == old_rank )
    return;*/
  x = this->peers.tl;
  if ( x->is_stopped && now - x->last_seen > x->activate_ns ) {
    this->peers.pop( x );
    delete x;
  }

  if ( this->me.rank < this->me.active_goal ) {
    if ( this->hb_id == 0 ) {
      if ( ! this->activate_now() )
        return;
    }
    if ( this->me.rank < this->me.active_goal && this->me.is_running ) {
      if ( state == RVFT_ACTIVE && p->rank >= this->me.active_goal ) {
        this->publish_rvftsub( _WARN, "TOO_MANY_ACTIVE" );
      }
      return;
    }
  }
  if ( this->me.rank >= this->me.active_goal ) {
    if ( this->hb_id != 0 ) {
      if ( ! this->do_callback( TIBRVFT_DEACTIVATE ) )
        return;
      if ( this->me.rank >= this->me.active_goal ) {
        this->me.is_running = false;
        this->publish( NULL, "ACTIVE_STOP", 0 );
      }
      else
        return;
    }
  }
  this->stop_timers();
  this->prepare();
}

void
TibrvFtPeer::unreachable_cb( tibrvMsg ) noexcept
{
  this->is_stopped = true;
  this->me->update_peer( this, NULL, RVFT_STOP );
}
void
TibrvFtPeer::session_stop_cb( tibrvMsg m ) noexcept
{
  const char * name;
  if ( tibrvMsg_GetString( m, "id", &name ) == TIBRV_OK ) {
    size_t len = ( name != NULL ? ::strlen( name ) : 0 );
    if ( len > 0 && ::memcmp( name, &this->inbox[ 7 ], len ) == 0 ) {
      this->is_stopped = true;
      this->me->update_peer( this, NULL, RVFT_STOP );
    }
  }
}
void
TibrvFtPeer::host_stop_cb( tibrvMsg ) noexcept
{
  this->is_stopped = true;
  this->me->update_peer( this, NULL, RVFT_STOP );
}
void
api_FtMember::inbox_cb( tibrvMsg m ) noexcept
{
  if ( debug_rvft ) pt( "inbox: ", m );
  this->update_peer( NULL, m, RVFT_INBOX );
}
void
api_FtMember::start_cb( tibrvMsg m ) noexcept
{
  if ( debug_rvft ) pt( "start: ", m );
  this->update_peer( NULL, m, RVFT_START );
}
void
api_FtMember::sync_cb( tibrvMsg m ) noexcept
{
  if ( debug_rvft ) pt( "sync: ", m );
  this->update_peer( NULL, m, RVFT_SYNC );
}
void
api_FtMember::stop_cb( tibrvMsg m ) noexcept
{
  if ( debug_rvft ) pt( "stop: ", m );
  this->update_peer( NULL, m, RVFT_STOP );
}
void
api_FtMember::status_cb( tibrvMsg m ) noexcept
{
  if ( debug_rvft ) pt( "status: ", m );
  this->update_peer( NULL, m, RVFT_STATUS );
}
void
api_FtMember::active_hb_cb( tibrvMsg m ) noexcept
{
  this->update_peer( NULL, m, RVFT_ACTIVE );
}
void
api_FtMember::active_start_cb( tibrvMsg m ) noexcept
{
  if ( debug_rvft ) pt( "active_start: ", m );
  this->update_peer( NULL, m, RVFT_ACTIVE );
}
void
api_FtMember::active_stop_cb( tibrvMsg m ) noexcept
{
  if ( debug_rvft ) pt( "active_stop: ", m );
  this->update_peer( NULL, m, RVFT_ACTIVE_STOP );
}

tibrv_u64
api_FtMember::update_time( void ) noexcept
{
  tibrv_u64 now = current_monotonic_time_ns();
  bool      changed = false;

  this->me.last_seen = now;
  for ( TibrvFtPeer *x = this->peers.hd; x != NULL; x = x->next ) {
    if ( x->is_running ) {
      if ( now > x->last_seen && now - x->last_seen > x->activate_ns ) {
        x->is_running = false;
        changed = true;
      }
    }
    if ( ! x->is_running && x->sync_time != 0 && x->rank == 0 ) {
      if ( x->sync_time + x->activate_ns < now ) {
        x->is_unresponsive = true;
        changed = true;
      }
    }
  }
  if ( changed ) {
    this->peers.sort<compare_peer>();

    uint16_t i = 0;
    for ( TibrvFtPeer *x = this->peers.hd; x != NULL; x = x->next ) {
      x->rank = i++;
      if ( debug_rvft )
        x->print();
    }
  }
  return now;
}

void
api_FtMember::activate_timer_cb( void ) noexcept
{
  if ( this->is_destroyed )
    return;
  pthread_mutex_lock( &this->mutex );
  if ( this->hb_id == 0 ) {
    this->stop_timers();
    this->update_time();
    if ( debug_rvft ) {
      pt( "activate_timer_cb" );
      printf( " rank=%u\n", this->me.rank );
    }
    if ( this->me.rank < this->me.active_goal ) {
      this->me.is_running = true;
      this->publish( NULL, "ACTIVE_START", 0 );
      if ( this->do_callback( TIBRVFT_ACTIVATE ) ) {
        this->api.CreateTimer( &this->hb_id, this->queue, tibrv_ft_hb_timer_cb,
                               this->me.hb_ival, this );
      }
    }
    if ( this->hb_id == 0 )
      this->prepare();
  }
  pthread_mutex_unlock( &this->mutex );
}

bool
api_FtMember::activate_now( void ) noexcept
{
  tibrv_u64 now = this->update_time();
  double    uptime = (double) ( this->me.last_seen - this->start_time ) / NS_DBL;
  if ( uptime >= this->me.activate_ival && uptime >= this->me.prepare_ival ) {
    this->stop_timers();
    if ( this->me.rank < this->me.active_goal ) {
      if ( this->me.prepare_ival > 0 &&
           this->me.activate_ival > this->me.prepare_ival ) {
        tibrv_u64 prepare_ns = (tibrv_u64) ( this->me.prepare_ival * NS_DBL );
        if ( this->prepare_time + prepare_ns < now ) {
          this->prepare_time = now;
          this->do_callback( TIBRVFT_PREPARE_TO_ACTIVATE );
        }
      }
      if ( ! this->is_destroyed && this->me.rank < this->me.active_goal ) {
        this->me.is_running = true;
        this->publish( NULL, "ACTIVE_START", 0 );
        if ( this->do_callback( TIBRVFT_ACTIVATE ) &&
             this->me.rank < this->me.active_goal ) {
          this->api.CreateTimer( &this->hb_id, this->queue,
                                 tibrv_ft_hb_timer_cb, this->me.hb_ival, this );
        }
      }
    }
    if ( this->hb_id == 0 )
      this->prepare();
  }
  return this->hb_id != 0;
}

void
api_FtMember::prepare_timer_cb( void ) noexcept
{
  if ( this->is_destroyed )
    return;
  pthread_mutex_lock( &this->mutex );
  if ( this->hb_id == 0 ) {
    this->stop_timers();
    tibrv_u64 now = this->update_time();
    if ( debug_rvft ) {
      pt( "prepare_timer_cb" );
      printf( " rank=%u\n", this->me.rank );
    }
    if ( this->me.rank < this->me.active_goal ) {
      tibrv_u64 prepare_ns = (tibrv_u64) ( this->me.prepare_ival * NS_DBL );
      if ( this->prepare_time + prepare_ns < now ) {
        this->prepare_time = now;
        this->do_callback( TIBRVFT_PREPARE_TO_ACTIVATE );
      }
    }
    if ( ! this->is_destroyed && this->me.rank < this->me.active_goal )
      this->api.CreateTimer( &this->activate_id, this->queue,
                             tibrv_ft_activate_timer_cb,
                       this->me.activate_ival - this->me.prepare_ival, this );
    if ( this->activate_id == 0 )
      this->prepare();
  }
  pthread_mutex_unlock( &this->mutex );
}

bool
api_FtMember::do_callback( tibrvftAction action ) noexcept
{
  pthread_mutex_unlock( &this->mutex );
  if ( this->cb != NULL && ! this->is_destroyed )
    this->cb( this->id, this->name, action, (void *) this->cl );
  pthread_mutex_lock( &this->mutex );
  return ! this->is_destroyed;
}

void
api_FtMember::hb_timer_cb( void ) noexcept
{
  if ( this->is_destroyed )
    return;
  pthread_mutex_lock( &this->mutex );
  if ( this->me.rank < this->me.active_goal ) {
    this->me.last_seen = current_monotonic_time_ns();
    this->me.is_running = true;
    this->publish( NULL, "ACTIVE_HB", 0 );
  }
  else {
    this->prepare();
  }
  pthread_mutex_unlock( &this->mutex );
}

tibrv_status
Tibrv_API::DestroyFtMember( tibrvftMember memb ) noexcept
{
  api_FtMember *ft = this->get<api_FtMember>( memb, TIBRV_FTMEMBER );
  if ( ft == NULL )
    return TIBRV_INVALID_DISPATCHABLE;
  pthread_mutex_lock( &ft->mutex );
  ft->is_destroyed = true;
  ft->stop_timers();
  ft->publish( NULL, "STOP", 0 );
  for ( size_t i = 0; i < sizeof( ft->cb_id ) / sizeof( ft->cb_id[ 0 ] ); i++ ) {
    if ( ft->cb_id[ i ] != 0 )
      this->DestroyEvent( ft->cb_id[ i ], NULL );
  }
  pthread_mutex_unlock( &ft->mutex );
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::DestroyExFtMember( tibrvftMember memb, tibrvftMemberOnComplete cb ) noexcept
{
  api_FtMember *ft = this->get<api_FtMember>( memb, TIBRV_FTMEMBER );
  if ( ft == NULL )
    return TIBRV_INVALID_DISPATCHABLE;
  const void * cl = ft->cl;
  this->DestroyFtMember( memb );
  cb( memb, (void *) cl );
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::GetFtMemberQueue( tibrvftMember memb, tibrvQueue * q ) noexcept
{
  api_FtMember *ft = this->get<api_FtMember>( memb, TIBRV_FTMEMBER );
  if ( ft == NULL )
    return TIBRV_INVALID_DISPATCHABLE;
  *q = ft->queue;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::GetFtMemberTransport( tibrvftMember memb, tibrvTransport * tport ) noexcept
{
  api_FtMember *ft = this->get<api_FtMember>( memb, TIBRV_FTMEMBER );
  if ( ft == NULL )
    return TIBRV_INVALID_DISPATCHABLE;
  *tport = ft->tport;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::GetFtMemberGroupName( tibrvftMember memb, const char ** name ) noexcept
{
  api_FtMember *ft = this->get<api_FtMember>( memb, TIBRV_FTMEMBER );
  if ( ft == NULL )
    return TIBRV_INVALID_DISPATCHABLE;
  *name = ft->name;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::GetFtMemberWeight( tibrvftMember memb, tibrv_u16 * weight ) noexcept
{
  api_FtMember *ft = this->get<api_FtMember>( memb, TIBRV_FTMEMBER );
  if ( ft == NULL )
    return TIBRV_INVALID_DISPATCHABLE;
  *weight = ft->me.weight;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::SetFtMemberWeight( tibrvftMember memb, tibrv_u16 weight ) noexcept
{
  api_FtMember *ft = this->get<api_FtMember>( memb, TIBRV_FTMEMBER );
  if ( ft == NULL )
    return TIBRV_INVALID_DISPATCHABLE;
  if ( weight != ft->me.weight ) {
    ft->me.weight = weight;
    ft->publish( NULL, "STATUS", 1 );
  }
  return TIBRV_OK;
}

void tibrv_ftmon_stop_cb          ( tibrvEvent ,  tibrvMsg m,  void *cl ) noexcept { ((api_FtMonitor *) cl)->stop_cb( m ); }
void tibrv_ftmon_active_hb_cb     ( tibrvEvent ,  tibrvMsg m,  void *cl ) noexcept { ((api_FtMonitor *) cl)->active_hb_cb( m ); }
void tibrv_ftmon_active_start_cb  ( tibrvEvent ,  tibrvMsg m,  void *cl ) noexcept { ((api_FtMonitor *) cl)->active_start_cb( m ); }
void tibrv_ftmon_active_stop_cb   ( tibrvEvent ,  tibrvMsg m,  void *cl ) noexcept { ((api_FtMonitor *) cl)->active_stop_cb( m ); }
void tibrv_ftmon_inactive_timer_cb( tibrvEvent ,  tibrvMsg  ,  void *cl ) noexcept { ((api_FtMonitor *) cl)->inactive_cb(); }
void tibrv_ftmon_unreachable_cb   ( tibrvEvent ,  tibrvMsg m,  void *cl ) noexcept { ((TibrvFtPeer *) cl)->unreachable_mon_cb( m ); }
void tibrv_ftmon_session_stop_cb  ( tibrvEvent ,  tibrvMsg m,  void *cl ) noexcept { ((TibrvFtPeer *) cl)->session_stop_mon_cb( m ); }
void tibrv_ftmon_host_stop_cb     ( tibrvEvent ,  tibrvMsg m,  void *cl ) noexcept { ((TibrvFtPeer *) cl)->host_stop_mon_cb( m ); }
tibrv_status
Tibrv_API::CreateFtMonitor( tibrvftMonitor * m, tibrvQueue queue, tibrvftMonitorCallback cb,
                            tibrvTransport tport, const char * name, tibrv_f64 lost_ival,
                            const void * closure ) noexcept
{
  *m = TIBRV_INVALID_ID;
  api_Queue     * q   = this->get<api_Queue>( queue, TIBRV_QUEUE );
  api_Transport * t   = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  size_t          len = ( name == NULL ? 0 : ::strlen( name ) + 1 );
  if ( q == NULL ) return TIBRV_INVALID_QUEUE;
  if ( t == NULL ) return TIBRV_INVALID_TRANSPORT;
  if ( len <= 1 || len >= MAX_FT_SUBJECT_LEN - sizeof( FT_SUBJECT_EXTRA ) )
    return TIBRV_INVALID_NAME;
  api_FtMonitor * mon = this->make<api_FtMonitor>( TIBRV_FTMONITOR, len );
  char          * group_name = (char *) (void *) &mon[ 1 ];
  char            subject[ MAX_FT_SUBJECT_LEN ];
  CatPtr          p( subject );
  tibrv_status    status;
  mon->queue     = queue;
  mon->cb        = cb;
  mon->cl        = closure;
  mon->tport     = tport;
  mon->name      = group_name;
  mon->lost_ival = lost_ival;
  mon->lost_ns   = (tibrv_u64) ( lost_ival * NS_DBL );
  ::memcpy( group_name, name, len );

  /* _RVFT.STOP.TIBRVFT_TIME_EXAMPLE 
   * _RVFT.ACTIVE_HB.TIBRVFT_TIME_EXAMPLE
   * _RVFT.ACTIVE_START.TIBRVFT_TIME_EXAMPLE
   * _RVFT.ACTIVE_STOP.TIBRVFT_TIME_EXAMPLE */
  static struct { const char *s;  tibrvEventCallback cb; } ar[] = {
  { "STOP",         tibrv_ftmon_stop_cb },
  { "ACTIVE_HB",    tibrv_ftmon_active_hb_cb },
  { "ACTIVE_START", tibrv_ftmon_active_start_cb },
  { "ACTIVE_STOP",  tibrv_ftmon_active_stop_cb }
  };
  for ( size_t i = 0; i < sizeof( ar ) / sizeof( ar[ 0 ] ); i++ ) {
    p.begin().s( "_RVFT." ).s( ar[ i ].s ).c( '.' ).b( name, len - 1 ).end();
    status = this->CreateListener( &mon->cb_id[ i ], queue, tport, ar[ i ].cb,
                                   NULL, subject, mon );
    if ( status != TIBRV_OK )
      return status;
  }

  *m = mon->id;
  return TIBRV_OK;
}

void
api_FtMonitor::update_peer( TibrvFtPeer * p,  tibrvMsg msg,
                            FtState state ) noexcept
{
  TibrvFtPeer * x;
  api_Msg     * m = (api_Msg *) msg;
  uint32_t      weight = 0;
  tibrv_u64     now = current_monotonic_time_ns();

  if ( this->is_destroyed )
    return;
  if ( p == NULL ) {
    if ( m->reply_len < 16 )
      return;
  }
  if ( msg != NULL && tibrvMsg_GetU32( msg, _RVFT_WEIGHT, &weight ) != TIBRV_OK )
    return;

  pthread_mutex_lock( &this->mutex );
  if ( p == NULL ) {
    for ( p = this->peers.hd; p != NULL; p = p->next ) {
      if ( ::strcmp( m->reply, p->inbox ) == 0 ) {
        p->last_seen = now;
        p->sync_time = 0;
        p->weight    = weight;
        p->is_unresponsive = false;
        break;
      }
    }
  }
  if ( p != NULL ) {
    if ( p->is_stopped || state == RVFT_STOP )
      p->stop_events( this->api );
  }
  uint32_t active_goal   = 0,
           hb_ival       = 0,
           prepare_ival  = 0,
           activate_ival = 0;
   bool    set_values    = true;

  if ( msg == NULL ||
       tibrvMsg_GetU32( msg, _RVFT_MAX_ACTIVES, &active_goal ) != TIBRV_OK ||
       tibrvMsg_GetU32( msg, _RVFT_HB_INTERVAL, &hb_ival ) != TIBRV_OK ||
       tibrvMsg_GetU32( msg, _RVFT_PEND_INTERVAL, &prepare_ival ) != TIBRV_OK ||
       tibrvMsg_GetU32( msg, _RVFT_ACT_INTERVAL, &activate_ival ) != TIBRV_OK ){
    set_values = false;
  }

  /*if ( p == NULL || state == RVFT_SYNC )
    this->publish( NULL, m->reply, 1 );*/
  if ( p == NULL ) {
    void * mem = ::malloc( sizeof( TibrvFtPeer ) + m->reply_len + 1 );
    p = new ( mem ) TibrvFtPeer( NULL, this );
    char * inbox = (char *) (void *) &p[ 1 ];
    ::memcpy( inbox, m->reply, m->reply_len + 1 );
    p->inbox  = inbox;
    p->weight = weight;
    p->start_listeners( this->api, this->queue, this->tport,
                        tibrv_ftmon_unreachable_cb,
                        tibrv_ftmon_session_stop_cb,
                        tibrv_ftmon_host_stop_cb );
    this->peers.push_tl( p );
  }
  if ( set_values ) {
    p->active_goal   = active_goal;
    p->hb_ival       = (double) hb_ival / 1000.0;
    p->hb_ns         = (tibrv_u64) ( p->hb_ival * NS_DBL );
    p->prepare_ival  = (double) prepare_ival / 1000.0;
    p->activate_ival = (double) activate_ival / 1000.0;
    p->activate_ns   = (tibrv_u64) ( p->activate_ival * NS_DBL );
  }
  p->last_seen = now;
  p->sync_time = 0;
  p->is_unresponsive = false;
  if ( state == RVFT_ACTIVE )
    p->is_running = true;
  else if ( state == RVFT_ACTIVE_STOP || state == RVFT_STOP || state == RVFT_START )
    p->is_running = false;

  for ( x = this->peers.hd; x != NULL; x = x->next ) {
    if ( now > x->last_seen && now - x->last_seen > p->activate_ns )
      x->is_running = false;
  }
  this->peers.sort<compare_peer>();

  uint16_t i = 0, active_count = 0;
  for ( x = this->peers.hd; x != NULL; x = x->next ) {
    x->rank = i++;
    if ( x->is_running )
      active_count++;
  }
  x = this->peers.tl;
  if ( x->is_stopped && now - x->last_seen > x->activate_ns ) {
    this->peers.pop( x );
    delete x;
  }
  uint16_t last_active_count = this->last_active_count;
  this->last_active_count = active_count;

  if ( this->inactive_id != 0 ) {
    this->api.DestroyEvent( this->inactive_id, NULL );
    this->inactive_id = 0;
  }
  this->api.CreateTimer( &this->inactive_id, this->queue,
                         tibrv_ftmon_inactive_timer_cb,
                         this->lost_ival, this );
  pthread_mutex_unlock( &this->mutex );

  if ( active_count != last_active_count )
    this->cb( this->id, this->name, active_count, (void *) this->cl );
}

void
api_FtMonitor::inactive_cb( void ) noexcept
{
  TibrvFtPeer * x;
  tibrv_u64     now = current_monotonic_time_ns();
  uint16_t      active_count = 0;
  if ( this->is_destroyed )
    return;
  pthread_mutex_lock( &this->mutex );
  for ( x = this->peers.hd; x != NULL; x = x->next ) {
    if ( x->is_running ) {
      if ( now - x->last_seen > this->lost_ns )
        x->is_running = false;
    }
    if ( x->is_running )
      active_count++;
  }
  uint16_t last_active_count = this->last_active_count;
  this->last_active_count = active_count;
  if ( this->inactive_id != 0 ) {
    if ( active_count == 0 ) {
      this->api.DestroyEvent( this->inactive_id, NULL );
      this->inactive_id = 0;
    }
  }
  pthread_mutex_unlock( &this->mutex );

  if ( active_count != last_active_count )
    this->cb( this->id, this->name, active_count, (void *) this->cl );
}

void
TibrvFtPeer::unreachable_mon_cb( tibrvMsg ) noexcept
{
  this->is_stopped = true;
  this->mon->update_peer( this, NULL, RVFT_STOP );
}
void
TibrvFtPeer::session_stop_mon_cb( tibrvMsg m ) noexcept
{
  const char * name;
  if ( tibrvMsg_GetString( m, "id", &name ) == TIBRV_OK ) {
    size_t len = ( name != NULL ? ::strlen( name ) : 0 );
    if ( len > 0 && ::memcmp( name, &this->inbox[ 7 ], len ) == 0 ) {
      this->is_stopped = true;
      this->mon->update_peer( this, NULL, RVFT_STOP );
    }
  }
}
void
TibrvFtPeer::host_stop_mon_cb( tibrvMsg ) noexcept
{
  this->is_stopped = true;
  this->mon->update_peer( this, NULL, RVFT_STOP );
}
void
api_FtMonitor::stop_cb( tibrvMsg m ) noexcept
{
  if ( debug_rvft ) pt( "stop: ", m );
  this->update_peer( NULL, m, RVFT_STOP );
}
void
api_FtMonitor::active_hb_cb( tibrvMsg m ) noexcept
{
  if ( debug_rvft ) pt( "active_hb: ", m );
  this->update_peer( NULL, m, RVFT_ACTIVE );
}
void
api_FtMonitor::active_start_cb( tibrvMsg m ) noexcept
{
  if ( debug_rvft ) pt( "active_start: ", m );
  this->update_peer( NULL, m, RVFT_ACTIVE );
}
void
api_FtMonitor::active_stop_cb( tibrvMsg m ) noexcept
{
  if ( debug_rvft ) pt( "active_stop: ", m );
  this->update_peer( NULL, m, RVFT_ACTIVE_STOP );
}

tibrv_status
Tibrv_API::DestroyFtMonitor( tibrvftMonitor m ) noexcept
{
  api_FtMonitor *mon = this->get<api_FtMonitor>( m, TIBRV_FTMONITOR );
  if ( mon == NULL )
    return TIBRV_INVALID_DISPATCHABLE;
  pthread_mutex_lock( &mon->mutex );
  mon->is_destroyed = true;
  if ( mon->inactive_id != 0 ) {
    this->DestroyEvent( mon->inactive_id, NULL );
    mon->inactive_id = 0;
  }
  for ( size_t i = 0; i < sizeof( mon->cb_id ) / sizeof( mon->cb_id[ 0 ] ); i++ ) {
    if ( mon->cb_id[ i ] != 0 )
      this->DestroyEvent( mon->cb_id[ i ], NULL );
  }
  pthread_mutex_unlock( &mon->mutex );
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::DestroyExFtMonitor( tibrvftMonitor m, tibrvftMonitorOnComplete cb ) noexcept
{
  api_FtMonitor *mon = this->get<api_FtMonitor>( m, TIBRV_FTMONITOR );
  if ( mon == NULL )
    return TIBRV_INVALID_DISPATCHABLE;
  const void * cl = mon->cl;
  this->DestroyFtMonitor( m );
  cb( m, (void *) cl );
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::GetFtMonitorQueue( tibrvftMonitor m, tibrvQueue * q ) noexcept
{
  api_FtMonitor *mon = this->get<api_FtMonitor>( m, TIBRV_FTMONITOR );
  if ( mon == NULL )
    return TIBRV_INVALID_DISPATCHABLE;
  *q = mon->queue;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::GetFtMonitorTransport( tibrvftMonitor m, tibrvTransport * tport ) noexcept
{
  api_FtMonitor *mon = this->get<api_FtMonitor>( m, TIBRV_FTMONITOR );
  if ( mon == NULL )
    return TIBRV_INVALID_DISPATCHABLE;
  *tport = mon->tport;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::GetFtMonitorGroupName( tibrvftMonitor m, const char ** name ) noexcept
{
  api_FtMonitor *mon = this->get<api_FtMonitor>( m, TIBRV_FTMONITOR );
  if ( mon == NULL )
    return TIBRV_INVALID_DISPATCHABLE;
  *name = mon->name;
  return TIBRV_OK;
}

extern Tibrv_API * tibrv_api;
}

extern "C" {

using namespace rv7;

const char *
tibrvft_Version( void )
{
  return "sassrv-" kv_stringify( SASSRV_VER );
}

tibrv_status
tibrvftMember_Create( tibrvftMember * memb, tibrvQueue q,
                      tibrvftMemberCallback cb, tibrvTransport tport,
                      const char * name, tibrv_u16 weight,
                      tibrv_u16 active_goal, tibrv_f64 hb_ival,
                      tibrv_f64 prepare_ival, tibrv_f64 activate_ival,
                      const void *cl )
{
  return tibrv_api->CreateFtMember( memb, q, cb, tport, name, weight,
                                    active_goal, hb_ival, prepare_ival,
                                    activate_ival, cl );
}

tibrv_status
tibrvftMember_Destroy( tibrvftMember memb )
{
  return tibrv_api->DestroyFtMember( memb );
}

tibrv_status
tibrvftMember_DestroyEx( tibrvftMember memb, tibrvftMemberOnComplete cb )
{
  return tibrv_api->DestroyExFtMember( memb, cb );
}

tibrv_status
tibrvftMember_GetQueue( tibrvftMember memb, tibrvQueue * q )
{
  return tibrv_api->GetFtMemberQueue( memb, q );
}

tibrv_status
tibrvftMember_GetTransport( tibrvftMember memb, tibrvTransport * tport )
{
  return tibrv_api->GetFtMemberTransport( memb, tport );
}

tibrv_status
tibrvftMember_GetGroupName( tibrvftMember memb, const char ** name )
{
  return tibrv_api->GetFtMemberGroupName( memb, name );
}

tibrv_status
tibrvftMember_GetWeight( tibrvftMember memb, tibrv_u16 *weight )
{
  return tibrv_api->GetFtMemberWeight( memb, weight );
}

tibrv_status
tibrvftMember_SetWeight( tibrvftMember memb, tibrv_u16 weight )
{
  return tibrv_api->SetFtMemberWeight( memb, weight );
}

tibrv_status
tibrvftMonitor_Create( tibrvftMonitor *mon, tibrvQueue q,
                       tibrvftMonitorCallback cb, tibrvTransport tport,
                       const char * name, tibrv_f64 lost_ival, const void * cl )
{
  return tibrv_api->CreateFtMonitor( mon, q, cb, tport, name, lost_ival, cl );
}

tibrv_status
tibrvftMonitor_Destroy( tibrvftMonitor mon )
{
  return tibrv_api->DestroyFtMonitor( mon );
}

tibrv_status
tibrvftMonitor_DestroyEx( tibrvftMonitor mon, tibrvftMonitorOnComplete cb )
{
  return tibrv_api->DestroyExFtMonitor( mon, cb );
}

tibrv_status
tibrvftMonitor_GetQueue( tibrvftMonitor mon, tibrvQueue * q )
{
  return tibrv_api->GetFtMonitorQueue( mon, q );
}

tibrv_status
tibrvftMonitor_GetTransport( tibrvftMonitor mon, tibrvTransport * tport )
{
  return tibrv_api->GetFtMonitorTransport( mon, tport );
}

tibrv_status
tibrvftMonitor_GetGroupName( tibrvftMonitor mon, const char ** name )
{
  return tibrv_api->GetFtMonitorGroupName( mon, name );
}

}
