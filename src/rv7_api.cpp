#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <errno.h>
#include <fcntl.h>
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
#include <unistd.h>
#include <poll.h>
#else
#include <raikv/win.h>
/* RV7_TRACE=1 in the environment prints the api<->epoll thread handshake,
 * for debugging the windows port */
static int rv7_trace = -1;
#define RV7_TRACE( ... ) do { if ( rv7_trace < 0 ) rv7_trace = ( getenv( "RV7_TRACE" ) != NULL ); \
  if ( rv7_trace ) { fprintf( stderr, __VA_ARGS__ ); fflush( stderr ); } } while ( 0 )
#endif

#include <sassrv/ev_rv_client.h>
#include <raikv/ev_publish.h>
#include <sassrv/rv7api.h>
#include <sassrv/mc.h>
#include <sassrv/rv7cpp.h>

namespace rv7 {

Tibrv_API * tibrv_api;
int debug_api;

static inline timespec
ts_timeout( double timeout, double default_timeout = 0 ) {
  return api_ts_timeout( timeout, default_timeout );
}

static tibrv_status
api_status( ApiStatus st )
{
  switch ( st ) {
    case API_OK:                  return TIBRV_OK;
    case API_TIMEOUT:             return TIBRV_TIMEOUT;
    case API_INVALID_QUEUE:       return TIBRV_INVALID_QUEUE;
    case API_INVALID_QUEUE_GROUP: return TIBRV_INVALID_QUEUE_GROUP;
    case API_INVALID_EVENT:       return TIBRV_INVALID_EVENT;
    case API_INVALID_DISPATCHER:  return TIBRV_INVALID_DISPATCHER;
    case API_INVALID_ARG:         return TIBRV_INVALID_TIME_INTERVAL;
    default:                      return TIBRV_INIT_FAILURE;
  }
}

void
api_Transport::on_connect( EvSocket &conn ) noexcept
{
  if ( debug_api ) {
    int len = (int) conn.get_peer_address_strlen();
    printf( "Connected: %.*s\n", len, conn.peer_address.buf );
  }
  pthread_mutex_lock( &this->mutex );
  pthread_cond_broadcast( &this->cond );
  pthread_mutex_unlock( &this->mutex );
}

void *
tibrv_reconnect_thread( void *arg ) noexcept
{
  api_Transport & t = *(api_Transport *) arg;
  pthread_cond_t wait_cond;
  pthread_cond_init( &wait_cond, NULL );

  for (;;) {
    EvRvClientParameters parm( t.x.daemon, t.x.network, t.x.service );
    parm.opts |= kv::OPT_CONNECT_NB;

    EvPipeRec rec( OP_CREATE_TPORT, &t, &parm, &t.mutex, &t.cond );
    struct timespec wait = ts_timeout( 1.0 ); /* pause 1 sec for reconnect */

    pthread_mutex_lock( &t.mutex );
    if ( pthread_cond_timedwait( &wait_cond, &t.mutex, &wait ) == ETIMEDOUT ) {
      if ( ! t.is_destroyed )
        t.api.ev_read->exec( rec );

      if ( debug_api )
        printf( "Reconnecting...\n" );
      struct timespec ts = ts_timeout( 10.0 );
      while ( ! t.is_destroyed  &&
              t.client.rv_state > EvRvClient::ERR_CLOSE &&
              t.client.rv_state < EvRvClient::DATA_RECV ) {
        if ( pthread_cond_timedwait( &t.cond, &t.mutex, &ts ) == ETIMEDOUT ) {
          EvPipeRec rec2( OP_CLOSE_TPORT, &t, &parm, &t.mutex, &t.cond );
          t.api.ev_read->exec( rec2 );
        }
      }
    }
    if ( t.is_destroyed || t.client.rv_state == EvRvClient::DATA_RECV ) {
      if ( debug_api )
        printf( "Succussful reconnect...\n" );
      break;
    }
    pthread_mutex_unlock( &t.mutex );
  }
  if ( t.client.rv_state == EvRvClient::DATA_RECV ) {
    if ( t.x.session_len != t.client.session_len ||
         ::memcmp( t.x.session, t.client.session, t.x.session_len ) != 0 ) {
      fprintf( stderr, "Session different: %.*s (old) != %.*s (new)\n",
              (int) t.x.session_len, t.x.session,
              (int) t.client.session_len, t.client.session );
      ::memcpy( t.x.session, t.client.session, t.client.session_len );
      t.x.session_len = t.client.session_len;
    }
  }

  if ( ! t.is_destroyed ) {
    tibrvId max_id = t.api.next_id;
    for ( tibrvId id = 0; ! t.is_destroyed && id < max_id; id++ ) {
      api_Listener *l;
      if ( (l = t.api.get<api_Listener>( id, TIBRV_LISTENER )) != NULL ) {
        if ( l->tport == t.id &&
             t.client.is_inbox( l->subject, l->len ) == 0 ) {
          EvPipeRec rec( OP_SUBSCRIBE, &t, l, &t.mutex, &t.cond );
          t.api.ev_read->exec( rec );
        }
      }
    }
    t.reconnect_active = false;
  }
  pthread_mutex_unlock( &t.mutex );
  return NULL;
}

void
api_Transport::on_shutdown( EvSocket &conn,  const char *err,
                            size_t err_len ) noexcept
{
  if ( debug_api ) {
    int len = (int) conn.get_peer_address_strlen();
    printf( "Shutdown: %.*s %.*s\n", len, conn.peer_address.buf,
                                     (int) err_len, err );
  }
  /*if ( this->client.poll.quit == 0 )
    this->client.poll.quit = 1;*/
  pthread_mutex_lock( &this->mutex );
  pthread_cond_broadcast( &this->cond );

  if ( ! this->reconnect_active && this->x.session_len > 0 ) {
    this->reconnect_active = true;
    pthread_t id;
    pthread_attr_t attr;
    pthread_attr_init( &attr );
    pthread_attr_setdetachstate( &attr, 1 );
    pthread_create( &id, &attr, tibrv_reconnect_thread, this );
  }
  pthread_mutex_unlock( &this->mutex );
}

void
api_Transport::add_wildcard( uint16_t pref ) noexcept
{
  uint32_t val;
  size_t   pos;
  if ( this->wild_ht == NULL )
    this->wild_ht = UIntHashTab::resize( NULL );
  if ( this->wild_ht->find( pref, pos, val ) )
    this->wild_ht->set( pref, pos, val + 1 );
  else
    this->wild_ht->upsert_rsz( this->wild_ht, pref, 1 );
}

void
api_Transport::remove_wildcard( uint16_t pref ) noexcept
{
  uint32_t val;
  size_t   pos;
  if ( this->wild_ht != NULL ) {
    if ( this->wild_ht->find( pref, pos, val ) ) {
      if ( val == 1 )
        this->wild_ht->remove( pos );
      else
        this->wild_ht->set( pref, pos, val - 1 );
    }
  }
}

api_Msg *
api_Msg::make( EvPublish &pub,  RvMsg *rvmsg,  MsgTether *tether,
               tibrvEvent ev,  const void *cl ) noexcept
{
  void * p = NULL;

  if ( tether != NULL ) {
    pthread_mutex_lock( &tether->mutex );
    if ( ! tether->is_empty() ) {
      api_Msg * x = tether->hd;
      if ( ! x->in_queue ) {
        tether->pop_hd();
        x->~api_Msg(); /* recycled: run the dtor before constructing over it
                        * (frees a lazily created submsg_tether, whose mutex
                        * is a heap block on winpthreads) */
        p = x;
      }
    }
  }
  if ( p == NULL )
    p = ::malloc( sizeof( api_Msg ) );
  api_Msg * m = new ( p ) api_Msg( ev );
  size_t    len = rvmsg->msg_end - rvmsg->msg_off;
  uint8_t * ptr = &((uint8_t *) rvmsg->msg_buf)[ rvmsg->msg_off ];
  void    * buf = m->mem.memalloc( len, ptr );
  m->rvmsg       = RvMsg::unpack_rv( buf, 0, len, 0, NULL, m->mem );
  m->subject_len = pub.subject_len;
  m->subject     = m->mem.stralloc( pub.subject_len, pub.subject );
  m->cl          = cl;
  m->in_queue    = true;
  if ( pub.reply_len > 0 ) {
    m->reply_len = pub.reply_len;
    m->reply     = m->mem.stralloc( pub.reply_len, (char *) pub.reply );
  }
  if ( (m->owner = tether) != NULL ) {
    tether->push_tl( m );
    m->serial = tether->serial++;
    pthread_mutex_unlock( &tether->mutex );
  }
  return m;
}

void *
api_Msg::get_as_bytes( tibrv_u32 *size ) noexcept
{
  if ( this->wr_refs == this->rd_refs && this->rd != NULL ) {
    MDMsg &iter_msg = this->rd->iter->iter_msg();
    uint8_t * buf = (uint8_t *) iter_msg.msg_buf;
    if ( size != NULL ) {
      size_t len = iter_msg.msg_end - iter_msg.msg_off;
      *size = len;
    }
    return &buf[ iter_msg.msg_off ];
  }
  if ( this->wr_refs > this->rd_refs || this->rvmsg == NULL ) {
    tibrv_u32 z = this->wr.update_hdr();
    if ( size != NULL )
      *size = z;
    return this->wr.buf;
  }
  uint8_t * buf = (uint8_t *) this->rvmsg->msg_buf;
  if ( size != NULL ) {
    size_t len = this->rvmsg->msg_end - this->rvmsg->msg_off;
    *size = len;
  }
  return &buf[ this->rvmsg->msg_off ];
}

void
api_Msg::release( void ) noexcept
{
  MsgTether * t = this->submsg_tether;
  if ( t != NULL ) {
    pthread_mutex_lock( &t->mutex );
    while ( ! t->is_empty() ) {
      api_Msg *m = t->pop_tl();
      m->owner = NULL;
      delete m;
    }
  }
  while ( ! this->refs.is_empty() ) {
    TibrvMsgRef * ref = this->refs.pop_hd();
    delete ref;
  }
  if ( t != NULL )
    pthread_mutex_unlock( &t->mutex );
}

/* Lazy: the first sub-msg creates the tether (mutex + serial counter).  A
 * tibrvMsg is not thread-safe for concurrent modification, so the NULL check
 * is not raced by design. */
api_Msg *
api_Msg::make_submsg( void ) noexcept
{
  MsgTether * t = this->submsg_tether;
  if ( t == NULL ) {
    t = new ( ::malloc( sizeof( MsgTether ) ) ) MsgTether();
    this->submsg_tether = t;
  }
  api_Msg *m = new ( ::malloc( sizeof( api_Msg ) ) ) api_Msg( 0 );
  pthread_mutex_lock( &t->mutex );
  m->owner = t;
  t->push_tl( m );
  m->serial = t->serial++;
  pthread_mutex_unlock( &t->mutex );
  return m;
}

api_Msg::~api_Msg() noexcept
{
  this->release();
  if ( this->submsg_tether != NULL ) {
    delete this->submsg_tether;   /* ~MsgTether destroys the mutex */
    this->submsg_tether = NULL;
  }
}

bool
api_Transport::on_rv_msg( EvPublish &pub ) noexcept
{
  if ( this == this->api.process_tport )
    this->client.msg_in.mem.reuse();
  RvMsg * rvmsg = this->client.make_rv_msg( (void *) pub.msg, pub.msg_len,
                                            pub.msg_enc );
  if ( rvmsg == NULL )
    return true;
  api_Listener * l;
  pthread_mutex_lock( &this->mutex );
  for ( api_Rpc *r = this->rpc_list.hd; r != NULL; r = r->next ) {
    if ( r->hash == pub.subj_hash &&
         r->len == pub.subject_len &&
         ::memcmp( r->subject, pub.subject, r->len ) == 0 ) {
      if ( r->reply == NULL ) /* multiple replies ? */
        r->reply = api_Msg::make( pub, rvmsg, NULL, this->id, NULL );
      pthread_cond_broadcast( &this->cond );
      pthread_mutex_unlock( &this->mutex );
      return true;
    }
  }
  size_t i;
  if ( this->ht.ht != NULL ) {
    i = pub.subj_hash & this->ht.mask;
    for ( l = this->ht.ht[ i ].hd; l != NULL; l = l->next ) {
      if ( l->hash != pub.subj_hash || l->wild != 0 ||
           l->len != pub.subject_len ||
           ::memcmp( l->subject, pub.subject, l->len ) != 0 )
        continue;
      api_Queue * q = this->api.get<api_Queue>( l->queue, TIBRV_QUEUE );
      if ( q != NULL ) {
        pthread_mutex_lock( &q->mutex );
        if ( q->push( l->id, (void *) l->cb, (void *) l->vcb, l->cl,
                      api_Msg::make( pub, rvmsg, &q->tether, l->id, l->cl ) ) )
          ApiCore::queue_signal( *q );
        pthread_mutex_unlock( &q->mutex );
      }
    }
  }
  if ( this->wild_ht != NULL ) {
    uint32_t val, pref;
    size_t   pos;
    for ( bool b = this->wild_ht->first( pos ); b;
          b = this->wild_ht->next( pos ) ) {
      this->wild_ht->get( pos, pref, val );
      if ( pref - 1 >= pub.subject_len )
        continue;
      uint32_t h = kv_crc_c( pub.subject, pref - 1, pref );
      i = h & this->ht.mask;
      for ( l = this->ht.ht[ i ].hd; l != NULL; l = l->next ) {
        if ( l->hash != h || l->wild != pref ||
             ! match_rv_wildcard( l->subject, l->len, pub.subject,
                                  pub.subject_len ) )
          continue;
        api_Queue * q = this->api.get<api_Queue>( l->queue, TIBRV_QUEUE );
        if ( q != NULL ) {
          pthread_mutex_lock( &q->mutex );
          if ( q->push( l->id, (void *) l->cb, (void *) l->vcb, l->cl,
                   api_Msg::make( pub, rvmsg, &q->tether, l->id, l->cl ) ) )
            ApiCore::queue_signal( *q );
          pthread_mutex_unlock( &q->mutex );
        }
      }
    }
  }
  pthread_mutex_unlock( &this->mutex );

  return true;
}

/* Dispatch-flush bookkeeping, per thread: ids[] are the batching transports
 * that a callback on this thread appended to without triggering a flush.
 * Ids, not pointers: a transport destroyed mid-pass just fails the lookup.
 * The pass nesting itself is tracked by ApiCore (dispatch_end() runs when
 * the outermost pass on this thread finishes). */
static const uint32_t MAX_DISPATCH_DIRTY = 16;
struct DispatchTLS {
  uint32_t cnt;
  tibrvId  ids[ MAX_DISPATCH_DIRTY ];
};
static thread_local DispatchTLS tls_dispatch = { 0, { 0 } };

void
Tibrv_API::dispatch_end( void ) noexcept
{
  if ( tls_dispatch.cnt > 0 )
    this->flush_dispatch_sends();
}

void
Tibrv_API::release_msg( api_Msg *m ) noexcept
{
  MsgTether *t = m->owner;
  if ( t != NULL ) {
    pthread_mutex_lock( &t->mutex );
    m->reset();
    m->in_queue = false;
    m->release();
    pthread_mutex_unlock( &t->mutex );
  }
  else {
    m->in_queue = false;
  }
}

void
Tibrv_API::release_vec( api_Msg **vec,  tibrv_u32 count ) noexcept
{
  MsgTether *t = vec[ 0 ]->owner;
  tibrv_u32 i;
  if ( t != NULL ) {
    pthread_mutex_lock( &t->mutex );
    for ( i = 0; i < count; i++ ) {
      vec[ i ]->reset();
      vec[ i ]->in_queue = false;
      vec[ i ]->release();
    }
    pthread_mutex_unlock( &t->mutex );
  }
  else {
    for ( i = 0; i < count; i++ )
      vec[ 0 ]->in_queue = false;
  }
}

void
Tibrv_API::dispatch_event( ApiQueueEvent &ev ) noexcept
{
  api_Msg  * msg = (api_Msg *) ev.msg;
  api_Msg ** vec = (api_Msg **) ev.vec;
  if ( ev.cb != NULL ) {
    ( (tibrvEventCallback) ev.cb )( ev.id, msg, (void *) ev.cl );
    if ( msg != NULL )
      release_msg( msg );
    else {
      api_Timer *t = this->get<api_Timer>( ev.id, TIBRV_TIMER );
      if ( t != NULL )
        t->in_queue = false;
    }
  }
  else if ( ev.vcb != NULL ) {
    if ( ev.cnt == 1 ) {
      ( (tibrvEventVectorCallback) ev.vcb )( (void **) &ev.msg, 1 );
      release_msg( msg );
    }
    else {
      ( (tibrvEventVectorCallback) ev.vcb )( (void **) vec, ev.cnt );
      release_vec( vec, ev.cnt );
    }
  }
}

bool api_Transport::on_msg( kv::EvPublish &pub ) noexcept
{
  this->on_rv_msg( pub );
  return true;
}
void api_Transport::write( void ) noexcept {}
void api_Transport::read( void ) noexcept {}
void api_Transport::process( void ) noexcept {}
void api_Transport::release( void ) noexcept {}

tibrv_status
Tibrv_API::Open( void ) noexcept
{
  if ( this->open_pipe( 128 ) != API_OK )
    return TIBRV_INIT_FAILURE;
#if defined( _MSC_VER ) || defined( __MINGW32__ )
  RV7_TRACE( "open: socketpair fds %d %d\n", this->pfd[ 0 ], this->pfd[ 1 ] );
#endif
  EvPipe * pipe = new ( aligned_malloc( sizeof( EvPipe ) ) )
                  EvPipe( this->poll, this->pfd[ 1 ] );
  pipe->start( this->pfd[ 0 ], "tibrv_api_pipe" );
  this->ev_read = pipe;
  this->default_queue =
    this->make<api_Queue>( TIBRV_QUEUE, 0, TIBRV_DEFAULT_QUEUE );
  api_Transport * t =
    this->make<api_Transport>( TIBRV_TRANSPORT, 0, TIBRV_PROCESS_TRANSPORT );
  this->process_tport = t;

  EvRvClientParameters parm( "null", NULL, NULL, 0, 0 );
  t->client.rv_connect( parm, t, t );
  int fd = this->poll.get_null_fd();
  t->sock_opts = OPT_NO_POLL;
  t->PeerData::init_peer( this->poll.get_next_id(), fd, -1,
                                           NULL, "tibrv_process_transport" );
  t->set_name( "tibrv_process", 13 );
  t->poll.add_sock( t );
  t->me = t;
  ::memcpy( t->x.session, t->client.session, sizeof( t->x.session ) );
  t->x.session_len = t->client.session_len;

  kv::RoutePublish & sub_route = t->client.sub_route;
  PatternCvt cvt;
  const char * ibx = "_INBOX.>";
  size_t       len = ::strlen( ibx );
  cvt.convert_rv( ibx, len );
  uint32_t h = kv_crc_c( ibx, cvt.prefixlen,
                         sub_route.prefix_seed( cvt.prefixlen ) );
  NotifyPattern npat( cvt, ibx, len, NULL, 0, h, false, 'A', *t );
  sub_route.add_pat( npat );

  this->start_ev_thread();
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::CreateListener( tibrvEvent * event,  tibrvQueue queue,
                          tibrvTransport tport,  tibrvEventCallback cb,
                          tibrvEventVectorCallback vcb,
                          const char * subj,  const void * closure ) noexcept
{
  size_t len  = ( subj == NULL ? 0 : ::strlen( subj ) );
  *event = TIBRV_INVALID_ID;
  if ( len == 0 || ::strstr( subj, ".." ) != NULL ||
       subj[ 0 ] == '.' || subj[ len - 1 ] == '.' )
    return TIBRV_INVALID_SUBJECT;
  api_Queue     * q = this->get<api_Queue>( queue, TIBRV_QUEUE );
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( q == NULL ) return TIBRV_INVALID_QUEUE;
  if ( t == NULL ) return TIBRV_INVALID_TRANSPORT;
  const char   * wild = is_rv_wildcard( subj, len );
  api_Listener * l    = this->make<api_Listener>( TIBRV_LISTENER, len + 1 );
  if ( wild != NULL ) {
    l->wild = &wild[ 1 ] - subj;
    l->hash = kv_crc_c( subj, l->wild - 1, l->wild );
  }
  else {
    l->wild = 0;
    l->hash = kv_crc_c( subj, len, 0 );
  }
  l->subject = (char *) &l[ 1 ];
  l->len     = len;
  l->cb      = cb;
  l->vcb     = vcb;
  l->cl      = closure;
  l->queue   = queue;
  l->tport   = tport;
  ::memcpy( l->subject, subj, len + 1 );

  pthread_mutex_lock( &t->mutex );
  if ( wild != NULL )
    t->add_wildcard( l->wild );
  t->ht.push( l );
  if ( t->client.is_inbox( l->subject, l->len ) == 0 ) {
    EvPipeRec rec( OP_SUBSCRIBE, t, l, &t->mutex, &t->cond );
    this->ev_read->exec( rec );
  }
  pthread_mutex_unlock( &t->mutex );

  *event = l->id;
  return TIBRV_OK;
}

void
EvPipe::subscribe( EvPipeRec &rec ) noexcept
{
  const char * sub = rec.l->subject;
  size_t       len = rec.l->len;

  if ( rec.t->id != TIBRV_PROCESS_TRANSPORT )
    rec.t->client.subscribe( sub, len, NULL, 0 );
  else {
    kv::RoutePublish & sub_route = rec.t->client.sub_route;
    if ( ! is_rv_wildcard( sub, len ) ) {
      NotifySub nsub( sub, len, NULL, 0,
                      kv_crc_c( sub, len, 0 ), false, 'A', *rec.t );
      sub_route.add_sub( nsub );
    }
    else {
      PatternCvt cvt;

      if ( cvt.convert_rv( sub, len ) == 0 ) {
        uint32_t h = kv_crc_c( sub, cvt.prefixlen,
                               sub_route.prefix_seed( cvt.prefixlen ) );
        NotifyPattern npat( cvt, sub, len, NULL, 0, h, false, 'A', *rec.t );
        sub_route.add_pat( npat );
      }
    }
  }
}

tibrv_status
Tibrv_API::CreateTimer( tibrvEvent * event,  tibrvQueue queue,
                        tibrvEventCallback cb,  tibrv_f64 ival,
                        const void * closure ) noexcept
{
  uint32_t  id;
  ApiStatus st = this->create_timer( id, queue, (void *) cb, ival, closure );
  *event = ( st == API_OK ? id : TIBRV_INVALID_ID );
  return api_status( st );
}

tibrv_status
Tibrv_API::DestroyEvent( tibrvEvent event,  tibrvEventOnComplete cb ) noexcept
{
  tibrv_u32 type;
  if ( tibrvEvent_GetType( event, &type ) == TIBRV_OK ) {
    bool ok = true;
    switch ( type ) {
      case TIBRV_TIMER: {
        api_Timer * t = this->destroy_timer( event );
        if ( t == NULL )
          break;
        delete t;
        break;
      }
      case TIBRV_LISTENER: {
        api_Listener  * l = this->rem<api_Listener>( event, TIBRV_LISTENER );
        if ( l == NULL )
          break;
        api_Transport * t = this->get<api_Transport>( l->tport, TIBRV_TRANSPORT );
        l->cb = NULL;
        if ( t != NULL ) {
          EvPipeRec rec( OP_UNSUBSCRIBE, t, l, &t->mutex, &t->cond );
          pthread_mutex_lock( &t->mutex );
          if ( t->client.is_inbox( l->subject, l->len ) == 0 )
            this->ev_read->exec( rec );
          if ( l->wild != 0 )
            t->remove_wildcard( l->wild );
          t->ht.remove( l );
          pthread_mutex_unlock( &t->mutex );
        }
        delete l;
        break;
      }
      case TIBRV_QUEUE:
        break;

      default:
      case TIBRV_IO:
      case TIBRV_TRANSPORT:
        ok = false;
        break;
    }
    if ( cb != NULL )
      cb( event, NULL );
    if ( ok )
      return TIBRV_OK;
  }
  return TIBRV_INVALID_EVENT;
}

void
EvPipe::unsubscribe( EvPipeRec &rec ) noexcept
{
  const char * sub = rec.l->subject;
  size_t       len = rec.l->len;
  if ( rec.t->id != TIBRV_PROCESS_TRANSPORT )
    rec.t->client.unsubscribe( rec.l->subject, rec.l->len );
  else {
    kv::RoutePublish & sub_route = rec.t->client.sub_route;
    if ( ! is_rv_wildcard( sub, len ) ) {
      NotifySub nsub( sub, len, NULL, 0,
                      kv_crc_c( sub, len, 0 ), false, 'A', *rec.t );
      sub_route.del_sub( nsub );
    }
    else {
      PatternCvt cvt;

      if ( cvt.convert_rv( sub, len ) == 0 ) {
        uint32_t h = kv_crc_c( sub, cvt.prefixlen,
                               sub_route.prefix_seed( cvt.prefixlen ) );
        NotifyPattern npat( cvt, sub, len, NULL, 0, h, false, 'A', *rec.t );
        sub_route.del_pat( npat );
      }
    }
  }
}

tibrv_status
Tibrv_API::GetEventType( tibrvEvent event,  tibrvEventType * type ) noexcept
{
  *type = 0;
  pthread_mutex_lock( &this->map_mutex );
  if ( event < this->map_size && this->map[ event ].id == event &&
       this->map[ event ].ptr != NULL ) {
    *type = this->map[ event ].type;
  }
  pthread_mutex_unlock( &this->map_mutex );
  if ( *type != 0 )
    return TIBRV_OK;
  return TIBRV_INVALID_EVENT;
}

tibrv_status
Tibrv_API::GetEventQueue( tibrvEvent event,  tibrvQueue * queue ) noexcept
{
  *queue = 0;
  pthread_mutex_lock( &this->map_mutex );
  if ( event < this->map_size && this->map[ event ].id == event &&
       this->map[ event ].ptr != NULL ) {
    switch ( this->map[ event ].type ) {
      case TIBRV_TIMER:
        *queue = ((api_Timer *) this->map[ event ].ptr )->queue;
        break;
      case TIBRV_LISTENER:
        *queue = ((api_Listener *) this->map[ event ].ptr )->queue;
        break;
      case TIBRV_QUEUE:
        *queue = event;
        break;
      default: break;
    }
  }
  pthread_mutex_unlock( &this->map_mutex );
  if ( *queue != 0 )
    return TIBRV_OK;
  return TIBRV_INVALID_EVENT;
}

tibrv_status
Tibrv_API::GetListenerSubject( tibrvEvent event,  const char ** subject ) noexcept
{
  api_Listener *l = this->get<api_Listener>( event, TIBRV_LISTENER );
  if ( l != NULL ) {
    *subject = l->subject;
    return TIBRV_OK;
  }
  return TIBRV_INVALID_EVENT;
}

tibrv_status
Tibrv_API::GetListenerTransport( tibrvEvent event,  tibrvTransport * tport ) noexcept
{
  api_Listener *l = this->get<api_Listener>( event, TIBRV_LISTENER );
  if ( l != NULL ) {
    *tport = l->tport;
    return TIBRV_OK;
  }
  return TIBRV_INVALID_EVENT;
}

tibrv_status
Tibrv_API::GetTimerInterval( tibrvEvent event,  tibrv_f64 * ival ) noexcept
{
  double d;
  ApiStatus st = this->get_timer_interval( event, d );
  if ( st == API_OK )
    *ival = d;
  return api_status( st );
}

tibrv_status
Tibrv_API::ResetTimerInterval( tibrvEvent event,  tibrv_f64 ival ) noexcept
{
  return api_status( this->reset_timer_interval( event, ival ) );
}

tibrv_status
Tibrv_API::CreateQueue( tibrvQueue * q ) noexcept
{
  api_Queue * queue = this->make<api_Queue>( TIBRV_QUEUE );
  *q = queue->id;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::TimedDispatchQueue( tibrvQueue q, tibrv_f64 timeout ) noexcept
{
  return api_status( this->timed_dispatch_queue( q, timeout ) );
}

tibrv_status
Tibrv_API::TimedDispatchQueueOneEvent( tibrvQueue q,
                                       tibrv_f64 timeout ) noexcept
{
  return api_status( this->timed_dispatch_one_event( q, timeout ) );
}

tibrv_status
Tibrv_API::DestroyQueue( tibrvQueue q, tibrvQueueOnComplete cb,
                        const void * cl ) noexcept
{
  return api_status( this->destroy_queue( q, (ApiQueueOnComplete) cb, cl ) );
}

tibrv_status
Tibrv_API::GetQueueCount( tibrvQueue q, tibrv_u32 * num ) noexcept
{
  uint32_t n;
  ApiStatus st = this->get_queue_count( q, n );
  if ( st == API_OK )
    *num = n;
  return api_status( st );
}

tibrv_status
Tibrv_API::GetQueuePriority( tibrvQueue q, tibrv_u32 * priority ) noexcept
{
  api_Queue * queue = this->get<api_Queue>( q, TIBRV_QUEUE );
  if ( queue == NULL || queue->done )
    return TIBRV_INVALID_QUEUE;
  *priority = queue->priority;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::SetQueuePriority( tibrvQueue q, tibrv_u32 prio ) noexcept
{
  api_Queue * queue = this->get<api_Queue>( q, TIBRV_QUEUE );
  if ( queue == NULL || queue->done )
    return TIBRV_INVALID_QUEUE;
  queue->priority = prio;
  if ( queue->grp != NULL )
    queue->grp->update = true;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::GetQueueLimitPolicy( tibrvQueue q, tibrvQueueLimitPolicy * policy,
                               tibrv_u32 * max_ev, tibrv_u32 * discard ) noexcept
{
  api_Queue * queue = this->get<api_Queue>( q, TIBRV_QUEUE );
  if ( queue == NULL || queue->done )
    return TIBRV_INVALID_QUEUE;
  *policy  = (tibrvQueueLimitPolicy) queue->policy;
  *max_ev  = queue->max_ev;
  *discard = queue->discard;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::SetQueueLimitPolicy( tibrvQueue q, tibrvQueueLimitPolicy policy,
                               tibrv_u32 max_ev, tibrv_u32 discard ) noexcept
{
  api_Queue * queue = this->get<api_Queue>( q, TIBRV_QUEUE );
  if ( queue == NULL || queue->done )
    return TIBRV_INVALID_QUEUE;
  queue->policy  = (ApiQueueLimitPolicy) policy;
  queue->max_ev  = max_ev;
  queue->discard = discard;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::SetQueueName( tibrvQueue q, const char * name ) noexcept
{
  api_Queue * queue = this->get<api_Queue>( q, TIBRV_QUEUE );
  if ( queue == NULL || queue->done )
    return TIBRV_INVALID_QUEUE;
  this->set_string( queue->name, name );
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::GetQueueName( tibrvQueue q, const char ** name ) noexcept
{
  api_Queue * queue = this->get<api_Queue>( q, TIBRV_QUEUE );
  if ( queue == NULL || queue->done )
    return TIBRV_INVALID_QUEUE;
  *name = queue->name;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::SetQueueHook( tibrvQueue q, tibrvQueueHook hook, void * cl ) noexcept
{
  api_Queue * queue = this->get<api_Queue>( q, TIBRV_QUEUE );
  if ( queue == NULL || queue->done )
    return TIBRV_INVALID_QUEUE;
  queue->hook = hook;
  queue->hook_cl = cl;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::GetQueueHook( tibrvQueue q, tibrvQueueHook * hook ) noexcept
{
  api_Queue * queue = this->get<api_Queue>( q, TIBRV_QUEUE );
  if ( queue == NULL || queue->done )
    return TIBRV_INVALID_QUEUE;
  *hook = queue->hook;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::CreateQueueGroup( tibrvQueueGroup * grp ) noexcept
{
  api_QueueGroup * g = this->make<api_QueueGroup>( TIBRV_QUEUE_GROUP );
  *grp = g->id;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::TimedDispatchGroup( tibrvQueueGroup grp, tibrv_f64 timeout ) noexcept
{
  return api_status( this->timed_dispatch_group( grp, timeout ) );
}

tibrv_status
Tibrv_API::DestroyQueueGroup( tibrvQueueGroup grp ) noexcept
{
  return api_status( this->destroy_queue_group( grp ) );
}

tibrv_status
Tibrv_API::AddQueueGroup( tibrvQueueGroup grp, tibrvQueue q ) noexcept
{
  return api_status( this->add_queue_group( grp, q ) );
}

tibrv_status
Tibrv_API::RemoveQueueGroup( tibrvQueueGroup grp, tibrvQueue q ) noexcept
{
  return api_status( this->remove_queue_group( grp, q ) );
}

tibrv_status
Tibrv_API::CreateTransport( tibrvTransport * tport, const char * service,
                            const char * network, const char * daemon ) noexcept
{
#define alen( s ) ( s == NULL ? 0 : ( ::strlen( s ) + 1 ) )
  size_t add = alen( service ) + alen( network ) + alen( daemon );
#undef alen

  api_Transport * t = this->make<api_Transport>( TIBRV_TRANSPORT, add );
  *tport = t->id;
  EvRvClientParameters parm( daemon, network, service );
  parm.opts |= kv::OPT_CONNECT_NB;
  char * p = (char *) (void *) &t[ 1 ];

#define acat( x, s ) \
  { size_t l = ::strlen( s ) + 1; ::memcpy( p, s, l ); x = p; p += l; }
  if ( service != NULL )
    acat( t->x.service, service );
  if ( network != NULL )
    acat( t->x.network, network );
  if ( daemon != NULL )
    acat( t->x.daemon, daemon );
#undef acat

  EvPipeRec rec( OP_CREATE_TPORT, t, &parm, &t->mutex, &t->cond );
  tibrv_status ret = TIBRV_OK;

  pthread_mutex_lock( &t->mutex );
  this->ev_read->exec( rec );

  struct timespec ts = ts_timeout( 10.0 );
#if defined( _MSC_VER ) || defined( __MINGW32__ )
  RV7_TRACE( "create_tport: after connect rv_state=%d (ERR_CLOSE=%d DATA_RECV=%d) tick=%lu sock_err=%d\n",
             (int) t->client.rv_state, (int) EvRvClient::ERR_CLOSE, (int) EvRvClient::DATA_RECV,
             (unsigned long) GetTickCount(), (int) t->client.sock_err );
#endif
  while ( t->client.rv_state > EvRvClient::ERR_CLOSE &&
          t->client.rv_state < EvRvClient::DATA_RECV ) {
    int wr = pthread_cond_timedwait( &t->cond, &t->mutex, &ts );
#if defined( _MSC_VER ) || defined( __MINGW32__ )
    RV7_TRACE( "create_tport: timedwait=%d rv_state=%d tick=%lu\n", wr, (int) t->client.rv_state, (unsigned long) GetTickCount() );
#endif
    if ( wr == ETIMEDOUT ) {
      EvPipeRec rec2( OP_CLOSE_TPORT, t, &parm, &t->mutex, &t->cond );
      this->ev_read->exec( rec2 );
    }
  }
  if ( t->client.rv_state != EvRvClient::DATA_RECV )
    ret = TIBRV_DAEMON_NOT_CONNECTED;
  ::memcpy( t->x.session, t->client.session, sizeof( t->x.session ) );
  t->x.session_len = t->client.session_len;
  pthread_mutex_unlock( &t->mutex );
  if ( ret != TIBRV_OK )
    *tport = TIBRV_INVALID_ID;
  return ret;
}

void
EvPipe::create_tport( EvPipeRec &rec ) noexcept
{
  rec.t->client.rv_connect( *rec.parm, rec.t, rec.t );
}

void
EvPipe::close_tport( EvPipeRec &rec ) noexcept
{
  if ( rec.t->client.in_list( IN_ACTIVE_LIST ) )
    rec.t->client.idle_push( EV_CLOSE );
}

/* head of the calling thread's chain of send accumulators (one per tport) */
static thread_local SendCtx * tls_send_head = NULL;

void
Tibrv_API::note_dispatch_send( api_Transport * t ) noexcept
{
  DispatchTLS & d = tls_dispatch;
  if ( api_dispatch_tls.depth == 0 )          /* not a dispatch thread: backstops only */
    return;
  for ( uint32_t i = 0; i < d.cnt; i++ )
    if ( d.ids[ i ] == t->id )
      return;
  if ( d.cnt == MAX_DISPATCH_DIRTY ) { /* table full: flush this one now */
    this->flush_transport( t );
    return;
  }
  d.ids[ d.cnt++ ] = t->id;
}

void
Tibrv_API::flush_dispatch_sends( void ) noexcept
{
  DispatchTLS & d = tls_dispatch;
  uint32_t cnt = d.cnt;
  d.cnt = 0;
  for ( uint32_t i = 0; i < cnt; i++ ) {
    api_Transport * t = this->get<api_Transport>( d.ids[ i ], TIBRV_TRANSPORT );
    if ( t != NULL && ! t->is_destroyed )
      this->flush_transport( t );
  }
}

SendCtx *
Tibrv_API::get_send_ctx( api_Transport * t ) noexcept
{
  for ( SendCtx * c = tls_send_head; c != NULL; c = c->tls_next )
    if ( c->t == t )
      return c;
  SendCtx * c = new ( ::malloc( sizeof( SendCtx ) ) ) SendCtx( t );
  pthread_mutex_lock( &t->batch_mutex );
  t->writers.push_tl( c );
  pthread_mutex_unlock( &t->batch_mutex );
  c->tls_next   = tls_send_head;
  tls_send_head = c;
  return c;
}

/* Append a marshaled copy of m into c (caller holds c->lock). */
static bool
send_ctx_append( SendCtx * c,  api_Transport * t,  tibrvMsg * vec,
                 uint32_t cnt ) noexcept
{
  if ( c == NULL )
    return false;
  for ( uint32_t i = 0; i < cnt; i++ ) {
    api_Msg    * m    = (api_Msg *) vec[ i ];
    tibrv_u32    datalen;
    const void * data = m->get_as_bytes( &datalen );
    char       * subj = (char *) c->byte_mem.make( m->subject_len + 1 );
    ::memcpy( subj, m->subject, m->subject_len );
    subj[ m->subject_len ] = 0;
    const char * rep = NULL;
    if ( m->reply_len > 0 ) {
      char * r = (char *) c->byte_mem.make( m->reply_len + 1 );
      ::memcpy( r, m->reply, m->reply_len );
      r[ m->reply_len ] = 0;
      rep = r;
    }
    void * d = c->byte_mem.make( datalen );
    ::memcpy( d, data, datalen );
    if ( c->cnt == c->cap ) {
      uint32_t    ncap = c->cap ? c->cap * 2 : 64;
      EvPublish * np   = (EvPublish *) ::malloc( ncap * sizeof( EvPublish ) );
      if ( c->cnt > 0 )
        ::memcpy( (void *) np, (void *) c->pubs, c->cnt * sizeof( EvPublish ) );
      if ( c->pubs != NULL )
        ::free( c->pubs );
      c->pubs = np;
      c->cap  = ncap;
    }
    new ( &c->pubs[ c->cnt++ ] )
      EvPublish( subj, m->subject_len, rep, m->reply_len, d, datalen,
                 t->client.sub_route, *t->me, 0, RVMSG_TYPE_ID );
    c->bytes += (uint32_t) datalen;
  }
  return ( t->batch_size != 0 && c->bytes >= t->batch_size );
}

tibrv_status
Tibrv_API::flush_send_ctx( SendCtx * c ) noexcept
{
  return this->flush_transport( c->t );
}

tibrv_status
Tibrv_API::flush_transport( api_Transport * t ) noexcept
{
  if ( this->on_ev_thread() ) {  /* exec() would wait for ourselves */
    this->drain_transport( t );
    return TIBRV_OK;
  }
  EvPipeRec rec( OP_TPORT_DRAIN, t, (EvRvClientParameters *) NULL,
                 &t->mutex, &t->cond );
  pthread_mutex_lock( &t->mutex );
  this->ev_read->exec( rec );
  pthread_mutex_unlock( &t->mutex );
  return TIBRV_OK;
}

void
EvPipe::tport_drain( EvPipeRec &rec ) noexcept
{
  rec.t->api.drain_transport( rec.t );
}

/* flush every registered writer of a transport */
tibrv_status
Tibrv_API::Flush( tibrvTransport tport ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;
  return this->flush_transport( t );
}

void
Tibrv_API::free_transport_writers( api_Transport * t ) noexcept
{
  this->flush_transport( t );
  pthread_mutex_lock( &t->batch_mutex );
  while ( ! t->writers.is_empty() ) {
    SendCtx * c = t->writers.pop_hd();
    if ( c->pubs != NULL )
      ::free( c->pubs );
    c->~SendCtx();
    ::free( c );
  }
  pthread_mutex_unlock( &t->batch_mutex );
}

void
Tibrv_API::drain_transport( api_Transport * t ) noexcept
{
  pthread_mutex_lock( &t->batch_mutex );
  for ( SendCtx * c = t->writers.hd; c != NULL; c = c->next ) {
    pthread_mutex_lock( &c->lock );
    if ( c->t->id != TIBRV_PROCESS_TRANSPORT ) {
      for ( uint32_t i = 0; i < c->cnt; i++ )
        c->t->client.publish( c->pubs[ i ] );
    }
    else {
      for ( uint32_t i = 0; i < c->cnt; i++ ) {
        c->pubs[ i ].subj_hash =
          kv_crc_c( c->pubs[ i ].subject, c->pubs[ i ].subject_len, 0 );
        c->t->client.sub_route.forward_msg( c->pubs[ i ] );
      }
    }
    c->cnt   = 0;
    c->bytes = 0;
    c->byte_mem.reuse();
    pthread_mutex_unlock( &c->lock );
  }
  SendCtx * full;
  t->sb_pending = false;
  full = t->sb_fill;
  if ( full == NULL || full->cnt == 0 ) {
    pthread_mutex_unlock( &t->batch_mutex );
    return;
  }
  t->sb_fill  = t->sb_spare;   /* owners now append to the empty buffer */
  t->sb_spare = full;          /* reset below, before the next drain runs */
  pthread_mutex_unlock( &t->batch_mutex );

  if ( t->id != TIBRV_PROCESS_TRANSPORT ) {
    for ( uint32_t i = 0; i < full->cnt; i++ )
      t->client.publish( full->pubs[ i ] );
  }
  else {
    for ( uint32_t i = 0; i < full->cnt; i++ ) {
      full->pubs[ i ].subj_hash =
        kv_crc_c( full->pubs[ i ].subject, full->pubs[ i ].subject_len, 0 );
      t->client.sub_route.forward_msg( full->pubs[ i ] );
    }
  }
  full->cnt   = 0;
  full->bytes = 0;
  full->byte_mem.reuse();
}

void
Tibrv_API::free_send_buf( api_Transport * t ) noexcept
{
  if ( t->sb_timer_active ) {
    EvPipeRec rec( OP_STOP_BATCH_TMR, t, (EvRvClientParameters *) NULL,
                   &t->mutex, &t->cond );
    pthread_mutex_lock( &t->mutex );
    this->ev_read->exec( rec );
    pthread_mutex_unlock( &t->mutex );
  }
  this->flush_transport( t );

  if ( t->sb_fill != NULL ) {
    if ( t->sb_fill->pubs != NULL )
      ::free( t->sb_fill->pubs );
    t->sb_fill->~SendCtx();
    ::free( t->sb_fill );
    t->sb_fill = NULL;
  }
  if ( t->sb_spare != NULL ) {
    if ( t->sb_spare->pubs != NULL )
      ::free( t->sb_spare->pubs );
    t->sb_spare->~SendCtx();
    ::free( t->sb_spare );
    t->sb_spare = NULL;
  }
}

tibrv_status
Tibrv_API::Send( tibrvTransport tport, tibrvMsg msg ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;
  if ( t->batch_mode == TIBRV_TRANSPORT_TIMER_BATCH ) {
    SendCtx * c = this->get_send_ctx( t );
    pthread_mutex_lock( &c->lock );
    bool do_flush = send_ctx_append( c, t, &msg, 1 );
    pthread_mutex_unlock( &c->lock );
    if ( do_flush )
      return this->flush_send_ctx( c );
    if ( t->dispatch_flush )
      this->note_dispatch_send( t );
    return TIBRV_OK;
  }
  if ( t->batch_mode == TIBRV_TRANSPORT_SINGLE_BATCH ) {
    pthread_mutex_lock( &t->batch_mutex );
    bool do_drain = send_ctx_append( t->sb_fill, t, &msg, 1 ) && !t->sb_pending;
    if ( do_drain )
      t->sb_pending = true;
    pthread_mutex_unlock( &t->batch_mutex );
    if ( do_drain )
      return this->flush_transport( t );
    if ( t->dispatch_flush )
      this->note_dispatch_send( t );
    return TIBRV_OK;
  }
  api_Msg    * m    = (api_Msg *) msg;
  tibrv_u32    datalen;
  const void * data = m->get_as_bytes( &datalen );
  EvPublish pub( m->subject, m->subject_len, m->reply, m->reply_len,
                 data, datalen, t->client.sub_route, *t->me, 0, RVMSG_TYPE_ID );
  EvPipeRec rec( OP_TPORT_SEND, t, &pub, 1, &t->mutex, &t->cond );
  pthread_mutex_lock( &t->mutex );
  this->ev_read->exec( rec );
  pthread_mutex_unlock( &t->mutex );
  return TIBRV_OK;
}

void
EvPipe::tport_send( EvPipeRec &rec ) noexcept
{
  if ( rec.t->id != TIBRV_PROCESS_TRANSPORT )
    rec.t->client.publish( *rec.pub );
  else {
    rec.pub->subj_hash =
      kv_crc_c( rec.pub->subject, rec.pub->subject_len, 0 );
    rec.t->client.sub_route.forward_msg( *rec.pub );
  }
}

tibrv_status
Tibrv_API::Sendv( tibrvTransport tport, tibrvMsg * vec, tibrv_u32 cnt ) noexcept
{
  if ( cnt == 0 )
    return TIBRV_OK;

  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;

  if ( t->batch_mode == TIBRV_TRANSPORT_TIMER_BATCH ) {
    SendCtx * c = this->get_send_ctx( t );
    pthread_mutex_lock( &c->lock );
    bool do_flush = send_ctx_append( c, t, vec, cnt );
    pthread_mutex_unlock( &c->lock );
    if ( do_flush )
      return this->flush_send_ctx( c );
    if ( t->dispatch_flush )
      this->note_dispatch_send( t );
    return TIBRV_OK;
  }
  if ( t->batch_mode == TIBRV_TRANSPORT_SINGLE_BATCH ) {
    pthread_mutex_lock( &t->batch_mutex );
    bool do_drain = send_ctx_append( t->sb_fill, t, vec, cnt ) &&!t->sb_pending;
    if ( do_drain )
      t->sb_pending = true;
    pthread_mutex_unlock( &t->batch_mutex );
    if ( do_drain )
      return this->flush_transport( t );
    if ( t->dispatch_flush )
      this->note_dispatch_send( t );
    return TIBRV_OK;
  }

  MDMsgMem    tmp;
  void      * pvec = tmp.make( sizeof( EvPublish ) * cnt );
  EvPublish * pub  = (EvPublish *) pvec;

  for ( tibrv_u32 i = 0; i < cnt; i++ ) {
    api_Msg    * m       = (api_Msg *) vec[ i ];
    tibrv_u32    datalen;
    const void * data    = m->get_as_bytes( &datalen );
    new ( &pub[ i ] )
      EvPublish( m->subject, m->subject_len, m->reply, m->reply_len,
                 data, datalen, t->client.sub_route, *t->me, 0, RVMSG_TYPE_ID );
  }
  EvPipeRec rec( OP_TPORT_SENDV, t, pub, cnt, &t->mutex, &t->cond );
  pthread_mutex_lock( &t->mutex );
  this->ev_read->exec( rec );
  pthread_mutex_unlock( &t->mutex );
  return TIBRV_OK;
}

void
EvPipe::tport_sendv( EvPipeRec &rec ) noexcept
{
  if ( rec.t->id != TIBRV_PROCESS_TRANSPORT ) {
    for ( tibrv_u32 i = 0; i < rec.cnt; i++ )
      rec.t->client.publish( rec.pub[ i ] );
  }
  else {
    for ( tibrv_u32 i = 0; i < rec.cnt; i++ ) {
      rec.pub[ i ].subj_hash =
        kv_crc_c( rec.pub[ i ].subject, rec.pub[ i ].subject_len, 0 );
      rec.t->client.sub_route.forward_msg( rec.pub[ i ] );
    }
  }
}

void
EvPipe::start_batch_timer( EvPipeRec &rec ) noexcept
{
  if ( rec.t->sb_timer_active ) {
    this->poll.timer.remove_timer_cb( rec.t->sb_timer, (uint64_t) rec.t->id, 0);
  }
  rec.t->sb_timer_active = true;
  this->poll.timer.add_timer_double( rec.t->sb_timer, rec.t->batch_ival,
                                     (uint64_t) rec.t->id, 0 );
}

void
EvPipe::stop_batch_timer( EvPipeRec &rec ) noexcept
{
  this->poll.timer.remove_timer_cb( rec.t->sb_timer, (uint64_t) rec.t->id, 0 );
  rec.t->sb_timer_active = false;
}

bool
api_BatchTimer::timer_cb( uint64_t,  uint64_t ) noexcept
{
  /* on E: drain inline -- flush_transport() would exec() a pipe request and
   * wait for E (this thread) to complete it */
  this->t->api.drain_transport( this->t );
  return this->t->sb_timer_active;
}

tibrv_status
Tibrv_API::SendRequest( tibrvTransport tport, tibrvMsg msg, tibrvMsg * reply,
                        tibrv_f64 idle_timeout ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;
  if ( t->batch_mode != TIBRV_TRANSPORT_DEFAULT_BATCH )
    this->flush_transport( t );

  api_Msg * m = (api_Msg *) msg;
  if ( m->reply_len == 0 ) {
    char inbox[ MAX_RV_INBOX_LEN ];
    t->client.make_inbox( inbox, t->inbox_count++ );
    size_t len = ::strlen( inbox );
    m->reply = m->mem.stralloc( len, inbox );
    m->reply_len = len;
  }
  tibrv_u32    datalen;
  const void * data    = m->get_as_bytes( &datalen );
  EvPublish pub( m->subject, m->subject_len, m->reply, m->reply_len,
                 data, datalen, t->client.sub_route, *t->me, 0, RVMSG_TYPE_ID );
  EvPipeRec rec( OP_TPORT_SEND, t, &pub, 1, &t->mutex, &t->cond );
  api_Rpc   rpc( m->reply, m->reply_len,
                 kv_crc_c( m->reply, m->reply_len, 0 ) );
  pthread_mutex_lock( &t->mutex );
  t->rpc_list.push_hd( &rpc );
  this->ev_read->exec( rec );
  struct timespec ts = ts_timeout( idle_timeout );
  while ( rpc.reply == NULL ) {
    if ( idle_timeout >= 0.0 ) {
      if ( pthread_cond_timedwait( &t->cond, &t->mutex, &ts ) == ETIMEDOUT )
        break;
    }
    else {
      pthread_cond_wait( &t->cond, &t->mutex );
    }
  }
  *reply = rpc.reply;
  t->rpc_list.pop( &rpc );
  if ( rpc.reply != NULL )
    rpc.reply->in_queue = false;
  pthread_mutex_unlock( &t->mutex );

  if ( *reply == NULL )
    return TIBRV_NOT_FOUND;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::SendReply( tibrvTransport tport, tibrvMsg msg,
                      tibrvMsg request_msg ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;
  if ( t->batch_mode != TIBRV_TRANSPORT_DEFAULT_BATCH )
    this->flush_transport( t );

  api_Msg    * m       = (api_Msg *) msg,
             * r       = (api_Msg *) request_msg;
  tibrv_u32    datalen;
  const void * data    = m->get_as_bytes( &datalen );
  EvPublish pub( r->reply, r->reply_len, m->reply, m->reply_len,
                 data, datalen, t->client.sub_route, *t->me, 0, RVMSG_TYPE_ID );
  EvPipeRec rec( OP_TPORT_SEND, t, &pub, 1, &t->mutex, &t->cond );
  pthread_mutex_lock( &t->mutex );
  this->ev_read->exec( rec );
  pthread_mutex_unlock( &t->mutex );
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::DestroyTransport( tibrvTransport tport ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;

  this->free_transport_writers( t );
  this->free_send_buf( t );
  pthread_mutex_lock( &t->mutex );
  EvPipeRec rec2( OP_CLOSE_TPORT, t, (EvRvClientParameters *) NULL,
                  &t->mutex, &t->cond );
  t->api.ev_read->exec( rec2 );
  t->is_destroyed = true;
  pthread_mutex_unlock( &t->mutex );
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::CreateInbox( tibrvTransport tport, char * inbox_str,
                        tibrv_u32 inbox_len ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL ) {
    if ( inbox_len > 0 )
      *inbox_str = 0;
    return TIBRV_INVALID_TRANSPORT;
  }

  pthread_mutex_lock( &t->mutex );
  uint32_t num = t->inbox_count++;
  pthread_mutex_unlock( &t->mutex );

  char inbox[ MAX_RV_INBOX_LEN ];
  CatPtr p( inbox );
  p.s( "_INBOX." );
  if ( t->x.session_len > 0 ) {
    p.b( t->x.session, t->x.session_len )
     .c( '.' );
  }
  size_t len = p.u( num ).end();
  if ( inbox_len > 0 )
    ::memcpy( inbox_str, inbox, len + 1 <= inbox_len ? len + 1 : inbox_len );
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::GetService( tibrvTransport tport, const char ** service_string ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;
  *service_string = t->client.service;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::GetNetwork( tibrvTransport tport, const char ** network_string ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;
  *network_string = t->client.network;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::GetDaemon( tibrvTransport tport, const char ** daemon_string ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;
  *daemon_string = t->client.daemon;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::SetDescription( tibrvTransport tport, const char * descr ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;
  this->set_string( t->descr, descr );
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::GetDescription( tibrvTransport tport, const char ** descr ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;
  *descr = t->descr;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::SetSendingWaitLimit( tibrvTransport tport, tibrv_u32 num_bytes ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;
  t->wait_limit = num_bytes;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::GetSendingWaitLimit( tibrvTransport tport, tibrv_u32 * num_bytes ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;
  *num_bytes = t->wait_limit;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::SetBatchMode( tibrvTransport tport, tibrvTransportBatchMode mode ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;
  t->batch_mode = mode;
  if ( mode == TIBRV_TRANSPORT_SINGLE_BATCH && t->sb_fill == NULL ) {
    /* one shared, double-buffered accumulator + an E-thread flush timer */
    t->sb_fill  = new ( ::malloc( sizeof( SendCtx ) ) ) SendCtx( t );
    t->sb_spare = new ( ::malloc( sizeof( SendCtx ) ) ) SendCtx( t );
  }
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::SetBatchDispatchFlush( tibrvTransport tport, bool on ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;
  t->dispatch_flush = on;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::SetBatchInterval( tibrvTransport tport, tibrv_f64 secs ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;
  t->batch_ival = secs;
  if ( secs > 0.0 || t->sb_timer_active ) {
    EvPipeRec rec( secs > 0.0 ? OP_START_BATCH_TMR : OP_STOP_BATCH_TMR, t,
                   (EvRvClientParameters *) NULL, &t->mutex, &t->cond );
    pthread_mutex_lock( &t->mutex );
    this->ev_read->exec( rec );
    pthread_mutex_unlock( &t->mutex );
  }
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::SetBatchSize( tibrvTransport tport, tibrv_u32 num_bytes ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;
  t->batch_size = num_bytes;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::RequestReliability( tibrvTransport tport,
                               tibrv_f64 /*reliability*/ ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::CreateDispatcher( tibrvDispatcher * disp, tibrvDispatchable able,
                             tibrv_f64 idle_timeout ) noexcept
{
  api_Dispatcher * d = this->create_dispatcher( able, idle_timeout );
  *disp = d->id;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::JoinDispatcher( tibrvDispatcher disp ) noexcept
{
  return api_status( this->join_dispatcher( disp ) );
}

tibrv_status
Tibrv_API::SetDispatcherName( tibrvDispatcher disp, const char * name ) noexcept
{
  api_Dispatcher * d = this->get<api_Dispatcher>( disp, TIBRV_DISPATCHER );
  if ( d == NULL )
    return TIBRV_INVALID_DISPATCHABLE;
  this->set_string( d->name, name );
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::GetDispatcherName( tibrvDispatcher disp, const char ** name ) noexcept
{
  api_Dispatcher * d = this->get<api_Dispatcher>( disp, TIBRV_DISPATCHER );
  if ( d == NULL )
    return TIBRV_INVALID_DISPATCHABLE;
  *name = d->name;
  return TIBRV_OK;
}

}

extern "C" {

using namespace rv7;

const char *
tibrv_Version( void )
{
  return "sassrv-" kv_stringify( SASSRV_VER );
}

tibrv_status
tibrv_Open( void )
{
  if ( tibrv_api == NULL ) {
    tibrv_api = new ( aligned_malloc( sizeof( Tibrv_API ) ) ) Tibrv_API();
    return tibrv_api->Open();
  }
  return TIBRV_OK;
}

tibrv_status
tibrv_Close( void )
{
  return TIBRV_OK;
}

tibrv_status
tibrv_SetCodePages( char * /*host_codepage*/, char * /*net_codepage*/)
{
  return TIBRV_OK;
}

tibrv_status
tibrv_SetRVParameters( tibrv_u32 /*argc*/, const char  ** /*argv*/ )
{
  return TIBRV_NOT_PERMITTED;
}

tibrv_status
tibrv_OpenEx( const char * /*pathname*/ )
{
  return TIBRV_NOT_PERMITTED;
}

tibrv_bool
tibrv_IsIPM( void )
{
  return TIBRV_FALSE;
}

tibrv_status
tibrvEvent_CreateListener( tibrvEvent * event,  tibrvQueue q,
                           tibrvEventCallback cb,  tibrvTransport tport,
                           const char * subj,  const void * closure )
{
  return tibrv_api->CreateListener( event, q, tport, cb, NULL, subj, closure );
}

tibrv_status
tibrvEvent_CreateVectorListener( tibrvEvent * event,  tibrvQueue q,
                                 tibrvEventVectorCallback vcb,
                                 tibrvTransport tport, const char * subj,
                                 const void * closure )
{
  return tibrv_api->CreateListener( event, q, tport, NULL, vcb, subj, closure );
}

tibrv_status
tibrvEvent_CreateTimer( tibrvEvent * event,  tibrvQueue q,
                        tibrvEventCallback cb,  tibrv_f64 ival,
                        const void * closure )
{
  return tibrv_api->CreateTimer( event, q, cb, ival, closure );
}

tibrv_status
tibrvEvent_DestroyEx( tibrvEvent event,  tibrvEventOnComplete cb )
{
  return tibrv_api->DestroyEvent( event, cb );
}

tibrv_status
tibrvEvent_GetType( tibrvEvent event,  tibrvEventType * type )
{
  return tibrv_api->GetEventType( event, type );
}

tibrv_status
tibrvEvent_GetQueue( tibrvEvent event,  tibrvQueue * q )
{
  return tibrv_api->GetEventQueue( event, q );
}

tibrv_status
tibrvEvent_GetListenerSubject( tibrvEvent event,  const char ** subject )
{
  return tibrv_api->GetListenerSubject( event, subject );
}

tibrv_status
tibrvEvent_GetListenerTransport( tibrvEvent event,  tibrvTransport * tport )
{
  return tibrv_api->GetListenerTransport( event, tport );
}

tibrv_status
tibrvEvent_GetTimerInterval( tibrvEvent event,  tibrv_f64 * ival )
{
  return tibrv_api->GetTimerInterval( event, ival );
}

tibrv_status
tibrvEvent_ResetTimerInterval( tibrvEvent event,  tibrv_f64 ival )
{
  return tibrv_api->ResetTimerInterval( event, ival );
}

tibrv_status
tibrvQueue_Create( tibrvQueue * q )
{
  return tibrv_api->CreateQueue( q );
}

tibrv_status
tibrvQueue_TimedDispatch( tibrvQueue q, tibrv_f64 timeout )
{
  return tibrv_api->TimedDispatchQueue( q, timeout );
}

tibrv_status
tibrvQueue_TimedDispatchOneEvent( tibrvQueue q, tibrv_f64 timeout )
{
  return tibrv_api->TimedDispatchQueueOneEvent( q, timeout );
}

tibrv_status
tibrvQueue_DestroyEx( tibrvQueue q, tibrvQueueOnComplete cb, const void * cl )
{
  return tibrv_api->DestroyQueue( q, cb, cl );
}

tibrv_status
tibrvQueue_GetCount( tibrvQueue q, tibrv_u32 * num )
{
  return tibrv_api->GetQueueCount( q, num );
}

tibrv_status
tibrvQueue_GetPriority( tibrvQueue q, tibrv_u32 * priority )
{
  return tibrv_api->GetQueuePriority( q, priority );
}

tibrv_status
tibrvQueue_SetPriority( tibrvQueue q, tibrv_u32 prio )
{
  return tibrv_api->SetQueuePriority( q, prio );
}

tibrv_status
tibrvQueue_GetLimitPolicy( tibrvQueue q, tibrvQueueLimitPolicy * policy,
                           tibrv_u32 * max_ev, tibrv_u32 * discard )
{
  return tibrv_api->GetQueueLimitPolicy( q, policy, max_ev, discard );
}

tibrv_status
tibrvQueue_SetLimitPolicy( tibrvQueue q, tibrvQueueLimitPolicy policy,
                           tibrv_u32 max_ev, tibrv_u32 discard )
{
  return tibrv_api->SetQueueLimitPolicy( q, policy, max_ev, discard );
}

tibrv_status
tibrvQueue_SetName( tibrvQueue q, const char * name )
{
  return tibrv_api->SetQueueName( q, name );
}

tibrv_status
tibrvQueue_GetName( tibrvQueue q, const char ** name )
{
  return tibrv_api->GetQueueName( q, name );
}

tibrv_status
tibrvQueue_SetHook( tibrvQueue q, tibrvQueueHook hook, void * closure )
{
  return tibrv_api->SetQueueHook( q, hook, closure );
}

tibrv_status
tibrvQueue_GetHook( tibrvQueue q, tibrvQueueHook * hook )
{
  return tibrv_api->GetQueueHook( q, hook );
}

tibrv_status
tibrvQueueGroup_Create( tibrvQueueGroup * grp )
{
  return tibrv_api->CreateQueueGroup( grp );
}

tibrv_status
tibrvQueueGroup_TimedDispatch( tibrvQueueGroup grp, tibrv_f64 timeout )
{
  return tibrv_api->TimedDispatchGroup( grp, timeout );
}

tibrv_status
tibrvQueueGroup_Destroy( tibrvQueueGroup grp )
{
  return tibrv_api->DestroyQueueGroup( grp );
}

tibrv_status
tibrvQueueGroup_Add( tibrvQueueGroup grp, tibrvQueue q )
{
  return tibrv_api->AddQueueGroup( grp, q );
}

tibrv_status
tibrvQueueGroup_Remove( tibrvQueueGroup grp, tibrvQueue q )
{
  return tibrv_api->RemoveQueueGroup( grp, q );
}

tibrv_status
tibrvTransport_Create( tibrvTransport * tport, const char * service,
                       const char * network, const char * daemon )
{
  return tibrv_api->CreateTransport( tport, service, network, daemon );
}

tibrv_status
tibrvTransport_Send( tibrvTransport tport, tibrvMsg msg )
{
  return tibrv_api->Send( tport, msg );
}

tibrv_status
tibrvTransport_Sendv( tibrvTransport tport, tibrvMsg * vec, tibrv_u32 cnt )
{
  return tibrv_api->Sendv( tport, vec, cnt );
}

tibrv_status
tibrvTransport_Flush( tibrvTransport tport )
{
  return tibrv_api->Flush( tport );
}

tibrv_status
tibrvTransport_SendRequest( tibrvTransport tport, tibrvMsg msg,
                            tibrvMsg * reply, tibrv_f64 idle_timeout )
{
  return tibrv_api->SendRequest( tport, msg, reply, idle_timeout );
}

tibrv_status
tibrvTransport_SendReply( tibrvTransport tport, tibrvMsg msg,
                          tibrvMsg request_msg )
{
  return tibrv_api->SendReply( tport, msg, request_msg );
}

tibrv_status
tibrvTransport_Destroy( tibrvTransport tport )
{
  return tibrv_api->DestroyTransport( tport );
}

tibrv_status
tibrvTransport_CreateInbox( tibrvTransport tport, char * inbox_str,
                            tibrv_u32 inbox_len )
{
  return tibrv_api->CreateInbox( tport, inbox_str, inbox_len );
}

tibrv_status
tibrvTransport_GetService( tibrvTransport tport, const char ** service_string )
{
  return tibrv_api->GetService( tport, service_string );
}

tibrv_status
tibrvTransport_GetNetwork( tibrvTransport tport, const char ** network_string )
{
  return tibrv_api->GetNetwork( tport, network_string );
}

tibrv_status
tibrvTransport_GetDaemon( tibrvTransport tport, const char ** daemon_string )
{
  return tibrv_api->GetDaemon( tport, daemon_string );
}

tibrv_status
tibrvTransport_SetDescription( tibrvTransport tport, const char * descr )
{
  return tibrv_api->SetDescription( tport, descr );
}

tibrv_status
tibrvTransport_GetDescription( tibrvTransport tport, const char ** descr )
{
  return tibrv_api->GetDescription( tport, descr );
}

tibrv_status
tibrvTransport_SetSendingWaitLimit( tibrvTransport tport, tibrv_u32 num_bytes )
{
  return tibrv_api->SetSendingWaitLimit( tport, num_bytes );
}

tibrv_status
tibrvTransport_GetSendingWaitLimit( tibrvTransport tport,
                                    tibrv_u32 * num_bytes )
{
  return tibrv_api->GetSendingWaitLimit( tport, num_bytes );
}

tibrv_status
tibrvTransport_SetBatchMode( tibrvTransport tport,
                             tibrvTransportBatchMode mode )
{
  return tibrv_api->SetBatchMode( tport, mode );
}

tibrv_status
tibrvTransport_SetBatchSize( tibrvTransport tport, tibrv_u32 num_bytes )
{
  return tibrv_api->SetBatchSize( tport, num_bytes );
}

tibrv_status
tibrvTransport_SetBatchInterval( tibrvTransport tport, tibrv_f64 secs )
{
  return tibrv_api->SetBatchInterval( tport, secs );
}

tibrv_status
tibrvTransport_SetBatchDispatchFlush( tibrvTransport tport, tibrv_bool on )
{
  return tibrv_api->SetBatchDispatchFlush( tport, on != TIBRV_FALSE );
}

tibrv_status
tibrvTransport_CreateLicensed( tibrvTransport * tport, const char * service,
                               const char * network, const char * daemon,
                               const char * )
{
  return tibrvTransport_Create( tport, service, network, daemon );
}

tibrv_status
tibrvTransport_RequestReliability( tibrvTransport tport, tibrv_f64 reliability )
{
  return tibrv_api->RequestReliability( tport, reliability );
}

tibrv_status
tibrvDispatcher_CreateEx( tibrvDispatcher * disp,
                          tibrvDispatchable able, tibrv_f64 idle_timeout )
{
  return tibrv_api->CreateDispatcher( disp, able, idle_timeout );
}

tibrv_status
tibrvDispatcher_Join( tibrvDispatcher disp )
{
  return tibrv_api->JoinDispatcher( disp );
}

tibrv_status
tibrvDispatcher_Destroy( tibrvDispatcher disp )
{
  return tibrvDispatcher_Join( disp );
}

tibrv_status
tibrvDispatcher_SetName( tibrvDispatcher disp, const char * name )
{
  return tibrv_api->SetDispatcherName( disp, name );
}

tibrv_status
tibrvDispatcher_GetName( tibrvDispatcher disp, const char ** name )
{
  return tibrv_api->GetDispatcherName( disp, name );
}

const char *
tibrvStatus_GetText( tibrv_status status )
{
  switch ( status ) {
    case TIBRV_OK:                  return "OK";
    case TIBRV_INIT_FAILURE:        return "INIT_FAILURE";
    case TIBRV_INVALID_TRANSPORT:   return "INVALID_TRANSPORT";
    case TIBRV_INVALID_ARG:         return "INVALID_ARG";
    case TIBRV_NOT_INITIALIZED:     return "NOT_INITIALIZED";
    case TIBRV_ARG_CONFLICT:        return "ARG_CONFLICT";
    case TIBRV_SERVICE_NOT_FOUND:   return "SERVICE_NOT_FOUND";
    case TIBRV_NETWORK_NOT_FOUND:   return "NETWORK_NOT_FOUND";
    case TIBRV_DAEMON_NOT_FOUND:    return "DAEMON_NOT_FOUND";
    case TIBRV_NO_MEMORY:           return "NO_MEMORY";
    case TIBRV_INVALID_SUBJECT:     return "INVALID_SUBJECT";
    case TIBRV_DAEMON_NOT_CONNECTED:return "DAEMON_NOT_CONNECTED";
    case TIBRV_VERSION_MISMATCH:    return "VERSION_MISMATCH";
    case TIBRV_SUBJECT_COLLISION:   return "SUBJECT_COLLISION";
    case TIBRV_VC_NOT_CONNECTED:    return "VC_NOT_CONNECTED";
    case TIBRV_NOT_PERMITTED:       return "NOT_PERMITTED";
    case TIBRV_INVALID_NAME:        return "INVALID_NAME";
    case TIBRV_INVALID_TYPE:        return "INVALID_TYPE";
    case TIBRV_INVALID_SIZE:        return "INVALID_SIZE";
    case TIBRV_INVALID_COUNT:       return "INVALID_COUNT";
    case TIBRV_NOT_FOUND:           return "NOT_FOUND";
    case TIBRV_ID_IN_USE:           return "ID_IN_USE";
    case TIBRV_ID_CONFLICT:         return "ID_CONFLICT";
    case TIBRV_CONVERSION_FAILED:   return "CONVERSION_FAILED";
    case TIBRV_RESERVED_HANDLER:    return "RESERVED_HANDLER";
    case TIBRV_ENCODER_FAILED:      return "ENCODER_FAILED";
    case TIBRV_DECODER_FAILED:      return "DECODER_FAILED";
    case TIBRV_INVALID_MSG:         return "INVALID_MSG";
    case TIBRV_INVALID_FIELD:       return "INVALID_FIELD";
    case TIBRV_INVALID_INSTANCE:    return "INVALID_INSTANCE";
    case TIBRV_CORRUPT_MSG:         return "CORRUPT_MSG";
    case TIBRV_ENCODING_MISMATCH:   return "ENCODING_MISMATCH";
    case TIBRV_TIMEOUT:             return "TIMEOUT";
    case TIBRV_INTR:                return "INTR";
    case TIBRV_INVALID_DISPATCHABLE:return "INVALID_DISPATCHABLE";
    case TIBRV_INVALID_DISPATCHER:  return "INVALID_DISPATCHER";
    case TIBRV_INVALID_EVENT:       return "INVALID_EVENT";
    case TIBRV_INVALID_CALLBACK:    return "INVALID_CALLBACK";
    case TIBRV_INVALID_QUEUE:       return "INVALID_QUEUE";
    case TIBRV_INVALID_QUEUE_GROUP: return "INVALID_QUEUE_GROUP";
    case TIBRV_INVALID_TIME_INTERVAL:return "INVALID_TIME_INTERVAL";
    case TIBRV_INVALID_IO_SOURCE:   return "INVALID_IO_SOURCE";
    case TIBRV_INVALID_IO_CONDITION:return "INVALID_IO_CONDITION";
    case TIBRV_SOCKET_LIMIT:        return "SOCKET_LIMIT";
    case TIBRV_OS_ERROR:            return "OS_ERROR";
    case TIBRV_INSUFFICIENT_BUFFER: return "INSUFFICIENT_BUFFER";
    case TIBRV_EOF:                 return "EOF";
    case TIBRV_INVALID_FILE:        return "INVALID_FILE";
    case TIBRV_FILE_NOT_FOUND:      return "FILE_NOT_FOUND";
    case TIBRV_IO_FAILED:           return "IO_FAILED";
    case TIBRV_NOT_FILE_OWNER:      return "NOT_FILE_OWNER";
    case TIBRV_USERPASS_MISMATCH:   return "USERPASS_MISMATCH";
    case TIBRV_TOO_MANY_NEIGHBORS:  return "TOO_MANY_NEIGHBORS";
    case TIBRV_ALREADY_EXISTS:      return "ALREADY_EXISTS";
    case TIBRV_PORT_BUSY:           return "PORT_BUSY";
    case TIBRV_DELIVERY_FAILED:     return "DELIVERY_FAILED";
    case TIBRV_QUEUE_LIMIT:         return "QUEUE_LIMIT";
    case TIBRV_INVALID_CONTENT_DESC:return "INVALID_CONTENT_DESC";
    case TIBRV_INVALID_SERIALIZED_BUFFER:
                                    return "INVALID_SERIALIZED_BUFFER";
    case TIBRV_DESCRIPTOR_NOT_FOUND:return "DESCRIPTOR_NOT_FOUND";
    case TIBRV_CORRUPT_SERIALIZED_BUFFER:
                                    return "CORRUPT_SERIALIZED_BUFFER";
    case TIBRV_IPM_ONLY:            return "IPM_ONLY";
    default: break;
  }
  return "NOT_OK";
}

}
