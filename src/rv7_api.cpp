#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#include <poll.h>
#include <errno.h>
#include <stdarg.h>
#include <sys/time.h>

#include <sassrv/ev_rv_client.h>
#include <raimd/md_msg.h>
#include <raimd/md_dict.h>
#include <raikv/ev_publish.h>
#include <sassrv/rv7api.h>
#include <sassrv/mc.h>
#include <sassrv/rv7cpp.h>

namespace rv7 {

Tibrv_API * tibrv_api;
int debug_api;

static inline timespec
ts_timeout( double timeout, double default_timeout = 0 ) {
  struct timespec ts;
  if ( timeout < 0.0 )
    timeout = default_timeout;
  if ( timeout > 0.0 ) {
    clock_gettime( CLOCK_REALTIME, &ts );
    double frac, i;
    frac = modf( timeout, &i );
    ts.tv_sec  += i;
    ts.tv_nsec += frac * 1000000000.0;
    if ( ts.tv_nsec >= 1000000000 ) {
      ts.tv_sec++;
      ts.tv_nsec -= 1000000000;
    }
  }
  else {
    ts.tv_sec  = 0;
    ts.tv_nsec = 0;
  }
  return ts;
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
        p = x;
      }
    }
  }
  if ( p == NULL )
    p = ::malloc( sizeof( api_Msg ) );
  api_Msg * m   = new ( p ) api_Msg( ev );
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

void
api_Msg::release( void ) noexcept
{
  pthread_mutex_lock( &this->tether.mutex );
  while ( ! this->tether.is_empty() ) {
    api_Msg *m = this->tether.pop_tl();
    m->owner = NULL;
    delete m;
  }
  while ( ! this->refs.is_empty() ) {
    TibrvMsgRef * ref = this->refs.pop_hd();
    delete ref;
  }
  pthread_mutex_unlock( &this->tether.mutex );
}

api_Msg *
api_Msg::make_submsg( void ) noexcept
{
  api_Msg *m = new ( ::malloc( sizeof( api_Msg ) ) ) api_Msg( 0 );
  pthread_mutex_lock( &this->tether.mutex );
  m->owner = &this->tether;
  this->tether.push_tl( m );
  m->serial = this->tether.serial++;
  pthread_mutex_unlock( &this->tether.mutex );
  return m;
}

api_Msg::~api_Msg() noexcept
{
  this->release();
}

bool
api_Transport::on_rv_msg( EvPublish &pub ) noexcept
{
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
  size_t i = pub.subj_hash & this->ht.mask;
  for ( l = this->ht.ht[ i ].hd; l != NULL; l = l->next ) {
    if ( l->hash != pub.subj_hash || l->wild != 0 ||
         l->len != pub.subject_len ||
         ::memcmp( l->subject, pub.subject, l->len ) != 0 )
      continue;
    api_Queue * q = this->api.get<api_Queue>( l->queue, TIBRV_QUEUE );
    if ( q != NULL ) {
      api_QueueGroup * g = NULL;
      pthread_mutex_lock( &q->mutex );
      if ( q->push( l->id, l->cb, l->vcb, l->cl,
                    api_Msg::make( pub, rvmsg, &q->tether, l->id, l->cl ) ) ) {
        if ( (g = q->grp) == NULL )
          pthread_cond_broadcast( &q->cond );
      }
      pthread_mutex_unlock( &q->mutex );
      if ( g != NULL ) {
        pthread_mutex_lock( &g->mutex );
        pthread_cond_broadcast( &g->cond );
        pthread_mutex_unlock( &g->mutex );
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
          api_QueueGroup * g = NULL;
          pthread_mutex_lock( &q->mutex );
          if ( q->push( l->id, l->cb, l->vcb, l->cl,
                   api_Msg::make( pub, rvmsg, &q->tether, l->id, l->cl ) ) ) {
            if ( (g = q->grp) == NULL )
              pthread_cond_broadcast( &q->cond );
          }
          pthread_mutex_unlock( &q->mutex );
          if ( g != NULL ) {
            pthread_mutex_lock( &g->mutex );
            pthread_cond_broadcast( &g->cond );
            pthread_mutex_unlock( &g->mutex );
          }
        }
      }
    }
  }
  pthread_mutex_unlock( &this->mutex );

  return true;
}

bool
api_Timer::timer_cb( uint64_t /*timer_id*/,  uint64_t /*event_id*/ ) noexcept
{
  if ( this->cb != NULL ) {
    if ( this->in_queue )
      return true;
    api_Queue * q = this->api.get<api_Queue>( this->queue, TIBRV_QUEUE );
    if ( q != NULL ) {
      api_QueueGroup * g = NULL;
      pthread_mutex_lock( &q->mutex );
      this->in_queue = true;
      if ( q->push( this->id, this->cb, NULL, this->cl, NULL ) ) {
        if ( ( g = q->grp ) == NULL )
          pthread_cond_broadcast( &q->cond );
      }
      pthread_mutex_unlock( &q->mutex );
      if ( g != NULL ) {
        pthread_mutex_lock( &g->mutex );
        pthread_cond_broadcast( &g->cond );
        pthread_mutex_unlock( &g->mutex );
      }
      return true;
    }
  }
  return false;
}

void *
tibrv_epoll_thread( void *arg ) noexcept
{
  Tibrv_API & api = *(Tibrv_API *) arg;
  EvPoll &poll = api.poll;
  int idle_count = 0;
  for (;;) {
    int idle = poll.dispatch();
    if ( idle == EvPoll::DISPATCH_IDLE )
      idle_count++;
    else
      idle_count = 0; 
    poll.wait( idle_count > 10 ? 100 : 0 );
  }
  return NULL;
}

void *
tibrv_disp_thread( void *arg ) noexcept
{
  api_Dispatcher & disp = *(api_Dispatcher *) arg;
  while ( ! disp.quit ) {
    if ( tibrvQueue_TimedDispatch( disp.queue,
                                   disp.idle_timeout ) == TIBRV_INVALID_QUEUE )
      break;
  }
  pthread_mutex_lock( &disp.mutex );
  disp.done = true;
  pthread_cond_broadcast( &disp.cond );
  pthread_mutex_unlock( &disp.mutex );
  return NULL;
}

void
EvPipe::exec( EvPipeRec &rec ) noexcept
{
  uint8_t * p = (uint8_t *) &rec,
          * e = &p[ sizeof( EvPipeRec ) ];
  bool      complete = false;
  rec.complete = &complete;
  for (;;) {
    int n = ::write( this->write_fd, p, e - p );
    if ( n > 0 ) {
      p += n;
      if ( p == e )
        break;
    }
    struct pollfd fds = { this->write_fd, POLLOUT, POLLOUT };
    ::poll( &fds, 1, 10 );
  }
  while ( ! *rec.complete )
    pthread_cond_wait( rec.cond, rec.mutex );
  rec.complete = NULL;
}

bool
EvPipe::start( int fd,  const char *name ) noexcept
{
  this->PeerData::init_peer( this->poll.get_next_id(), fd, -1, NULL, name );
  return this->poll.add_sock( this ) == 0;
}

void
EvPipe::process( void ) noexcept
{
  for (;;) {
    size_t buflen = this->len - this->off; 
    if ( buflen < sizeof( EvPipeRec ) ) {
      this->pop( EV_PROCESS );
      return;
    }
    EvPipeRec rec;
    ::memcpy( &rec, &this->recv[ this->off ], sizeof( EvPipeRec ) );
    this->off += sizeof( EvPipeRec );
    switch ( rec.op ) {
      case OP_NONE:
        break;
      case OP_SUBSCRIBE: {
        const char * sub = rec.l->subject;
        size_t       len = rec.l->len;
        if ( rec.t->id != TIBRV_PROCESS_TRANSPORT )
          rec.t->client.subscribe( sub, len, NULL, 0 );
        else {
          NotifySub nsub( sub, len, NULL, 0,
                          kv_crc_c( sub, len, 0 ), false, 'A', *rec.t );
          rec.t->client.sub_route.add_sub( nsub );
        }
        break;
      }
      case OP_UNSUBSCRIBE: {
        const char * sub = rec.l->subject;
        size_t       len = rec.l->len;
        if ( rec.t->id != TIBRV_PROCESS_TRANSPORT )
          rec.t->client.unsubscribe( rec.l->subject, rec.l->len );
        else {
          NotifySub nsub( sub, len, NULL, 0,
                          kv_crc_c( sub, len, 0 ), false, 'A', *rec.t );
          rec.t->client.sub_route.del_sub( nsub );
        }
        break;
      }
      case OP_CREATE_TIMER: {
        TimerQueue & timer_q = this->poll.timer;
        api_Timer * t = rec.timer;
        timer_q.add_timer_double( *t, t->ival, t->id, 0 );
        break;
      }
      case OP_DESTROY_TIMER: {
        TimerQueue & timer_q = this->poll.timer;
        api_Timer * t = rec.timer;
        timer_q.remove_timer_cb( *t, t->id, 0 );
        break;
      }
      case OP_RESET_TIMER: {
        TimerQueue & timer_q = this->poll.timer;
        api_Timer * t = rec.timer;
        timer_q.remove_timer_cb( *t, t->id, 0 );
        timer_q.add_timer_double( *t, t->ival, t->id, 0 );
        break;
      }
      case OP_CREATE_TPORT:
        rec.t->client.rv_connect( *rec.parm, rec.t, rec.t );
        break;
      case OP_CLOSE_TPORT:
        if ( rec.t->client.in_list( IN_ACTIVE_LIST ) )
          rec.t->client.idle_push( EV_CLOSE );
        break;
      case OP_TPORT_SEND:
        if ( rec.t->id != TIBRV_PROCESS_TRANSPORT )
          rec.t->client.publish( *rec.pub );
        else {
          rec.pub->subj_hash =
            kv_crc_c( rec.pub->subject, rec.pub->subject_len, 0 );
          rec.t->client.sub_route.forward_msg( *rec.pub );
        }
        break;
      case OP_TPORT_SENDV:
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
        break;
    }
    pthread_mutex_lock( rec.mutex );
    *rec.complete = true;
    pthread_cond_broadcast( rec.cond );
    pthread_mutex_unlock( rec.mutex );
  }
}

void
TibrvQueueEvent::release( api_Msg *m ) noexcept
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
TibrvQueueEvent::release( api_Msg **vec,  tibrv_u32 count ) noexcept
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
TibrvQueueEvent::dispatch( void ) noexcept
{
  if ( this->cb != NULL ) {
    this->cb( this->id, this->msg, (void *) this->cl );
    if ( this->msg != NULL )
      this->release( this->msg );
    else {
      api_Timer *t = this->api.get<api_Timer>( this->id, TIBRV_TIMER );
      if ( t != NULL )
        t->in_queue = false;
    }
  }
  else if ( this->vcb != NULL ) {
    if ( this->cnt == 1 ) {
      this->vcb( (void **) &this->msg, 1 );
      this->release( this->msg );
    }
    else {
      this->vcb( (void **) this->vec, this->cnt );
      this->release( this->vec, this->cnt );
    }
  }
}

struct StrOutput : public MDOutput {
  ArrayOutput out;
  virtual int puts( const char *s ) noexcept;
  virtual int printf( const char *fmt,  ... ) noexcept final __attribute__((format(printf,2,3)));
  StrOutput() {}
};

int StrOutput::puts( const char *s ) noexcept {
  if ( s[ 0 ] == '}' && s[ 1 ] == '\0' ) {
    if ( this->out.ptr[ this->out.count - 1 ] == ' ' ) {
      this->out.ptr[ this->out.count - 1 ] = '}';
      return 0;
    }
  }
  return this->out.puts( s );
}

int
StrOutput::printf( const char *fmt,  ... ) noexcept
{
  va_list args;
  va_start( args, fmt );
  int n = this->out.vprintf( fmt, args );
  va_end( args );
  return n;
}

bool
api_Queue::push( tibrvId id,  tibrvEventCallback cb,
                 tibrvEventVectorCallback vcb,
                 const void *cl,  api_Msg *msg ) noexcept
{
  if ( vcb != NULL && ! this->list.is_empty() && id == this->list.tl->id ) {
    TibrvQueueEvent * e = this->list.tl;
    if ( e->cnt == 1 ) {
      size_t sz = 4 * sizeof( api_Msg * );
      this->mem_x[ this->mptr ].alloc( sz, &e->vec );
      e->vec[ 0 ] = e->msg;
      e->vec[ 1 ] = msg;
      e->cnt = 2;
    }
    else {
      if ( ( e->cnt & 3 ) == 0 ) {
        size_t osz = e->cnt * sizeof( api_Msg * ),
               nsz = ( e->cnt + 4 ) * sizeof( api_Msg * );
        this->mem_x[ this->mptr ].extend( osz, nsz, &e->vec );
      }
      e->vec[ e->cnt++ ] = msg;
    }
  }
  else {
    this->list.push_tl(
      new ( this->mem_x[ this->mptr ].make( sizeof( TibrvQueueEvent ) ) )
        TibrvQueueEvent( this->api, id, cb, vcb, cl, msg ) );
    if ( this->count++ == 0 )
      return true;
  }
  return false;
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
  if ( pipe2( this->pfd, O_CLOEXEC ) != 0 )
    return TIBRV_INIT_FAILURE;
  fcntl( this->pfd[ 0 ], F_SETFL, O_NONBLOCK | 
         fcntl( this->pfd[ 0 ], F_GETFL ) );
  pthread_mutex_init( &this->map_mutex, NULL );
  pthread_cond_init( &this->cond, NULL );
  this->poll.init( 128, false );
  this->ev_read = new ( aligned_malloc( sizeof( EvPipe ) ) )
                 EvPipe( this->poll, this->pfd[ 1 ] );
  this->ev_read->start( this->pfd[ 0 ], "tibrv_api_pipe" );
  this->default_queue =
    this->make<api_Queue>( TIBRV_QUEUE, 0, TIBRV_DEFAULT_QUEUE );
  this->process_tport =
    this->make<api_Transport>( TIBRV_TRANSPORT, 0, TIBRV_PROCESS_TRANSPORT );

  EvRvClientParameters parm( "null", NULL, NULL, 0, 0 );
  this->process_tport->client.rv_connect( parm, this->process_tport,
                                          this->process_tport );
  int fd = this->poll.get_null_fd();
  this->process_tport->sock_opts = OPT_NO_POLL;
  this->process_tport->PeerData::init_peer( this->poll.get_next_id(), fd, -1,
                                           NULL, "tibrv_process_transport" );
  this->process_tport->set_name( "tibrv_process", 13 );
  this->process_tport->poll.add_sock( this->process_tport );
  this->process_tport->me = this->process_tport;

  pthread_t id;
  pthread_attr_t attr;
  pthread_attr_init( &attr );
  pthread_attr_setdetachstate( &attr, 1 );
  pthread_create( &id, &attr, tibrv_epoll_thread, this );
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

tibrv_status
Tibrv_API::CreateTimer( tibrvEvent * event,  tibrvQueue queue,
                       tibrvEventCallback cb,  tibrv_f64 ival,
                       const void * closure ) noexcept
{
  *event = TIBRV_INVALID_ID;
  api_Queue * q = this->get<api_Queue>( queue, TIBRV_QUEUE );
  if ( q == NULL ) return TIBRV_INVALID_QUEUE;
  api_Timer * t = this->make<api_Timer>( TIBRV_TIMER );
  t->queue = queue;
  t->cb    = cb;
  t->cl    = closure;
  t->ival  = ival;

  EvPipeRec rec( OP_CREATE_TIMER, t, &q->mutex, &q->cond );
  pthread_mutex_lock( &q->mutex );
  this->ev_read->exec( rec );
  pthread_mutex_unlock( &q->mutex );

  *event = t->id;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::DestroyEvent( tibrvEvent event,  tibrvEventOnComplete cb ) noexcept
{
  tibrv_u32 type;
  if ( tibrvEvent_GetType( event, &type ) == TIBRV_OK ) {
    bool ok = true;
    switch ( type ) {
      case TIBRV_TIMER: {
        api_Timer * t = this->rem<api_Timer>( event, TIBRV_TIMER );
        if ( t == NULL )
          break;
        api_Queue * q = this->get<api_Queue>( t->queue, TIBRV_QUEUE );
        t->cb = NULL;
        if ( q != NULL ) {
          EvPipeRec rec( OP_DESTROY_TIMER, t, &q->mutex, &q->cond );
          pthread_mutex_lock( &q->mutex );
          this->ev_read->exec( rec );
          pthread_mutex_unlock( &q->mutex );
        }
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
  api_Timer *t = this->get<api_Timer>( event, TIBRV_TIMER );
  if ( t != NULL ) {
    *ival = t->ival;
    return TIBRV_OK;
  }
  return TIBRV_INVALID_EVENT;
}

tibrv_status
Tibrv_API::ResetTimerInterval( tibrvEvent event,  tibrv_f64 ival ) noexcept
{
  api_Timer * t = this->get<api_Timer>( event, TIBRV_TIMER );
  if ( t != NULL ) {
    t->ival = ival;
    api_Queue * q = this->get<api_Queue>( t->queue, TIBRV_QUEUE );
    if ( q == NULL ) return TIBRV_INVALID_QUEUE;
    EvPipeRec rec( OP_RESET_TIMER, t, &q->mutex, &q->cond );
    pthread_mutex_lock( &q->mutex );
    this->ev_read->exec( rec );
    pthread_mutex_unlock( &q->mutex );
    return TIBRV_OK;
  }
  return TIBRV_INVALID_EVENT;
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
  api_Queue * queue = this->get<api_Queue>( q, TIBRV_QUEUE );
  if ( queue == NULL || queue->done )
    return TIBRV_INVALID_QUEUE;
  pthread_mutex_lock( &queue->mutex );
  if ( queue->list.is_empty() ) {
    struct timespec ts = ts_timeout( timeout, 10.0 );
    pthread_cond_timedwait( &queue->cond, &queue->mutex, &ts );
  }
  if ( queue->list.is_empty() ) {
    pthread_mutex_unlock( &queue->mutex );
    if ( timeout > 0.0 )
      return TIBRV_TIMEOUT;
    return TIBRV_OK;
  }
  TibrvQueueEventList list2 = queue->list;
  queue->list.init();
  queue->mptr = ( queue->mptr + 1 ) % 2;
  queue->mem_x[ queue->mptr ].reuse();
  queue->count = 0;
  pthread_mutex_unlock( &queue->mutex );

  do {
    list2.pop_hd()->dispatch();
  } while ( ! list2.is_empty() );

  if ( queue->done && queue->cb != NULL ) {
    pthread_mutex_lock( &queue->mutex );
    if ( queue->cb != NULL ) {
      queue->cb( q, (void *) queue->cl );
      queue->cb = NULL;
    }
    pthread_mutex_unlock( &queue->mutex );
  }
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::TimedDispatchQueueOneEvent( tibrvQueue q,
                                       tibrv_f64 timeout ) noexcept
{
  api_Queue * queue = this->get<api_Queue>( q, TIBRV_QUEUE );
  if ( queue == NULL || queue->done )
    return TIBRV_INVALID_QUEUE;
  pthread_mutex_lock( &queue->mutex );
  if ( queue->list.is_empty() ) {
    struct timespec ts = ts_timeout( timeout, 10.0 );
    pthread_cond_timedwait( &queue->cond, &queue->mutex, &ts );
  }
  if ( queue->list.is_empty() ) {
    pthread_mutex_unlock( &queue->mutex );
    if ( timeout > 0.0 )
      return TIBRV_TIMEOUT;
    return  TIBRV_OK;
  }
  TibrvQueueEvent * ev = queue->list.pop_hd();
  queue->count--;
  if ( queue->list.is_empty() ) {
    queue->mptr = ( queue->mptr + 1 ) % 2;
    queue->mem_x[ queue->mptr ].reuse();
  }
  pthread_mutex_unlock( &queue->mutex );

  ev->dispatch();

  if ( queue->done && queue->cb != NULL ) {
    pthread_mutex_lock( &queue->mutex );
    if ( queue->cb != NULL ) {
      queue->cb( q, (void *) queue->cl );
      queue->cb = NULL;
    }
    pthread_mutex_unlock( &queue->mutex );
  }
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::DestroyQueue( tibrvQueue q, tibrvQueueOnComplete cb,
                        const void * cl ) noexcept
{
  api_Queue * queue = this->get<api_Queue>( q, TIBRV_QUEUE );
  if ( queue == NULL || queue->done )
    return TIBRV_INVALID_QUEUE;
  queue->done = true;
  if ( pthread_mutex_trylock( &queue->mutex ) == 0 ) {
    if ( cb != NULL )
      cb( q, (void *) cl );
    pthread_mutex_unlock( &queue->mutex );
  }
  else {
    queue->cb = cb;
    queue->cl = cl;
  }
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::GetQueueCount( tibrvQueue q, tibrv_u32 * num ) noexcept
{
  api_Queue * queue = this->get<api_Queue>( q, TIBRV_QUEUE );
  *num = 0;
  if ( queue == NULL || queue->done )
    return TIBRV_INVALID_QUEUE;
  pthread_mutex_lock( &queue->mutex );
  *num = queue->count;
  pthread_mutex_unlock( &queue->mutex );
  return TIBRV_OK;
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
  *policy  = queue->policy;
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
  queue->policy  = policy;
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
  queue->name = name;
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

static int
cmp_queue( const api_Queue &x,  const api_Queue &y )
{
  if ( x.priority > y.priority ) return -1;
  if ( x.priority == y.priority ) return 0;
  return 1;
}

tibrv_status
Tibrv_API::TimedDispatchGroup( tibrvQueueGroup grp, tibrv_f64 timeout ) noexcept
{
  api_QueueGroup * g = this->get<api_QueueGroup>( grp, TIBRV_QUEUE_GROUP );
  api_Queue      * queue;
  if ( g == NULL || g->done )
    return TIBRV_INVALID_QUEUE_GROUP;

  pthread_mutex_lock( &g->mutex );
  if ( g->update ) {
    g->list.sort<cmp_queue>();
    g->update = false;
  }
  for ( queue = g->list.hd; queue != NULL; queue = queue->next )
    if ( queue->count > 0 )
      break;
  if ( queue == NULL ) {
    struct timespec ts = ts_timeout( timeout, 10.0 );
    pthread_cond_timedwait( &g->cond, &g->mutex, &ts );
  }
  for ( queue = g->list.hd; queue != NULL; queue = queue->next )
    if ( queue->count > 0 )
      break;
  pthread_mutex_unlock( &g->mutex );
  if ( queue == NULL ) {
    if ( timeout > 0.0 )
      return TIBRV_TIMEOUT;
    return TIBRV_OK;
  }
  pthread_mutex_lock( &queue->mutex );
  TibrvQueueEventList list2;
  if ( queue->grp == g ) {
    list2 = queue->list;
    queue->list.init();
    queue->mptr = ( queue->mptr + 1 ) % 2;
    queue->mem_x[ queue->mptr ].reuse();
    queue->count = 0;
  }
  pthread_mutex_unlock( &queue->mutex );

  while ( ! list2.is_empty() )
    list2.pop_hd()->dispatch();
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::DestroyQueueGroup( tibrvQueueGroup grp ) noexcept
{
  api_QueueGroup * g = this->get<api_QueueGroup>( grp, TIBRV_QUEUE_GROUP );
  if ( g == NULL || g->done )
    return TIBRV_INVALID_QUEUE_GROUP;
  g->done = true;
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::AddQueueGroup( tibrvQueueGroup grp, tibrvQueue q ) noexcept
{
  api_QueueGroup * g     = this->get<api_QueueGroup>( grp, TIBRV_QUEUE_GROUP );
  api_Queue      * queue = this->get<api_Queue>( q, TIBRV_QUEUE );
  if ( queue == NULL || queue->done )
    return TIBRV_INVALID_QUEUE;
  if ( g == NULL || g->done )
    return TIBRV_INVALID_QUEUE_GROUP;
  pthread_mutex_lock( &queue->mutex );
  pthread_mutex_lock( &g->mutex );
  queue->grp = g;
  g->list.push_tl( queue );
  if ( g->count++ > 0 )
    g->list.sort<cmp_queue>();
  g->update = false;
  if ( queue->count > 0 )
    pthread_cond_broadcast( &g->cond );
  pthread_mutex_unlock( &g->mutex );
  pthread_mutex_unlock( &queue->mutex );
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::RemoveQueueGroup( tibrvQueueGroup grp, tibrvQueue q ) noexcept
{
  api_QueueGroup * g     = this->get<api_QueueGroup>( grp, TIBRV_QUEUE_GROUP );
  api_Queue      * queue = this->get<api_Queue>( q, TIBRV_QUEUE );
  if ( queue == NULL || queue->done )
    return TIBRV_INVALID_QUEUE;
  if ( g == NULL || g->done )
    return TIBRV_INVALID_QUEUE_GROUP;
  pthread_mutex_lock( &queue->mutex );
  pthread_mutex_lock( &g->mutex );
  queue->grp = NULL;
  g->list.pop( queue );
  g->count--;
  pthread_mutex_unlock( &g->mutex );
  pthread_mutex_unlock( &queue->mutex );
  return TIBRV_OK;
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
  while ( t->client.rv_state > EvRvClient::ERR_CLOSE &&
          t->client.rv_state < EvRvClient::DATA_RECV ) {
    if ( pthread_cond_timedwait( &t->cond, &t->mutex, &ts ) == ETIMEDOUT ) {
      EvPipeRec rec2( OP_CLOSE_TPORT, t, &parm, &t->mutex, &t->cond );
      this->ev_read->exec( rec2 );
    }
  }
  if ( t->client.rv_state != EvRvClient::DATA_RECV )
    ret = TIBRV_DAEMON_NOT_FOUND;
  ::memcpy( t->x.session, t->client.session, sizeof( t->x.session ) );
  t->x.session_len = t->client.session_len;
  pthread_mutex_unlock( &t->mutex );
  return ret;
}

tibrv_status
Tibrv_API::Send( tibrvTransport tport, tibrvMsg msg ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;
  api_Msg    * m       = (api_Msg *) msg;
  const void * data    = m->msg_data;
  size_t       datalen = m->msg_len;
  if ( datalen == 0 ) {
    data    = m->wr.buf;
    datalen = m->wr.update_hdr();
  }
  EvPublish pub( m->subject, m->subject_len, m->reply, m->reply_len,
                 data, datalen, t->client.sub_route, *t->me, 0, RVMSG_TYPE_ID );
  EvPipeRec rec( OP_TPORT_SEND, t, &pub, 1, &t->mutex, &t->cond );
  pthread_mutex_lock( &t->mutex );
  this->ev_read->exec( rec );
  pthread_mutex_unlock( &t->mutex );
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::Sendv( tibrvTransport tport, tibrvMsg * vec, tibrv_u32 cnt ) noexcept
{
  if ( cnt == 0 )
    return TIBRV_OK;

  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;
  MDMsgMem    tmp;
  void      * pvec = tmp.make( sizeof( EvPublish ) * cnt );
  EvPublish * pub  = (EvPublish *) pvec;

  for ( tibrv_u32 i = 0; i < cnt; i++ ) {
    api_Msg    * m       = (api_Msg *) vec[ i ];
    const void * data    = m->msg_data;
    size_t       datalen = m->msg_len;
    if ( datalen == 0 ) {
      data    = m->wr.buf;
      datalen = m->wr.update_hdr();
    }
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

tibrv_status
Tibrv_API::SendRequest( tibrvTransport tport, tibrvMsg msg, tibrvMsg * reply,
                       tibrv_f64 idle_timeout ) noexcept
{
  api_Transport * t = this->get<api_Transport>( tport, TIBRV_TRANSPORT );
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;
  api_Msg * m = (api_Msg *) msg;
  if ( m->reply_len == 0 ) {
    char inbox[ MAX_RV_INBOX_LEN ];
    t->client.make_inbox( inbox, t->inbox_count++ );
    size_t len = ::strlen( inbox );
    m->reply = m->mem.stralloc( len, inbox );
    m->reply_len = len;
  }
  const void * data    = m->msg_data;
  size_t       datalen = m->msg_len;
  if ( datalen == 0 ) {
    data    = m->wr.buf;
    datalen = m->wr.update_hdr();
  }
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
  api_Msg    * m       = (api_Msg *) msg,
             * r       = (api_Msg *) request_msg;
  const void * data    = m->msg_data;
  size_t       datalen = m->msg_len;
  if ( datalen == 0 ) {
    data    = m->wr.buf;
    datalen = m->wr.update_hdr();
  }
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
  if ( t == NULL )
    return TIBRV_INVALID_TRANSPORT;

  pthread_mutex_lock( &t->mutex );
  uint32_t num = t->inbox_count++;
  pthread_mutex_unlock( &t->mutex );

  char inbox[ MAX_RV_INBOX_LEN ];
  size_t len = 7;
  ::memcpy( inbox, "_INBOX.", len );
  ::memcpy( &inbox[ len ], t->x.session, t->x.session_len );
  len += t->x.session_len;
  inbox[ len++ ] = '.';
  len += uint32_to_string( num, &inbox[ len ] );
  inbox[ len ] = '\0';

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
  t->descr = descr;
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
  api_Dispatcher * d = this->make<api_Dispatcher>( TIBRV_DISPATCHER );
  *disp = d->id;
  d->queue = able;
  d->idle_timeout = idle_timeout;

  pthread_attr_t attr;
  pthread_attr_init( &attr );
  pthread_attr_setdetachstate( &attr, 1 );
  pthread_create( &d->thr_id, &attr, tibrv_disp_thread, d );

  return TIBRV_OK;
}

tibrv_status
Tibrv_API::JoinDispatcher( tibrvDispatcher disp ) noexcept
{
  api_Dispatcher * d = this->get<api_Dispatcher>( disp, TIBRV_DISPATCHER );
  if ( d == NULL )
    return TIBRV_INVALID_DISPATCHER;
  if ( d != NULL ) {
    api_Queue * q = this->get<api_Queue>( d->queue, TIBRV_QUEUE );
    bool q_locked = ( q != NULL && pthread_mutex_trylock( &q->mutex ) == 0 );
    d->quit = true;
    if ( q_locked ) {
      pthread_cond_broadcast( &q->cond );
      pthread_mutex_unlock( &q->mutex );

      if ( pthread_self() != d->thr_id ) {
        pthread_mutex_lock( &d->mutex );
        while ( ! d->done )
          pthread_cond_wait( &d->cond, &d->mutex );
        pthread_mutex_unlock( &d->mutex );
      }
    }
  }
  return TIBRV_OK;
}

tibrv_status
Tibrv_API::SetDispatcherName( tibrvDispatcher disp, const char * name ) noexcept
{
  api_Dispatcher * d = this->get<api_Dispatcher>( disp, TIBRV_DISPATCHER );
  if ( d == NULL )
    return TIBRV_INVALID_DISPATCHABLE;
  d->name = name;
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
  return TIBRV_OK;
}

tibrv_bool
tibrv_IsIPM( void )
{
  return TIBRV_FALSE;
}

tibrv_status
tibrvEvent_CreateListener( tibrvEvent * event,  tibrvQueue queue,
                           tibrvEventCallback cb,  tibrvTransport tport,
                           const char * subj,  const void * closure )
{
  return tibrv_api->CreateListener( event, queue, tport, cb, NULL, subj,
                                    closure );
}

tibrv_status
tibrvEvent_CreateVectorListener( tibrvEvent * event,  tibrvQueue queue,
                                 tibrvEventVectorCallback vcb,
                                 tibrvTransport tport, const char * subj,
                                 const void * closure )
{
  return tibrv_api->CreateListener( event, queue, tport, NULL, vcb, subj,
                                    closure );
}

tibrv_status
tibrvEvent_CreateTimer( tibrvEvent * event,  tibrvQueue queue,
                        tibrvEventCallback cb,  tibrv_f64 ival,
                        const void * closure )
{
  return tibrv_api->CreateTimer( event, queue, cb, ival, closure );
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
tibrvEvent_GetQueue( tibrvEvent event,  tibrvQueue * queue )
{
  return tibrv_api->GetEventQueue( event, queue );
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

tibrv_status
tibrvMsg_Create( tibrvMsg * msg )
{
  *msg = new ( ::malloc( sizeof( api_Msg ) ) ) api_Msg( 0 );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_CreateEx( tibrvMsg * msg,  tibrv_u32 /*initial*/ )
{
  return tibrvMsg_Create( msg );
}

tibrv_status
tibrvMsg_Destroy( tibrvMsg msg )
{
  if ( msg != NULL && ((api_Msg *) msg)->owner == NULL )
    delete (api_Msg *) msg;
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_Detach( tibrvMsg msg )
{
  api_Msg *m = (api_Msg *) msg;
  MsgTether *t;
  if ( msg != NULL && (t = m->owner) != NULL ) {
    pthread_mutex_lock( &t->mutex );
    m->owner->pop( m );
    m->owner = NULL;
    pthread_mutex_unlock( &t->mutex );
  }
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_Reset( tibrvMsg msg )
{
  api_Msg * m = (api_Msg *) msg;
  m->reset();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_Expand( tibrvMsg /*msg*/,  tibrv_i32 /*add*/ )
{
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_SetSendSubject( tibrvMsg msg,  const char * subject )
{
  api_Msg * m = (api_Msg *) msg;
  m->subject_len = ( subject == NULL ? 0: ::strlen( subject ) );
  m->subject = ( subject == NULL ? NULL :
                 m->mem.stralloc( m->subject_len, subject ) );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_GetSendSubject( tibrvMsg msg,  const char ** subject )
{
  api_Msg * m = (api_Msg *) msg;
  *subject = m->subject;
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_SetReplySubject( tibrvMsg msg,  const char * reply )
{
  api_Msg * m = (api_Msg *) msg;
  m->reply_len = ( reply == NULL ? 0 : ::strlen( reply ) );
  m->reply = ( reply == NULL ? NULL : m->mem.stralloc( m->reply_len, reply ) );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_GetReplySubject( tibrvMsg msg,  const char ** reply )
{
  api_Msg * m = (api_Msg *) msg;
  if ( (*reply = m->reply) != NULL )
    return TIBRV_OK;
  return TIBRV_NOT_FOUND;
}

tibrv_status
tibrvMsg_GetEvent( tibrvMsg msg,  tibrvEvent * id )
{
  *id = ((api_Msg *) msg)->event;
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_GetClosure( tibrvMsg msg,  void ** closure )
{
  *closure = (void *) ((api_Msg *) msg)->cl;
  return TIBRV_OK;
}

static inline void *
get_as_bytes( tibrvMsg msg, tibrv_u32 *size )
{
  api_Msg * m = (api_Msg *) msg;
  tibrv_u32 z = m->wr.update_hdr();
  if ( z == 8 ) {
    if ( m->msg_enc == RVMSG_TYPE_ID && m->msg_len > 8 ) {
      if ( size != NULL )
        *size = m->msg_len;
      return (void *) m->msg_data;
    }
  }
  if ( size != NULL )
    *size = z;
  return m->wr.buf;
}

static inline RvMsg *
get_as_rvmsg( tibrvMsg msg )
{
  RvMsg * rvmsg = ((api_Msg *) msg)->rvmsg;
  if ( rvmsg == NULL ) {
    tibrv_u32 sz;
    void    * buf = get_as_bytes( msg, &sz );
    rvmsg = RvMsg::unpack_rv( buf, 0, sz, 0, NULL, ((api_Msg *) msg)->mem );
  }
  return rvmsg;
}

tibrv_status
tibrvMsg_GetNumFields( tibrvMsg msg,  tibrv_u32 * num_flds )
{
  RvMsg * rvmsg = get_as_rvmsg( msg );
  MDFieldIter * iter;
  tibrv_u32     i = 0;
  if ( rvmsg != NULL && rvmsg->get_field_iter( iter ) == 0 ) {
    if ( iter->first() == 0 )
      for ( i = 1; iter->next() == 0; i++ )
        ;
  }
  *num_flds = i;
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_GetByteSize( tibrvMsg msg,  tibrv_u32 * size )
{
  RvMsg * rvmsg = ((api_Msg *) msg)->rvmsg;
  if ( rvmsg != NULL )
    *size = rvmsg->msg_end - rvmsg->msg_off;
  else
    get_as_bytes( msg, size );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_ConvertToString( tibrvMsg msg,  const char ** str )
{
  api_Msg * m = (api_Msg *) msg;
  RvMsg * rvmsg = get_as_rvmsg( msg );
  StrOutput tmp;
  tmp.puts( "{" );
  rvmsg->print( &tmp, 0, "%s=", NULL );
  tmp.puts( "}" );
  *str = m->mem.stralloc( tmp.out.count, tmp.out.ptr );

  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddField( tibrvMsg msg,  tibrvMsgField * field )
{
  switch ( field->type ) {
    case TIBRVMSG_MSG        : return tibrvMsg_AddMsgEx( msg, field->name, field->data.msg, field->id );
    case TIBRVMSG_DATETIME   : return tibrvMsg_AddDateTimeEx( msg, field->name, &field->data.date, field->id );
    case TIBRVMSG_OPAQUE     : return tibrvMsg_AddOpaqueEx( msg, field->name, field->data.buf, field->size, field->id );
    case TIBRVMSG_STRING     : return tibrvMsg_AddStringEx( msg, field->name, field->data.str, field->id );
    case TIBRVMSG_BOOL       : return tibrvMsg_AddBoolEx( msg, field->name, field->data.boolean, field->id );
    case TIBRVMSG_I8         : return tibrvMsg_AddI8Ex( msg, field->name, field->data.i8, field->id );
    case TIBRVMSG_U8         : return tibrvMsg_AddU8Ex( msg, field->name, field->data.u8, field->id );
    case TIBRVMSG_I16        : return tibrvMsg_AddI16Ex( msg, field->name, field->data.i16, field->id );
    case TIBRVMSG_U16        : return tibrvMsg_AddU16Ex( msg, field->name, field->data.u16, field->id );
    case TIBRVMSG_I32        : return tibrvMsg_AddI32Ex( msg, field->name, field->data.i32, field->id );
    case TIBRVMSG_U32        : return tibrvMsg_AddU32Ex( msg, field->name, field->data.u32, field->id );
    case TIBRVMSG_I64        : return tibrvMsg_AddI64Ex( msg, field->name, field->data.i64, field->id );
    case TIBRVMSG_U64        : return tibrvMsg_AddU64Ex( msg, field->name, field->data.u64, field->id );
    case TIBRVMSG_F32        : return tibrvMsg_AddF32Ex( msg, field->name, field->data.f32, field->id );
    case TIBRVMSG_F64        : return tibrvMsg_AddF64Ex( msg, field->name, field->data.f64, field->id );
    case TIBRVMSG_IPPORT16   : return tibrvMsg_AddIPPort16Ex( msg, field->name, field->data.ipport16, field->id );
    case TIBRVMSG_IPADDR32   : return tibrvMsg_AddIPAddr32Ex( msg, field->name, field->data.ipaddr32, field->id );
    case TIBRVMSG_ENCRYPTED  : return TIBRV_NOT_PERMITTED;
    case TIBRVMSG_NONE       : return TIBRV_OK;
    case TIBRVMSG_I8ARRAY    : return tibrvMsg_AddI8ArrayEx( msg, field->name, (tibrv_i8 *) field->data.array, field->count, field->id );
    case TIBRVMSG_U8ARRAY    : return tibrvMsg_AddU8ArrayEx( msg, field->name, (tibrv_u8 *) field->data.array, field->count, field->id );
    case TIBRVMSG_I16ARRAY   : return tibrvMsg_AddI16ArrayEx( msg, field->name, (tibrv_i16 *) field->data.array, field->count, field->id );
    case TIBRVMSG_U16ARRAY   : return tibrvMsg_AddU16ArrayEx( msg, field->name, (tibrv_u16 *) field->data.array, field->count, field->id );
    case TIBRVMSG_I32ARRAY   : return tibrvMsg_AddI32ArrayEx( msg, field->name, (tibrv_i32 *) field->data.array, field->count, field->id );
    case TIBRVMSG_U32ARRAY   : return tibrvMsg_AddU32ArrayEx( msg, field->name, (tibrv_u32 *) field->data.array, field->count, field->id );
    case TIBRVMSG_I64ARRAY   : return tibrvMsg_AddI64ArrayEx( msg, field->name, (tibrv_i64 *) field->data.array, field->count, field->id );
    case TIBRVMSG_U64ARRAY   : return tibrvMsg_AddU64ArrayEx( msg, field->name, (tibrv_u64 *) field->data.array, field->count, field->id );
    case TIBRVMSG_F32ARRAY   : return tibrvMsg_AddF32ArrayEx( msg, field->name, (tibrv_f32 *) field->data.array, field->count, field->id );
    case TIBRVMSG_F64ARRAY   : return tibrvMsg_AddF64ArrayEx( msg, field->name, (tibrv_f64 *) field->data.array, field->count, field->id );
    case TIBRVMSG_XML        : return tibrvMsg_AddXmlEx( msg, field->name, field->data.buf, field->size, field->id );
    case TIBRVMSG_STRINGARRAY: return tibrvMsg_AddStringArrayEx( msg, field->name, (const char **) field->data.array, field->count, field->id );
    case TIBRVMSG_MSGARRAY   : return tibrvMsg_AddMsgArrayEx( msg, field->name, (tibrvMsg *) field->data.array, field->count, field->id );
    default                  : return TIBRV_NOT_PERMITTED;
  }
}

tibrv_status
tibrvMsg_UpdateField( tibrvMsg msg,  tibrvMsgField * field )
{
  switch ( field->type ) {
    case TIBRVMSG_MSG        : return tibrvMsg_UpdateMsgEx( msg, field->name, field->data.msg, field->id );
    case TIBRVMSG_DATETIME   : return tibrvMsg_UpdateDateTimeEx( msg, field->name, &field->data.date, field->id );
    case TIBRVMSG_OPAQUE     : return tibrvMsg_UpdateOpaqueEx( msg, field->name, field->data.buf, field->size, field->id );
    case TIBRVMSG_STRING     : return tibrvMsg_UpdateStringEx( msg, field->name, field->data.str, field->id );
    case TIBRVMSG_BOOL       : return tibrvMsg_UpdateBoolEx( msg, field->name, field->data.boolean, field->id );
    case TIBRVMSG_I8         : return tibrvMsg_UpdateI8Ex( msg, field->name, field->data.i8, field->id );
    case TIBRVMSG_U8         : return tibrvMsg_UpdateU8Ex( msg, field->name, field->data.u8, field->id );
    case TIBRVMSG_I16        : return tibrvMsg_UpdateI16Ex( msg, field->name, field->data.i16, field->id );
    case TIBRVMSG_U16        : return tibrvMsg_UpdateU16Ex( msg, field->name, field->data.u16, field->id );
    case TIBRVMSG_I32        : return tibrvMsg_UpdateI32Ex( msg, field->name, field->data.i32, field->id );
    case TIBRVMSG_U32        : return tibrvMsg_UpdateU32Ex( msg, field->name, field->data.u32, field->id );
    case TIBRVMSG_I64        : return tibrvMsg_UpdateI64Ex( msg, field->name, field->data.i64, field->id );
    case TIBRVMSG_U64        : return tibrvMsg_UpdateU64Ex( msg, field->name, field->data.u64, field->id );
    case TIBRVMSG_F32        : return tibrvMsg_UpdateF32Ex( msg, field->name, field->data.f32, field->id );
    case TIBRVMSG_F64        : return tibrvMsg_UpdateF64Ex( msg, field->name, field->data.f64, field->id );
    case TIBRVMSG_IPPORT16   : return tibrvMsg_UpdateIPPort16Ex( msg, field->name, field->data.ipport16, field->id );
    case TIBRVMSG_IPADDR32   : return tibrvMsg_UpdateIPAddr32Ex( msg, field->name, field->data.ipaddr32, field->id );
    case TIBRVMSG_ENCRYPTED  : return TIBRV_NOT_PERMITTED;
    case TIBRVMSG_NONE       : return TIBRV_OK;
    case TIBRVMSG_I8ARRAY    : return tibrvMsg_UpdateI8ArrayEx( msg, field->name, (tibrv_i8 *) field->data.array, field->count, field->id );
    case TIBRVMSG_U8ARRAY    : return tibrvMsg_UpdateU8ArrayEx( msg, field->name, (tibrv_u8 *) field->data.array, field->count, field->id );
    case TIBRVMSG_I16ARRAY   : return tibrvMsg_UpdateI16ArrayEx( msg, field->name, (tibrv_i16 *) field->data.array, field->count, field->id );
    case TIBRVMSG_U16ARRAY   : return tibrvMsg_UpdateU16ArrayEx( msg, field->name, (tibrv_u16 *) field->data.array, field->count, field->id );
    case TIBRVMSG_I32ARRAY   : return tibrvMsg_UpdateI32ArrayEx( msg, field->name, (tibrv_i32 *) field->data.array, field->count, field->id );
    case TIBRVMSG_U32ARRAY   : return tibrvMsg_UpdateU32ArrayEx( msg, field->name, (tibrv_u32 *) field->data.array, field->count, field->id );
    case TIBRVMSG_I64ARRAY   : return tibrvMsg_UpdateI64ArrayEx( msg, field->name, (tibrv_i64 *) field->data.array, field->count, field->id );
    case TIBRVMSG_U64ARRAY   : return tibrvMsg_UpdateU64ArrayEx( msg, field->name, (tibrv_u64 *) field->data.array, field->count, field->id );
    case TIBRVMSG_F32ARRAY   : return tibrvMsg_UpdateF32ArrayEx( msg, field->name, (tibrv_f32 *) field->data.array, field->count, field->id );
    case TIBRVMSG_F64ARRAY   : return tibrvMsg_UpdateF64ArrayEx( msg, field->name, (tibrv_f64 *) field->data.array, field->count, field->id );
    case TIBRVMSG_XML        : return tibrvMsg_UpdateXmlEx( msg, field->name, field->data.buf, field->size, field->id );
    case TIBRVMSG_STRINGARRAY: return tibrvMsg_UpdateStringArrayEx( msg, field->name, (const char **) field->data.array, field->count, field->id );
    case TIBRVMSG_MSGARRAY   : return tibrvMsg_UpdateMsgArrayEx( msg, field->name, (tibrvMsg *) field->data.array, field->count, field->id );
    default                  : return TIBRV_NOT_PERMITTED;
  }
}

tibrv_status
tibrvMsg_CreateFromBytes( tibrvMsg * msg,  const void * bytes )
{
  MDMsgMem      mem;
  size_t        sz = get_u32<MD_BIG>( &((uint8_t *) bytes)[ 0 ] );
  RvMsg       * m  = RvMsg::unpack_rv( (void *) bytes, 0, sz, 0, NULL, mem );
  if ( m == NULL ) {
    *msg = NULL;
    return TIBRV_CORRUPT_MSG;
  }
  tibrvMsg_Create( msg );
  api_Msg & x = *(api_Msg *) msg;
  x.wr.append_rvmsg( *m );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_GetAsBytes( tibrvMsg msg,   const void ** ptr )
{
  RvMsg * rvmsg = ((api_Msg *) msg)->rvmsg;
  if ( rvmsg != NULL )
    *ptr = &((uint8_t *) rvmsg->msg_buf)[ rvmsg->msg_off ];
  else
    *ptr = get_as_bytes( msg, NULL );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_GetAsBytesCopy( tibrvMsg msg, void * ptr, tibrv_u32 size )
{
  RvMsg * rvmsg = ((api_Msg *) msg)->rvmsg;
  const void * tmp;
  tibrv_u32 tmp_size;
  if ( rvmsg != NULL ) {
    tmp = &((uint8_t *) rvmsg->msg_buf)[ rvmsg->msg_off ];
    tmp_size = rvmsg->msg_end - rvmsg->msg_off;
  }
  else {
    tmp = get_as_bytes( msg, &tmp_size );
  }
  if ( tmp_size > size )
    return TIBRV_INVALID_ARG;
  ::memcpy( ptr, tmp, tmp_size );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_CreateCopy( const tibrvMsg msg,  tibrvMsg * copy )
{
  api_Msg * m  = (api_Msg *) msg,
          * cp = new ( ::malloc( sizeof( api_Msg ) ) ) api_Msg( 0 );
  if ( m->subject_len > 0 ) {
    cp->subject_len = m->subject_len;
    cp->subject     = cp->mem.stralloc( m->subject_len, m->subject );
  }
  if ( m->reply_len > 0 ) {
    cp->reply_len = m->reply_len;
    cp->reply     = cp->mem.stralloc( m->reply_len, m->reply );
  }
  if ( m->msg_enc == RVMSG_TYPE_ID ) {
    cp->msg_enc  = RVMSG_TYPE_ID;
    cp->msg_len  = m->msg_len;
    cp->msg_data = cp->mem.memalloc( m->msg_len, m->msg_data );
  }
  cp->wr.append_writer( m->wr );
  *copy = cp;
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_MarkReferences( tibrvMsg msg )
{
  api_Msg     * m   = (api_Msg *) msg;
  TibrvMsgRef * ref = new ( ::malloc( sizeof( TibrvMsgRef ) ) ) TibrvMsgRef();
  ref->blk_ptr = m->mem.blk_ptr;
  ref->mem_off = m->mem.mem_off;
  ref->serial  = m->tether.serial;
  m->refs.push_hd( ref );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_ClearReferences( tibrvMsg msg )
{
  api_Msg * m = (api_Msg *) msg, * sub, * next;
  pthread_mutex_lock( &m->tether.mutex );
  if ( ! m->refs.is_empty() ) {
    TibrvMsgRef * ref = m->refs.pop_hd();
    for ( sub = m->tether.hd; sub != NULL; sub = next ) {
      next = sub->next;
      if ( sub->serial > ref->serial ) {
        m->tether.pop( sub );
        sub->owner = NULL;
        delete sub;
      }
    }
    m->mem.reset( ref->blk_ptr, ref->mem_off );
    delete ref;
  }
  pthread_mutex_unlock( &m->tether.mutex );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_GetCurrentTime( tibrvMsgDateTime * cur )
{
  struct timespec ts;
  clock_gettime( CLOCK_REALTIME, &ts );
  cur->sec  = ts.tv_sec;
  cur->nsec = ts.tv_nsec - ( ts.tv_nsec % 1000 );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_GetCurrentTimeString( char * local,  char * gmt )
{
  const char *fmt = "%Y-%m-%d %H:%M:%S";
  struct timespec ts;
  struct tm tm;
  clock_gettime( CLOCK_REALTIME, &ts );
  if ( gmt != NULL ) {
    gmtime_r( &ts.tv_sec, &tm );
    strftime( gmt, TIBRVMSG_DATETIME_STRING_SIZE, fmt, &tm );
    /*154,979,000 Z*/
    char * p = &gmt[ ::strlen( gmt ) ],
         * e = &gmt[ TIBRVMSG_DATETIME_STRING_SIZE ];
    snprintf( p, e-p, "%luZ",
              ( ts.tv_nsec - ( ts.tv_nsec % 1000 ) ) + 1000000000 );
    *p = '.';
  }
  if ( local != NULL ) {
    localtime_r( &ts.tv_sec, &tm );
    strftime( local, TIBRVMSG_DATETIME_STRING_SIZE, fmt, &tm );
  }
  return TIBRV_OK;
}
}

namespace {

static inline size_t name_len( const char * name,  size_t add )
  { return ( name == NULL ? 0 : ::strlen( name ) + 1 ) + add; }
const char *fid_name( char *fbuf,  const char *name,  uint16_t id ) {
  size_t len = name_len( name, 0 ); 
  if ( len >= 255 - 2 )
    len = 255 - 2;
  ::memcpy( fbuf, name, len );
  if ( len > 0 )
    fbuf[ len - 1 ] = '\0';
  fbuf[ len ] = (char) ( id >> 8 );
  fbuf[ len + 1 ] = (char) ( id & 0xff );
  return fbuf;
}

#define FNAME_ARG ( id == 0 ? name : fid_name( fbuf, name, id ) ), \
                  name_len( name, id == 0 ? 0 : 2 )

static inline RvMsgWriter &
get_writer( tibrvMsg msg ) {
  return ((api_Msg *) msg)->wr;
}

static inline MDFieldReader &
get_reader( tibrvMsg msg )
{
  api_Msg * m = (api_Msg *) msg;
  if ( m->rd == NULL ) {
    RvMsg * rvmsg = m->rvmsg;
    if ( rvmsg == NULL ) {
      tibrv_u32 sz;
      void    * buf = get_as_bytes( msg, &sz );
      rvmsg = RvMsg::unpack_rv( buf, 0, sz, 0, NULL, m->mem );
    }
    m->rd = new ( m->mem.make( sizeof( MDFieldReader ) ) )
            MDFieldReader( *rvmsg );
  }
  return *m->rd;
}

template<class T>
static inline tibrv_status
get_value( MDFieldReader &rd, T * value, MDType type )
{
  if ( rd.get_value( value, sizeof( *value ), type ) )
    return TIBRV_OK;
  return TIBRV_ARG_CONFLICT;
}

template<class T>
static inline tibrv_status
get_value( tibrvMsg msg, const char *name, T * value, tibrv_u16 id,
           MDType type )
{
  char fbuf[ 256 ];
  MDFieldReader & rd = get_reader( msg );
  if ( rd.find( FNAME_ARG ) )
    return get_value( rd, value, type );
  return TIBRV_NOT_FOUND;
}

static inline tibrv_status
get_string( MDFieldReader &rd, char ** value,  tibrv_u32 *len )
{
  size_t sz;
  if ( rd.get_string( *value, sz ) ) {
    *len = sz;
    return TIBRV_OK;
  }
  return TIBRV_ARG_CONFLICT;
}

static inline tibrv_status
get_string( tibrvMsg msg, const char *name, char ** value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  MDFieldReader & rd = get_reader( msg );
  tibrv_u32 len;
  if ( rd.find( FNAME_ARG ) )
    return get_string( rd, value, &len );
  return TIBRV_NOT_FOUND;
}

static inline tibrv_status
get_opaque( MDFieldReader &rd, void ** value, tibrv_u32 *len )
{
  size_t sz;
  if ( rd.get_opaque( *value, sz ) ) {
    *len = (tibrv_u32) sz;
    return TIBRV_OK;
  }
  return TIBRV_ARG_CONFLICT;
}

static inline tibrv_status
get_opaque( tibrvMsg msg, const char *name, void ** value, tibrv_u32 *len,
            tibrv_u16 id )
{
  char fbuf[ 256 ];
  MDFieldReader & rd = get_reader( msg );
  if ( rd.find( FNAME_ARG ) )
    return get_opaque( rd, value, len );
  return TIBRV_NOT_FOUND;
}

template<class T>
static inline tibrv_status
get_array_value( MDFieldReader &rd, T ** value, MDType type,  tibrv_u32 *count )
{
  size_t cnt;
  *count = 0;
  if ( ! rd.get_array_count( cnt ) )
    return TIBRV_ARG_CONFLICT;
  if ( cnt > 0 ) {
    *value = (T *) rd.iter->iter_msg.mem->make( cnt * sizeof( T ) );
    if ( ! rd.get_array_value( *value, cnt, sizeof( T ), type ) )
      return TIBRV_ARG_CONFLICT;
    *count = (tibrv_u32) cnt;
  }
  else {
    *value = NULL;
  }
  return TIBRV_OK;
}

template<class T>
static inline tibrv_status
get_array_value( tibrvMsg msg, const char *name, T ** value, tibrv_u16 id,
                 MDType type,  tibrv_u32 *count )
{
  char fbuf[ 256 ];
  MDFieldReader & rd = get_reader( msg );
  if ( rd.find( FNAME_ARG ) )
    return get_array_value( rd, value, type, count );
  return TIBRV_NOT_FOUND;
}

static inline tibrv_status
get_msg( tibrvMsg msg,  MDFieldReader &rd,  tibrvMsg *sub )
{
  MDMsg *rvmsg;
  *sub = NULL;
  if ( rd.iter->iter_msg.get_sub_msg( rd.mref, rvmsg, rd.iter ) == 0 ) {
    api_Msg * smsg = ((api_Msg *) msg)->make_submsg();
    char * msg_buf = &((char *) rvmsg->msg_buf)[ rvmsg->msg_off ];
    size_t msg_len = rvmsg->msg_end - rvmsg->msg_off;
    smsg->rvmsg = new ( smsg->mem.make( sizeof( RvMsg ) ) )
      RvMsg( smsg->mem.memalloc( msg_len, msg_buf ), 0, msg_len, rvmsg->dict,
             smsg->mem );
    *sub = smsg;
    return TIBRV_OK;
  }
  return TIBRV_ARG_CONFLICT;
}

static inline tibrv_status
get_datetime( MDFieldReader &rd,  tibrvMsgDateTime *value )
{
  MDStamp stamp;
  if ( get_value( rd, &stamp, MD_DATETIME ) == TIBRV_OK ) {
    uint64_t ns = stamp.nanos();
    value->sec  = ns / 1000000000;
    value->nsec = ns % 1000000000;
    return TIBRV_OK;
  }
  return TIBRV_NOT_FOUND;
}

static inline tibrv_status
get_msg_array( tibrvMsg msg,  MDFieldReader &rd,  tibrvMsg **array,  tibrv_u32 *count )
{
  size_t cnt;
  *count = 0;
  if ( ! rd.get_array_count( cnt ) )
    return TIBRV_ARG_CONFLICT;
  if ( cnt > 0 ) {
    MDMsgMem * mem = rd.iter->iter_msg.mem;
    tibrvMsg * ar  = (tibrvMsg *) mem->make( cnt * sizeof( tibrvMsg * ) );
    for ( size_t i = 0; i < cnt; i++ ) {
      api_Msg * m = ((api_Msg *) msg)->make_submsg();
      MDReference aref;
      if ( rd.iter->iter_msg.get_array_ref( rd.mref, i, aref ) == 0 ) {
        m->msg_enc  = RVMSG_TYPE_ID;
        m->msg_data = m->mem.memalloc( aref.fsize, aref.fptr );
        m->msg_len  = aref.fsize;
        ar[ i ] = m;
      }
      else {
        return TIBRV_ARG_CONFLICT;
      }
    }
    *array = ar;
    *count = (tibrv_u32) cnt;
  }
  return TIBRV_OK;
}

static tibrv_status
get_field( tibrvMsg msg,  MDFieldReader &rd,  tibrvMsgField * field ) noexcept
{
  MDName nm;
  ((RvFieldIter *) rd.iter)->get_name( nm );
  field->name  = nm.fname;
  field->size  = 0;
  field->count = 0;
  field->type  = ((RvFieldIter *) rd.iter)->type;
  field->id    = nm.fid;
  memset( &field->data, 0, sizeof( field->data ) );
  switch ( field->type ) {
    case TIBRVMSG_MSG        : return get_msg( msg, rd, &field->data.msg );
    case TIBRVMSG_DATETIME   : return get_datetime( rd, &field->data.date );
    case TIBRVMSG_OPAQUE     : return get_opaque( rd, (void **) &field->data.buf, &field->size );
    case TIBRVMSG_STRING     : return get_string( rd, (char **) &field->data.str, &field->size );
    case TIBRVMSG_BOOL       : return get_value( rd, &field->data.boolean, MD_BOOLEAN );
    case TIBRVMSG_I8         : return get_value( rd, &field->data.i8, MD_INT );
    case TIBRVMSG_U8         : return get_value( rd, &field->data.u8, MD_UINT );
    case TIBRVMSG_I16        : return get_value( rd, &field->data.i16, MD_INT );
    case TIBRVMSG_U16        : return get_value( rd, &field->data.u16, MD_UINT );
    case TIBRVMSG_I32        : return get_value( rd, &field->data.i32, MD_INT );
    case TIBRVMSG_U32        : return get_value( rd, &field->data.u32, MD_UINT );
    case TIBRVMSG_I64        : return get_value( rd, &field->data.i64, MD_INT );
    case TIBRVMSG_U64        : return get_value( rd, &field->data.u64, MD_UINT );
    case TIBRVMSG_F32        : return get_value( rd, &field->data.f32, MD_REAL );
    case TIBRVMSG_F64        : return get_value( rd, &field->data.f64, MD_REAL );
    case TIBRVMSG_IPPORT16   : return get_value( rd, &field->data.ipport16, MD_IPDATA );
    case TIBRVMSG_IPADDR32   : return get_value( rd, &field->data.ipaddr32, MD_IPDATA );
    case TIBRVMSG_ENCRYPTED  : return TIBRV_NOT_PERMITTED;
    case TIBRVMSG_NONE       : return TIBRV_OK;
    case TIBRVMSG_I8ARRAY    : return get_array_value( rd, (tibrv_i8 **) &field->data.array, MD_INT, &field->count );
    case TIBRVMSG_U8ARRAY    : return get_array_value( rd, (tibrv_u8 **) &field->data.array, MD_UINT, &field->count );
    case TIBRVMSG_I16ARRAY   : return get_array_value( rd, (tibrv_i16 **) &field->data.array, MD_INT, &field->count );
    case TIBRVMSG_U16ARRAY   : return get_array_value( rd, (tibrv_u16 **) &field->data.array, MD_UINT, &field->count );
    case TIBRVMSG_I32ARRAY   : return get_array_value( rd, (tibrv_i32 **) &field->data.array, MD_INT, &field->count );
    case TIBRVMSG_U32ARRAY   : return get_array_value( rd, (tibrv_u32 **) &field->data.array, MD_UINT, &field->count );
    case TIBRVMSG_I64ARRAY   : return get_array_value( rd, (tibrv_i64 **) &field->data.array, MD_INT, &field->count );
    case TIBRVMSG_U64ARRAY   : return get_array_value( rd, (tibrv_u64 **) &field->data.array, MD_UINT, &field->count );
    case TIBRVMSG_F32ARRAY   : return get_array_value( rd, (tibrv_f32 **) &field->data.array, MD_REAL, &field->count );
    case TIBRVMSG_F64ARRAY   : return get_array_value( rd, (tibrv_f64 **) &field->data.array, MD_REAL, &field->count );
    case TIBRVMSG_XML        : return get_string( rd, (char **) &field->data.str, &field->size );
    case TIBRVMSG_STRINGARRAY: return get_array_value( rd, (char ***) &field->data.array, MD_STRING, &field->count );
    case TIBRVMSG_MSGARRAY   : return get_msg_array( msg, rd, (tibrvMsg **) &field->data.array, &field->count );
    default                  : return TIBRV_NOT_PERMITTED;
  }
}

struct UpdGeom {
  MDMsgMem        mem;
  RvMsgWriter   & wr;
  RvMsg         * rvmsg;
  MDFieldReader   rd;
  void          * fld_data;
  size_t          fld_size,
                  fld_end;

  UpdGeom( RvMsgWriter &w )
    : wr( w ),
      rvmsg( RvMsg::unpack_rv( w.buf, 0, w.update_hdr(), 0, NULL, this->mem ) ),
      rd( *this->rvmsg ), fld_data( 0 ), fld_size( 0 ), fld_end( 0 ) {}

  UpdGeom( RvMsgWriter &w, const char *fname,  size_t fnamelen )
    : wr( w ),
      rvmsg( RvMsg::unpack_rv( w.buf, 0, w.update_hdr(), 0, NULL, this->mem ) ),
      rd( *this->rvmsg ), fld_data( 0 ), fld_size( 0 ), fld_end( 0 ) {
    if ( this->rd.find( fname, fnamelen ) )
      this->prefix();
  }

  void prefix( void ) {
    this->fld_end = this->rd.iter->field_end;
    if ( this->wr.off > this->fld_end ) {
      this->fld_size = this->wr.off - this->fld_end;
      this->fld_data = this->mem.make( this->fld_size );
      ::memcpy( this->fld_data, &this->wr.buf[ this->fld_end ], this->fld_size );
    }
    this->wr.off = this->rd.iter->field_start;
  }
  void fin( void ) {
    if ( this->fld_size > 0 ) {
      if ( this->wr.off != this->fld_end )
        this->wr.append_buffer( this->fld_data, this->fld_size );
      else
        this->wr.off += this->fld_size;
    }
  }
};
}
extern "C" {

tibrv_status
tibrvMsg_AddIPAddr32Ex( tibrvMsg msg,  const char * name,  tibrv_ipaddr32 value,  tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_ipdata( FNAME_ARG, value );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddIPPort16Ex( tibrvMsg msg, const char * name, tibrv_ipport16 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_ipdata( FNAME_ARG, value );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddDateTimeEx( tibrvMsg msg, const char * name, const tibrvMsgDateTime * value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  MDStamp stamp( (uint64_t) value->sec * 1000000ULL +
                 (uint64_t) value->nsec / 1000ULL, MD_RES_MICROSECS );
  get_writer( msg ).append_stamp( FNAME_ARG, stamp );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddBoolEx( tibrvMsg msg, const char * name, tibrv_bool value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_BOOLEAN );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddI8Ex( tibrvMsg msg, const char * name, tibrv_i8 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_INT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddI8ArrayEx( tibrvMsg msg, const char * name, const tibrv_i8 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_INT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddU8Ex( tibrvMsg msg, const char * name, tibrv_u8 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_UINT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddU8ArrayEx( tibrvMsg msg, const char * name, const tibrv_u8 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_UINT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddI16Ex( tibrvMsg msg, const char * name, tibrv_i16 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_INT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddI16ArrayEx( tibrvMsg msg, const char * name, const tibrv_i16 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_INT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddU16Ex( tibrvMsg msg, const char * name, tibrv_u16 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_UINT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddU16ArrayEx( tibrvMsg msg, const char * name, const tibrv_u16 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_UINT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddI32Ex( tibrvMsg msg, const char * name, tibrv_i32 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_INT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddI32ArrayEx( tibrvMsg msg, const char * name, const tibrv_i32 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_INT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddU32Ex( tibrvMsg msg, const char * name, tibrv_u32 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_UINT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddU32ArrayEx( tibrvMsg msg, const char * name, const tibrv_u32 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_UINT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddI64Ex( tibrvMsg msg, const char * name, tibrv_i64 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_INT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddI64ArrayEx( tibrvMsg msg, const char * name, const tibrv_i64 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_INT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddU64Ex( tibrvMsg msg, const char * name, tibrv_u64 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_UINT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddU64ArrayEx( tibrvMsg msg, const char * name, const tibrv_u64 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_UINT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddF32Ex( tibrvMsg msg, const char * name, tibrv_f32 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_REAL );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddF32ArrayEx( tibrvMsg msg, const char * name, const tibrv_f32 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_REAL );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddF64Ex( tibrvMsg msg, const char * name, tibrv_f64 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_REAL );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddF64ArrayEx( tibrvMsg msg, const char * name, const tibrv_f64 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_REAL );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddStringArrayEx( tibrvMsg msg, const char * name, const char ** value, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_string_array( FNAME_ARG, (char **) value, num, 0 );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddMsgEx( tibrvMsg msg, const char * name, tibrvMsg value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  RvMsgWriter & w = get_writer( msg );
  RvMsgWriter & v = get_writer( value );
  RvMsgWriter submsg( w.mem, NULL, 0 );
  w.append_msg( FNAME_ARG, submsg );
  submsg.append_writer( v );
  w.update_hdr( submsg );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddMsgArrayEx( tibrvMsg msg, const char * name, const tibrvMsg * value, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  RvMsgWriter & w = get_writer( msg );
  size_t aroff;
  w.append_msg_array( FNAME_ARG, aroff );
  for ( tibrv_u32 i = 0; i < num; i++ ) {
    RvMsgWriter & v = get_writer( value[ i ] );
    RvMsgWriter submsg( w.mem, NULL, 0 );
    w.append_msg_elem( submsg );
    submsg.append_writer( v );
    w.update_hdr( submsg );
  }
  w.update_array_hdr( aroff, num );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddStringEx( tibrvMsg msg, const char * name, const char * value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  size_t len = ( value == NULL ? 0 : ::strlen( value ) + 1 );
  get_writer( msg ).append_string( FNAME_ARG, value, len );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddOpaqueEx( tibrvMsg msg, const char * name, const void * value, tibrv_u32 size, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_opaque( FNAME_ARG, value, size );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddXmlEx( tibrvMsg msg, const char * name, const void * value, tibrv_u32 size, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_xml( FNAME_ARG, (const char *) value, size );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_GetFieldEx( tibrvMsg msg, const char * name, tibrvMsgField * field, tibrv_u16 id )
{
  char fbuf[ 256 ];
  MDFieldReader & rd = get_reader( msg );
  if ( rd.find( FNAME_ARG ) )
    return get_field( msg, rd, field );
  return TIBRV_NOT_FOUND;
}

tibrv_status
tibrvMsg_GetFieldInstance( tibrvMsg msg,  const char * name,
                           tibrvMsgField * field,  tibrv_u32 inst )
{
  MDFieldReader & rd = get_reader( msg );
  if ( inst > 0 ) {
    size_t name_len = ( name == NULL ? 0 : ::strlen( name ) + 1 );
    for ( bool b = rd.first(); b; b = rd.next() ) {
      if ( ((RvFieldIter *) rd.iter)->is_named( name, name_len ) ) {
        if ( --inst == 0 )
          return get_field( msg, rd, field );
      }
    }
  }
  return TIBRV_NOT_FOUND;
}

tibrv_status
tibrvMsg_GetFieldByIndex( tibrvMsg msg,  tibrvMsgField * field,  tibrv_u32 idx )
{
  MDFieldReader & rd = get_reader( msg );
  bool b = true;
  if ( idx == 0 || rd.iter->field_index > idx )
    b = rd.first();
  while ( b && rd.iter->field_index < idx )
    b = rd.next();
  if ( b ) {
    if ( rd.iter->field_index == idx ) {
      rd.iter->get_reference( rd.mref );
      return get_field( msg, rd, field );
    }
  }
  return TIBRV_NOT_FOUND;
}

tibrv_status
tibrvMsg_RemoveFieldInstance( tibrvMsg msg,  const char * name, tibrv_u32 inst )
{
  UpdGeom g( get_writer( msg ) );
  MDFieldReader & rd = g.rd;
  if ( inst > 0 ) {
    size_t name_len = ( name == NULL ? 0 : ::strlen( name ) + 1 );
    for ( bool b = rd.first(); b; b = rd.next() ) {
      if ( ((RvFieldIter *) rd.iter)->is_named( name, name_len ) ) {
        if ( --inst == 0 ) {
          g.prefix();
          g.fin();
          return TIBRV_OK;
        }
      }
    }
  }
  return TIBRV_NOT_FOUND;
}

tibrv_status
tibrvMsg_GetMsgEx( tibrvMsg msg, const char * name, tibrvMsg * sub, tibrv_u16 id )
{
  char fbuf[ 256 ];
  MDFieldReader & rd = get_reader( msg );
  *sub = NULL;
  if ( rd.find( FNAME_ARG ) )
    return get_msg( msg, rd, sub );
  return TIBRV_NOT_FOUND;
}

tibrv_status
tibrvMsg_GetIPAddr32Ex( tibrvMsg msg, const char * name, tibrv_ipaddr32 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_IPDATA );
}

tibrv_status
tibrvMsg_GetIPPort16Ex( tibrvMsg msg, const char * name, tibrv_ipport16 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_IPDATA );
}

tibrv_status
tibrvMsg_GetDateTimeEx( tibrvMsg msg, const char * name, tibrvMsgDateTime * value, tibrv_u16 id )
{
  MDStamp stamp;
  if ( get_value( msg, name, &stamp, id, MD_DATETIME ) == TIBRV_OK ) {
    uint64_t ns = stamp.nanos();
    value->sec  = ns / 1000000000;
    value->nsec = ns % 1000000000;
    return TIBRV_OK;
  }
  return TIBRV_NOT_FOUND;
}

tibrv_status
tibrvMsg_GetBoolEx( tibrvMsg msg, const char * name, tibrv_bool * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_BOOLEAN );
}

tibrv_status
tibrvMsg_GetI8Ex( tibrvMsg msg, const char * name, tibrv_i8 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_INT );
}

tibrv_status
tibrvMsg_GetI8ArrayEx( tibrvMsg msg, const char * name, const tibrv_i8 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_i8 **) array, id, MD_INT, count );
}

tibrv_status
tibrvMsg_GetU8Ex( tibrvMsg msg, const char * name, tibrv_u8 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_UINT );
}

tibrv_status
tibrvMsg_GetU8ArrayEx( tibrvMsg msg, const char * name, const tibrv_u8 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_u8 **) array, id, MD_UINT, count );
}

tibrv_status
tibrvMsg_GetI16Ex( tibrvMsg msg, const char * name, tibrv_i16 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_INT );
}

tibrv_status
tibrvMsg_GetI16ArrayEx( tibrvMsg msg, const char * name, const tibrv_i16 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_i16 **) array, id, MD_INT, count );
}

tibrv_status
tibrvMsg_GetU16Ex( tibrvMsg msg, const char * name, tibrv_u16 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_UINT );
}

tibrv_status
tibrvMsg_GetU16ArrayEx( tibrvMsg msg, const char * name, const tibrv_u16 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_u16 **) array, id, MD_UINT, count );
}

tibrv_status
tibrvMsg_GetI32Ex( tibrvMsg msg, const char * name, tibrv_i32 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_INT );
}

tibrv_status
tibrvMsg_GetI32ArrayEx( tibrvMsg msg, const char * name, const tibrv_i32 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_i32 **) array, id, MD_INT, count );
}

tibrv_status
tibrvMsg_GetU32Ex( tibrvMsg msg, const char * name, tibrv_u32 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_UINT );
}

tibrv_status
tibrvMsg_GetU32ArrayEx( tibrvMsg msg, const char * name, const tibrv_u32 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_u32 **) array, id, MD_UINT, count );
}

tibrv_status
tibrvMsg_GetI64Ex( tibrvMsg msg, const char * name, tibrv_i64 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_INT );
}

tibrv_status
tibrvMsg_GetI64ArrayEx( tibrvMsg msg, const char * name, const tibrv_i64 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_i64 **) array, id, MD_INT, count );
}

tibrv_status
tibrvMsg_GetU64Ex( tibrvMsg msg, const char * name, tibrv_u64 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_UINT );
}

tibrv_status
tibrvMsg_GetU64ArrayEx( tibrvMsg msg, const char * name, const tibrv_u64 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_u64 **) array, id, MD_UINT, count );
}

tibrv_status
tibrvMsg_GetF32Ex( tibrvMsg msg, const char * name, tibrv_f32 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_REAL );
}

tibrv_status
tibrvMsg_GetF32ArrayEx( tibrvMsg msg, const char * name, const tibrv_f32 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_f32 **) array, id, MD_REAL, count );
}

tibrv_status
tibrvMsg_GetF64Ex( tibrvMsg msg, const char * name, tibrv_f64 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_REAL );
}

tibrv_status
tibrvMsg_GetF64ArrayEx( tibrvMsg msg, const char * name, const tibrv_f64 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_f64 **) array, id, MD_REAL, count );
}

tibrv_status
tibrvMsg_GetStringArrayEx( tibrvMsg msg, const char * name, const char *** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (char ***) array, id, MD_STRING, count );
}

tibrv_status
tibrvMsg_GetMsgArrayEx( tibrvMsg msg, const char * name, const tibrvMsg ** array, tibrv_u32 *count, tibrv_u16 id )
{
  char fbuf[ 256 ];
  MDFieldReader & rd = get_reader( msg );
  *array = NULL;
  *count = 0;
  if ( rd.find( FNAME_ARG ) )
    return get_msg_array( msg, rd, (tibrvMsg **) array, count );
  return TIBRV_NOT_FOUND;
}

tibrv_status
tibrvMsg_GetStringEx( tibrvMsg msg, const char * name, const char ** value, tibrv_u16 id )
{
  return get_string( msg, name, (char **) value, id );
}

tibrv_status
tibrvMsg_GetOpaqueEx( tibrvMsg msg, const char * name, const void ** value, tibrv_u32*size, tibrv_u16 id)
{
  return get_opaque( msg, name, (void **) value, size, id );
}

tibrv_status
tibrvMsg_GetXmlEx( tibrvMsg msg, const char * name, const void ** value, tibrv_u32*size, tibrv_u16 id)
{
  char fbuf[ 256 ];
  MDFieldReader & rd = get_reader( msg );
  if ( rd.find( FNAME_ARG ) )
    return get_string( rd, (char **) value, size );
  return TIBRV_NOT_FOUND;
}

tibrv_status
tibrvMsg_RemoveFieldEx( tibrvMsg msg, const char * name, tibrv_u16 id)
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateMsgEx( tibrvMsg msg, const char * name, tibrvMsg value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  RvMsgWriter & v = get_writer( value );
  RvMsgWriter submsg( g.wr.mem, NULL, 0 );
  g.wr.append_msg( FNAME_ARG, submsg );
  submsg.append_writer( v );
  g.wr.update_hdr( submsg );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateIPAddr32Ex( tibrvMsg msg, const char * name, tibrv_ipaddr32 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_ipdata( FNAME_ARG, value );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateIPPort16Ex( tibrvMsg msg, const char * name, tibrv_ipport16 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_ipdata( FNAME_ARG, value );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateDateTimeEx( tibrvMsg msg, const char * name, const tibrvMsgDateTime * value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  MDStamp stamp( (uint64_t) value->sec * 1000000ULL +
                 (uint64_t) value->nsec / 1000ULL, MD_RES_MICROSECS );
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_stamp( FNAME_ARG, stamp );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateBoolEx( tibrvMsg msg, const char * name, tibrv_bool value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_BOOLEAN );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateI8Ex( tibrvMsg msg, const char * name, tibrv_i8 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_INT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateI8ArrayEx( tibrvMsg msg, const char * name, const tibrv_i8 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_INT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateU8Ex( tibrvMsg msg, const char * name, tibrv_u8 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_UINT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateU8ArrayEx( tibrvMsg msg, const char * name, const tibrv_u8 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_UINT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateI16Ex( tibrvMsg msg, const char * name, tibrv_i16 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_INT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateI16ArrayEx( tibrvMsg msg, const char * name, const tibrv_i16 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_INT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateU16Ex( tibrvMsg msg, const char * name, tibrv_u16 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_UINT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateU16ArrayEx( tibrvMsg msg, const char * name, const tibrv_u16 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_UINT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateI32Ex( tibrvMsg msg, const char * name, tibrv_i32 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_INT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateI32ArrayEx( tibrvMsg msg, const char * name, const tibrv_i32 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_INT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateU32Ex( tibrvMsg msg, const char * name, tibrv_u32 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_UINT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateU32ArrayEx( tibrvMsg msg, const char * name, const tibrv_u32 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_UINT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateI64Ex( tibrvMsg msg, const char * name, tibrv_i64 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_INT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateI64ArrayEx( tibrvMsg msg, const char * name, const tibrv_i64 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_INT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateU64Ex( tibrvMsg msg, const char * name, tibrv_u64 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_UINT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateU64ArrayEx( tibrvMsg msg, const char * name, const tibrv_u64 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_UINT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateF32Ex( tibrvMsg msg, const char * name, tibrv_f32 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_REAL );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateF32ArrayEx( tibrvMsg msg, const char * name, const tibrv_f32 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_REAL );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateF64Ex( tibrvMsg msg, const char * name, tibrv_f64 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_REAL );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateF64ArrayEx( tibrvMsg msg, const char * name, const tibrv_f64 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_REAL );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateStringArrayEx( tibrvMsg msg, const char * name, const char ** value, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_string_array( FNAME_ARG, (char **) value, num, 0 );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateMsgArrayEx( tibrvMsg msg, const char * name, const tibrvMsg * value, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  size_t aroff;
  g.wr.append_msg_array( FNAME_ARG, aroff );
  for ( tibrv_u32 i = 0; i < num; i++ ) {
    RvMsgWriter & v = get_writer( value[ i ] );
    RvMsgWriter submsg( g.wr.mem, NULL, 0 );
    g.wr.append_msg_elem( submsg );
    submsg.append_writer( v );
    g.wr.update_hdr( submsg );
  }
  g.wr.update_array_hdr( aroff, num );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateStringEx( tibrvMsg msg, const char * name, const char * value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  size_t len = ( value == NULL ? 0 : ::strlen( value ) + 1 );
  g.wr.append_string( FNAME_ARG, value, len );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateOpaqueEx( tibrvMsg msg, const char * name, const void * value, tibrv_u32 size, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_opaque( FNAME_ARG, value, size );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateXmlEx( tibrvMsg msg, const char * name, const void * value, tibrv_u32 size, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_xml( FNAME_ARG, (const char *) value, size );
  g.fin();
  return TIBRV_OK;
}

}

