#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <signal.h>
#include <unistd.h>

#include <sassrv/ev_rv_client.h>
#include <raimd/md_msg.h>
#include <raimd/md_dict.h>
#include <raikv/ev_publish.h>
#include <sassrv/rvapi.h>
#include <sassrv/mc.h>

#pragma GCC diagnostic ignored "-Wunused-parameter"

using namespace rai;
using namespace md;
using namespace kv;
using namespace sassrv;

namespace {

struct rv_Listener_ht;
struct rv_Session_api;

struct rv_Listener_api {
  rv_Listener_api  * next,
                   * back;
  rv_Session_api   & session;
  char             * subject;
  uint16_t           len, wild;
  uint32_t           hash;
  rv_MessageCallback cb;
  void             * cl;
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  rv_Listener_api( rv_Session_api &s )
    : next( 0 ), back( 0 ), session( s ), subject( 0 ), len( 0 ), wild( 0 ),
      hash( 0 ), cb( 0 ), cl( 0 ) {}
};

typedef DLinkList< rv_Listener_api > rv_Listener_list;

struct rv_Listener_ht {
  rv_Listener_list * ht;
  size_t             mask,
                     count;
  rv_Listener_ht( void ) : ht( 0 ), mask( 0 ), count( 0 ) {}
  void init( size_t sz ) {
    this->mask  = sz - 1;
    this->count = 0;
    this->ht    = (rv_Listener_list *) ::malloc( sizeof( this->ht[ 0 ] ) *
                                                 ( this->mask + 1 )  );
    ::memset( (void *) this->ht, 0,
              sizeof( this->ht[ 0 ] ) * ( this->mask + 1 ) );
  }
  void resize( void ) {
    size_t sz  = this->mask + 1,
           nsz = sz * 2;
    rv_Listener_list * oht = this->ht;
    this->init( oht == NULL ? 16 : nsz );
    if ( oht != NULL ) {
      for ( size_t i = 0; i < sz; i++ ) {
        while ( ! oht[ i ].is_empty() ) {
          rv_Listener_api * l = oht[ i ].pop_hd();
          this->push( l );
        }
      }
      ::free( oht );
    }
  }
  void fini( void ) {
    ::free( this->ht );
    this->ht   = 0;
    this->mask = 0;
  }
  void push( rv_Listener_api *l ) {
    if ( this->count >= this->mask )
      this->resize();
    size_t i = l->hash & this->mask;
    this->ht[ i ].push_tl( l );
    this->count++;
  }
  void remove( rv_Listener_api *l ) {
    size_t i = l->hash & this->mask;
    this->ht[ i ].pop( l );
    this->count--;
  }
};

struct rv_Timer_api : public EvTimerCallback { 
  rv_Session_api  & session;
  rv_TimerCallback  cb;
  void            * cl;
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  rv_Timer_api( rv_Session_api &s ) : session( s ), cb( 0 ), cl( 0 ) {}
  virtual ~rv_Timer_api() {}

  virtual bool timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept;
};

struct rv_Signal_api {
  rv_Session_api  & session;
  rv_Signal_api   * next,
                  * back;
  rv_SignalCallback cb;
  void            * cl;
  struct sigaction  old_action;
  int               signo;
  bool              signaled;
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  rv_Signal_api( rv_Session_api &s,  int sig ) : session( s ), next( 0 ),
    back( 0 ), cb( 0 ), cl( 0 ), signo( sig ), signaled( false ) {}
  virtual ~rv_Signal_api() {}
};

typedef DLinkList< rv_Signal_api > rv_Signal_list;

struct rv_Session_api : public EvConnectionNotify, public RvClientCB {
  void * operator new( size_t, void *ptr ) { return ptr; }

  rv_Session_api * next,
                 * back;
  EvPoll           poll;
  EvRvClient       client;
  rv_Listener_ht   ht;
  UIntHashTab    * wild_ht;
  uint32_t         busy, inbox_count;
  rv_Signal_list   signal_list;
  bool             signaled;

  rv_Session_api() : next( 0 ), back( 0 ), client( this->poll ), wild_ht( 0 ),
                     busy( 0 ), inbox_count( 2 ), signaled( false ) {}
  void add_wildcard( uint16_t pref ) {
    uint32_t val;
    size_t   pos;
    if ( this->wild_ht == NULL )
      this->wild_ht = UIntHashTab::resize( NULL );
    if ( this->wild_ht->find( pref, pos, val ) )
      this->wild_ht->set( pref, pos, val + 1 );
    else
      this->wild_ht->upsert_rsz( this->wild_ht, pref, 1 );
  }
  void remove_wildcard( uint16_t pref ) {
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
  virtual void on_connect( EvSocket &conn ) noexcept;
  virtual void on_shutdown( EvSocket &conn,  const char *err,
                            size_t err_len ) noexcept;
  virtual bool on_rv_msg( EvPublish &pub ) noexcept;
};

typedef DLinkList< rv_Session_api > rv_Session_list;

}

void
rv_Session_api::on_connect( EvSocket &conn ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  printf( "Connected: %.*s\n", len, conn.peer_address.buf );
}

void
rv_Session_api::on_shutdown( EvSocket &conn,  const char *err,
                             size_t err_len ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  printf( "Shutdown: %.*s %.*s\n", len, conn.peer_address.buf,
                                   (int) err_len, err );
  if ( this->poll.quit == 0 )
    this->poll.quit = 1;
}

bool
rv_Timer_api::timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept
{
  this->session.busy |= 2;
  if ( this->cb != NULL )
    this->cb( this, this->cl );
  this->session.busy &= ~2;
  if ( this->cb == NULL ) {
    delete this;
    return false;
  }
  return true;
}

bool
rv_Session_api::on_rv_msg( EvPublish &pub ) noexcept
{
  rv_Listener_api * l;
  this->busy |= 1;
  size_t i = pub.subj_hash & this->ht.mask;
  for ( l = this->ht.ht[ i ].hd; l != NULL; l = l->next ) {
    if ( l->hash != pub.subj_hash || l->cb == NULL || l->wild != 0 ||
         l->len != pub.subject_len ||
         ::memcmp( l->subject, pub.subject, l->len ) != 0 )
      continue;
    l->cb( l, pub.subject, (char *) pub.reply, pub.msg_enc, pub.msg_len,
           (void *) pub.msg, l->cl );
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
        if ( l->hash != h || l->cb == NULL || l->wild != pref ||
             ! match_rv_wildcard( l->subject, l->len, pub.subject,
                                  pub.subject_len ) )
          continue;
        l->cb( l, pub.subject, (char *) pub.reply, pub.msg_enc, pub.msg_len,
               (void *) pub.msg, l->cl );
      }
    }
  }
  this->busy &= ~1;

  return true;
}

static rv_Session_list session_list;

extern "C" {

rv_Status
rv_MainLoop( rv_Session session )
{
  rv_Session_api & sess = *(rv_Session_api *) session;
  EvPoll &poll = sess.poll;
  int idle_count = 0;
  for (;;) {
    /* loop 5 times before quiting, time to flush writes */
    if ( sess.signaled ) {
      sess.signaled = false;
      for ( rv_Signal_api * s = sess.signal_list.hd; s != NULL;
            s = s->next ) {
        if ( s->signaled ) {
          s->signaled = false;
          if ( s->cb != NULL )
            s->cb( s, s->cl );
        }
      }
    }
    if ( poll.quit >= 5 && idle_count > 0 )
      break;
    /* dispatch network events */
    int idle = poll.dispatch();
    if ( idle == EvPoll::DISPATCH_IDLE )
      idle_count++;
    else
      idle_count = 0; 
    /* wait for network events */
    poll.wait( idle_count > 255 ? 100 : 0 );
  }   
  return RV_OK;
}

const char *
rv_ErrorText( rv_Session session,  rv_Status status )
{
  if ( status == RV_OK )
    return "OK";
  return "NOT_OK";
}

static void
rv_signal_handler( int signum )
{
  for ( rv_Session_api * sess = session_list.hd; sess != NULL;
        sess = sess->next ) {
    for ( rv_Signal_api * s = sess->signal_list.hd; s != NULL; s = s->next ) {
      if ( s->signo == signum )
        sess->signaled = s->signaled = true;
    }
  }
}

rv_Status
rv_CreateSignal( rv_Session session,  rv_Signal * signal,  int signo,
                 rv_SignalCallback cb,  void * closure )
{
  rv_Session_api & sess = *(rv_Session_api *) session;
  rv_Signal_api  * s = new ( ::malloc( sizeof( rv_Signal_api ) ) )
    rv_Signal_api( sess, signo );
  s->cb = cb;
  s->cl = closure;
  sess.signal_list.push_tl( s );

  if ( signal != NULL )
    *signal = s;
  struct sigaction new_action;

  new_action.sa_handler = rv_signal_handler;
  sigemptyset( &new_action.sa_mask );
  new_action.sa_flags = 0;
  sigaction( signo, &new_action, &s->old_action );

  return RV_OK;
}

rv_Status
rv_Init( rv_Session * session, rv_Name service, rv_Name network,
         rv_Name daemon )
{
  rv_Session_api *sess = new ( aligned_malloc( sizeof( rv_Session_api ) ) )
    rv_Session_api();
  sess->poll.init( 5, false );
  EvRvClientParameters parm( daemon, network, service, 0, 0 );
  if ( ! sess->client.connect( parm, sess, sess ) )
    return RV_NOT_CONNECTED;
  *session = sess;
  session_list.push_tl( sess );
  return RV_OK;
}

rv_Status
rv_Term( rv_Session session )
{
  rv_Session_api *sess = (rv_Session_api *) session;
  if ( sess != NULL )
    sess->client.idle_push( EV_SHUTDOWN );
  return RV_OK;
}

rv_Status
rv_ListenSubject( rv_Session session, rv_Listener * listener,
                  rv_Name subject, rv_MessageCallback callback,
                  rv_Name reply,  void * closure )
{
  if ( subject == NULL || ::strstr( subject, ".." ) != NULL ||
       subject[ 0 ] == '.' || subject[ 0 ] == '\0' )
    return RV_INVALID_SUBJECT;
  rv_Session_api  & sess = *(rv_Session_api *) session;
  size_t            len  = strlen( subject );
  const char      * wild = is_rv_wildcard( subject, len );
  rv_Listener_api * l    = new ( ::malloc( sizeof( rv_Listener_api ) + len + 1 ) )
    rv_Listener_api( sess );
  if ( wild != NULL ) {
    l->wild = &wild[ 1 ] - subject;
    l->hash = kv_crc_c( subject, l->wild - 1, l->wild );
    sess.add_wildcard( l->wild );
  }
  else {
    l->wild = 0;
    l->hash = kv_crc_c( subject, len, 0 );
  }
  l->subject = (char *) &l[ 1 ];
  l->len     = len;
  l->cb      = callback;
  l->cl      = closure;
  ::memcpy( l->subject, subject, len + 1 );
  sess.ht.push( l );
  if ( listener != NULL )
    *listener = l;
  sess.client.subscribe( subject, len, reply,
                         reply == NULL ? 0 : ::strlen( reply ) );
  return RV_OK;
}

rv_Status
rv_ListenInbox( rv_Session session, rv_Listener * listener,
                char *inbox_str,  size_t inbox_len,
                rv_MessageCallback callback, void * closure )
{
  rv_Session_api  & sess = *(rv_Session_api *) session;
  char inbox[ MAX_RV_INBOX_LEN ];
  sess.client.make_inbox( inbox, sess.inbox_count++ );
  size_t len = ::strlen( inbox );
  if ( inbox_len > 0 )
    ::memcpy( inbox_str, inbox, len + 1 <= inbox_len ? len + 1 : inbox_len );
  rv_Listener_api * l    = new ( ::malloc( sizeof( rv_Listener_api ) + len + 1 ) )
    rv_Listener_api( sess );
  l->wild    = 0;
  l->hash    = kv_crc_c( inbox, len, 0 );
  l->subject = (char *) &l[ 1 ];
  l->len     = len;
  l->cb      = callback;
  l->cl      = closure;
  ::memcpy( l->subject, inbox, len + 1 );
  sess.ht.push( l );
  if ( listener != NULL )
    *listener = l;
  return RV_OK;
}

rv_Status
rv_Close( rv_Listener listener )
{
  rv_Listener_api * l = (rv_Listener_api *) listener;
  if ( l->cb != NULL ) {
    rv_Session_api & sess = l->session;
    l->cb = NULL;

    if ( l->len <= sizeof( "_INBOX." ) ||
         ::memcmp( l->subject, "_INBOX.", 7 ) != 0 )
      sess.client.unsubscribe( l->subject, l->len );

    if ( sess.busy == 0 ) {
      if ( l->wild != 0 )
        sess.remove_wildcard( l->wild );
      sess.ht.remove( l );
      delete l;
    }
  }
  return RV_OK;
}

const char *
rv_Subject( rv_Listener listener )
{
  rv_Listener_api * l = (rv_Listener_api *) listener;
  return l->subject;
}

rv_Status
rv_Send( rv_Session session, rv_Name subject,  rvmsg_Type type,
         size_t data_len, void * data )
{
  printf( "rv_Send\n" );
  return RV_OK;
}

rv_Status
rv_SendWithReply( rv_Session session, rv_Name subject, rv_Name reply,
                  rvmsg_Type type, size_t data_len, void * data )
{
  printf( "rv_SendWithReply\n" );
  return RV_OK;
}

rv_Status
rv_CreateTimer( rv_Session session, rv_Timer * timer, float ms,
                rv_TimerCallback cb, void * closure )
{
  rv_Session_api & sess = *(rv_Session_api *) session;
  rv_Timer_api   * t    = new ( ::malloc( sizeof( rv_Timer_api ) ) )
    rv_Timer_api( sess );

  t->cb = cb;
  t->cl = closure;
  TimerQueue & timer_q  = sess.client.poll.timer;
  timer_q.add_timer_millis( *t, (uint64_t) ms, (size_t) (void *) t, 0 );
  if ( timer != NULL )
    *timer = t;
  return RV_OK;
}

rv_Status
rv_DestroyTimer( rv_Timer timer )
{
  rv_Timer_api * t = (rv_Timer_api *) timer;
  if ( t->cb != NULL ) {
    rv_Session_api & sess = t->session;
    sess.client.poll.timer.remove_timer_cb( *t, (size_t) (void *) t, 0 );
    t->cb = NULL;
    if ( sess.busy == 0 )
      delete t;
  }
  return RV_OK;
}

rv_Status
rv_GetStats( rv_Session session, struct rv_Stats * stats )
{
  rv_Session_api & sess = *(rv_Session_api *) session;
  ::memset( stats, 0, sizeof( *stats ) );
  if ( sess.client.svc != NULL ) {
    stats->bytes_recv    = sess.client.svc->db.output_bytes;
    stats->msgs_recv     = sess.client.svc->db.msgs_recv;
    stats->lost_seqno    = sess.client.svc->db.lost_seqno();
    stats->repeat_seqno  = sess.client.svc->db.repeat_seqno();
    stats->reorder_seqno = sess.client.svc->db.reorder_seqno();
    stats->nak_count     = sess.client.svc->db.nak_count();
    if ( stats->lost_seqno != 0 ) {
      stats->lost_src       = sess.client.svc->db.lost_seqno_list.ptr;
      stats->lost_src_count = sess.client.svc->db.lost_seqno_list.count;
    }
    if ( stats->repeat_seqno != 0 ) {
      stats->repeat_src       = sess.client.svc->db.repeat_seqno_list.ptr;
      stats->repeat_src_count = sess.client.svc->db.repeat_seqno_list.count;
    }
    if ( stats->reorder_seqno != 0 ) {
      stats->reorder_src       = sess.client.svc->db.reorder_seqno_list.ptr;
      stats->reorder_src_count = sess.client.svc->db.reorder_seqno_list.count;
    }
    if ( stats->nak_count != 0 ) {
      stats->nak_src       = sess.client.svc->db.nak_count_list.ptr;
      stats->nak_src_count = sess.client.svc->db.nak_count_list.count;
    }

    struct rv_Stats tmp = *stats;
    stats->bytes_recv    -= sess.client.svc->db.last_output_bytes;
    stats->msgs_recv     -= sess.client.svc->db.last_msgs_recv;
    stats->lost_seqno    -= sess.client.svc->db.last_lost;
    stats->repeat_seqno  -= sess.client.svc->db.last_repeat;
    stats->reorder_seqno -= sess.client.svc->db.last_reorder;
    stats->nak_count     -= sess.client.svc->db.last_nak;

    sess.client.svc->db.last_output_bytes = tmp.bytes_recv;
    sess.client.svc->db.last_msgs_recv    = tmp.msgs_recv;
    sess.client.svc->db.last_lost         = tmp.lost_seqno;
    sess.client.svc->db.last_repeat       = tmp.repeat_seqno;
    sess.client.svc->db.last_reorder      = tmp.reorder_seqno;
    sess.client.svc->db.last_nak          = tmp.nak_count;
  }
  return RV_OK;
}

rv_Status
rv_Print( void *data,  size_t data_len,  int type )
{
  MDOutput mout;
  MDMsgMem mem;
  MDMsg * m = MDMsg::unpack( data, 0, data_len, type, NULL, mem );
  if ( m != NULL ) {
    mout.puts( "{ " );
    m->print( &mout, 0, NULL, NULL );
    mout.puts( "}" );
  }
  else {
    mout.puts( "\n" );
    mout.print_hex( data, data_len );
  }
  return RV_OK;
}

}
