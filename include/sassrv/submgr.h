#ifndef __rai_sassrv__submgr_h__
#define __rai_sassrv__submgr_h__

#include <sassrv/rv_host.h>

namespace rai {
namespace sassrv {

extern int subdb_debug;
static const uint32_t HOST_QUERY_INTERVAL      = 30,
                      HOST_TIMEOUT_INTERVAL    = 120;
static const uint32_t SESSION_QUERY_INTERVAL   = 30,
                      SESSION_TIMEOUT_INTERVAL = 60;

enum HostState {
  RV_HOST_UNKNOWN = 0,
  RV_HOST_START   = 1,
  RV_HOST_STATUS  = 2,
  RV_HOST_QUERY   = 3,
  RV_HOST_STOP    = 4,
};
#define MAX_HOST_STATE 5
const char *get_host_state_str( HostState s ) noexcept;

enum SessionState {
  RV_SESSION_UNKNOWN = 0,
  RV_SESSION_RV5     = 1,
  RV_SESSION_QUERY   = 2,
  RV_SESSION_STOP    = 3,
  RV_SESSION_RV7     = 4,
  RV_SESSION_SELF    = 5
};
#define MAX_SESSION_STATE 6
const char *get_session_state_str( SessionState s ) noexcept;

struct Subscription {
  uint32_t subject_id,
           refcnt,
           start_mono,
           ref_mono,
           hash;
  uint16_t len;
  char     value[ 2 ];

  void start( uint32_t sub_id,  uint32_t cur_mono ) {
    this->subject_id = sub_id;
    this->refcnt     = 0;
    this->start_mono = cur_mono;
    this->ref_mono   = cur_mono;
  }
  void ref( uint32_t cur_mono ) {
    this->ref_mono = cur_mono;
  }
};

struct Session {
  kv::UIntHashTab * sub_ht; /* subject_id -> subj_hash */
  uint32_t          host_id,
                    session_id,
                    start_mono,
                    ref_mono,
                    stop_mono,
                    query_mono;
  SessionState      state;
  uint32_t          hash;
  uint16_t          len;
  char              value[ 2 ];

  void start( uint32_t host_id,  uint32_t sess_id,  uint32_t cur_mono,
              bool is_start ) {
    this->sub_ht     = kv::UIntHashTab::resize( NULL );
    this->host_id    = host_id;
    this->session_id = sess_id;
    this->start_mono = is_start ? cur_mono : 0;
    this->ref_mono   = cur_mono;
    this->stop_mono  = 0;
    this->query_mono = 0;
    this->state      = is_start ? RV_SESSION_RV5 :
                       this->has_daemon() ? RV_SESSION_RV7 : RV_SESSION_QUERY;
  }
  void stop( uint32_t cur_mono ) {
    this->state     = RV_SESSION_STOP;
    this->stop_mono = cur_mono;
    if ( this->sub_ht != NULL ) {
      delete this->sub_ht;
      this->sub_ht = NULL;
    }
  }
  bool add_subject( Subscription &script ) {
    size_t pos;
    if ( ! this->sub_ht->find( script.subject_id, pos ) ) {
      this->sub_ht->set_rsz( this->sub_ht, script.subject_id, pos,
                             script.hash );
      script.refcnt++;
      return true;
    }
    return false;
  }
  bool rem_subject( Subscription &script ) {
    if ( script.refcnt > 0 ) {
      size_t pos;
      if ( this->sub_ht->find( script.subject_id, pos ) ) {
        this->sub_ht->remove( pos );
        script.refcnt--;
        return true;
      }
    }
    return false;
  }
  bool has_daemon( void ) const {
    const char *s = this->value,
               *e = &this->value[ len ];
    for ( ; &s[ 8 ] < e; s++ ) {
      if ( *s == '.' && ::memcmp( s+1, "DAEMON.", 7 ) == 0 )
        return true;
    }
    return false;
  }
};

struct Host {
  kv::UIntHashTab * sess_ht; /* session_id -> session_hash */
  uint32_t  host_id,
            start_mono,
            status_mono,
            ref_mono,
            stop_mono,
            query_mono,
            data_loss;
  HostState state;

  void start( uint32_t id,  uint32_t cur_mono,  bool is_start,
              bool is_status ) {
    this->sess_ht     = kv::UIntHashTab::resize( NULL );
    this->host_id     = id;
    this->start_mono  = is_start  ? cur_mono : 0;
    this->status_mono = is_status ? cur_mono : 0;
    this->ref_mono    = cur_mono;
    this->stop_mono   = 0;
    this->query_mono  = 0;
    this->data_loss   = 0;
    this->state       = is_start ? RV_HOST_START : RV_HOST_QUERY;
  }

  void stop( uint32_t cur_mono ) {
    this->stop_mono = cur_mono;
    this->state     = RV_HOST_STOP;
    if ( this->sess_ht != NULL ) {
      delete this->sess_ht;
      this->sess_ht = NULL;
    }
  }

  void status( uint32_t cur_mono ) {
    this->check_query_needed( cur_mono );
    this->status_mono = cur_mono;
    this->ref_mono    = cur_mono;
    if ( this->state != RV_HOST_QUERY )
      this->state = RV_HOST_STATUS;
  }

  void ref( uint32_t cur_mono ) {
    this->ref_mono = cur_mono;
  }

  uint32_t check_query_needed( uint32_t cur_mono ) {
    uint32_t late_secs = 0;
    if ( this->state != RV_HOST_QUERY ) {
      if ( this->state == RV_HOST_STOP )
        return 0;
      if ( cur_mono > this->ref_mono ) {
        if ( (late_secs = cur_mono - this->ref_mono) > 100 )
          this->state = RV_HOST_QUERY;
      }
    }
    if ( late_secs > 100 )
      return late_secs;
    return 0;
  }

  bool add_session( Session &sess ) {
    size_t pos;
    if ( ! this->sess_ht->find( sess.session_id, pos ) ) {
      this->sess_ht->set_rsz( this->sess_ht, sess.session_id, pos, sess.hash );
      return true;
    }
    return false;
  }
  bool rem_session( Session &sess ) {
    size_t pos;
    if ( this->sess_ht->find( sess.session_id, pos ) ) {
      this->sess_ht->remove( pos );
      return true;
    }
    return false;
  }
};

struct StartListener {
  Session      & session;
  Subscription & sub;
  const char   * reply;
  uint16_t       reply_len;
  bool           is_listen_start;

  StartListener( Session &sess,  Subscription &script,  const char *rep,
                 size_t len,  bool is_listen )
    : session( sess ), sub( script ), reply( rep ), reply_len( len ),
      is_listen_start( is_listen ) {}
};

struct StopListener {
  Session      & session;
  Subscription & sub;
  bool           is_orphan,
                 is_listen_stop;

  StopListener( Session &sess,  Subscription &script,  bool is_orph,
                bool is_listen )
    : session( sess ), sub( script ), is_orphan( is_orph ),
      is_listen_stop( is_listen ) {}
};

struct SnapListener {
  Subscription & sub;
  const char   * reply;
  uint16_t       reply_len,
                 flags;

  SnapListener( Subscription &script,  const char *rep,  size_t len,
                uint16_t fl )
    : sub( script ), reply( rep ), reply_len( len ), flags( fl ) {}
};

struct SubscriptionListener {
  virtual void on_listen_start( StartListener &add ) noexcept;
  virtual void on_listen_stop ( StopListener  &rem ) noexcept;
  virtual void on_snapshot    ( SnapListener  &snp ) noexcept;
};

struct GCCounters {
  uint32_t active,
           removed;
  GCCounters() : active( 0 ), removed( 0 ) {}
};

struct Filter {
  char * wild;
  size_t wild_len;
};

struct SubscriptionDB {
  EvRvClient               & client;   /* monitor this network */
  SubscriptionListener     * cb;       /* on_listen_start() */
  kv::UIntHashTab          * host_ht,  /* host_id -> host_tab[ index ] */
                           * sess_ht;  /* session_id -> session_tab[ hash ] */
  kv::ArrayCount<Host, 8>    host_tab;    /* array of Host */
  kv::RouteVec<Session>      session_tab; /* hash, session -> Session */
  kv::RouteVec<Subscription> sub_tab;     /* hash, subject -> Subscription */
  kv::ArrayCount<Filter, 4>  filters;     /* listen wildcards, eg: RSF.> */
  uint32_t                   cur_mono,    /* monotonic time in seconds */
                             next_session_id, /* unique id for sessions */
                             next_subject_id, /* unique id for subjscts */
                             soft_host_query, /* = host_tab.count, refresh all */
                             first_free_host; /* reuse stopped Host[] */
  GCCounters                 subscriptions, /* track how many active/removed */
                             sessions,
                             hosts;
  uint32_t                   next_gc;
  bool                       is_subscribed, /* start_subscriptions() called */
                             is_all_subscribed; /* no filtering */
  md::MDOutput             * mout; /* debug log output */

  SubscriptionDB( EvRvClient &c,  SubscriptionListener *sl ) noexcept;

  void add_wildcard( const char *wildcard ) noexcept;
  bool is_matched( const char *sub,  size_t sub_len ) noexcept;
  void start_subscriptions( bool all ) noexcept;
  void stop_subscriptions( void ) noexcept;
  void do_subscriptions( bool is_subscribe ) noexcept;
  void do_wild_subscription( Filter &f,  bool is_subscribe,
                             int k ) noexcept;

  void process_events( void ) noexcept;
  bool process_pub( kv::EvPublish &pub ) noexcept;
  void gc( void ) noexcept;

  void send_host_query( uint32_t i ) noexcept;
  void send_session_query( Host &host,  Session &session ) noexcept;

  void mark_sessions( Host &host ) noexcept;
  void stop_marked_sessions( Host &host ) noexcept;

  void host_start( uint32_t host_id ) noexcept;
  void host_stop( uint32_t host_id ) noexcept;
  Host & host_ref( uint32_t host_id,  bool is_status ) noexcept;

  Session * first_session( Host &host,  size_t &pos ) noexcept;
  Session * next_session( Host &host,  size_t &pos ) noexcept;
  Session * get_session( uint32_t sess_id,  uint32_t sess_hash ) noexcept;
  Session * get_session( uint32_t sess_id ) noexcept;

  void unsub_host( Host &host ) noexcept;
  void unsub_session( Session &sess ) noexcept;

  Session & session_start( uint32_t host_id,  const char *session_name,
                           size_t session_len ) noexcept;
  void session_stop( uint32_t host_id,  const char *session_name,
                     size_t session_len ) noexcept;
  Session & session_ref( const char *session_name,
                         size_t session_len ) noexcept;

  void add_session( Host &host,  Session &sess ) noexcept;
  void rem_session( Host &host,  Session &sess ) noexcept;

  void mark_subscriptions( Session &session ) noexcept;
  void stop_marked_subscriptions( Session &session ) noexcept;

  Subscription * first_subject( Session &session,  size_t &pos ) noexcept;
  Subscription * next_subject( Session &session,  size_t &pos ) noexcept;
  Subscription * get_subject( uint32_t sub_id,  uint32_t sub_hash ) noexcept;

  Subscription & listen_start( Session &session,  const char *sub,
                               size_t sub_len, bool &is_added ) noexcept;
  Subscription & listen_ref( Session &session,  const char *sub,
                             size_t sub_len,  bool &is_added ) noexcept;
  Subscription & listen_stop( Session &session,  const char *sub,
                              size_t sub_len,  bool &is_orphan ) noexcept;
  Subscription & snapshot( const char *sub,  size_t sub_len ) noexcept;
};

/*
 * host 3F8B37B7
 * _RV.INFO.SYSTEM.HOST.STATUS.3F8B37B7:
 *
 * session 0745DF63
 * _RV.INFO.SYSTEM.SESSION.START.0745DF63:
 *   id : 0745DF63.13A66A64C6D14115812C0
 *
 * listen EQTG.EQBDV.5.N
 * _RV.INFO.SYSTEM.LISTEN.START.EQTG.EQBDV.5.N
 *   id : 0745DF63.13A66A64C6D14115812C0
 *   id : 0745DF63.DAEMON.600E7B2F9AEC7
 */

}
}
#endif
