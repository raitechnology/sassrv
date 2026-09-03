#ifndef __rai_sassrv__submgr_h__
#define __rai_sassrv__submgr_h__

#include <sassrv/rv_host.h>
#include <raimd/md_hash_tab.h>

namespace rai {
namespace sassrv {

struct RvSubscription {
  uint32_t subject_id,
           refcnt : 31,
           ref7cnt : 1,
           start_mono,
           ref_mono,
           hash;
  uint16_t len;
  char     value[ 2 ];

  void start( uint32_t sub_id,  uint32_t cur_mono ) {
    this->subject_id = sub_id;
    this->refcnt     = 0;
    this->ref7cnt    = 0;
    this->start_mono = cur_mono;
    this->ref_mono   = cur_mono;
  }
  void ref( uint32_t cur_mono ) {
    this->ref_mono = cur_mono;
  }
};

struct RvSessionEntry {
  enum SessionState {
    RV_SESSION_UNKNOWN = 0,
    RV_SESSION_CID     = 1,
    RV_SESSION_RV5     = 2,
    RV_SESSION_QUERY   = 3,
    RV_SESSION_STOP    = 4,
    RV_SESSION_RV7     = 5,
    RV_SESSION_SELF    = 6
  };
  #define MAX_SESSION_STATE 7
  static const char *get_session_state_str( SessionState s ) noexcept;

  kv::UIntHashTab * sub_ht; /* subject_id -> subj_hash */
  char            * user_id;
  uint32_t          host_id,
                    session_id,
                    start_mono,
                    ref_mono,
                    stop_mono,
                    query_mono;
  SessionState      state;
  uint16_t          cid;
  bool              has_daemon;
  uint32_t          hash;
  uint16_t          len;
  char              value[ 2 ];

  void start( uint32_t host_id,  uint16_t cid,  uint32_t sess_id,
              uint32_t cur_mono,  bool is_start,  const char *user,
              size_t user_len ) {
    const char *s = this->value,
               *e = &this->value[ this->len ];
    this->has_daemon = false;
    for ( ; &s[ 8 ] < e; s++ ) {
      if ( *s == '.' && ::memcmp( s+1, "DAEMON.", 7 ) == 0 ) {
        this->has_daemon = true;
        break;
      }
    }
    this->sub_ht     = kv::UIntHashTab::resize( NULL );
    this->user_id    = NULL;
    this->host_id    = host_id;
    this->cid        = cid;
    this->session_id = sess_id;
    this->start_mono = is_start ? cur_mono : 0;
    this->ref_mono   = cur_mono;
    this->stop_mono  = 0;
    this->query_mono = 0;
    this->state      = is_start ? RV_SESSION_RV5 :
                       this->has_daemon ? RV_SESSION_RV7 : RV_SESSION_QUERY;
    if ( cid != 0 )
      this->state = RV_SESSION_CID;
    this->add_user( user, user_len );
  }
  void add_user( const char *user,  size_t user_len ) {
    if ( user != NULL ) {
      this->user_id = (char *) ::malloc( user_len + 1 );
      ::memcpy( this->user_id, user, user_len );
      this->user_id[ user_len ] = '\0';
    }
    else if ( this->user_id != NULL ) {
      ::free( this->user_id );
      this->user_id = NULL;
    }
  }
  void stop( uint32_t cur_mono ) {
    this->state     = RV_SESSION_STOP;
    this->stop_mono = cur_mono;
    if ( this->sub_ht != NULL ) {
      delete this->sub_ht;
      this->sub_ht = NULL;
    }
    if ( this->user_id != NULL ) {
      ::free( this->user_id );
      this->user_id = NULL;
    }
  }
  bool add_subject( RvSubscription &script ) {
    size_t pos;
    if ( ! this->sub_ht->find( script.subject_id, pos ) ) {
      this->sub_ht->set_rsz( this->sub_ht, script.subject_id, pos,
                             script.hash );
      script.refcnt++;
      if ( this->has_daemon )
        script.ref7cnt = 1;
      return true;
    }
    return false;
  }
  bool rem_subject( RvSubscription &script ) {
    if ( script.refcnt > 0 ) {
      size_t pos;
      if ( this->sub_ht->find( script.subject_id, pos ) ) {
        this->sub_ht->remove( pos );
        script.refcnt--;
        if ( this->has_daemon )
          script.ref7cnt = 0;
        return true;
      }
    }
    return false;
  }
};

struct RvHostEntry {
  enum HostState {
    RV_HOST_UNKNOWN = 0,
    RV_HOST_CID     = 1,
    RV_HOST_START   = 2,
    RV_HOST_STATUS  = 3,
    RV_HOST_QUERY   = 4,
    RV_HOST_STOP    = 5,
  };
  #define MAX_HOST_STATE 6
  static const char *get_host_state_str( HostState s ) noexcept;

  kv::UIntHashTab * sess_ht; /* session_id -> session_hash */
  uint32_t  host_id,
            start_mono,
            status_mono,
            ref_mono,
            stop_mono,
            query_mono,
            data_loss,
            rv7_sess_id;
  HostState state;
  uint16_t  cid;

  void start( uint32_t host_id,  uint16_t cid,  uint32_t cur_mono,
              bool is_start,  bool is_status ) {
    this->sess_ht     = kv::UIntHashTab::resize( NULL );
    this->host_id     = host_id;
    this->cid         = cid;
    this->start_mono  = is_start  ? cur_mono : 0;
    this->status_mono = is_status ? cur_mono : 0;
    this->ref_mono    = cur_mono;
    this->stop_mono   = 0;
    this->query_mono  = 0;
    this->data_loss   = 0;
    this->rv7_sess_id = 0;
    this->state       = is_start ? RV_HOST_START : RV_HOST_QUERY;
    if ( cid != 0 )
      this->state = RV_HOST_CID;
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
    if ( this->state != RV_HOST_QUERY && this->state != RV_HOST_CID )
      this->state = RV_HOST_STATUS;
  }

  void ref( uint32_t cur_mono ) {
    this->ref_mono = cur_mono;
  }

  uint32_t check_query_needed( uint32_t cur_mono ) {
    uint32_t late_secs = 0;
    if ( this->state != RV_HOST_QUERY && this->state != RV_HOST_CID ) {
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

  bool add_session( RvSessionEntry &sess ) {
    size_t pos;
    if ( ! this->sess_ht->find( sess.session_id, pos ) ) {
      this->sess_ht->set_rsz( this->sess_ht, sess.session_id, pos, sess.hash );
      if ( sess.has_daemon )
        this->rv7_sess_id = sess.session_id;
      return true;
    }
    return false;
  }
  bool rem_session( RvSessionEntry &sess ) {
    size_t pos;
    if ( this->sess_ht->find( sess.session_id, pos ) ) {
      this->sess_ht->remove( pos );
      if ( sess.has_daemon )
        this->rv7_sess_id = 0;
      return true;
    }
    return false;
  }
};

struct RvSass3Key {
  uint32_t     subj_hash,
               subject_id,
               pid;
  const char * user_id,
             * host,
             * app;

  RvSass3Key() : subj_hash( 0 ), subject_id( 0 ), pid( 0 ),
                 user_id( 0 ), host( 0 ), app( 0 ) {}
  size_t hash( void ) const {
    return this->subj_hash ^
           kv_hash_uint( this->subject_id ) ^
           kv_hash_uint( this->pid ) ^
           kv_crc_c( this->user_id, ::strlen( this->user_id ), 0 ) ^
           kv_crc_c( this->host, ::strlen( this->host ), 0 ) ^
           kv_crc_c( this->app, ::strlen( this->app ), 0 );
  }
  bool equals( const RvSass3Key &k ) const {
    return k.subj_hash == this->subj_hash &&
           k.subject_id == this->subject_id &&
           k.pid == this->pid &&
           ::strcmp( k.user_id, this->user_id ) == 0 &&
           ::strcmp( k.host, this->host ) == 0 &&
           ::strcmp( k.app, this->app ) == 0;
  }
};

struct RvSass3Entry : public RvSass3Key {
  RvSass3Entry * next,
               * back;
  uint32_t       start_mono,
                 ref_mono;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  RvSass3Entry() : next( 0 ), back( 0 ), start_mono( 0 ),
                   ref_mono( 0 ) {}
  bool expired( uint32_t cur_mono ) {
    return this->ref_mono + 480 < cur_mono;
  }
  void ref( uint32_t cur_mono ) {
    this->ref_mono = cur_mono;
  }
  static RvSass3Entry *create( RvSass3Key &k,  uint32_t cur_mono ) {
    size_t sz = sizeof( RvSass3Entry ) +
                ::strlen( k.user_id ) +
                ::strlen( k.host ) +
                ::strlen( k.app ) + 3;

    kv::CatPtr e( ::malloc( sz ) );
    RvSass3Entry * entry = new ( e.start ) RvSass3Entry();
    e.ptr = (char *) &entry[ 1 ];

    entry->subj_hash  = k.subj_hash;
    entry->subject_id = k.subject_id;
    entry->pid        = k.pid;
    entry->user_id    = e.cstr( k.user_id );
    entry->host       = e.cstr( k.host );
    entry->app        = e.cstr( k.app );
    entry->next       = NULL;
    entry->back       = NULL;
    entry->start_mono = cur_mono;
    entry->ref_mono   = cur_mono;
    return entry;
  }
};

struct RvSubscriptionListener {
  struct Start {
    RvSessionEntry & session;
    RvSubscription & sub;
    const char     * reply;
    uint16_t         reply_len;
    bool             is_listen_start,
                     coll;

    Start( RvSessionEntry &sess,  RvSubscription &script,  const char *rep,
           size_t len,  bool is_listen,  bool col )
      : session( sess ), sub( script ), reply( rep ), reply_len( len ),
        is_listen_start( is_listen ), coll( col ) {}
  };

  struct Stop {
    RvSessionEntry & session;
    RvSubscription & sub;
    bool             is_orphan,
                     is_listen_stop,
                     coll;

    Stop( RvSessionEntry &sess,  RvSubscription &script,  bool is_orph,
          bool is_listen,  bool col )
      : session( sess ), sub( script ), is_orphan( is_orph ),
        is_listen_stop( is_listen ), coll( col ) {}
  };

  struct Snap {
    RvSubscription & sub;
    RvSessionEntry * session;
    const char     * reply;
    uint16_t         reply_len,
                     flags;

    Snap( RvSubscription &script,  RvSessionEntry *sess,
          const char *rep,  size_t len,  uint16_t fl )
      : sub( script ), session( sess ), reply( rep ), reply_len( len ),
        flags( fl ) {}
  };

  struct Sass3 {
    RvSass3Entry   & sass3;
    RvSubscription & sub;
    const char     * reply;
    uint16_t         reply_len,
                     flags;
    bool             is_orphan,
                     is_asserted;

    Sass3( RvSass3Entry &sa3, RvSubscription &script,
           const char *rep,  size_t len,  uint16_t fl,  bool orph,
           bool assert )
      : sass3( sa3 ), sub( script ), reply( rep ),
        reply_len( len ), flags( fl ), is_orphan( orph ),
        is_asserted( assert ) {}
  };

  struct Tic {
    RvSubscription & sub;
    RvSessionEntry * session;
    const char     * reply;
    uint16_t         reply_len;

    Tic( RvSubscription &script,  RvSessionEntry *sess,
         const char *rep,  size_t len )
      : sub( script ), session( sess ), reply( rep ), reply_len( len ) {}
  };

  virtual void on_listen_start( Start &add ) noexcept;
  virtual void on_listen_stop ( Stop  &rem ) noexcept;
  virtual void on_snapshot    ( Snap  &snp ) noexcept;
  virtual void on_sass3       ( Sass3 &sa3 ) noexcept;
  virtual void on_tic_reply   ( Tic   &tic ) noexcept;
};

struct EvRvClient;
struct RvSubscriptionDB {
  static const uint32_t HOST_QUERY_INTERVAL      = 30,
                        HOST_TIMEOUT_INTERVAL    = 120;
  static const uint32_t SESSION_QUERY_INTERVAL   = 30,
                        SESSION_TIMEOUT_INTERVAL = 60;
  struct GCCounters {
    uint32_t active,
             removed;
    GCCounters() : active( 0 ), removed( 0 ) {}
    void reset( void ) {
      this->active  = 0;
      this->removed = 0;
    }
  };

  struct Filter {
    char * wild;     /* listen for _RV.INFO.LISTEN.START.wild */
    size_t wild_len;
  };

  typedef kv::ArrayCount< RvHostEntry, 8 >           HostTab;
  typedef kv::RouteVec< RvSessionEntry >             SessionTab;
  typedef kv::RouteVec< RvSubscription >             SubscriptionTab;
  typedef kv::ArrayCount< Filter, 4 >                ListenFilterTab;
  typedef kv::DLinkList< RvSass3Entry >              Sass3List;
  typedef md::MDHashTabT< RvSass3Key, RvSass3Entry > Sass3Tab;

  EvRvClient               & client;   /* monitor this network */
  RvSubscriptionListener   * cb;       /* on_listen_start() */
  kv::UIntHashTab          * host_ht,  /* host_id -> host_tab[ index ] */
                           * sess_ht;  /* session_id -> session_tab[ hash ] */
  HostTab                    host_tab;    /* array of discovered hosts */
  SessionTab                 session_tab; /* hash, session -> RvSessionEntry */
  SubscriptionTab            sub_tab;     /* hash, subject -> RvSubscription */
  ListenFilterTab            filters;     /* listen wildcards, eg: RSF.> */
  Sass3List                  sass3_list;
  Sass3Tab                   sass3_tab;
  uint32_t                   cur_mono,    /* monotonic time in seconds */
                             next_session_ctr, /* unique counter for sessions */
                             next_subject_ctr, /* unique counter for subjscts */
                             soft_host_query, /* = host_tab.count, refresh all */
                             first_free_host; /* reuse stopped Host_tab[] */
  GCCounters                 subscriptions, /* track how many active/removed */
                             sessions,
                             hosts;
  uint32_t                   next_gc,
                             host_inbox_base,
                             session_inbox_base;
  bool                       is_subscribed, /* start_subscriptions() called */
                             is_all_subscribed, /* no filtering */
                             is_sass2,
                             is_sass3,
                             is_tic;
  md::MDOutput             * mout; /* debug log output */

  RvSubscriptionDB( EvRvClient &c,  RvSubscriptionListener *sl ) noexcept;

  void release( void ) noexcept;
  void add_wildcard( const char *wildcard ) noexcept;
  bool is_matched( const char *sub,  size_t sub_len ) noexcept;
  void start_subscriptions( bool all,  bool s2,  bool s3,  bool tic ) noexcept;
  void stop_subscriptions( void ) noexcept;
  void do_subscriptions( bool is_subscribe ) noexcept;
  void do_wild_subscription( Filter &f,  bool is_subscribe,  int k ) noexcept;
  uint32_t next_session_id( void ) noexcept;
  uint32_t next_subject_id( void ) noexcept;

  void process_events( void ) noexcept;
  bool process_pub( kv::EvPublish &pub ) noexcept;
  bool process_pub2( kv::EvPublish &pub,  const char *subject,
                     size_t subject_len,  const char *reply,
                     size_t reply_len ) noexcept;
  void gc( void ) noexcept;
  void make_sync( md::RvMsgWriter &w ) noexcept;
  bool make_host_sync( md::RvMsgWriter &w,  uint32_t i ) noexcept;
  void update_sync( md::RvMsg &msg ) noexcept;

  void send_host_query( uint32_t i ) noexcept;
  void send_session_query( RvHostEntry &host,
                           RvSessionEntry &session ) noexcept;
  void mark_sessions( RvHostEntry &host ) noexcept;
  void stop_marked_sessions( RvHostEntry &host ) noexcept;

  void host_start( uint32_t host_id,  uint16_t cid ) noexcept;
  void host_stop( uint32_t host_id,  uint16_t cid ) noexcept;
  RvHostEntry & host_ref( uint32_t host_id,  uint16_t cid,
                          bool is_status ) noexcept;
  void query_rv7_session( uint32_t host_id ) noexcept;
  RvSessionEntry * get_session( const char *sess,  size_t sess_len ) noexcept;
  RvSessionEntry * get_rv7_session( uint32_t host_id ) noexcept;
  RvSessionEntry * first_session( RvHostEntry &host,  size_t &pos ) noexcept;
  RvSessionEntry * next_session( RvHostEntry &host,  size_t &pos ) noexcept;
  RvSessionEntry * get_session( uint32_t sess_id, uint32_t sess_hash ) noexcept;
  RvSessionEntry * get_session( uint32_t sess_id ) noexcept;

  void unsub_host( RvHostEntry &host ) noexcept;
  void unsub_session( RvSessionEntry &sess ) noexcept;
  void unsub_all( void ) noexcept;

  RvHostEntry *host_exists( uint32_t host_id,  uint16_t cid ) noexcept;
  void session_start( uint32_t host_id,  uint16_t cid,
                      const char *session_name,
                      size_t session_len,  bool is_self,
                      const char *u,  size_t ulen ) noexcept;
  void session_stop( uint32_t host_id,  uint16_t cid,
                     const char *session_name,  size_t session_len ) noexcept;
  RvSessionEntry & session_ref( uint16_t cid,  const char *session_name,
                                size_t session_len ) noexcept;

  void add_session( RvHostEntry &host,  RvSessionEntry &sess ) noexcept;
  void rem_session( RvHostEntry &host,  RvSessionEntry &sess ) noexcept;

  void mark_subscriptions( RvSessionEntry &session ) noexcept;
  void stop_marked_subscriptions( RvSessionEntry &session ) noexcept;

  RvSubscription & get_subscription( const char *sub,
                                     size_t sub_len ) noexcept;
  RvSubscription * first_subject( RvSessionEntry &session,
                                  size_t &pos ) noexcept;
  RvSubscription * next_subject( RvSessionEntry &session,
                                 size_t &pos ) noexcept;
  RvSubscription * get_subject( uint32_t sub_id,  uint32_t sub_hash ) noexcept;

  RvSubscription & listen_start( RvSessionEntry &session,  const char *sub,
                                 size_t sub_len, bool &is_added,
                                 bool &coll ) noexcept;
  RvSubscription & listen_ref( RvSessionEntry &session,  const char *sub,
                               size_t sub_len,  bool &is_added,
                               bool &coll ) noexcept;
  bool deref_subscription( RvSessionEntry &session,  RvSubscription &script,
                           bool &coll ) noexcept;
  RvSubscription & listen_stop( RvSessionEntry &session,  const char *sub,
                                size_t sub_len,  bool &is_orphan,
                                bool &coll ) noexcept;
  RvSubscription & snapshot( const char *sub,  size_t sub_len,
                             const char *sess,  size_t sess_len,
                             RvSessionEntry *&session ) noexcept;
  RvSubscription & tic( const char *sub,  size_t sub_len,
                        const char *sess,  size_t sess_len,
                        RvSessionEntry *&session ) noexcept;
  RvSass3Entry & sass3( RvSubscription &script,
                        const char *u, size_t ulen,
                        const char *h, size_t hlen,
                        const char *a, size_t alen,
                        uint32_t pid,  uint32_t flags ) noexcept;
  size_t sub_hash_count( const char *sub,  size_t sub_len,
                         uint32_t sub_hash ) noexcept;
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
