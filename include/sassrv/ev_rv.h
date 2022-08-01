#ifndef __rai_sassrv__ev_rv_h__
#define __rai_sassrv__ev_rv_h__

extern "C" {
  struct pcre2_real_code_8;
  struct pcre2_real_match_data_8;
  struct sockaddr_in;
  const char *sassrv_get_version( void );
}

#include <raikv/ev_tcp.h>
#include <raikv/route_ht.h>
#include <raikv/dlinklist.h>
#include <raikv/array_space.h>
#include <sassrv/rv_host.h>

namespace rai {
namespace sassrv {

extern uint32_t rv_debug;

typedef kv::ArrayCount<RvHost *, 4> RvHostTab;
typedef kv::ArrayCount<RvDaemonRpc *, 4> RvDaemonTab;

/* tcp listener for accepting EvRvService connections */
struct EvRvListen : public kv::EvTcpListen/*, public RvHost*/ {
  void * operator new( size_t, void *ptr ) { return ptr; }

  kv::RoutePublish & sub_route;
  RvHostTab   host_tab;
  RvDaemonTab daemon_tab;
  RvHost    * dummy_host;
  uint32_t    host_timer_id; /* timer for HOST.STATUS */
  uint16_t    ipport;
  bool        has_service_prefix;

  EvRvListen( kv::EvPoll &p,  kv::RoutePublish &sr ) noexcept;
  EvRvListen( kv::EvPoll &p ) noexcept;
  /* EvListen */
  virtual EvSocket *accept( void ) noexcept;
  virtual int listen( const char *ip,  int port,  int opts ) noexcept;

  RvHostError start_network( RvHost *&h, const RvMcast &mc,  const char *net,
                             size_t net_len,  const char *svc,
                             size_t svc_len ) noexcept;
  virtual int start_host( RvHost &h ) noexcept; /* send _RV.INFO.SYSTEM.HOST.START */
  virtual int stop_host( RvHost &h ) noexcept;  /* send _RV.INFO.SYSTEM.HOST.STOP */
  int start_host2( RvHost &h,  uint32_t delay_secs ) noexcept;
};

/* count the number of segments in a subject:  4 = A.B.C.D */
static inline uint16_t
count_segments( const char *value,  size_t len )
{
  uint16_t     n = 1;
  const char * p = (const char *) ::memchr( value, '.', len );
  while ( p != NULL ) {
    n += 1;
    p  = (const char *) ::memchr( &p[ 1 ], '.', len - ( &p[ 1 ] - value ) );
  }
  return n;
}
/* the inbox subjects are not forwarded to '>' (only to *.hex or _INBOX.>) */
static inline bool
is_inbox_subject( const char *sub,  size_t sublen )
{
  if ( sublen > 6 && ::memcmp( sub, "_INBOX.", 6 ) == 0 )
    return true;
  return false;
}
/* these subjects do not have LISTEN.START notication and they do not show in
 * subscription queries to _INBOX.DAEMON.iphex */
static inline bool
is_restricted_subject( const char *sub,  size_t sublen )
{
  if ( sublen > 3 && ::memcmp( sub, "_RV.", 3 ) == 0 )
    return true;
  return is_inbox_subject( sub, sublen );
}
/* an entry of the subscription table */
struct RvSubRoute {
  uint32_t hash;       /* hash of subject */
  uint32_t msg_cnt,    /* number of messages matched */
           refcnt;     /* count of subscribes to subject */
  uint16_t len;        /* length of subject */
  char     value[ 2 ]; /* the subject string */
  uint16_t segments( void ) const {
    return count_segments( this->value, this->len );
  }
};
/* status returned by the subscription table lookups */
enum RvSubStatus {
  RV_SUB_OK        = 0,
  RV_SUB_EXISTS    = 1,
  RV_SUB_NOT_FOUND = 2
};

struct RvSubRoutePos {
  RvSubRoute * rt;
  uint32_t v;
  uint16_t off;
};
/* the subscription table for a EvRvService connection */
struct RvSubMap {
  kv::RouteVec<RvSubRoute> tab;

  bool is_null( void ) const {
    return this->tab.vec_size == 0;
  }
  size_t sub_count( void ) const {
    return this->tab.pop_count();
  }
  void release( void ) {
    this->tab.release();
  }
  /* put in new sub
   * tab[ sub ] => {cnt} */
  RvSubStatus put( uint32_t h,  const char *sub,  size_t len,  uint32_t &cnt,
                   bool &collision ) {
    kv::RouteLoc loc;
    uint32_t     hcnt;
    RvSubRoute * rt = this->tab.upsert2( h, sub, len, loc, hcnt );
    if ( rt == NULL )
      return RV_SUB_NOT_FOUND;
    collision = ( hcnt > 0 );
    if ( loc.is_new ) {
      rt->msg_cnt = 0;
      rt->refcnt = 1;
      cnt = 1;
      return RV_SUB_OK;
    }
    cnt = ++rt->refcnt;
    return RV_SUB_EXISTS;
  }
  /* update cnt for sub
   * tab[ sub ] => {cnt++} */
  RvSubStatus updcnt( uint32_t h,  const char *sub,  size_t len ) const {
    RvSubRoute * rt = this->tab.find( h, sub, len );
    if ( rt == NULL )
      return RV_SUB_NOT_FOUND;
    rt->msg_cnt++;
    return RV_SUB_OK;
  }
  RvSubStatus find( uint32_t h,  const char *sub,  size_t len,
                    bool &collision ) {
    kv::RouteLoc loc;
    uint32_t     hcnt;
    RvSubRoute * rt = this->tab.find2( h, sub, len, loc, hcnt );
    if ( rt == NULL ) {
      collision = ( hcnt > 0 );
      return RV_SUB_NOT_FOUND;
    }
    collision = ( hcnt > 1 );
    return RV_SUB_OK;
  }
  /* remove tab[ sub ] */
  RvSubStatus rem( uint32_t h,  const char *sub,  size_t len,  uint32_t &cnt,
                   bool &collision ) {
    kv::RouteLoc loc;
    uint32_t     hcnt;
    RvSubRoute * rt = this->tab.find2( h, sub, len, loc, hcnt );
    if ( rt == NULL )
      return RV_SUB_NOT_FOUND;
    collision = ( hcnt > 1 );
    cnt = --rt->refcnt;
    if ( rt->refcnt == 0 )
      this->tab.remove( loc );
    return RV_SUB_OK;
  }
  /* iterate first tab[ sub ] */
  bool first( RvSubRoutePos &pos ) {
    pos.rt = this->tab.first( pos.v, pos.off );
    return pos.rt != NULL;
  }
  /* iterate next tab[ sub ] */
  bool next( RvSubRoutePos &pos ) {
    pos.rt = this->tab.next( pos.v, pos.off );
    return pos.rt != NULL;
  }
  bool rem_collision( RvSubRoute *rt ) {
    kv::RouteLoc loc;
    RvSubRoute * rt2;
    rt->refcnt = ~(uint32_t) 0;
    if ( (rt2 = this->tab.find_by_hash( rt->hash, loc )) != NULL ) {
      do {
        if ( rt2->refcnt != ~(uint32_t) 0 )
          return true;
      } while ( (rt2 = this->tab.find_next_by_hash( rt->hash, loc )) != NULL );
    }
    return false;
  }
};
/* match wild, list of these for each pattern prefix */
struct RvWildMatch {
  RvWildMatch             * next,
                          * back;
  pcre2_real_code_8       * re;         /* pcre match the subject */
  pcre2_real_match_data_8 * md;
  uint32_t                  msg_cnt,    /* count of msgs matched */
                            refcnt;     /* how many times subscribed */
  uint16_t                  len;        /* length of the pattern subject */
  char                      value[ 2 ]; /* the pattern subject */

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  RvWildMatch( size_t patlen,  const char *pat,  pcre2_real_code_8 *r,
               pcre2_real_match_data_8 *m )
    : next( 0 ), back( 0 ), re( r ), md( m ), msg_cnt( 0 ),
      refcnt( 1 ), len( (uint16_t) patlen ) {
    ::memcpy( this->value, pat, patlen );
    this->value[ patlen ] = '\0';
  }
  uint16_t segments( void ) const {
    return count_segments( this->value, this->len );
  }
  static RvWildMatch *create( size_t patlen,  const char *pat,
                           pcre2_real_code_8 *r, pcre2_real_match_data_8 *m ) {
    size_t sz = sizeof( RvWildMatch ) + patlen - 2;
    void * p  = ::malloc( sz );
    if ( p == NULL ) return NULL;
    return new ( p ) RvWildMatch( patlen, pat, r, m );
  }
};
/* an entry in the pattern subscribe table */
struct RvPatternRoute {
  uint32_t                   hash,       /* hash of the pattern prefix */
                             count;
  kv::DLinkList<RvWildMatch> list;
  uint16_t                   len;        /* length of the pattern subject */
  char                       value[ 2 ]; /* the pattern subject */
};

struct RvPatternRoutePos {
  RvPatternRoute * rt;
  uint32_t v;
  uint16_t off;
};
/* a pattern subscribe table */
struct RvPatternMap {
  kv::RouteVec<RvPatternRoute> tab;
  size_t sub_count;

  RvPatternMap() : sub_count( 0 ) {}
  bool is_null( void ) const {
    return this->tab.vec_size == 0;
  }
  void release( void ) noexcept;
  /* put in new sub
   * tab[ sub ] => {cnt} */
  RvSubStatus put( uint32_t h,  const char *sub,  size_t len,
                   RvPatternRoute *&rt,  bool &collision ) {
    kv::RouteLoc loc;
    uint32_t     hcnt;
    rt = this->tab.upsert2( h, sub, len, loc, hcnt );
    if ( rt == NULL )
      return RV_SUB_NOT_FOUND;
    collision = ( hcnt > 0 );
    if ( loc.is_new ) {
      rt->count = 0;
      rt->list.init();
      return RV_SUB_OK;
    }
    return RV_SUB_EXISTS;
  }

  RvSubStatus find( uint32_t h,  const char *sub,  size_t len,
                    RvPatternRoute *&rt ) {
    rt = this->tab.find( h, sub, len );
    if ( rt == NULL )
      return RV_SUB_NOT_FOUND;
    return RV_SUB_OK;
  }
  RvSubStatus find( uint32_t h,  const char *sub,  size_t len,
                    RvPatternRoute *&rt,  bool &collision ) {
    kv::RouteLoc loc;
    return this->find( h, sub, len, loc, rt, collision );
  }
  RvSubStatus find( uint32_t h,  const char *sub,  size_t len,
                    kv::RouteLoc &loc, RvPatternRoute *&rt,  bool &collision ) {
    uint32_t hcnt;
    rt = this->tab.find2( h, sub, len, loc, hcnt );
    if ( rt == NULL ) {
      collision = ( hcnt > 0 );
      return RV_SUB_NOT_FOUND;
    }
    collision = ( hcnt > 1 );
    return RV_SUB_OK;
  }
  /* iterate first tab[ sub ] */
  bool first( RvPatternRoutePos &pos ) {
    pos.rt = this->tab.first( pos.v, pos.off );
    return pos.rt != NULL;
  }
  /* iterate next tab[ sub ] */
  bool next( RvPatternRoutePos &pos ) {
    pos.rt = this->tab.next( pos.v, pos.off );
    return pos.rt != NULL;
  }
  bool rem_collision( RvPatternRoute *rt,  RvWildMatch *m ) {
    kv::RouteLoc     loc;
    RvPatternRoute * rt2;
    RvWildMatch    * m2;
    m->refcnt = ~(uint32_t) 0;
    if ( (rt2 = this->tab.find_by_hash( rt->hash, loc )) != NULL ) {
      do {
        for ( m2 = rt2->list.tl; m2 != NULL; m2 = m2->back ) {
          if ( m2->refcnt != ~(uint32_t) 0 )
            return true;
        }
      } while ( (rt2 = this->tab.find_next_by_hash( rt->hash, loc )) != NULL );
    }
    return false;
  }
};

static const uint32_t RV_STATUS_IVAL = 90; /* HOST.STATUS interval */

enum RvStatus {
  RV_OK        = 0,
  ERR_RV_MSG   = 1, /* bad msg format */
  ERR_RV_REF   = 2, /* bad data reference */
  ERR_RV_MTYPE = 3, /* bad msg mtype field */
  ERR_RV_SUB   = 4, /* bad msg sub field */
  ERR_RV_DATA  = 5  /* bad msg data field */
};
#define RV_STATUS_STRINGS { "ok", "bad msg format", "bad rv reference", \
                            "bad rv mtype", "bad rv subject", "bad rv data" }
/* the message envelope { sub : X, data : Y, mtype : D, return : Z } */
struct RvMsgIn {
  md::MDMsgMem      mem;      /* memory for message unpacking and subject */
  md::RvMsg       * msg;      /* msg data, has the following fields */
  md::RvFieldIter * iter;     /* iterator for parsing fields */
  md::MDReference   data;     /* the data : Y field */
  char            * reply;    /* the return : Z field */
  uint16_t          sublen,   /* string length of this->sub */
                    replylen; /* string length of this->reply */
  bool              is_wild;  /* if a '*' or '>' pattern match in sub */
  uint8_t           mtype;    /* the type message (Data, Info, Listen, Cancel)*/
  char            * sub,      /* the subject of message, sub : X */
                    prefix[ MAX_RV_SERVICE_LEN + 8 ], /* _service. */
                    sub_buf[ 64 ]; /* buffer for subject string decoded */
  uint16_t          prefix_len; /* if has_service_prefix _7500. */

  RvMsgIn() { this->init(); }

  void init( void ) {
    this->msg        = NULL;
    this->iter       = NULL;
    this->sub        = this->sub_buf;
    this->sublen     = 0;
    this->replylen   = 0;
    this->prefix_len = 0;
  }
  void release( void ) {
    this->init();
    this->mem.reuse();
  }
  void set_prefix( const char *pref,  uint16_t len ) {
    uint16_t i = sizeof( this->prefix ),
             j = len;
    this->prefix[ --i ] = '.';
    for (;;) {
      this->prefix[ --i ] = pref[ --j ];
      if ( i == 1 || j == 0 )
        break;
    }
    this->prefix[ --i ] = '_';
    this->prefix_len = sizeof( this->prefix ) - i;
  }
  void pre_subject( char *&str,  size_t &sz ) {
    str = this->sub - this->prefix_len;
    sz  = this->sublen + this->prefix_len;
  }
  size_t cat_pre_subject( char *str,  const char *suf,  size_t suflen ) {
    size_t sz = this->prefix_len;
    ::memcpy( str, this->sub - sz, sz );
    ::memcpy( &str[ sz ], suf, suflen );
    sz += suflen; str[ sz ] = '\0';
    return sz;
  }
  void make_pre_subject( char *&str,  size_t &sz,  char *suf, size_t suflen ) {
    if ( this->prefix_len > 0 ) {
      str = (char *) this->mem.make( suflen + this->prefix_len + 1 );
      sz  = this->cat_pre_subject( str, suf, suflen );
    }
    else {
      str = suf;
      sz  = suflen;
    }
  }
  bool subject_to_string( const uint8_t *buf,  size_t buflen ) noexcept;
  int unpack( void *msgbuf,  size_t msglen ) noexcept;
  void print( int status,  void *m,  size_t len ) noexcept;
};

struct RvIDLElem {
  uint32_t hash,         /* hash of subject */
           msg_loss,     /* number of messages lost */
           pub_msg_loss; /* count lost published */
  uint16_t len;          /* length of subject */
  char     value[ 2 ];   /* the subject string */
};

struct RvIDLQueue {
  kv::RouteVec<RvIDLElem> tab;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  RvIDLQueue() {}
  ~RvIDLQueue() {
    this->tab.release();
  }
};

struct EvRvService : public kv::EvConnection {
  void * operator new( size_t, void *ptr ) { return ptr; }
  enum RvState {
    VERS_RECV          = 0, /* handshake version */
    INFO_RECV          = 1, /* handshake info */
    DATA_RECV          = 2, /* normal processing when state >= DATA_RECV */
    IS_RV_DAEMON       = 4, /* rv7+ uses a DAEMON session for the service */
    IS_RV_SESSION      = 8, /* each session is distinct, based on the conn */
    SENT_HOST_START    = 16, /* sent a host start message */
    SENT_SESSION_START = 32, /* sent a session start message */
    SENT_SESSION_STOP  = 64  /* sent a session stop message */
  };
  kv::RoutePublish & sub_route;
  RvMsgIn      msg_in;         /* current message recvd */
  RvSubMap     sub_tab;        /* subscriptions open by connection */
  RvPatternMap pat_tab;        /* pattern subscriptions open by connection */
  EvRvListen & listener;
  RvHost     * host;           /* the session stats */
  char         session[ 64 ],  /* session id of this connection */
               control[ 64 ],  /* the inbox name */
               userid[ 64 ],   /* the userid */
               gob[ 16 ];      /* deamon generated session id */
  uint16_t     svc_state,      /* the rv states */
               session_len,    /* lengths for the above */
               control_len,
               userid_len,
               gob_len,
               vmaj,           /* vhat version of client is connected */
               vmin,
               vupd;
  bool         host_started,
               sent_initresp,
               sent_rvdconn;
  RvIDLQueue * loss_queue;
  uint64_t     timer_id;       /* timerid unique for this service */
  uint32_t     pub_events,
               pub_status[ 4 ]; /* loss, start, cycle, restart */

  EvRvService( kv::EvPoll &p,  const uint8_t t,  EvRvListen &l )
    : kv::EvConnection( p, t ), sub_route( l.sub_route ),
      listener( l ), loss_queue( 0 ) {}
  void initialize_state( uint64_t id ) {
    this->svc_state     = VERS_RECV;
    this->host          = NULL;
    this->session_len   = 0;
    this->control_len   = 0;
    this->userid_len    = 0;
    this->gob_len       = 0;
    this->vmaj          = 0;
    this->vmin          = 0;
    this->vupd          = 0;
    this->host_started  = false;
    this->sent_initresp = false;
    this->sent_rvdconn  = false;
    this->timer_id      = id;
    this->msg_in.init();
    ::memset( this->session, 0, (char *) (void *) &this->timer_id - 
                                &this->session[ 0 ] );
    ::strcpy( this->userid, "nobody" );
    this->userid_len = 6;
    if ( this->loss_queue != NULL ) {
      delete this->loss_queue;
      this->loss_queue = NULL;
    }
    this->pub_events = 0;
    for ( int i = 0; i < 4; i++ )
      this->pub_status[ i ] = 0;
  }
  void send_info( bool agree ) noexcept; /* info rec during connection start */
  int dispatch_msg( void *msg,  size_t msg_len ) noexcept; /* route msgs */
  int respond_info( void ) noexcept; /* parse and reply info msg ('I') */
  void send_start( void ) noexcept;
  void send_stop( void ) noexcept;
  void add_sub( void ) noexcept;     /* add subscription ('L') */
  void rem_sub( void ) noexcept;     /* unsubscribe subject ('C') */
  void rem_all_sub( void ) noexcept; /* when client disconnects, this clears */
  bool fwd_pub( void ) noexcept;     /* fwd a message from client to network */
  /* forward a message from network to client */
  bool fwd_msg( kv::EvPublish &pub ) noexcept;
  void inbound_data_loss( const char *sub,  size_t sublen,
                          kv::EvPublish &pub ) noexcept;
  bool pub_inbound_data_loss( void ) noexcept;
  static void print( void *m,  size_t len ) noexcept;
  /* EvSocket */
  virtual void read( void ) noexcept final;
  virtual void process( void ) noexcept final;
  virtual void process_close( void ) noexcept final;
  virtual void process_shutdown( void ) noexcept final;
  virtual void release( void ) noexcept final;
  virtual bool timer_expire( uint64_t tid, uint64_t eid ) noexcept final;
  virtual bool hash_to_sub( uint32_t h, char *k, size_t &klen ) noexcept final;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept final;
  virtual uint8_t is_subscribed( const kv::NotifySub &sub ) noexcept final;
  virtual uint8_t is_psubscribed( const kv::NotifyPattern &pat ) noexcept final;
};

/* temporary list of sessions for subscription listing */
struct RvServiceLink {
  void * operator new( size_t, void *ptr ) { return ptr; }
  RvServiceLink * next, * back;
  EvRvService   & svc;
  uint8_t         bits[ 256 ];

  RvServiceLink( EvRvService *s ) : next( 0 ), back( 0 ), svc( *s ) {}
  /* quickly filter duplicate subjects by hashing a 2048 bit array */
  void set( uint32_t h ) {
    this->bits[ ( h >> 3 ) & 0xff ] = 1 << ( h & 7 );
  }
  uint8_t test( uint32_t h ) const {
    return this->bits[ ( h >> 3 ) & 0xff ] & ( 1 << ( h & 7 ) );
  }
  /* used to uniquely identify a list of subjects among a list of sessions */
  bool check_subject( RvSubRoute &rt,  uint16_t prelen ) {
    const char * subj = rt.value;
    size_t       len  = rt.len;
    uint32_t     hash = rt.hash;
    if ( len <= prelen ||
         is_restricted_subject( &subj[ prelen ], len - prelen ) )
      return false;
    this->set( hash );
    for ( RvServiceLink *link = this->back; link != NULL; link = link->back ) {
      if ( link->test( hash ) != 0 )
        if ( link->svc.sub_tab.tab.find( hash, subj, len ) != NULL )
          return false;
    }
    return true;
  }
  /* used to uniquely identify a list of patterns among a list of sessions */
  bool check_pattern( RvPatternRoute &rt,  uint16_t prelen,  RvWildMatch *check ) {
    const char * pat  = rt.value;
    size_t       len  = rt.len;
    uint32_t     hash = rt.hash;
    if ( len < prelen ||
         is_restricted_subject( &pat[ prelen ], len - prelen ) )
      return false;
    for ( RvServiceLink *link = this->back; link != NULL; link = link->back ) {
      RvPatternRoute * rt;
      if ( (rt = link->svc.pat_tab.tab.find( hash, pat, len )) != NULL ) {
        for ( RvWildMatch *m = rt->list.hd; m != NULL; m = m->next ) {
          if ( m->len == check->len &&
               ::memcmp( m->value, check->value, m->len ) == 0 )
            return false;
        }
      }
    }
    return true;
  }
};

#define is_rv_debug kv_unlikely( rv_debug != 0 )
extern uint32_t rv_debug;

}
}

#endif
