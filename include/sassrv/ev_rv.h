#ifndef __rai_sassrv__ev_rv_h__
#define __rai_sassrv__ev_rv_h__

extern "C" {
  struct pcre2_real_code_8;
  struct pcre2_real_match_data_8;
  struct sockaddr_in;
}

#include <raikv/ev_tcp.h>
#include <raikv/route_ht.h>
#include <sassrv/rv_host.h>

namespace rai {
namespace sassrv {

/* tcp listener for accepting EvRvService connections */
struct EvRvListen : public kv::EvTcpListen, public RvHost {
  void * operator new( size_t, void *ptr ) { return ptr; }
  EvRvListen( kv::EvPoll &p ) noexcept;
  /* EvListen */
  virtual bool accept( void ) noexcept;
  int listen( const char *ip,  int port,  int opts );
  void host_status( void ) noexcept; /* send _RV.INFO.SYSTEM.HOST.STATUS */
  /* start / stop network */
  virtual int start_host( void ) noexcept; /* send _RV.INFO.SYSTEM.HOST.START */
  virtual int stop_host( void ) noexcept;  /* send _RV.INFO.SYSTEM.HOST.STOP */
  /* EvSocket */
  virtual bool timer_expire( uint64_t tid, uint64_t eid ) noexcept final;
  virtual void process( void ) noexcept final;
  virtual void process_close( void ) noexcept final;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept final;

  void subscribe_daemon_inbox( void ) noexcept;
  void unsubscribe_daemon_inbox( void ) noexcept;
  void send_sessions( const void *reply,  size_t reply_len ) noexcept;
  void send_subscriptions( const char *session,  size_t session_len,
                           const void *reply,  size_t reply_len ) noexcept;
};

/* count the number of segments in a subject:  4 = A.B.C.D */
static inline uint16_t
count_segments( const char *value,  uint16_t len )
{
  uint16_t     n = 1;
  const char * p = (const char *) ::memchr( value, '.', len );
  while ( p != NULL ) {
    n += 1;
    p  = (const char *) ::memchr( &p[ 1 ], '.', len - ( &p[ 1 ] - value ) );
  }
  return n;
}
/* these subjects do not have LISTEN.START notication and they do not show in
 * subscription queries to _INBOX.DAEMON.iphex */
static inline bool
is_restricted_subject( const char *sub,  size_t sublen )
{
  if ( sublen > 3 && ::memcmp( sub, "_RV.", 3 ) == 0 )
    return true;
  if ( sublen > 6 && ::memcmp( sub, "_INBOX.", 6 ) == 0 )
    return true;
  return false;
}
/* an entry of the subscription table */
struct RvSubRoute {
  uint32_t hash;       /* hash of subject */
  uint32_t msg_cnt;    /* number of messages matched */
  uint32_t refcnt;     /* count of subscribes to subject */
  uint16_t len;        /* length of subject */
  char     value[ 2 ]; /* the subject string */
  bool equals( const void *s,  uint16_t l ) const {
    return l == this->len && ::memcmp( s, this->value, l ) == 0;
  }
  void copy( const void *s,  uint16_t l ) {
    ::memcpy( this->value, s, l );
  }
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
  RvSubStatus put( uint32_t h,  const char *sub,  size_t len,  uint32_t &cnt ) {
    kv::RouteLoc loc;
    RvSubRoute * rt = this->tab.upsert( h, sub, len, loc );
    if ( rt == NULL )
      return RV_SUB_NOT_FOUND;
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
  /* remove tab[ sub ] */
  RvSubStatus rem( uint32_t h,  const char *sub,  size_t len,  uint32_t &cnt ) {
    kv::RouteLoc loc;
    RvSubRoute * rt = this->tab.find( h, sub, len, loc );
    if ( rt == NULL )
      return RV_SUB_NOT_FOUND;
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
};
/* an entry in the pattern subscribe table */
struct RvPatternRoute {
  uint32_t                  hash,       /* hash of the pattern prefix */
                            msg_cnt;    /* count of msgs matched */
  pcre2_real_code_8       * re;         /* pcre match the subject */
  pcre2_real_match_data_8 * md;
  uint32_t                  refcnt;     /* how many times subscribed */
  uint16_t                  len;        /* length of the pattern subject */
  char                      value[ 2 ]; /* the pattern subject */

  bool equals( const void *s,  uint16_t l ) const {
    return l == this->len && ::memcmp( s, this->value, l ) == 0;
  }
  void copy( const void *s,  uint16_t l ) {
    ::memcpy( this->value, s, l );
  }
  uint16_t segments( void ) const {
    return count_segments( this->value, this->len );
  }
};

struct RvPatternRoutePos {
  RvPatternRoute * rt;
  uint32_t v;
  uint16_t off;
};
/* a pattern subscribe table */
struct RvPatternMap {
  kv::RouteVec<RvPatternRoute> tab;

  bool is_null( void ) const {
    return this->tab.vec_size == 0;
  }
  size_t sub_count( void ) const {
    return this->tab.pop_count();
  }
  void release( void ) noexcept;
  /* put in new sub
   * tab[ sub ] => {cnt} */
  RvSubStatus put( uint32_t h,  const char *sub,  size_t len,
                   RvPatternRoute *&rt,  uint32_t &cnt ) {
    kv::RouteLoc loc;
    rt = this->tab.upsert( h, sub, len, loc );
    if ( rt == NULL )
      return RV_SUB_NOT_FOUND;
    if ( loc.is_new ) {
      rt->msg_cnt = 0;
      rt->re = NULL;
      rt->md = NULL;
      rt->refcnt = 1;
      cnt = 1;
      return RV_SUB_OK;
    }
    cnt = ++rt->refcnt;
    return RV_SUB_EXISTS;
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
                    sub_buf[ 64 ]; /* buffer for subject string decoded */

  RvMsgIn() { this->init(); }

  void init( void ) {
    this->msg      = NULL;
    this->iter     = NULL;
    this->sub      = this->sub_buf;
    this->sublen   = 0;
    this->replylen = 0;
  }
  void release( void ) {
    this->init();
    this->mem.reuse();
  }
  bool subject_to_string( const uint8_t *buf,  size_t buflen ) noexcept;
  int unpack( void *msgbuf,  size_t msglen ) noexcept;
  void print( void ) noexcept;
};

struct EvRvService : public kv::EvConnection {
  void * operator new( size_t, void *ptr ) { return ptr; }
  enum RvState {
    VERS_RECV,        /* handshake version */
    INFO_RECV,        /* handshake info */
    DATA_RECV,        /* normal protocol processing when state >= DATA_RECV */
    DATA_RECV_DAEMON, /* rv7+ uses a DAEMON session for the service */
    DATA_RECV_SESSION /* each session is distinct, based on the connection */
  };
  RvMsgIn      msg_in;         /* current message recvd */
  RvSubMap     sub_tab;        /* subscriptions open by connection */
  RvPatternMap pat_tab;        /* pattern subscriptions open by connection */
  RvHost     & stat;           /* the session stats */
  RvState      state;          /* one of the above states */
  char         session[ 64 ],  /* session id of this connection */
               control[ 64 ],  /* the inbox name */
               userid[ 64 ],   /* the userid */
               gob[ 16 ];      /* deamon generated session id */
  uint16_t     session_len,    /* lengths for the above */
               control_len,
               userid_len,
               gob_len,
               vmaj,           /* vhat version of client is connected */
               vmin,
               vupd;
  uint64_t     timer_id;       /* timerid unique for this service */

  EvRvService( kv::EvPoll &p,  const uint8_t t,
               RvHost &st ) : kv::EvConnection( p, t ), stat( st ) {}
  void initialize_state( uint64_t id ) {
    this->state    = VERS_RECV;
    this->timer_id = id;
    this->msg_in.init();
    ::memset( this->session, 0, (char *) (void *) &this->timer_id - 
                                &this->session[ 0 ] );
    ::strcpy( this->userid, "nobody" );
    this->userid_len = 6;
  }
  void send_info( bool agree ) noexcept; /* info rec during connection start */
  int dispatch_msg( void *msg,  size_t msg_len ) noexcept; /* route msgs */
  int respond_info( void ) noexcept; /* parse and reply info msg ('I') */
  void add_sub( void ) noexcept;     /* add subscription ('L') */
  void rem_sub( void ) noexcept;     /* unsubscribe subject ('C') */
  void rem_all_sub( void ) noexcept; /* when client disconnects, this clears */
  bool fwd_pub( void ) noexcept;     /* fwd a message from client to network */
  /* forward a message from network to client */
  bool fwd_msg( kv::EvPublish &pub,  const void *sid,  size_t sid_len ) noexcept;
  /* EvSocket */
  virtual void process( void ) noexcept final;
  virtual void process_close( void ) noexcept final;
  virtual void release( void ) noexcept final;
  virtual bool timer_expire( uint64_t tid, uint64_t eid ) noexcept final;
  virtual bool hash_to_sub( uint32_t h, char *k, size_t &klen ) noexcept final;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept final;
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
  bool check_subject( const char *subj,  uint16_t len,  uint32_t hash ) {
    if ( is_restricted_subject( subj, len ) )
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
  bool check_pattern( const char *pat,  uint16_t len,  uint32_t hash ) {
    if ( is_restricted_subject( pat, len ) )
      return false;
    this->set( hash );
    for ( RvServiceLink *link = this->back; link != NULL; link = link->back ) {
      if ( link->test( hash ) != 0 )
        if ( link->svc.pat_tab.tab.find( hash, pat, len ) != NULL )
          return false;
    }
    return true;
  }
};

}
}
#endif
