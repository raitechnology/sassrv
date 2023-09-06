#ifndef __rai_sassrv__ft_h__
#define __rai_sassrv__ft_h__

#include <sassrv/rv_host.h>

/*
 * ft state transitions:
 *
 *    +--------+--------+
 *    v        |        |
 * listen -> join +-> primary --+
 *    ^           |     ^       |
 *    |           |     |       |
 *    |           |     v       |
 *    +-----------+-> secondary +->  finish
 *
 * primary has rank 1, secondaries have rankds 2 ... N
 *
 * weight is the primary method of determining the primary,
 *
 * heavier weights with greater weight values have higher ranks,
 * where 1 is the highest rank
 *
 * equal weight ties are broken using the wall clock utc time in nanosecs, the
 * oldest with a stamp less than another stamp will become primary before
 * others with the same weight
 */
namespace rai {
namespace sassrv {

struct RvFtListener {
  virtual void on_ft_change( uint8_t action ) noexcept;
  virtual void on_ft_sync( kv::EvPublish &pub ) noexcept;
};

static const char     FT_MSG_TYPE[]   = "FT_MSG_TYPE",  /* enum FtMsgType */
                      FT_STATE[]      = "FT_STATE",     /* enum FtState */
                      FT_START_NS[]   = "FT_START_NS",  /* utc stamp nanos */
                      FT_CUR_TIME[]   = "FT_CUR_TIME",  /* utc stamp nanos */
                      FT_FIN_TIME[]   = "FT_FIN_TIME",  /* utc stamp to exit */
                      FT_WEIGHT[]     = "FT_WEIGHT",    /* weight of sender */
                      FT_USER[]       = "FT_USER",      /* arbitrary str */
                      FT_MEMBER_CNT[] = "FT_MEMBER_CNT",/* how many in grp */
                      FT_HB_IVAL[]    = "FT_HB_IVAL",   /* the heartbeat ms */
                      FT_ACT_IVAL[]   = "FT_ACT_IVAL",  /* the activation ms */
                      FT_PREP_IVAL[]  = "FT_PREP_IVAL", /* the prepare ms */
                      FT_SYNC_INBOX[] = "FT_SYNC_INBOX";/* sync msgs */
static const size_t   FT_MAX_FIELDS   = 12;

struct FtParameters {
  uint64_t     weight;       /* initial weight setting, could change later */
  uint32_t     join_ms,      /* how long to stay in listen state */
               heartbeat_ms, /* interval of heartbeat / keepalive sends */
               activate_ms,  /* when a peer is considered dead */
               prepare_ms,   /* when a peer should prepare to become primary */
               finish_ms,    /* how long in the finish state, then exit */
               inbox_num,    /* what inbox number to use for ACK msgs */
               sync_inbox_num;/* inbox number for sync msgs */
  const char * ft_sub,       /* subject all messaes are sent, except ACK */
             * user;         /* arbitrary string, not used in the algo */
  size_t       ft_sub_len,   /* len of ft_sub */
               user_len;     /* len of user */
  FtParameters()
    : weight( 20 ), join_ms( 500 ), heartbeat_ms( 3000 ), activate_ms( 7000 ),
      prepare_ms( 6500 ), finish_ms( 500 ),
      inbox_num( 10 ), sync_inbox_num( 11 ), ft_sub( "_FT.GRP" ),
      user( "none" ), ft_sub_len( 7 ), user_len( 4 ) {}
};

struct FtPeerMsg {
  uint8_t      type,       /* FtMsgType */
               state;      /* FtState */
  uint32_t     member_cnt, /* current count of active peers, including self */
               hb_ival,    /* time hb/keepalive are sent, in ms */
               act_ival,   /* time after hb to consider peer dead */
               prep_ival;  /* time after hb to prepare peer for primary */
  uint64_t     start_ns,   /* utc start timestamp, older is higher rank */
               weight,     /* current weight */
               cur_time,   /* utc time msg was sent */
               fin_time;   /* utc time peer will exit */
  const char * reply;      /* if ANNOUNCE type, will have reply for ACK */
  size_t       reply_len;
  char         user[ 64 ], /* name of peer */
               sync_inbox[ MAX_RV_INBOX_LEN ];

  FtPeerMsg( const char *rep,  size_t rep_len ) {
    ::memset( (void *) this, 0, sizeof( *this ) );
    this->type      = 0xff;
    this->state     = 0xff;
    this->reply     = rep;
    this->reply_len = rep_len;
  }
  size_t iter_map( md::MDIterMap *mp ) {
    size_t n = 0;
    mp[ n++ ].uint  ( FT_MSG_TYPE   , this->type );
    mp[ n++ ].uint  ( FT_STATE      , this->state );
    mp[ n++ ].uint  ( FT_START_NS   , this->start_ns );
    mp[ n++ ].uint  ( FT_WEIGHT     , this->weight );
    mp[ n++ ].uint  ( FT_CUR_TIME   , this->cur_time );
    mp[ n++ ].uint  ( FT_FIN_TIME   , this->fin_time );
    mp[ n++ ].uint  ( FT_MEMBER_CNT , this->member_cnt );
    mp[ n++ ].uint  ( FT_HB_IVAL    , this->hb_ival );
    mp[ n++ ].uint  ( FT_ACT_IVAL   , this->act_ival );
    mp[ n++ ].uint  ( FT_PREP_IVAL  , this->prep_ival );
    mp[ n++ ].string( FT_USER       , this->user, sizeof( this->user ) );
    mp[ n++ ].string( FT_SYNC_INBOX , this->sync_inbox,
                                      sizeof( this->sync_inbox ) );
    return n;
  }
  void print( void ) const noexcept;
};

struct FtStateCount {
  uint32_t primary,
           secondary,
           join;
  FtStateCount() : primary( 0 ), secondary( 0 ), join( 0 ) {}
  void update( uint8_t old_state,  uint8_t new_state ) noexcept;
  uint32_t member_count( void ) const {
    return this->primary + this->secondary + this->join;
  }
  void init( void ) {
    this->primary = this->secondary = this->join = 0;
  }
};

struct FtPeer {
  uint64_t start_ns,      /* advertised start time (wall clock) */
           weight,        /* advertised weight */
           last_rcv_ns;   /* last mono time of on_msg */
  int64_t  latency_accum; /* track skew of sys clocks */
  uint32_t pos,           /* current rank 1 -> N, primary = 1 */
           latency_cnt;   /* how many sample latenceies */
  const uint8_t state;    /* advertised state, PRIMARY, SECONDARY, JOINED */
  uint8_t  old_state;
  char     user[ 64 ],
           sync_inbox[ MAX_RV_INBOX_LEN ];

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  FtPeer() : state( 0 ) { this->zero(); }
  FtPeer( const FtPeerMsg *m,  FtStateCount &count ) : state( 0 ) {
    this->zero();
    if ( m != NULL )
      this->set( *m, count );
  }
  void zero( void ) {
    ::memset( (void *) this, 0, sizeof( *this ) );
  }
  void set( const FtPeerMsg &m,  FtStateCount &count ) {
    this->start_ns  = m.start_ns;
    this->weight    = m.weight;
    this->old_state = this->state;
    (uint8_t &) this->state = m.state;
    count.update( this->old_state, this->state );
    ::memcpy( this->user, m.user, sizeof( this->user ) );
    ::memcpy( this->sync_inbox, m.sync_inbox, sizeof( this->sync_inbox ) );
  }
  void update_state( uint8_t new_state,  FtStateCount &count ) {
    this->old_state = this->state;
    (uint8_t &) this->state = new_state;
    count.update( this->old_state, this->state );
  }
  int64_t latency( void ) const {
    if ( ( this->latency_cnt % 256 ) == 0 )
      return 0;
    return this->latency_accum / ( this->latency_cnt % 256 );
  }
  void accum_latency( int64_t lat ) {
    if ( ( this->latency_cnt + 1 ) % 256 == 0 ) {
      this->latency_accum = this->latency();
      this->latency_cnt++;
    }
    this->latency_accum += lat;
    this->latency_cnt++;
  }
  static bool is_greater( const FtPeer &p1,  const FtPeer &p2 ) noexcept;
  static bool is_greater_weight( const FtPeer &p1,  const FtPeer &p2 ) noexcept;
};

struct FtQueue : public kv::ArrayCount<FtPeer *, 4> {
  uint64_t seqno;
  void insert( FtPeer *p ) noexcept;
  void reorder( void ) noexcept;
  uint32_t get_pos( FtPeer *p ) noexcept;
  void remove( FtPeer *p ) noexcept;
  void update( FtPeer *p ) noexcept;
  bool in_order( FtPeer *p ) const {
    if ( p->pos == 0 )
      return false;
    return this->in_order2( p );
  }
  bool in_order2( FtPeer *p ) const noexcept;
  void print( void ) noexcept;
  FtQueue() : seqno( 0 ) {}
};
typedef kv::IntHashTabT<uint64_t, FtPeer *> FtPeerHT;

struct FtPeerFree {
  uint64_t block[ ( sizeof( FtPeer ) * 16 ) / sizeof( uint64_t ) ];
  uint16_t used_mask;
  FtPeerFree * next;
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  FtPeerFree( FtPeerFree *n ) : used_mask( 0 ), next( n ) {}
  bool is_all_used( void ) noexcept;
  void * make_peer( void ) noexcept;
  void release_peer( void *p ) noexcept;
};

static const size_t MAX_FT_SUB_LEN = 64;

struct RvFt : public kv::EvTimerCallback {
  enum FtAction {
    ACTION_LISTEN     = 0, /* after subscribing */
    ACTION_JOIN       = 1, /* listen for peers before joining them */
    ACTION_ACTIVATE   = 2, /* become the primary */
    ACTION_DEACTIVATE = 3, /* deactivate, become a secondary */
    ACTION_HEARTBEAT  = 4, /* sending the heartbeat */
    ACTION_PREPARE    = 5, /* prepare to become leader */
    ACTION_FINISH     = 6, /* prepare to exit */
    ACTION_UPDATE     = 7
  };
  #define RVFT_ACTION_COUNT 8
  static const char *action_str[ RVFT_ACTION_COUNT ];
  #define RVFT_ACTION_STR { "listen", "join", "activate", "deactivate", "heartbeat", "prepare", "finish", "update" }

  enum FtState {
    STATE_LISTEN    = 0, /* just listening, not joining */
    STATE_JOIN      = 1, /* planning to join after join_ms expires */
    STATE_PRIMARY   = 2, /* currently ranks number 1 */
    STATE_SECONDARY = 3, /* ranks lower than primary */
    STATE_FINISH    = 4  /* exiting the group */
  };
  #define RVFT_STATE_COUNT 5
  static const char *state_str[ RVFT_STATE_COUNT ];
  #define RVFT_STATE_STR { "listen", "join", "primary", "secondary", "finish" }

  enum FtMsgType {
    ANNOUNCE   = 0, /* annoucing a state change, peers respond w/ACK messages */
    HEARTBEAT  = 1, /* primary sends these */
    KEEPALIVE  = 2, /* secondaries send these */
    ACTIVATE   = 3, /* notify intention to become primary */
    DEACTIVATE = 4, /* notify secondary status */
    ACK        = 5, /* ack the ANNOUNCE messages */
    FINISH     = 6, /* in the process of exiting */
    EXITING    = 7  /* final notify, leave the group */
  };
  #define RVFT_MSG_TYPE_COUNT 8
  static const char *msg_type_str[ RVFT_MSG_TYPE_COUNT ];
  #define RVFT_MSG_TYPE_STR { "announce", "heartbeat", "keepalive", "activate", "deactivate", "ack", "finish", "exiting" }

  kv::EvPoll   & poll;
  EvRvClient   & client;          /* monitor this network */
  FtQueue        ft_queue;        /* all active peers, including self */
  FtPeerHT     * peer_ht;         /* index [ start_ns ] -> FtPeer */
  RvFtListener * cb;              /* on_listen_start() */
  FtPeer         me;              /* my peer, inserted into ft_queue */
  uint64_t       tid,             /* timer id, this FT instance */
                 state_ns,        /* last state transistion */
                 start_mono,      /* start monotonic time */
                 last_rcv_ns,     /* recv monotonic time */
                 last_hb_ns,      /* hb monotonic time */
                 activate_ns,     /* when changed to primary */
                 deactivate_ns,   /* when changed to secondary */
                 prepare_origin,  /* prepare monotonic stamp */
                 prepare_expires, /* prepare delta from origin */
                 activate_origin, /* activate monotinic stamp */
                 activate_expires,/* activate delta from orgin */
                 oldest_peer,     /* copy the hb, activate, ivals from oldest */
                 last_seqno;      /* last notify ft_queue seqno */
  int64_t        accuracy_warn;   /* warn if timer callback is not accurate */
  uint32_t       join_ms,         /* millis from listen -> join states */
                 heartbeat_ms,    /* millis between hb/keepalive */
                 activate_ms,     /* millis after primary hb to takeover */
                 prepare_ms,      /* millis after primary hb to prepare */
                 finish_ms,       /* millis to linger in finish state b4 exit */
                 inbox_num,       /* which inbox to use for ACK replies */
                 sync_inbox_num,  /* ft where sync msgs are sent */
                 timer_mask;      /* 1 << ACTION bits, set when timer active */
  FtStateCount   state_count;
  FtPeerFree   * ft_free;         /* unused peer mem */
  char           ft_sub[ MAX_FT_SUB_LEN ]; /* subject to publish */
  size_t         ft_sub_len;
  char           sync_inbox[ MAX_RV_INBOX_LEN ];
  md::MDOutput * mout;            /* debug log output */
  uint64_t       last_warn;

  RvFt( EvRvClient &c,  RvFtListener *ftl ) noexcept;

  void release( void ) noexcept;
  void start( FtParameters &param ) noexcept; /* start hb, go to listen state */
  void activate( void ) noexcept;   /* join the ft network, listen -> join */
  void deactivate( void ) noexcept; /* go to a listen state, run -> listen */
  uint32_t stop( void ) noexcept;   /* start exit notify, return ms time */

  void notify_change( uint8_t action ) noexcept; /* call on_ft_change() */
  bool notify_update( void ) noexcept; /* call on_ft_change() if needed */

  /* each action has a timer associated with it, these track which are set */
  void timer_active( FtAction action ) {
    this->timer_mask |= 1 << action;
  }
  bool timer_clear( FtAction action ) {
    uint32_t mask = 1 << action;
    bool test = ( this->timer_mask & mask ) != 0;
    this->timer_mask &= ~mask;
    return test;
  }
  bool timer_test_set( FtAction action ) {
    uint32_t mask = 1 << action;
    bool test = ( this->timer_mask & mask ) != 0;
    this->timer_mask |= mask;
    return test;
  }
  bool timer_is_set( FtAction action ) const {
    uint32_t mask = 1 << action;
    return ( this->timer_mask & mask ) != 0;
  }

  void finish( void ) noexcept;     /* final call before exit */
  void stop_timers( void ) noexcept;/* stop activate, deactivate timers */

  void set_state( FtState state ) noexcept; /* alter run state */
  void set_timer( FtAction action,  uint64_t delta_ms, /* start an action */
                  uint64_t &ori_ns,  uint64_t &exp_ns ) noexcept;
  void set_prepare_timer( void ) noexcept; /* prepare to takeover */
  void set_activate_timer( void ) noexcept; /* after prepare, time to activate*/

  int64_t expired_delta_ms( uint64_t stamp_ns,  uint64_t delta_ms ) noexcept;
  void trim_ft_queue( void ) noexcept;
  void prepare_takeover( uint8_t action,  uint64_t deadline ) noexcept;
  virtual bool timer_cb( uint64_t tid,  uint64_t action ) noexcept;
  void start_hb( uint32_t hb_ival,  uint32_t act_ival, 
                 uint32_t prep_ival ) noexcept;
  bool check_latency( FtPeer &p,  int64_t ns ) noexcept;
  void on_peer_msg( const FtPeerMsg &peer ) noexcept;
  bool process_pub( kv::EvPublish &pub ) noexcept;
  void send_msg( FtMsgType type,  const char *dest = NULL,
                 size_t dest_len = 0 ) noexcept;
  FtPeer *make_peer( const FtPeerMsg *msg = NULL ) noexcept;
  void release_peer( FtPeer *p ) noexcept;
  void warn( const char *fmt,  ... ) noexcept __attribute__((format(printf,2,3)));
};

}
}
#endif
