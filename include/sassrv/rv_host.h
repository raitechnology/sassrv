#ifndef __rai_sassrv__rv_host_h__
#define __rai_sassrv__rv_host_h__

#include <raimd/rv_msg.h>

namespace rai {
namespace sassrv {

struct RvHost;
struct EvRvService;
struct EvRvListen;

/* advisory fields that go into host status publishes */
enum RvAdv {
  #define m(x) ( 1U << x )
  ADV_HOSTADDR = m(0),  ADV_MS       = m(13),
  ADV_SN       = m(1),  ADV_BS       = m(14),
  ADV_OS       = m(2),  ADV_MR       = m(15),
  ADV_VER      = m(3),  ADV_BR       = m(16),
  ADV_HTTPADDR = m(4),  ADV_PS       = m(17),
  ADV_HTTPPORT = m(5),  ADV_PR       = m(18),
  ADV_TIME     = m(6),  ADV_RX       = m(19),
  ADV_ID       = m(7),  ADV_PM       = m(20),
  ADV_USERID   = m(8),  ADV_IDL      = m(21),
  ADV_RLI      = m(9),  ADV_ODL      = m(22),
  ADV_UP       = m(10), ADV_IPPORT   = m(23),
  ADV_SUB      = m(11), ADV_SERVICE  = m(24),
  ADV_REFCNT   = m(12), ADV_NETWORK  = m(25),
                        ADV_IRRS     = m(26),
                        ADV_ORRS     = m(27),
  #undef m

  ADV_HOST_START  = ADV_HOSTADDR | ADV_SN | ADV_OS | ADV_VER | ADV_HTTPADDR |
                    ADV_HTTPPORT | ADV_TIME | ADV_UP | ADV_RLI | ADV_IPPORT |
                    ADV_SERVICE | ADV_NETWORK,
  ADV_STATS       = ADV_MS | ADV_BS | ADV_MR | ADV_BR | ADV_PS | ADV_PR |
                    ADV_RX | ADV_PM | ADV_IDL | ADV_ODL | ADV_IRRS | ADV_ORRS,
  ADV_HOST_STATUS = ADV_HOSTADDR | ADV_SN | ADV_OS | ADV_VER | ADV_HTTPADDR |
                    ADV_HTTPPORT | ADV_TIME | ADV_UP | ADV_RLI | ADV_STATS |
                    ADV_IPPORT | ADV_SERVICE | ADV_NETWORK,
  ADV_HOST_STOP   = ADV_HOSTADDR | ADV_TIME,
  ADV_SESSION     = ADV_HOSTADDR | ADV_ID | ADV_USERID,

  ADV_HOST_COMMON = ADV_HOST_START | ADV_HOST_STATUS |
                    ADV_HOST_STOP  | ADV_SESSION,

  ADV_LISTEN      = ADV_ID | ADV_SUB | ADV_REFCNT
};

static const size_t MAX_RV_NETWORK_LEN = 1680,
                    MAX_RV_SERVICE_LEN = 32,
                    MAX_RV_HOST_ID_LEN = 64;

struct RvFwdAdv {
  const char * userid;
  size_t       userid_len;
  const char * session;
  size_t       session_len;
  const char * subj_prefix;
  size_t       fix_len;
  const char * subj_suffix;
  size_t       suf_len;
  const char * reply;
  size_t       reply_len;
  uint32_t     refcnt;

  RvFwdAdv( RvHost &host,  EvRvService *svc,  const char *prefix,
            size_t prefix_len,  int flags ) noexcept;
  RvFwdAdv( RvHost &host,  const char *userid,  size_t userid_len,
            const char *session,  size_t session_len,  const char *prefix,
            size_t prefix_len,  int flags,  uint32_t ref = 0,
            const char *suffix = NULL,  size_t suffix_len = 0,
            const char *rep = NULL,  size_t rep_len = 0 ) noexcept;
  void fwd( RvHost &host,  int flags ) noexcept;
};

enum RvHostError {
  HOST_OK                = 0,
  ERR_GETHOSTNAME_FAILED = 1, /* gethostname() failed */
  ERR_NO_INTERFACE_FOUND = 2, /* hostname addr does not match interface */
  ERR_HOSTNAME_NOT_FOUND = 3, /* hostname does not resolve */
  ERR_SAME_SVC_TWO_NETS  = 5, /* the same service with two different networks */
  ERR_NETWORK_NOT_FOUND  = 17,/* network not found */
  ERR_BAD_SERVICE_NUM    = 18,/* svc number bad */
  ERR_BAD_PARAMETERS     = 19,/* the length of network overfl MAX_RV_NETWORK */
  ERR_START_HOST_FAILED  = 20 /* EvRvListener::host_start() failed */
};

const char * get_rv_host_error( int status ) noexcept;

/* eth0    ; 228.8.8.8,226.6.6.6 ; 224.4.4.4 */
/* host_ip   recv_ip[ 2 ],         send_ip  (all in network order) */
struct RvMcast {
  static const uint32_t MAX_RECV_MCAST = 59;
  uint32_t host_ip,                   /* eth0 */
           netmask,                   /* netmask for host_ip */
           send_ip,                   /* 224.4.4.4 */
           recv_ip[ MAX_RECV_MCAST ], /* 228.8.8.8,226.6.6.6 */
           recv_cnt,                  /* 2 */
           fake_ip;
  /* these are in network order */
  RvMcast() : host_ip( 0 ), netmask( 0 ), send_ip( 0 ), recv_cnt( 0 ),
              fake_ip( 0 ) {}
  RvMcast( const RvMcast &mc ) { this->copy( mc ); }
  void copy( const RvMcast &mc ) {
    this->host_ip = mc.host_ip; this->netmask =  mc.netmask;
    this->send_ip = mc.send_ip; this->recv_cnt = mc.recv_cnt;
    this->fake_ip = mc.fake_ip;
    ::memcpy( this->recv_ip, mc.recv_ip, sizeof( this->recv_ip[ 0 ] ) *
              mc.recv_cnt );
  }
  /* fill out above fields by parsing a rv network string:
   * "eth0;228.8.8.8,226.6.6.6;224.4.4.4", reeturn RvHostError if failed */
  RvHostError parse_network( const char *network,  size_t net_len ) noexcept;

  static bool is_empty_string( const char *s ) noexcept;
  /* return ip4 addr */
  static uint32_t lookup_host_ip4( const char *host ) noexcept;
  /* return ip4 addr, resolve netmask by checking device */
  static uint32_t lookup_host_ip4( const char *host,
                                   uint32_t &netmask ) noexcept;
  /* return ip4 addr, netmask by looking up device */
  static uint32_t lookup_dev_ip4( const char *dev,
                                  uint32_t &netmask ) noexcept;
  static uint16_t ip4_string( uint32_t ip,  char *buf ) noexcept;
  static uint16_t ip4_hex_string( uint32_t ip,  char *buf ) noexcept;
  void print( void ) noexcept;
};

/* host stats for the service */
struct RvDaemonRpc;
struct RvHost;

struct RvDaemonTab : public kv::ArrayCount<RvDaemonRpc *, 4> {
  void * operator new( size_t, void *ptr ) { return ptr; }
  RvDaemonTab() {}
};

struct RvHostTab : public kv::ArrayCount<RvHost *, 4> {
  void * operator new( size_t, void *ptr ) { return ptr; }
  RvHostTab() {}
};

struct RvHostNet {
  const char * service,
             * network;
  uint16_t     service_len,
               network_len;
  char         svc_buf[ 8 ];
  uint16_t     ipport;
  bool         has_service_prefix;
  RvHostNet( const char *s, uint16_t slen,
             const char *n, uint16_t nlen,
             uint16_t port,  bool has_svc_pre )
      : service( s ),        network( n ),
        service_len( slen ), network_len( nlen ),
        ipport( port ), has_service_prefix( has_svc_pre ) {}

  RvHostNet( uint16_t svc,  uint16_t port,  bool has_svc_pre )
      : network( 0 ), network_len( 0 ) {
    uint16_t i = 0, div = 10000;
    for ( ; div > 1; div /= 10 ) {
      if ( svc >= div )
        this->svc_buf[ i++ ] = ( ( svc / div ) % 10 ) + '0';
    }
    this->svc_buf[ i++ ] = ( svc % 10 ) + '0';
    this->svc_buf[ i ]   = '\0';
    this->service        = this->svc_buf;
    this->service_len    = i;
    this->ipport         = port;
    this->has_service_prefix = has_svc_pre;
  }
};

struct RvHostDB {
  RvHostTab   * host_tab;
  RvDaemonTab * daemon_tab;

  RvHostDB() : host_tab( 0 ), daemon_tab( 0 ) {}
  bool get_service( RvHost *&h,  const RvHostNet &hn ) noexcept;
  bool get_service( RvHost *&h,  uint16_t svc ) noexcept;
  int start_service( RvHost *&h,  kv::EvPoll &poll,  kv::RoutePublish &sr,
                     const RvHostNet &hn ) noexcept;
};

struct RvDataLossElem {
  uint32_t     hash,         /* hash of subject */
               msg_loss,     /* number of messages lost */
               pub_msg_loss, /* count lost published */
               pub_host;     /* source of loss */
  const char * pub_host_id;
  uint16_t     len;          /* length of subject */
  char         value[ 2 ];   /* the subject string */
};

struct RvDataLossQueue {
  kv::RouteVec<RvDataLossElem> tab;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  RvDataLossQueue() {}
  ~RvDataLossQueue() {
    this->tab.release();
  }
};

struct RvPubLoss {
  kv::EvSocket    * sock;
  RvDataLossQueue * loss_queue;
  uint32_t          pub_status[ 4 ],
                    pub_events;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  RvPubLoss(  kv::EvSocket *s ) {
    ::memset( (void *) this, 0, sizeof( *this ) );
    this->sock = s;
  }
  ~RvPubLoss() {
    if ( this->loss_queue != NULL )
      delete this->loss_queue;
  }
  void data_loss( RvHost &host,  kv::EvPublish &pub,
                  const char *pub_host_id ) noexcept;
};

struct RvLossArray : public kv::ArrayCount<RvPubLoss *, 16> {
  uint32_t pub_status[ 4 ],
           pub_events, refs;
  RvLossArray() : pub_events( 0 ), refs( 0 ) {
    for ( int i = 0; i < 4; i++ ) this->pub_status[ i ] = 0;
  }
  void remove_loss_entry( RvHost &host,  uint32_t fd ) noexcept;
  void print_events( RvHost &host ) noexcept;
};

struct RvHostStat {
  uint64_t time_ns, /* time published */
           ms, bs,  /* msgs sent, bytes sent */
           mr, br,  /* msgs recv, bytes recv */
           ps, pr,  /* pkts sent, pkts recv */
           rx, pm,  /* retrans, pkts missed */
           idl, odl;/* inbound dataloss, outbound dataloss */
  void zero( void ) {
    ::memset( (void *) this, 0, sizeof( *this ) );
  }
  void copy( const RvHostStat &cpy ) {
    ::memcpy( (void *) this, &cpy, sizeof( RvHostStat ) );
  }
};

struct RvDaemonSub {
  uint32_t hash,       /* hash of subject */
           refcnt;     /* daemon ref cnt */
  uint16_t len;        /* length of subject */
  char     value[ 2 ]; /* the subject string */
};
typedef kv::RouteVec<RvDaemonSub> RvDaemonMap;

struct RvHost : public kv::EvSocket {
  RvHostDB         & db;
  kv::RoutePublish & sub_route;
  RvDaemonMap        sub_map;

  char          host[ 256 ],       /* gethostname */
                session_ip[ 16 ],  /* address: 0A040416, sess_ip is quad fmt */
                daemon_id[ 64 ],   /* hexip.DAEMON.gob */
                network[ MAX_RV_NETWORK_LEN ], /* network string */
                service[ MAX_RV_SERVICE_LEN ], /* service string */
                sess_ip[ 4 * 4 ],  /* sess ip address (may be same as host_ip) */
                host_id[ MAX_RV_HOST_ID_LEN ];
  uint16_t      host_len,          /* len of above */
                daemon_len,        /* len of this->daomon_id[] */
                network_len,       /* len of this->network[] */
                service_len,       /* len of this->service[] */
                service_port,      /* service in network order */
                service_num,       /* service in host order */
                sess_ip_len,       /* len of sess_ip[] */
                host_id_len,       /* len of host_id[] */
                http_port;         /* http listen port (network order) */
  uint32_t      http_addr,         /* http tcp addr (network order) */
                host_ip;
  uint16_t      ipport;            /* daemon listen tcp port (network order) */
  bool          network_started,   /* if start_network() called and succeeded */
                daemon_subscribed, /* if _INBOX.DAEMON.ABCDEF is subscribed */
                start_in_progress, /* when network start is in progress */
                has_service_prefix;/* if _7500. service prefix is used */
  static const size_t session_ip_len = 8;
  RvHostStat    stat,
                previous_stat[ 2 ];
  uint64_t      host_status_count, /* count of host.stat msgs sent */
                start_stamp,       /* when service started */
                active_clients,    /* count of connections using this service */
                last_session_us,
                timer_id,
                host_status_timer,
                host_delay_timer,
                host_loss_timer;
  RvDaemonRpc * rpc;
  RvMcast       mcast;
  RvLossArray   loss_array;
  const char  * dataloss_outbound_sub,
              * dataloss_inbound_sub;
  size_t        dataloss_outbound_len,
                dataloss_inbound_len;
  uint32_t      dataloss_outbound_hash,
                dataloss_inbound_hash;

  void * operator new( size_t, void *ptr ) { return ptr; }
  RvHost( RvHostDB &d,  kv::EvPoll &poll,  kv::RoutePublish &sr,  
          const char *svc,  size_t svc_len,  uint16_t port,
          bool has_svc_pre ) noexcept;

  bool copy( const RvHostNet & hn ) {
    if ( (size_t) hn.service_len + 1 > sizeof( this->service ) ||
         (size_t) hn.network_len + 1 > sizeof( this->network ) )
      return false;
    ::memcpy( this->service, hn.service, this->service_len = hn.service_len );
    ::memcpy( this->network, hn.network, this->network_len = hn.network_len );
    this->service[ this->service_len ] = '\0';
    this->network[ this->network_len ] = '\0';
    return true;
  }
  int init_host( void ) noexcept;
  bool is_same_network( const RvHostNet &hn ) const {
    return ( (size_t) this->network_len == hn.network_len &&
             (size_t) this->service_len == hn.service_len &&
             ::memcmp( this->network, hn.network, hn.network_len ) == 0 &&
             ::memcmp( this->service, hn.service, hn.service_len ) == 0 );
  }
  uint16_t get_service( void ) {
    return (uint16_t) kv::string_to_uint64( this->service, this->service_len );
  }
  void zero_stats( uint64_t now ) {
    this->stat.zero();
    this->previous_stat[ 0 ].zero();
    this->previous_stat[ 1 ].zero();
    this->stat.time_ns = now;
    this->start_stamp  = now;
  }
  int check_network( const RvHostNet &hn ) noexcept;
  void start_daemon( void ) noexcept;
  int start_network( const RvMcast &mc,  const RvHostNet &hn ) noexcept;
  int copy_network( const RvMcast &mc,  const RvHostNet &hn ) noexcept;
  void send_host_start( EvRvService *svc ) noexcept;
  void send_session_start( EvRvService &svc ) noexcept;
  void send_session_start( kv::EvSocket &sock ) noexcept;
  void send_session_start( const char *user,  size_t user_len,
                           const char *session,  size_t session_len ) noexcept;
  void send_host_stop( EvRvService *svc ) noexcept;
  void send_session_stop( EvRvService &svc ) noexcept;
  void send_session_stop( kv::EvSocket &sock ) noexcept;
  void send_session_stop( const char *user,  size_t user_len,
                          const char *session,  size_t session_len ) noexcept;
  void send_listen_start( EvRvService &svc,  const char *sub,  size_t sublen,
                          const char *rep,  size_t replen,
                          uint32_t refcnt ) noexcept;
  void send_listen_start( const char *session,  size_t session_len,
                          const char *sub,  size_t sublen,
                          const char *rep,  size_t replen,
                          uint32_t refcnt ) noexcept;
  void send_listen_stop( EvRvService &svc,  const char *sub,  size_t sublen,
                         uint32_t refcnt ) noexcept;
  void send_listen_stop( const char *session,  size_t session_len,
                         const char *sub,  size_t sublen,
                         uint32_t refcnt ) noexcept;
  void inbound_data_loss( kv::EvSocket &dest,  kv::EvPublish &pub,
                          const char *pub_host_id ) noexcept;
  void clear_loss_entry( kv::EvSocket &dest ) noexcept;
  bool pub_inbound_data_loss( void ) noexcept;
  bool send_inbound_data_loss( RvPubLoss &loss ) noexcept;
  bool stop_network( void ) noexcept;
  static inline size_t time_to_str( uint64_t ns,  char *str ) {
    return utime_to_str( ns / 1000, str );
  }
  static size_t utime_to_str( uint64_t us,  char *str ) noexcept;
  size_t make_session( uint64_t ns,  char session[ MAX_SESSION_LEN ] ) noexcept;

  void send_host_status( void ) noexcept; /* send _RV.INFO.SYSTEM.HOST.STATUS */
  void send_outbound_data_loss( uint32_t msg_loss,  uint32_t pub_host,
                                const char *pub_host_id ) noexcept;
  void data_loss_error( uint64_t bytes_lost,  const char *err,
                        size_t errlen ) noexcept;
  /* start / stop network */
  int start_host2( uint32_t delay_secs ) noexcept;
  int start_host( void ) noexcept; /* send _RV.INFO.SYSTEM.HOST.START */
  int stop_host( void ) noexcept;  /* send _RV.INFO.SYSTEM.HOST.STOP */
  virtual bool timer_expire( uint64_t tid, uint64_t eid ) noexcept;
  virtual void process( void ) noexcept;
  virtual void write( void ) noexcept;
  virtual void read( void ) noexcept;
  virtual void release( void ) noexcept;
  void reassert_subs( void ) noexcept;
  uint32_t add_ref( const char *sub,  size_t sublen,  uint32_t h ) noexcept;
  uint32_t rem_ref( const char *sub,  size_t sublen,  uint32_t h,
                    uint32_t cnt ) noexcept;
};

struct DaemonInbox {
  char     buf[ MAX_RV_SERVICE_LEN + 2 + 22 + 2 ];
  uint32_t h;
  uint16_t len,
           svc_len;

  bool equals( const DaemonInbox &ibx ) const {
    return ibx.h == this->h && ibx.len == this->len &&
           ::memcmp( ibx.buf, this->buf, this->len ) == 0;
  }

  DaemonInbox( RvHost &host ) {
    uint16_t i = 0;
    if ( host.has_service_prefix ) {
      this->buf[ i++ ] = '_';
      ::memcpy( &this->buf[ i ], host.service, host.service_len );
      i += host.service_len;
      this->buf[ i++ ] = '.';
    }
    this->svc_len = i;
    ::memcpy( &this->buf[ i ], "_INBOX.", 7 );       i += 7;
    ::memcpy( &this->buf[ i ], host.session_ip, 8 ); i += 8;
    ::memcpy( &this->buf[ i ], ".DAEMON", 8 );       i += 7;
    this->len = i;
    this->h = kv_crc_c( this->buf, i, 0 );
  }
};

struct RvDaemonRpc : public kv::EvSocket {
  kv::RoutePublish & sub_route;
  DaemonInbox ibx;
  uint32_t host_refs;
  uint16_t svc;

  void * operator new( size_t, void *ptr ) { return ptr; }
  RvDaemonRpc( RvHost &h ) noexcept;

  int init_rpc( void ) noexcept;
  virtual void process( void ) noexcept;
  virtual void write( void ) noexcept;
  virtual void read( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_close( void ) noexcept;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept;
  virtual uint8_t is_subscribed( const kv::NotifySub &sub ) noexcept;

  void subscribe_daemon_inbox( void ) noexcept;
  void unsubscribe_daemon_inbox( void ) noexcept;
  void send_sessions( const void *reply,  size_t reply_len ) noexcept;
  void send_subscriptions( const char *session,  size_t session_len,
                           const void *reply,  size_t reply_len ) noexcept;
};

/* useful when literal field names with strlen arg:  SARG( "network" ) */
#define SARG( str ) str, sizeof( str )

}
}

#endif
