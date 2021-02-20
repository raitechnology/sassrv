#ifndef __rai_sassrv__rv_host_h__
#define __rai_sassrv__rv_host_h__

#include <raimd/rv_msg.h>

namespace rai {
namespace sassrv {

/* advisory fields that go into host status publishes */
enum RvAdv {
  ADV_HOSTADDR = 0x1,     ADV_BS       = 0x1000,
  ADV_SN       = 0x2,     ADV_MR       = 0x2000,
  ADV_OS       = 0x4,     ADV_BR       = 0x4000,
  ADV_VER      = 0x8,     ADV_PS       = 0x8000,
  ADV_HTTPADDR = 0x10,    ADV_PR       = 0x10000,
  ADV_HTTPPORT = 0x20,    ADV_RX       = 0x20000,
  ADV_TIME     = 0x40,    ADV_PM       = 0x40000,
  ADV_ID       = 0x80,    ADV_IDL      = 0x80000,
  ADV_USERID   = 0x100,   ADV_ODL      = 0x100000,
  ADV_RLI      = 0x200,   ADV_IPPORT   = 0x200000,
  ADV_UP       = 0x400,   ADV_SERVICE  = 0x400000,
  ADV_MS       = 0x800,   ADV_NETWORK  = 0x800000,

  ADV_HOST_START  = ADV_HOSTADDR | ADV_SN | ADV_OS | ADV_VER | ADV_HTTPADDR |
                    ADV_HTTPPORT | ADV_TIME | ADV_UP | ADV_RLI | ADV_IPPORT |
                    ADV_SERVICE | ADV_NETWORK,
  ADV_HOST_STATUS = ADV_HOSTADDR | ADV_SN | ADV_OS | ADV_VER | ADV_HTTPADDR |
                    ADV_HTTPPORT | ADV_TIME | ADV_UP | ADV_RLI | ADV_MS |
                    ADV_BS | ADV_BR | ADV_MR | ADV_PS | ADV_PR | ADV_RX |
                    ADV_PM | ADV_IDL | ADV_ODL | ADV_IPPORT | ADV_SERVICE |
                    ADV_NETWORK,
  ADV_HOST_STOP   = ADV_HOSTADDR | ADV_TIME,
  ADV_SESSION     = ADV_HOSTADDR | ADV_ID | ADV_USERID
};

enum RvHostError {
  HOST_OK                = 0,
  ERR_GETHOSTNAME_FAILED = 1, /* gethostname() failed */
  ERR_NO_INTERFACE_FOUND = 2, /* hostname addr does not match interface */
  ERR_HOSTNAME_NOT_FOUND = 3, /* hostname does not resolve */
  ERR_SAME_SVC_TWO_NETS  = 5, /* the same service with two different networks */
  ERR_NETWORK_NOT_FOUND  = 17,/* network not found */
  ERR_BAD_SERVICE_NUM    = 18,/* svc number bad */
  ERR_BAD_PARAMETERS     = 19,/* the length of network overflows MAX_NETWORK */
  ERR_START_HOST_FAILED  = 20 /* EvRvListener::host_start() failed */
};

/* eth0    ; 228.8.8.8,226.6.6.6 ; 224.4.4.4 */
/* host_ip   recv_ip[ 2 ],         send_ip  (all in network order) */
struct RvMcast {
  static const uint32_t MAX_RECV_MCAST = 60;
  uint32_t host_ip,                   /* eth0 */
           netmask,                   /* netmask for host_ip */
           send_ip,                   /* 224.4.4.4 */
           recv_ip[ MAX_RECV_MCAST ], /* 228.8.8.8,226.6.6.6 */
           recv_cnt;                  /* 2 */
  /* these are in network order */
  RvMcast() : host_ip( 0 ), netmask( 0 ), send_ip( 0 ), recv_cnt( 0 ) {}
  RvMcast( const RvMcast &mc ) { this->copy( mc ); }
  void copy( const RvMcast &mc ) {
    this->host_ip = mc.host_ip; this->netmask =  mc.netmask;
    this->send_ip = mc.send_ip; this->recv_cnt = mc.recv_cnt;
    ::memcpy( this->recv_ip, mc.recv_ip, sizeof( this->recv_ip[ 0 ] ) *
              mc.recv_cnt );
  }
  /* fill out above fields by parsing a rv network string:
   * "eth0;228.8.8.8,226.6.6.6;224.4.4.4", reeturn RvHostError if failed */
  RvHostError parse_network( const char *network ) noexcept;

  static bool is_empty_string( const char *s ) noexcept;
  /* return ip4 addr */
  static uint32_t lookup_host_ip4( const char *host ) noexcept;
  /* return ip4 addr, resolve netmask by checking device */
  static uint32_t lookup_host_ip4( const char *host,
                                   uint32_t &netmask ) noexcept;
  /* return ip4 addr, netmask by looking up device */
  static uint32_t lookup_dev_ip4( const char *dev,
                                  uint32_t &netmask ) noexcept;
  void print( void ) noexcept;
};

struct EvRvService;
struct EvRvListen;
/* host stats for the service */
struct RvHost {
  static const size_t MAX_NETWORK_LEN = 1680,
                      MAX_SERVICE_LEN = 32;
  EvRvListen & listener;
  char         host[ 256 ],       /* gethostname */
               session_ip[ 16 ],  /* ip address string 0A040416 */
               daemon_id[ 64 ],   /* hexip.DAEMON.gob */
               network[ MAX_NETWORK_LEN ], /* network string */
               service[ MAX_SERVICE_LEN ]; /* service string */
  uint16_t     host_len,          /* len of above */
               daemon_len,        /* len of this->daomon_id[] */
               network_len,       /* len of this->network[] */
               service_len,       /* len of this->service[] */
               service_port,      /* service in network order */
               ipport;            /* tcp listen port, network order */
  bool         network_started;   /* if start_network() called and succeeded */
  static const size_t session_ip_len = 8;
  uint64_t     ms, bs,            /* msgs sent, bytes sent */
               mr, br,            /* msgs recv, bytes recv */
               ps, pr,            /* pkts sent, pkts recv */
               rx, pm,            /* retrans, pkts missed */
               idl, odl,          /* inbound dataloss, outbound dataloss */
               host_status_count, /* count of host.stat msgs sent */
               start_stamp,       /* when service started */
               active_clients;    /* count of connections using this service */
  RvMcast      mcast;

  RvHost( EvRvListen &l ) : listener( l ) {
    ::memset( this->host, 0, (char *) (void *) &this->mcast - this->host );
  }
  void zero_stats( uint64_t now ) {
    ::memset( &this->ms, 0, (char *) (void *) &this->start_stamp -
                            (char *) (void *) &this->ms );
    this->start_stamp = now;
  }
  RvHostError start_network( const RvMcast &mc,  const char *net,
                             size_t net_len,  const char *svc,
                             size_t svc_len ) noexcept;
  void send_start( bool snd_host,  bool snd_sess,  EvRvService *svc ) noexcept;
  void send_stop( bool snd_host,  bool snd_sess,  EvRvService *svc ) noexcept;
  void stop_network( void ) noexcept;
  size_t pack_advisory( md::RvMsgWriter &msg,  const char *subj_prefix,
                        char *subj_buf,  int flags,
                        EvRvService *svc ) noexcept;
  static size_t time_to_str( uint64_t ns,  char *str ) noexcept;
};

/* useful when literal field names with strlen arg:  SARG( "network" ) */
#define SARG( str ) str, sizeof( str )

}
}

#endif
