#ifndef __rai_sassrv__ev_rv_client_h__
#define __rai_sassrv__ev_rv_client_h__

#include <sassrv/ev_rv.h>

namespace rai {
namespace trdp {
  struct TrdpSvc;
}
namespace sassrv {

struct EvRvClientParameters {
  const char * daemon,
             * network,
             * service,
             * userid;
  int          port,
               opts;
  EvRvClientParameters( const char *d = NULL,  const char *n = NULL,
                        const char *s = "7500",  const char *u = NULL,
                        int p = 7500,  int o = kv::DEFAULT_TCP_CONNECT_OPTS )
    : daemon( d ), network( n ), service( s ),
      userid( u ), port( p ), opts( o ) {}
};

struct EvRvClient;
struct RvClientCB {
  RvClientCB() {}
  virtual bool on_rv_msg( kv::EvPublish &pub ) noexcept;
};

static const size_t MAX_RV_INBOX_LEN = 88; /* _INBOX.<session>.<number> */

struct EvRvClient : public kv::EvConnection, public kv::RouteNotify {
  void * operator new( size_t, void *ptr ) { return ptr; }
  enum RvState {
    ERR_CLOSE,
    VERS_RECV, /* handshake version */
    INFO_RECV, /* handshake info */
    INIT_RECV, /* initialize network parameters */
    CONN_RECV, /* recv connected or refused */
    DATA_RECV  /* normal protocol processing when state == DATA_RECV */
  };
  kv::RoutePublish & sub_route;
  RvMsgIn      msg_in;         /* current message recvd */
  RvClientCB * cb;
  RvState      rv_state;       /* one of the above states */
  bool         no_write;
  char         session[ 64 ],  /* session id of this connection */
               control[ 64 ],  /* the inbox name */
               userid[ 64 ],   /* the userid */
               gob[ 16 ];      /* deamon generated session id */
  uint64_t     start_stamp;    /* ns time of session creation */
  uint8_t      fwd_all_msgs,   /* send publishes */
               fwd_all_subs;   /* send subscriptons */
  uint16_t     session_len,    /* lengths for the above */
               control_len,
               userid_len,
               gob_len,
               vmaj,           /* vhat version of client is connected */
               vmin,
               vupd,
               ipport;
  uint32_t     ipaddr;
  const char * daemon,
             * network,
             * service;
  void       * save_buf,
             * param_buf;
  size_t       save_len;
  md::MDMsgMem spc;
  trdp::TrdpSvc * svc;

  EvRvClient( kv::EvPoll &p ) noexcept;

  /* connect to a NATS server */
  bool connect( EvRvClientParameters &p,
                kv::EvConnectionNotify *n = NULL,
                RvClientCB *c = NULL ) noexcept;
  bool is_connected( void ) const {
    return this->EvSocket::fd != -1;
  }
  /* restart the protocol parser */
  void initialize_state( bool is_null ) noexcept;
  uint16_t make_inbox( char *inbox, uint32_t num ) noexcept;
  uint64_t is_inbox( const char *sub,  size_t sub_len ) {
    size_t off = this->control_len - 1;
    if ( off >= sub_len || sub[ off ] < '0' || sub[ off ] > '9' ||
         ::memcmp( sub, this->control, off ) != 0 )
      return 0;
    return kv::string_to_uint64( &sub[ off ], sub_len - off );
  }
  static void trace_msg( char dir,  void *msg, size_t msglen ) noexcept;
  void send_vers( void ) noexcept;
  void send_info( void ) noexcept;
  void send_init_rec( void ) noexcept;
  int recv_info( void ) noexcept;
  int recv_conn( void ) noexcept;
  int dispatch_msg( void *msgbuf, size_t msglen ) noexcept;
  bool fwd_pub( void ) noexcept;
  bool queue_send( const void *buf,  size_t buflen,  const void *msg = NULL,
                   size_t msglen = 0 ) noexcept;
  void flush_pending_send( void ) noexcept;
  bool publish( kv::EvPublish &pub ) noexcept;
  md::RvMsg *make_rv_msg( void *msg,  size_t msg_len,
                          uint32_t msg_enc ) noexcept;
  virtual void process( void ) noexcept;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept;
  virtual void process_close( void ) noexcept;
  virtual void release( void ) noexcept;
    /* a new subscription */
  void subscribe( const char *sub,  size_t sublen,
                  const char *rep = NULL,  size_t replen = 0 ) noexcept;
  void subscribe( const char *sub ) {
    this->subscribe( sub, ::strlen( sub ) );
  }
  void unsubscribe( const char *sub,  size_t sublen ) noexcept;
  void unsubscribe( const char *sub ) {
    this->unsubscribe( sub, ::strlen( sub ) );
  }
  virtual void on_sub( kv::NotifySub &sub ) noexcept;
  /* an unsubscribed sub */
  virtual void on_unsub( kv::NotifySub &sub ) noexcept;
  /* a new pattern subscription */
  void do_psub( const char *prefix,  uint8_t prefix_len ) noexcept;
  virtual void on_psub( kv::NotifyPattern &pat ) noexcept;
  /* an unsubscribed pattern sub */
  virtual void on_punsub( kv::NotifyPattern &pat ) noexcept;
  /* reassert subs after reconnect */
  virtual void on_reassert( uint32_t fd,  kv::RouteVec<kv::RouteSub> &sub_db,
                            kv::RouteVec<kv::RouteSub> &pat_db ) noexcept;
};

static inline bool
match_rv_wildcard( const char *wild,  size_t wild_len,
                   const char *sub,  size_t sub_len ) noexcept
{
  const char * w   = wild,
             * end = &wild[ wild_len ];
  size_t       k   = 0;

  for (;;) {
    if ( k == sub_len || w == end ) {
      if ( k == sub_len && w == end )
        return true;
      return false; /* no match */
    }
    if ( *w == '>' &&
         ( ( w == wild || *(w-1) == '.' ) && w+1 == end ) )
      return true;
    else if ( *w == '*' &&
              ( ( w   == wild || *(w-1) == '.' ) && /* * || *. || .* || .*. */
                ( w+1 == end  || *(w+1) == '.' ) ) ) {
      for (;;) {
        if ( k == sub_len || sub[ k ] == '.' )
          break;
        k++;
      }
      w++;
      continue;
    }
    if ( *w != sub[ k ] )
      return false; /* no match */
    w++;
    k++;
  }
}

static inline const char *
is_rv_wildcard( const char *wild,  size_t wild_len ) noexcept
{
  const char * w   = wild,
             * end = &wild[ wild_len ];

  for ( ; ; w++ ) {
    if ( w == end )
      return NULL;
    if ( *w == '>' &&
         ( ( w == wild || *(w-1) == '.' ) && w+1 == end ) )
      return w;
    else if ( *w == '*' &&
              ( ( w   == wild || *(w-1) == '.' ) && /* * || *. || .* || .*. */
                ( w+1 == end  || *(w+1) == '.' ) ) ) {
      return w;
    }
  }
}

}
}
#endif
