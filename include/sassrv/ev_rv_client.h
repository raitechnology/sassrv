#ifndef __rai_sassrv__ev_rv_client_h__
#define __rai_sassrv__ev_rv_client_h__

#include <sassrv/ev_rv.h>

namespace rai {
namespace sassrv {

struct EvRvClientParameters {
  const char * daemon,
             * network,
             * service;
  int          port,
               opts;
  EvRvClientParameters( const char *d = NULL,  const char *n = NULL,
                        const char *s = "7500",  int p = 7500,
                        int o = kv::DEFAULT_TCP_CONNECT_OPTS )
    : daemon( d ), network( n ), service( s ), port( p ), opts( o ) {}
};

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
  RvMsgIn      msg_in;         /* current message recvd */
  RvState      rv_state;       /* one of the above states */
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
  const char * network,
             * service;

  EvRvClient( kv::EvPoll &p ) noexcept;

  /* connect to a NATS server */
  bool connect( EvRvClientParameters &p,
                kv::EvConnectionNotify *n = NULL ) noexcept;
  bool is_connected( void ) const {
    return this->EvSocket::fd != -1;
  }
  /* restart the protocol parser */
  void initialize_state( void ) {
    this->rv_state    = VERS_RECV;
    this->session_len = 0;
    this->control_len = 0;
    this->userid_len  = 0;
    this->gob_len     = 0;
    this->vmaj        = 5;
    this->vmaj        = 4;
    this->vupd        = 2;
    this->ipport      = 0;
    this->ipaddr      = 0;
    this->network     = NULL;
    this->service     = NULL;
    this->notify      = NULL;
  }
  static void trace_msg( char dir,  void *msg, size_t msglen ) noexcept;
  void send_vers( void ) noexcept;
  void send_info( void ) noexcept;
  void send_init_rec( void ) noexcept;
  int recv_info( void ) noexcept;
  int recv_conn( void ) noexcept;
  int dispatch_msg( void *msgbuf, size_t msglen ) noexcept;
  bool fwd_pub( void ) noexcept;

  virtual void process( void ) noexcept;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept;
  virtual void release( void ) noexcept;
    /* a new subscription */
  void do_sub( const char *sub,  size_t sublen,
               const char *rep,  size_t replen ) noexcept;
  virtual void on_sub( uint32_t h,  const char *sub,  size_t sublen,
                       uint32_t src_fd,  uint32_t rcnt,  char src_type,
                       const char *rep,  size_t rlen ) noexcept;
  /* an unsubscribed sub */
  virtual void on_unsub( uint32_t h,  const char *sub,  size_t sublen,
                         uint32_t src_fd,  uint32_t rcnt,
                         char src_type ) noexcept;
  /* a new pattern subscription */
  void do_psub( const char *prefix,  uint8_t prefix_len ) noexcept;
  virtual void on_psub( uint32_t h,  const char *pattern,  size_t patlen,
                        const char *prefix,  uint8_t prefix_len,
                        uint32_t src_fd,  uint32_t rcnt,
                        char src_type ) noexcept;
  /* an unsubscribed pattern sub */
  virtual void on_punsub( uint32_t h,  const char *pattern,  size_t patlen,
                          const char *prefix,  uint8_t prefix_len,
                          uint32_t src_fd,  uint32_t rcnt,
                          char src_type ) noexcept;
  /* reassert subs after reconnect */
  virtual void on_reassert( uint32_t fd,  kv::RouteVec<kv::RouteSub> &sub_db,
                            kv::RouteVec<kv::RouteSub> &pat_db ) noexcept;
};

}
}
#endif
