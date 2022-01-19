#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <ctype.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include <linux/types.h>
#include <linux/ip.h>
#include <linux/if.h>
#include <raikv/util.h>
#include <raikv/ev_publish.h>
#include <sassrv/ev_rv.h>

using namespace rai;
using namespace sassrv;
using namespace kv;
using namespace md;

size_t
RvHost::time_to_str( uint64_t ns,  char *str ) noexcept
{
  uint64_t n;
  uint8_t  j, k;
  n = ns / 1000;
  for ( j = 0; ( n >> j ) != 0; j += 4 )
    ;
  for ( k = 0; j > 0; ) {
    j -= 4;
    str[ k++ ] = hexchar2( ( n >> j ) & 0xf );
  }
  str[ k ] = '\0';
  return k;
}

const char *
rai::sassrv::get_rv_host_error( int status ) noexcept
{
  switch ( status ) {
    case HOST_OK:
      return "host ok";
    case ERR_GETHOSTNAME_FAILED:
      return "gethostname failed";
    case ERR_NO_INTERFACE_FOUND:
      return "the hostname addr does not match interface";
    case ERR_HOSTNAME_NOT_FOUND:
      return "the hostname does not resolve";
    case ERR_SAME_SVC_TWO_NETS:
      return "the same service used with two different networks";
    case ERR_NETWORK_NOT_FOUND:
      return "network is not found";
    case ERR_BAD_SERVICE_NUM:
      return "the service number invalid";
    case ERR_BAD_PARAMETERS:
      return "the length of network overflows MAX_NETWORK";
    case ERR_START_HOST_FAILED:
      return "host start failed";
    default:
      return "host error";
  }
}

RvHostError
RvHost::start_network( const RvMcast &mc,  const char *net,  size_t net_len,
                       const char *svc,  size_t svc_len ) noexcept
{
  if ( this->network_started ) {
    if ( (size_t) this->network_len == net_len &&
         (size_t) this->service_len == svc_len &&
         ::memcmp( this->network, net, net_len ) == 0 &&
         ::memcmp( this->service, svc, svc_len ) == 0 )
      return HOST_OK;
    return ERR_SAME_SVC_TWO_NETS;
  }
  if ( net_len >= MAX_NETWORK_LEN || svc_len >= MAX_SERVICE_LEN )
    return ERR_BAD_PARAMETERS;

  ::memcpy( this->service, svc, svc_len );
  this->service[ svc_len ] = '\0';
  int n = atoi( this->service );
  if ( n <= 0 || n > 0xffff )
    return ERR_BAD_SERVICE_NUM;

  this->network_len = net_len;
  this->service_len = svc_len;
  this->service_port = htons( n );
  ::memcpy( this->network, net, net_len );
  this->network[ net_len ] = '\0';

  const uint8_t * q = (const uint8_t *) (const void *) &mc.host_ip;
  this->zero_stats( kv_current_realtime_ns() );
  this->mcast.copy( mc );
  for ( int i = 0; i < 8; i += 2 ) {
    this->session_ip[ i ] = hexchar2( ( q[ i/2 ] >> 4 ) & 0xf );
    this->session_ip[ i+1 ] = hexchar2( q[ i/2 ] & 0xf );
  }
  this->session_ip[ this->session_ip_len ] = '\0';
  ::memcpy( this->daemon_id, this->session_ip, this->session_ip_len );
  ::memcpy( &this->daemon_id[ this->session_ip_len ], ".DAEMON.", 8 );
  this->daemon_len = this->session_ip_len + 8;
  this->daemon_len += time_to_str( this->start_stamp,
                                   &this->daemon_id[ this->daemon_len ] );
  if ( this->listener.start_host() != 0 )
    return ERR_START_HOST_FAILED;
  this->network_started = true;
/*  printf( "start network: %.*s, \"%.*s\"\n",
          (int) svc_len, svc, (int) net_len, net );*/
  return HOST_OK;
}

void
RvHost::stop_network( void ) noexcept
{
/*  printf( "stop network:  %.*s, \"%.*s\"\n",
          (int) this->service_len, this->service, (int) this->network_len,
          this->network ); */
  this->listener.stop_host();
  this->network_started = false;
}

void
RvHost::send_start( bool snd_host,  bool snd_sess,  EvRvService *svc ) noexcept
{
  uint8_t     buf[ 8 * 1024 ];
  RvMsgWriter rvmsg( buf, sizeof( buf ) );
  char        subj[ 64 ];
  size_t      sublen, size;

  if ( snd_host ) {
    static const char start[] = "_RV.INFO.SYSTEM.HOST.START.";
    sublen = this->pack_advisory( rvmsg, start, subj, ADV_HOST_START, svc );
    size   = rvmsg.update_hdr();
    EvPublish pub( subj, sublen, NULL, 0, buf, size,
                   this->listener.sub_route, this->listener.fd,
                   kv_crc_c( subj, sublen, 0 ), RVMSG_TYPE_ID, 'p' );
    this->listener.sub_route.forward_msg( pub, NULL, 0, NULL );
    rvmsg.reset();
  }
  if ( snd_sess ) {
    static const char sess[] = "_RV.INFO.SYSTEM.SESSION.START.";
    sublen = this->pack_advisory( rvmsg, sess, subj, ADV_SESSION, svc );
    size = rvmsg.update_hdr();
    EvPublish pub( subj, sublen, NULL, 0, buf, size,
                   this->listener.sub_route, this->listener.fd,
                   kv_crc_c( subj, sublen, 0 ), RVMSG_TYPE_ID, 'p' );
    this->listener.sub_route.forward_msg( pub, NULL, 0, NULL );
  }
}

void
RvHost::send_stop( bool snd_host,  bool snd_sess,  EvRvService *svc ) noexcept
{
  uint8_t     buf[ 1024 ];
  char        subj[ 64 ];
  size_t      sublen,
              size;
  RvMsgWriter rvmsg( buf, sizeof( buf ) );

  if ( snd_sess ) {
    static const char sess[] = "_RV.INFO.SYSTEM.SESSION.STOP.";
    sublen = this->pack_advisory( rvmsg, sess, subj, ADV_SESSION, svc );
    size   = rvmsg.update_hdr();
    EvPublish pub( subj, sublen, NULL, 0, buf, size,
                   this->listener.sub_route, this->listener.fd,
                   kv_crc_c( subj, sublen, 0 ), RVMSG_TYPE_ID, 'p' );
    this->listener.sub_route.forward_msg( pub, NULL, 0, NULL );
    rvmsg.reset();
  }
  if ( snd_host ) {
    static const char stop[] = "_RV.INFO.SYSTEM.HOST.STOP.";
    sublen = this->pack_advisory( rvmsg, stop, subj, ADV_HOST_STOP, svc );
    size   = rvmsg.update_hdr();
    EvPublish pub( subj, sublen, NULL, 0, buf, size,
                   this->listener.sub_route, this->listener.fd,
                   kv_crc_c( subj, sublen, 0 ), RVMSG_TYPE_ID, 'p' );
    this->listener.sub_route.forward_msg( pub, NULL, 0, NULL );
  }
}

size_t
RvHost::pack_advisory( RvMsgWriter &msg,  const char *subj_prefix,
                       char *subj_buf,  int flags,
                       EvRvService *svc ) noexcept
{
  size_t sublen;

  ::strcpy( subj_buf, subj_prefix );
  sublen = ::strlen( subj_buf );
  ::memcpy( &subj_buf[ sublen ], this->session_ip, this->session_ip_len );
  sublen += this->session_ip_len;
  subj_buf[ sublen ] = '\0';

  /* _RV.<class>.SYSTEM */
  const char * s = ::strchr( &subj_prefix[ 4 ], '.' );
  size_t       class_len = (size_t) ( s - &subj_prefix[ 4 ] );
  char         class_str[ 8 ]; /* INFO, ERROR */
  if ( class_len < 8 ) {
    ::memcpy( class_str, &subj_prefix[ 4 ], class_len );
    class_str[ class_len ] = '\0';
  }
  else {
    ::strcpy( class_str, "UNKN" );
    class_len = 4;
  }

  msg.append_string( SARG( "ADV_CLASS" ), class_str, class_len + 1 );
  msg.append_string( SARG( "ADV_SOURCE" ), SARG( "SYSTEM" ) );
  /* skip prefix: _RV.INFO.SYSTEM. */
  msg.append_string( SARG( "ADV_NAME" ), &subj_buf[ 12 + class_len ],
                     sublen - ( 12 + class_len ) + 1 );

  if ( ( flags & ADV_HOSTADDR ) != 0 )
    msg.append_ipdata( SARG( "hostaddr" ), this->mcast.host_ip );
  if ( ( flags & ADV_SN ) != 0 )
    msg.append_uint( SARG( "sn" ), (uint32_t) 1 );
  if ( ( flags & ADV_OS ) != 0 )
    msg.append_uint( SARG( "os" ), (uint8_t) 1 );
  if ( ( flags & ADV_VER ) != 0 )
    msg.append_string( SARG( "ver" ), SARG( "5.4.2" ) );
  /* no daemon http port */
#if 0
  if ( ( flags & ADV_HTTPADDR ) != 0 ) {
  }
  if ( ( flags & ADV_HTTPPORT ) != 0 ) {
  }
#endif
  uint64_t now = kv_current_realtime_ns();
  if ( ( flags & ADV_TIME ) != 0 ) {
    uint64_t s = now / 1000000000,
             m = ( now / 1000000 ) % 1000;
    s  = ( s << 32 ) | m;
    msg.append_type( SARG( "time" ), s, MD_DATETIME );
  }
  if ( ( flags & ADV_ID ) != 0 && svc != NULL )
    msg.append_string( SARG( "id" ), svc->session, svc->session_len + 1 );
  if ( ( flags & ADV_USERID ) != 0 && svc != NULL )
    msg.append_string( SARG( "userid" ), svc->userid, svc->userid_len + 1 );
  if ( ( flags & ADV_UP ) != 0 ) {
    uint64_t s  = ( now - this->start_stamp ) / 1000000000;
    msg.append_uint( SARG( "up" ), (uint32_t) s );
  }
  if ( ( flags & ADV_MS ) != 0 )
    msg.append_uint( SARG( "ms" ), (uint64_t) this->ms );
  if ( ( flags & ADV_BS ) != 0 )
    msg.append_uint( SARG( "bs" ), (uint64_t) this->bs );
  if ( ( flags & ADV_MR ) != 0 )
    msg.append_uint( SARG( "mr" ), (uint64_t) this->mr );
  if ( ( flags & ADV_BR ) != 0 )
    msg.append_uint( SARG( "br" ), (uint64_t) this->br );
  if ( ( flags & ADV_PS ) != 0 )
    msg.append_uint( SARG( "ps" ), (uint64_t) this->ps );
  if ( ( flags & ADV_PR ) != 0 )
    msg.append_uint( SARG( "pr" ), (uint64_t) this->pr );
  if ( ( flags & ADV_RX ) != 0 )
    msg.append_uint( SARG( "rx" ), (uint64_t) this->rx );
  if ( ( flags & ADV_PM ) != 0 )
    msg.append_uint( SARG( "pm" ), (uint64_t) this->pm );
  if ( ( flags & ADV_IDL ) != 0 )
    msg.append_uint( SARG( "idl" ), (uint64_t) this->idl );
  if ( ( flags & ADV_ODL ) != 0 )
    msg.append_uint( SARG( "odl" ), (uint64_t) this->odl );
  if ( ( flags & ADV_IPPORT ) != 0 )
    msg.append_ipdata( SARG( "ipport" ), this->ipport );
  if ( ( flags & ADV_SERVICE ) != 0 )
    msg.append_string( SARG( "service" ), this->service, this->service_len + 1);
  if ( ( flags & ADV_NETWORK ) != 0 )
    msg.append_string( SARG( "network" ), this->network, this->network_len + 1);
  return sublen;
}

bool
RvMcast::is_empty_string( const char *s ) noexcept
{
  if ( s != NULL )
    for ( ; *s != '\0' && isspace( *s ); s++ )
      ;
  return ( s == NULL || s[ 0 ] == '\0' );
}

uint32_t
RvMcast::lookup_host_ip4( const char *host ) noexcept
{
  struct addrinfo * h = NULL,
                  * res;
  uint32_t ipaddr = 0;
  if ( ::getaddrinfo( host, NULL, NULL, &h ) == 0 ) {
    for ( res = h; res != NULL; res = res->ai_next ) {
      if ( res->ai_family == AF_INET &&
           res->ai_addrlen >= sizeof( struct sockaddr_in ) ) {
        struct sockaddr_in *in = (struct sockaddr_in *) res->ai_addr;
        ::memcpy( &ipaddr, &in->sin_addr.s_addr, 4 );
        break;
      }
    }
    ::freeaddrinfo( h );
  }
  return ipaddr;
}

uint32_t
RvMcast::lookup_host_ip4( const char *host,  uint32_t &netmask ) noexcept
{
  uint32_t ipaddr;
  netmask = 0;
  if ( (ipaddr = lookup_host_ip4( host )) == 0 )
    return 0;

  ifconf conf;
  ifreq  ifbuf[ 256 ],
       * ifp, ifa, ifm;
  int    s  = ::socket( PF_INET, SOCK_DGRAM, IPPROTO_UDP );

  ::memset( ifbuf, 0, sizeof( ifbuf ) );
  ::memset( &conf, 0, sizeof( conf ) );

  conf.ifc_len = sizeof( ifbuf );
  conf.ifc_buf = (char *) ifbuf;

  if ( ::ioctl( s, SIOCGIFCONF, &conf ) != -1 ) {
    ifp = ifbuf;
    /* for each interface */
    for ( ; (uint8_t *) ifp < &((uint8_t *) ifbuf)[ conf.ifc_len ]; ifp++ ) {
      ::strcpy( ifa.ifr_name, ifp->ifr_name );
      ::strcpy( ifm.ifr_name, ifp->ifr_name );

      /* fetch flags check if multicast exists, get address and netmask */
      if ( ::ioctl( s, SIOCGIFADDR, &ifa )    >= 0 &&
           ifa.ifr_addr.sa_family             == AF_INET &&
           ::ioctl( s, SIOCGIFNETMASK, &ifm ) >= 0 ) {
        uint32_t mask, addr;
        mask = ((struct sockaddr_in &) ifm.ifr_netmask).sin_addr.s_addr;
        addr = ((struct sockaddr_in &) ifa.ifr_addr).sin_addr.s_addr;

        if ( ( addr & mask ) == ( ipaddr & mask ) ) {
          netmask = mask;
          ipaddr  = addr;
          goto found_address;
        }
      }
    }
  }
  ::close( s );
  return 0;
found_address:;
  ::close( s );
  return ipaddr;
}

uint32_t
RvMcast::lookup_dev_ip4( const char *dev,  uint32_t &netmask ) noexcept
{
  ifreq    ifa, ifm;
  uint32_t ipaddr = 0;
  int      s  = ::socket( PF_INET, SOCK_DGRAM, IPPROTO_UDP );
  netmask = 0;
  if ( ::strlen( dev ) < sizeof( ifa.ifr_name ) ) {
    ::strcpy( ifa.ifr_name, dev );
    ::strcpy( ifm.ifr_name, dev );
    if ( ::ioctl( s, SIOCGIFADDR, &ifa ) >= 0 &&
         ifa.ifr_addr.sa_family          == AF_INET ) {
      ipaddr = ((struct sockaddr_in &) ifa.ifr_addr).sin_addr.s_addr;
      if ( ::ioctl( s, SIOCGIFNETMASK, &ifm ) >= 0 )
        netmask = ((struct sockaddr_in &) ifm.ifr_addr).sin_addr.s_addr;
    }
  }
  ::close( s );
  return ipaddr;
}

RvHostError
RvMcast::parse_network( const char *network ) noexcept
{
  char tmp_buf[ 4 * 1024 ],
       recv_host[ 16 ],
       host[ 256 ],
     * recv_part[ MAX_RECV_MCAST ],
     * send_part,
     * net_part,
     * ptr;
  RvHostError status = HOST_OK;
  ::memset( this, 0, sizeof( *this ) );

  /* network is format: network;mcast,mcast;sendmcast:port,port */
  send_part = NULL;
  ::strncpy( tmp_buf, network ? network : "", sizeof( tmp_buf ) );
  tmp_buf[ sizeof( tmp_buf ) - 1 ] = '\0';
  net_part = tmp_buf;
  while ( isspace( *net_part ) )
    net_part++;

  /* find the recv mcast addresses, after the semicolon on network spec */
  if ( (ptr = ::strchr( net_part, ';' )) != NULL ) {
    *ptr++ = '\0';
    recv_part[ 0 ] = ptr;
    this->recv_cnt = 1;
    /* find the send mcast address, after the semicolon on the recv spec */
    if ( (ptr = ::strchr( ptr, ';' )) != NULL ) {
      *ptr++ = '\0';
      send_part = ptr;
    }
    /* parse list of mcast addrs */
    for ( ptr = recv_part[ 0 ]; ; ) {
      if ( (ptr = ::strchr( ptr, ',' )) == NULL )
        break;
      *ptr++ = '\0';
      recv_part[ this->recv_cnt++ ] = ptr;
      if ( this->recv_cnt == MAX_RECV_MCAST )
        break;
    }
  }
  /* if no recv address, use zeros so that net parser doesn't error */
  if ( this->recv_cnt == 0 || ( this->recv_cnt == 1 &&
                                is_empty_string( recv_part[ 0 ] ) ) ) {
    ::strncpy( recv_host, "0.0.0.0", sizeof( recv_host ) );
    recv_part[ 0 ] = recv_host;
    this->recv_cnt = 1;
  }
  if ( is_empty_string( send_part ) )
    send_part = recv_part[ 0 ];

  /* lookup the hosts */
  this->send_ip = this->lookup_host_ip4( send_part );
  if ( this->send_ip == 0 && ::strcmp( send_part, "0.0.0.0" ) != 0 )
    status = ERR_NETWORK_NOT_FOUND;
  for ( uint32_t i = 0; i < this->recv_cnt; i++ ) {
    this->recv_ip[ i ] = this->lookup_host_ip4( recv_part[ i ] );
    if ( this->recv_ip[ i ] == 0 && ::strcmp( recv_part[ i ], "0.0.0.0" ) != 0 )
      status = ERR_NETWORK_NOT_FOUND;
  }
  if ( is_empty_string( net_part ) ) {
    if ( ::gethostname( host, sizeof( host ) ) != 0 ) {
      host[ 0 ] = '\0';
      status = ERR_GETHOSTNAME_FAILED;
    }
    net_part = host;
  }
  if ( ! is_empty_string( net_part ) ) {
    this->host_ip = this->lookup_host_ip4( net_part, this->netmask );
    if ( this->host_ip == 0 )
      this->host_ip = this->lookup_dev_ip4( net_part, this->netmask );
    if ( this->host_ip == 0 )
      status = ERR_NO_INTERFACE_FOUND;
  }
  return status;
}

void
RvMcast::print( void ) noexcept
{
  uint8_t *q;
  if ( this->host_ip != 0 ) {
    q = (uint8_t *) &this->host_ip;
    printf( "%u.%u.%u.%u", q[ 0 ], q[ 1 ], q[ 2 ], q[ 3 ] );
  }
  if ( this->recv_cnt != 1 || this->recv_ip[ 0 ] != this->send_ip ) {
    q = (uint8_t *) &this->recv_ip[ 0 ];
    printf( ";%u.%u.%u.%u", q[ 0 ], q[ 1 ], q[ 2 ], q[ 3 ] );
    for ( uint32_t i = 1; i < this->recv_cnt; i++ ) {
      q = (uint8_t *) &this->recv_ip[ i ];
      printf( ",%u.%u.%u.%u", q[ 0 ], q[ 1 ], q[ 2 ], q[ 3 ] );
    }
  }
  if ( this->send_ip != 0 ) {
    q = (uint8_t *) &this->send_ip;
    printf( ";%u.%u.%u.%u\n", q[ 0 ], q[ 1 ], q[ 2 ], q[ 3 ] );
  }
  else {
    printf( "\n" );
  }
}

