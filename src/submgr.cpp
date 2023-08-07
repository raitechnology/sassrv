#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <netinet/in.h>
#include <sassrv/ev_rv_client.h>
#include <sassrv/submgr.h>
#include <raimd/md_msg.h>
#include <raikv/ev_publish.h>

using namespace rai;
using namespace kv;
using namespace sassrv;
using namespace md;

int rai::sassrv::subdb_debug;
#define is_subdb_debug kv_unlikely( subdb_debug != 0 )
static const uint64_t NS = 1000 * 1000 * 1000;

namespace {
static const char host_status[]   = _RV_INFO_HOST_STATUS ".>",
                  host_start[]    = _RV_INFO_HOST_START ".>",
                  host_stop[]     = _RV_INFO_HOST_STOP ".>",
                  session_start[] = _RV_INFO_SESSION_START ".>",
                  session_stop[]  = _RV_INFO_SESSION_STOP ".>",
                  listen_start[]  = _RV_INFO_LISTEN_START ".>",
                  listen_stop[]   = _RV_INFO_LISTEN_STOP ".>",
                  snap[]          = "_SNAP.>";
enum SubKind {
  IS_HOST_STATUS   = 0,
  IS_HOST_START    = 1,
  IS_HOST_STOP     = 2,
  IS_SESSION_START = 3,
  IS_SESSION_STOP  = 4,
  IS_LISTEN_START  = 5,
  IS_LISTEN_STOP   = 6,
  IS_SNAP          = 7,
  MAX_SUB_KIND     = 8
};

struct SubMatch {
  const char * sub;
  uint32_t     len;
  SubKind      kind;
};

static const SubMatch rv_info_sub[ MAX_SUB_KIND ] = {
{ host_status  , sizeof( host_status ) - 1  , IS_HOST_STATUS },
{ host_start   , sizeof( host_start ) - 1   , IS_HOST_START },
{ host_stop    , sizeof( host_stop ) - 1    , IS_HOST_STOP },
{ session_start, sizeof( session_start ) - 1, IS_SESSION_START },
{ session_stop , sizeof( session_stop ) - 1 , IS_SESSION_STOP },
{ listen_start , sizeof( listen_start ) - 1 , IS_LISTEN_START },
{ listen_stop  , sizeof( listen_stop ) - 1  , IS_LISTEN_STOP },
{ snap         , sizeof( snap ) - 1         , IS_SNAP }
};

static const SubMatch *
match_rv_info( const char *sub,  uint32_t sub_len ) noexcept
{
  const SubMatch * m = rv_info_sub;
  for ( int i = 0; i < MAX_SUB_KIND; i++ ) {
    if ( sub_len > m->len ) {
      for ( uint32_t len = m->len - 2; ; len-- ) {
        if ( sub[ len ] != m->sub[ len ] )
          break;
        if ( len == 0 )
          return m;
      }
    }
    m++;
  }
  return NULL;
}
static char * hms( time_t t,  char *buf ) noexcept {
  struct tm tm;
  localtime_r( &t, &tm );
  ::snprintf( buf, 32, "%02d:%02d:%02d (%u)",
              tm.tm_hour, tm.tm_min, tm.tm_sec, (uint32_t) ( t % 3600 ) );
  return buf;
}

static const char *host_state_str[] = {
  "unknown", "start", "status", "query", "stop"
};
static const char *session_state_str[] = {
  "unknown", "rv5", "query", "stop", "rv7", "self"
};
#if __cplusplus >= 201103L
static_assert( MAX_HOST_STATE == ( sizeof( host_state_str ) / sizeof( host_state_str[ 0 ] ) ), "host_state_str" );
static_assert( MAX_SESSION_STATE == ( sizeof( session_state_str ) / sizeof( session_state_str[ 0 ] ) ), "session_state_str" );
#endif
}
const char *rai::sassrv::get_host_state_str( HostState s ) noexcept {
  return s < MAX_HOST_STATE ? host_state_str[ s ] : "bad host state"; }
const char *rai::sassrv::get_session_state_str( SessionState s ) noexcept {
  return s < MAX_SESSION_STATE ? session_state_str[ s ] : "bad session state"; }

static inline uint32_t hexval( char c ) {
  return ( c >= '0' && c <= '9' ) ? ( c - '0' ) :
         ( c >= 'A' && c <= 'F' ) ? ( c - 'A' + 0xa ) :
         ( c >= 'a' && c <= 'f' ) ? ( c - 'a' + 0xa ) : 0;
}
static inline uint32_t string_to_host_id( const char *str ) {
  uint32_t id = 0;
  for ( int i = 0, j = 8; i < 8; i++ )
    id |= hexval( str[ i ] ) << ( --j * 4 );
  return id;
}
static inline char * host_id_to_string( uint32_t host_id,  char *str ) {
  for ( int i = 8; i > 0; ) {
    str[ --i ] = hexchar2( host_id & 0xf );
    host_id >>= 4;
  }
  str[ 8 ] = '\0';
  return str;
}

SubscriptionDB::SubscriptionDB( EvRvClient &c,
                                SubscriptionListener *sl ) noexcept
              : client( c ), cb( sl ), host_ht( 0 ), sess_ht( 0 ),
                cur_mono( 0 ), next_session_id( 0 ), next_subject_id( 0 ),
                soft_host_query( 0 ), first_free_host( 0 ), next_gc( 0 ),
                is_subscribed( false ), is_all_subscribed( false ), mout( 0 )
{
  this->host_ht = kv::UIntHashTab::resize( NULL );
  this->sess_ht = kv::UIntHashTab::resize( NULL );
}

void SubscriptionListener::on_listen_start( StartListener & ) noexcept {}
void SubscriptionListener::on_listen_stop ( StopListener  & ) noexcept {}
void SubscriptionListener::on_snapshot    ( SnapListener  & ) noexcept {}

void
SubscriptionDB::add_wildcard( const char *wildcard ) noexcept
{
  size_t len = ::strlen( wildcard );
  char * s   = (char *) ::malloc( len + 1 );
  Filter &f = this->filters.push();
  ::memcpy( s, wildcard, len + 1 );
  f.wild     = s;
  f.wild_len = len;
}

static bool
match_rv_wildcard( const char *wild,  size_t wild_len,
                   const char *sub,  size_t sub_len )
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

bool
SubscriptionDB::is_matched( const char *sub,  size_t sub_len ) noexcept
{
  if ( this->filters.count == 0 )
    return true;
  for ( size_t i = 0; i < this->filters.count; i++ ) {
    if ( match_rv_wildcard( this->filters[ i ].wild,
                            this->filters[ i ].wild_len,
                            sub, sub_len ) )
      return true;
  }
  return false;
}

void
SubscriptionDB::start_subscriptions( bool all ) noexcept
{
  if ( this->is_subscribed )
    return;
  this->is_subscribed = true;
  this->is_all_subscribed = all;
  this->cur_mono = this->client.poll.mono_ns / NS;
  this->next_gc  = this->cur_mono + 131;
  this->do_subscriptions( true );
  Host &host = this->host_ref( ntohl( this->client.ipaddr ), true );
  host.state = RV_HOST_QUERY;
  Session &session = this->session_start( host.host_id, this->client.session,
                                          this->client.session_len );
  session.state = RV_SESSION_SELF;
}

void
SubscriptionDB::stop_subscriptions( void ) noexcept
{
  if ( ! this->is_subscribed )
    return;
  this->is_subscribed = false;
  this->do_subscriptions( false );
}

void
SubscriptionDB::do_subscriptions( bool is_subscribe ) noexcept
{
  const char * un = ( is_subscribe ? "" : "un" );
  for ( int i = 0; i < MAX_SUB_KIND; i++ ) {
    const SubMatch & match = rv_info_sub[ i ];

    if ( ! this->is_all_subscribed &&
         ( i == IS_LISTEN_START || i == IS_LISTEN_STOP || i == IS_SNAP ) ) {
      for ( size_t j = 0; j < this->filters.count; j++ )
        this->do_wild_subscription( this->filters.ptr[ j ], is_subscribe, i );
    }
    else {
      if ( this->mout != NULL )
        this->mout->printf( "%ssubscribe (%.*s)\n", un, match.len, match.sub );
      if ( is_subscribe )
        this->client.subscribe( match.sub, match.len );
      else
        this->client.unsubscribe( match.sub, match.len );
    }
  }
}

void
SubscriptionDB::do_wild_subscription( Filter &f,  bool is_subscribe,
                                      int k ) noexcept
{
  const char * un = ( is_subscribe ? "" : "un" );
  const SubMatch & match = rv_info_sub[ k ];
  size_t           len   = f.wild_len + match.len;
  CatMalloc        sub( len );

  sub.b( match.sub, match.len - 1 ) /* strip '>' */
     .b( f.wild, f.wild_len ).end();

  if ( this->mout != NULL )
    this->mout->printf( "%ssubscribe (%s)\n", un, sub.start );

  if ( is_subscribe )
    this->client.subscribe( sub.start, sub.len() );
  else
    this->client.unsubscribe( sub.start, sub.len() );
}

Host &
SubscriptionDB::host_ref( uint32_t host_id,  bool is_status ) noexcept
{
  uint32_t i;
  size_t   pos;
  if ( this->mout != NULL )
    this->mout->printf( "> host %s %08X %s\n",
       is_status ? "status" : "ref", host_id,
       this->host_ht->find( host_id, pos, i ) ? "exists" : "new" );

  if ( ! this->host_ht->find( host_id, pos, i ) ) {
    for (;;) {
      if ( this->first_free_host == this->host_tab.count ||
           this->host_tab.ptr[ this->first_free_host ].state == RV_HOST_STOP )
        break;
    }
    i = this->first_free_host++;
    this->hosts.active++;
    this->host_ht->set_rsz( this->host_ht, host_id, pos, i );
    Host & host = ( i < this->host_tab.count ? this->host_tab.ptr[ i ] :
                    this->host_tab.push() );
    host.start( host_id, this->cur_mono, false, is_status );
    if ( this->mout != NULL ) {
      if ( host.state == RV_HOST_QUERY )
        this->mout->printf( "! host %08X query, no start\n", host_id );
    }
    return host;
  }

  Host    & host = this->host_tab.ptr[ i ];
  HostState old_state = host.state;
  if ( host.state == RV_HOST_STOP ) {
    this->hosts.active++;
    host.start( host_id, this->cur_mono, false, is_status );
  }
  else if ( is_status )
    host.status( this->cur_mono );
  else
    host.ref( this->cur_mono );
  if ( this->mout != NULL ) {
    if ( old_state != host.state && host.state == RV_HOST_QUERY )
      this->mout->printf( "! host %08X query, no start\n", host_id );
  }
  return host;
}

void
SubscriptionDB::host_start( uint32_t host_id ) noexcept
{
  uint32_t i;
  size_t   pos;
  if ( this->mout != NULL )
    this->mout->printf( "> host start %08X %s\n", host_id,
       this->host_ht->find( host_id, pos, i ) ? "exists" : "new" );

  if ( ! this->host_ht->find( host_id, pos, i ) ) {
    i = this->host_tab.count;
    this->host_ht->set_rsz( this->host_ht, host_id, pos, i );
    Host & host = this->host_tab.push();
    host.start( host_id, this->cur_mono, true, false );
  }
  else {
    Host   & host = this->host_tab.ptr[ i ];
    HostState old_state = host.state;
    host.start( host_id, this->cur_mono, true, false );
    if ( old_state != RV_HOST_STOP ) {
      host.state = RV_HOST_QUERY;
      if ( this->mout != NULL )
        this->mout->printf( "! query %08X, start with no host stop\n", host_id );
    }
  }
}

void
SubscriptionDB::host_stop( uint32_t host_id ) noexcept
{
  uint32_t i;
  size_t   pos;
  if ( this->mout != NULL )
    this->mout->printf( "> host stop %08X\n", host_id );
  if ( this->host_ht->find( host_id, pos, i ) ) {
    Host &host = this->host_tab.ptr[ i ];
    this->unsub_host( host );
  }
}

void
SubscriptionDB::unsub_host( Host &host ) noexcept
{
  if ( this->mout != NULL )
    this->mout->printf( "> unsub host %08X\n", host.host_id );
  size_t pos;
  for ( Session * entry = this->first_session( host, pos ); entry != NULL;
        entry = this->next_session( host, pos ) ) {
    this->unsub_session( *entry );
  }
  host.stop( this->cur_mono );
  this->hosts.active--;
  this->hosts.removed++;
}

void
SubscriptionDB::unsub_session( Session &sess ) noexcept
{
  if ( this->mout != NULL )
    this->mout->printf( "> unsub session %08X.%u\n", sess.host_id, sess.session_id );
  size_t pos;
  for ( Subscription * sub = this->first_subject( sess, pos ); sub != NULL;
        sub = this->next_subject( sess, pos ) ) {
    if ( --sub->refcnt == 0 ) {
      this->subscriptions.active--;
      this->subscriptions.removed++;
    }
    if ( this->cb != NULL ) {
      StopListener op( sess, *sub, false, false );
      this->cb->on_listen_stop( op );
    }
  }
  if ( this->sess_ht->find( sess.session_id, pos ) )
    this->sess_ht->remove( pos );
  sess.stop( this->cur_mono );
}

Session &
SubscriptionDB::session_start( uint32_t host_id,  const char *session_name,
                               size_t session_len ) noexcept
{
  if ( this->mout != NULL )
    this->mout->printf( "> session start %08X %.*s\n", host_id,
                        (int) session_len, session_name );
  Host       & host = this->host_ref( host_id, false );
  uint32_t     hash = kv_crc_c( session_name, session_len, 0 );
  RouteLoc     loc;
  Session    * entry;
  SessionState old_state = RV_SESSION_STOP;

  entry = this->session_tab.upsert( hash, session_name, session_len, loc );
  if ( ! loc.is_new ) {
    if ( entry->state != RV_SESSION_STOP ) {
      old_state = entry->state;
      this->rem_session( host, *entry ); /* no stop session */
    }
  }
  entry->start( host_id, ++this->next_session_id, this->cur_mono, true );
  if ( old_state != RV_SESSION_STOP ) {
    entry->state = RV_SESSION_QUERY;
    if ( this->mout != NULL )
      this->mout->printf( "! query %08X %.*s, start with no sesion stop\n",
                          host_id, entry->len, entry->value );
  }
  this->add_session( host, *entry );
  return *entry;
}

void
SubscriptionDB::session_stop( uint32_t host_id,  const char *session_name,
                              size_t session_len ) noexcept
{
  if ( this->mout != NULL )
    this->mout->printf( "> session stop %08X %.*s\n", host_id,
                       (int) session_len, session_name );
  Host    & host = this->host_ref( host_id, false );
  uint32_t  hash = kv_crc_c( session_name, session_len, 0 );
  RouteLoc  loc;
  Session * entry;

  entry = this->session_tab.find( hash, session_name, session_len, loc );
  if ( entry != NULL )
    this->rem_session( host, *entry );
}

Session &
SubscriptionDB::session_ref( const char *session_name,
                             size_t session_len ) noexcept
{
  if ( this->mout != NULL )
    this->mout->printf( "> session ref %.*s\n",
                       (int) session_len, session_name );
  uint32_t  hash = kv_crc_c( session_name, session_len, 0 );
  RouteLoc  loc;
  Session * entry;

  entry = this->session_tab.upsert( hash, session_name, session_len, loc );
  if ( loc.is_new || entry->state == RV_SESSION_STOP ) {
    uint32_t host_id = string_to_host_id( session_name );
    Host   & host    = this->host_ref( host_id, false );
    entry->start( host_id, ++this->next_session_id, this->cur_mono, false );
    this->add_session( host, *entry );
  }
  entry->ref_mono = this->cur_mono;
  return *entry;
}

void
SubscriptionDB::add_session( Host &host,  Session &sess ) noexcept
{
  if ( host.add_session( sess ) ) {
    this->sess_ht->upsert_rsz( this->sess_ht, sess.session_id, sess.hash );
    this->sessions.active++;
  }
}

void
SubscriptionDB::rem_session( Host &host,  Session &sess ) noexcept
{
  this->unsub_session( sess );
  if ( host.rem_session( sess ) ) {
    this->sessions.active--;
    this->sessions.removed++;
  }
}

Subscription &
SubscriptionDB::listen_start( Session &session,  const char *sub,
                              size_t sub_len,  bool &is_added ) noexcept
{
  if ( this->mout != NULL )
    this->mout->printf( "> listen start %.*s %.*s\n",
                        session.len, session.value, (int) sub_len, sub );
  return this->listen_ref( session, sub, sub_len, is_added );
}

Subscription &
SubscriptionDB::listen_ref( Session & session,  const char *sub,
                            size_t sub_len,  bool &is_added ) noexcept
{
  if ( sub_len > 0 && sub[ sub_len - 1 ] == '\0' )
    sub_len--;
  RouteLoc       loc;
  uint32_t       subj_hash = kv_crc_c( sub, sub_len, 0 );
  Subscription * script;

  script = this->sub_tab.upsert( subj_hash, sub, sub_len, loc );
  if ( loc.is_new )
    script->start( ++this->next_subject_id, this->cur_mono );
  else
    script->ref( this->cur_mono );
  is_added = session.add_subject( *script );
  if ( is_added && script->refcnt == 1 )
    this->subscriptions.active++;
  return *script;
}

Subscription &
SubscriptionDB::listen_stop( Session &session,  const char *sub,
                             size_t sub_len,  bool &is_orphan ) noexcept
{
  if ( this->mout != NULL )
    this->mout->printf( "> listen stop %.*s %.*s\n",
                        session.len, session.value, (int) sub_len, sub );
  RouteLoc       loc;
  uint32_t       subj_hash = kv_crc_c( sub, sub_len, 0 );
  Subscription * script;

  script    = this->sub_tab.find( subj_hash, sub, sub_len, loc );
  is_orphan = ( script == NULL );

  if ( script != NULL ) {
    if ( session.rem_subject( *script ) ) {
      if ( script->refcnt == 0 ) {
        this->subscriptions.active--;
        this->subscriptions.removed++;
      }
    }
    else {
      is_orphan = true;
    }
  }
  else {
    script = this->sub_tab.insert( subj_hash, sub, sub_len, loc );
    script->start( ++this->next_subject_id, this->cur_mono );
  }
  if ( is_orphan ) {
    if ( this->mout != NULL )
      this->mout->printf( "! listen stop without start %.*s %.*s\n",
                        session.len, session.value, (int) sub_len, sub );
    if ( session.state != RV_SESSION_SELF )
      session.state = RV_SESSION_QUERY;
  }
  return *script;
}

Subscription &
SubscriptionDB::snapshot( const char *sub,  size_t sub_len ) noexcept
{
  if ( this->mout != NULL )
    this->mout->printf( "> snapshot %.*s\n", (int) sub_len, sub );
  RouteLoc       loc;
  uint32_t       subj_hash = kv_crc_c( sub, sub_len, 0 );
  Subscription * script;

  script = this->sub_tab.find( subj_hash, sub, sub_len, loc );
  if ( script == NULL ) {
    script = this->sub_tab.insert( subj_hash, sub, sub_len, loc );
    script->start( ++this->next_subject_id, this->cur_mono );
  }
  return *script;
}

Session *
SubscriptionDB::first_session( Host &host,  size_t &pos ) noexcept
{
  uint32_t sess_id, sess_hash;
  if ( host.sess_ht == NULL )
    return NULL;
  if ( ! host.sess_ht->first( pos ) )
    return NULL;
  host.sess_ht->get( pos, sess_id, sess_hash );
  return this->get_session( sess_id, sess_hash );
}

Session *
SubscriptionDB::next_session( Host &host,  size_t &pos ) noexcept
{
  uint32_t sess_id, sess_hash;
  if ( ! host.sess_ht->next( pos ) )
    return NULL;
  host.sess_ht->get( pos, sess_id, sess_hash );
  return this->get_session( sess_id, sess_hash );
}

Session *
SubscriptionDB::get_session( uint32_t sess_id,  uint32_t sess_hash ) noexcept
{
  RouteLoc  loc;
  Session * session = this->session_tab.find_by_hash( sess_hash, loc );
  for (;;) {
    if ( session == NULL || session->session_id == sess_id )
      return session;
    session = this->session_tab.find_next_by_hash( sess_hash, loc );
  }
}

Session *
SubscriptionDB::get_session( uint32_t sess_id ) noexcept
{
  size_t pos;
  uint32_t sess_hash;
  if ( this->sess_ht->find( sess_id, pos, sess_hash ) )
    return this->get_session( sess_id, sess_hash );
  return NULL;
}

void
SubscriptionDB::mark_sessions( Host &host ) noexcept
{
  Session * entry;
  size_t pos;
  if ( (entry = this->first_session( host, pos )) != NULL ) {
    do {
      if ( entry->ref_mono < host.query_mono )
        entry->ref_mono = 0;
    } while ( (entry = this->next_session( host, pos )) != NULL );
  }
}

void
SubscriptionDB::stop_marked_sessions( Host &host ) noexcept
{
  Session * entry;
  size_t    pos;
  uint32_t  end_sess_hash[ 256 ],
            end_sess_id[ 256 ];
  uint32_t  i = 0;

  for (;;) {
    if ( (entry = this->first_session( host, pos )) != NULL ) {
      do {
        if ( entry->ref_mono == 0 ) {
          end_sess_hash[ i ] = entry->hash;
          end_sess_id[ i ]   = entry->session_id;
          if ( this->mout != NULL ) {
            this->mout->printf( "! stop %08X %.*s, marked\n", host.host_id,
                                (int) entry->len, entry->value );
          }
          if ( ++i == 256 )
            break;
        }
        else {
          if ( entry->state != RV_SESSION_SELF ) {
            entry->state = RV_SESSION_QUERY;
            if ( this->mout != NULL ) {
              this->mout->printf( "! query %08X %.*s, marked\n", host.host_id,
                                  (int) entry->len, entry->value );
            }
          }
        }
      } while ( (entry = this->next_session( host, pos )) != NULL );
    }
    for ( uint32_t j = 0; j < i; j++ ) {
      entry = this->get_session( end_sess_id[ j ], end_sess_hash[ j ] );
      this->rem_session( host, *entry );
    }
    if ( i < 256 )
      return;
  }
}

Subscription *
SubscriptionDB::first_subject( Session &session,  size_t &pos ) noexcept
{
  uint32_t sub_id, sub_hash;
  if ( session.sub_ht == NULL )
    return NULL;
  if ( ! session.sub_ht->first( pos ) )
    return NULL;
  session.sub_ht->get( pos, sub_id, sub_hash );
  return this->get_subject( sub_id, sub_hash );
}

Subscription *
SubscriptionDB::next_subject( Session &session,  size_t &pos ) noexcept
{
  uint32_t sub_id, sub_hash;
  if ( ! session.sub_ht->next( pos ) )
    return NULL;
  session.sub_ht->get( pos, sub_id, sub_hash );
  return this->get_subject( sub_id, sub_hash );
}

Subscription *
SubscriptionDB::get_subject( uint32_t sub_id,  uint32_t sub_hash ) noexcept
{
  RouteLoc       loc;
  Subscription * sub = this->sub_tab.find_by_hash( sub_hash, loc );
  for (;;) {
    if ( sub == NULL || sub->subject_id == sub_id )
      return sub;
    sub = this->sub_tab.find_next_by_hash( sub_hash, loc );
  }
}

void
SubscriptionDB::mark_subscriptions( Session &session ) noexcept
{
  Subscription * sub;
  size_t pos;
  if ( (sub = this->first_subject( session, pos )) != NULL ) {
    do {
      if ( sub->start_mono < session.query_mono )
        sub->ref_mono = 0;
    } while ( (sub = this->next_subject( session, pos )) != NULL );
  }
}

void
SubscriptionDB::stop_marked_subscriptions( Session &session ) noexcept
{
  Subscription * sub;
  size_t         pos;
  uint32_t       end_sub_hash[ 256 ],
                 end_sub_id[ 256 ];
  uint32_t       i = 0;

  for (;;) {
    if ( (sub = this->first_subject( session, pos )) != NULL ) {
      do {
        if ( sub->ref_mono == 0 ) {
          end_sub_hash[ i ] = sub->hash;
          end_sub_id[ i ]   = sub->subject_id;
          if ( this->mout != NULL ) {
            this->mout->printf( "! sub %.*s, marked %.*s\n",
                                sub->len, sub->value,
                                session.len, session.value );
          }
          if ( ++i == 256 )
            break;
        }
      } while ( (sub = this->next_subject( session, pos )) != NULL );
    }
    for ( uint32_t j = 0; j < i; j++ ) {
      sub = this->get_subject( end_sub_id[ j ], end_sub_hash[ j ] );
      if ( session.rem_subject( *sub ) ) {
        if ( sub->refcnt == 0 ) {
          this->subscriptions.active--;
          this->subscriptions.removed++;
        }
        if ( this->cb != NULL ) {
          StopListener op( session, *sub, false, false );
          this->cb->on_listen_stop( op );
        }
      }
    }
    if ( i < 256 )
      return;
  }
}

void
SubscriptionDB::send_host_query( uint32_t i ) noexcept
{
  Host & host = this->host_tab.ptr[ i ];

  if ( host.query_mono + HOST_QUERY_INTERVAL > this->cur_mono )
    return;
  if ( host.ref_mono + HOST_TIMEOUT_INTERVAL < this->cur_mono ) {
    if ( this->mout != NULL ) {
      this->mout->printf( "! host %08X timeout intval reached, stop host\n",
                          host.host_id );
    }
    this->unsub_host( host );
    return;
  }

  uint8_t  buf[ 128 ];
  char     daemon_inbox[ MAX_RV_INBOX_LEN ],
           inbox[ MAX_RV_INBOX_LEN ],
           host_id_buf[ 16 ];
  size_t   inbox_len;

  inbox_len = this->client.make_inbox( inbox, i * 2 + 2 ); /* even */
  RvMsgWriter msg( buf, sizeof( buf ) );
  msg.append_string( "op", "get" )
     .append_string( "what", "sessions" )
     .update_hdr();

  host_id_to_string( host.host_id, host_id_buf );
  CatPtr p( daemon_inbox );
  p.s( "_INBOX." ).s( host_id_buf ).s( ".DAEMON" );
  EvPublish pub( daemon_inbox, p.end(), inbox, inbox_len,
                 msg.buf, msg.off, this->client.sub_route, this->client,
                 0, RVMSG_TYPE_ID );
  this->client.publish( pub );
  host.query_mono = this->cur_mono;

  if ( this->mout != NULL ) {
    this->mout->printf( "> pub get session to %08X\n", host.host_id );
  }
}

void
SubscriptionDB::send_session_query( Host &host,  Session &session ) noexcept
{
  if ( session.query_mono + SESSION_QUERY_INTERVAL > this->cur_mono )
    return;
  if ( session.ref_mono + SESSION_TIMEOUT_INTERVAL < this->cur_mono ) {
    if ( this->mout != NULL ) {
      this->mout->printf( "! session %.*s timeout intval reached, query host %08X\n",
                          session.len, session.value, host.host_id );
    }
    host.state = RV_HOST_QUERY;
    return;
  }
  uint8_t  buf[ 128 + EvConnection::MAX_SESSION_LEN ];
  char     daemon_inbox[ MAX_RV_INBOX_LEN ],
           inbox[ MAX_RV_INBOX_LEN ],
           host_id_buf[ 16 ];
  size_t   inbox_len;
  uint32_t i = session.session_id;

  inbox_len = this->client.make_inbox( inbox, i * 2 + 3 ); /* odd */
  if ( session.len == this->client.session_len &&
       ::memcmp( session.value, this->client.session, session.len ) == 0 ) {
    session.state = RV_SESSION_SELF;
  }
  else {
    RvMsgWriter msg( buf, sizeof( buf ) );
    msg.append_string( "op", "get" )
       .append_string( "what", "subscriptions" )
       .append_string( "session", session.value )
       .update_hdr();

    host_id_to_string( host.host_id, host_id_buf );
    CatPtr p( daemon_inbox );
    p.s( "_INBOX." ).s( host_id_buf ).s( ".DAEMON" );
    EvPublish pub( daemon_inbox, p.end(), inbox, inbox_len,
                   msg.buf, msg.off, this->client.sub_route, this->client,
                   0, RVMSG_TYPE_ID );
    this->client.publish( pub );
    session.query_mono = this->cur_mono;

    if ( this->mout != NULL ) {
      this->mout->printf( "> pub get subscriptions to %08X %.*s\n", host.host_id,
                           session.len, session.value );
    }
  }
}

void
SubscriptionDB::process_events( void ) noexcept
{
  this->cur_mono = this->client.poll.mono_ns / NS;

  while ( this->soft_host_query > 0 ) {
    if ( this->soft_host_query < this->host_tab.count &&
         this->host_tab.ptr[ this->soft_host_query ].state == RV_HOST_QUERY )
      break;
    Host & host = this->host_tab.ptr[ --this->soft_host_query ];
    if ( host.state != RV_HOST_STOP ) {
      host.state = RV_HOST_QUERY;
      break;
    }
  }
  for ( uint32_t i = 0; i < this->host_tab.count; i++ ) {
    Host & host = this->host_tab.ptr[ i ];
    uint32_t secs;
    if ( host.state == RV_HOST_STOP )
      continue;
    if ( (secs = host.check_query_needed( this->cur_mono )) > 0 ) {
      char buf[ 32 ];
      if ( this->mout != NULL ) {
        this->mout->printf( "! host %08X is missing for %u seconds\n",
                            host.host_id, secs );
        time_t now = this->client.poll.mono_to_utc_secs( this->cur_mono ),
               ref = this->client.poll.mono_to_utc_secs( host.ref_mono ),
               sta = this->client.poll.mono_to_utc_secs( host.status_mono );
        this->mout->printf(   "! cur time    %s\n", hms( now, buf ) );
        if ( host.ref_mono != 0 )
          this->mout->printf( "! ref time    %s\n", hms( ref, buf ) );
        if ( host.status_mono != 0 )
          this->mout->printf( "! status time %s\n", hms( sta, buf) );
      }
    }
    if ( host.state == RV_HOST_QUERY )
      this->send_host_query( i );
    if ( host.state < RV_HOST_QUERY && host.sess_ht != NULL ) {
      Session * entry;
      size_t    pos;

      if ( (entry = this->first_session( host, pos )) != NULL ) {
        do {
          if ( entry->state == RV_SESSION_QUERY ) {
            this->send_session_query( host, *entry );
            if ( host.state == RV_HOST_QUERY ) {
              this->send_host_query( i );
              break;
            }
          }
        } while ( (entry = this->next_session( host, pos )) != NULL );
      }
    }
  }
  if ( this->next_gc < this->cur_mono ) {
    this->next_gc = this->cur_mono + 131;
    this->gc();
  }
  if ( this->mout != NULL )
    this->mout->flush();
}

bool
SubscriptionDB::process_pub( EvPublish &pub ) noexcept
{
  const char     * subject     = pub.subject;
  size_t           subject_len = pub.subject_len;
  const SubMatch * match       = match_rv_info( subject, subject_len );
  uint64_t         which       = 0;

  if ( match == NULL ) {
    which = this->client.is_inbox( subject, subject_len );
    if ( which < 2 )
      return false;
  }
  this->cur_mono = this->client.poll.mono_ns / NS;
  if ( this->mout != NULL ) {
    char buf[ 32 ];
    time_t now = this->client.poll.mono_to_utc_secs( this->cur_mono );
    this->mout->printf( "-- %s\n## %.*s\n", hms( now, buf ),
                        (int) subject_len, subject );
  }
  MDMsgMem mem;
  RvMsg  * m = NULL;
  if ( pub.msg_len > 0 ) {
    m = RvMsg::unpack_rv( (void *) pub.msg, 0, pub.msg_len, 0, NULL, mem );
    if ( m != NULL ) {
      if ( this->mout != NULL ) {
        m->print( this->mout );
        this->mout->flush();
      }
    }
    else {
      fprintf( stderr, "unpack_rv error %.*s\n", (int) subject_len, subject );
      return true;
    }
  }
  else if ( this->mout != NULL ) {
    this->mout->flush();
  }
  /* if is not an inbox */
  if ( match != NULL ) {
    /* these have host-id in the subject:
     *   _RV.INFO.SYSTEM.HOST.START.<host-id>
     *   _RV.INFO.SYSTEM.HOST.STATUS.<host-id>
     *   _RV.INFO.SYSTEM.HOST.STOP.<host-id>
     *   _RV.INFO.SYSTEM.SESSION.START.<host-id>.<session-name>
     *   _RV.INFO.SYSTEM.SESSION.STOP.<host-id>.<session-name>
     */
    uint32_t id = ( match->kind <= IS_SESSION_STOP ?
                    string_to_host_id( &subject[ match->len - 1 ] ) : 0 );
    char   * x     = NULL,
           * s     = NULL;
    size_t   xlen  = 0,
             slen  = 0;
    uint32_t idl   = 0,
             odl   = 0;
    uint16_t flags = 0;

    switch ( match->kind ) {
      case IS_SESSION_START:
      case IS_SESSION_STOP :
      case IS_LISTEN_START :
      case IS_LISTEN_STOP  :
      case IS_HOST_STATUS  :
      case IS_SNAP         : {
        if ( m == NULL )
          break;
        MDFieldReader rd( *m );
        /* get inbound-data-loss and outbound-data-loss */
        if ( match->kind == IS_SNAP ) {
          if ( rd.find( "flags" ) ) 
            rd.get_uint( flags );
        }
        else if ( match->kind == IS_HOST_STATUS ) {
          if ( rd.find( "idl" ) )
            rd.get_uint( idl );
          if ( rd.find( "odl" ) )
            rd.get_uint( odl );
        }
        /* get id: session-name, sub: subject */
        else {
          if ( rd.find( "id" ) )
            rd.get_string( x, xlen );
          if ( match->kind >= IS_LISTEN_START && match->kind <= IS_LISTEN_STOP ) {
            if ( rd.find( "sub" ) )
              rd.get_string( s, slen );
          }
        }
        break;
      }
      default:
        break;
    }

    switch ( match->kind ) {
      case IS_HOST_STATUS  : {
        Host &host = this->host_ref( id, true );
        if ( host.data_loss != idl + odl ) {
          if ( this->mout != NULL ) {
            this->mout->printf( "! query host %08X dataloss %u -> %u\n",
                                host.host_id, host.data_loss, idl + odl );
          }
          host.state = RV_HOST_QUERY;
          host.data_loss = idl + odl;
          if ( host.host_id == ntohl( this->client.ipaddr ) )
            this->soft_host_query = this->host_tab.count;
        }
        break;
      }
      case IS_HOST_START   : this->host_start( id ); break;
      case IS_HOST_STOP    : this->host_stop( id ); break;
      case IS_SESSION_START: this->session_start( id, x, xlen ); break;
      case IS_SESSION_STOP : this->session_stop( id, x, xlen ); break;
      case IS_LISTEN_START : {
        Session & session  = this->session_ref( x, xlen );
        if ( session.state == RV_SESSION_SELF )
          break;
        bool           is_added = false;
        Subscription & script   = this->listen_start( session, s, slen,
                                                      is_added );
        if ( this->cb != NULL ) {
          StartListener op( session, script, (const char *) pub.reply,
                            pub.reply_len, true );
          this->cb->on_listen_start( op );
        }
        break;
      }
      case IS_LISTEN_STOP: {
        Session & session = this->session_ref( x, xlen );
        if ( session.state == RV_SESSION_SELF )
          break;
        bool           is_orphan = false;
        Subscription & script    = this->listen_stop( session, s, slen,
                                                      is_orphan );
        if ( this->cb != NULL ) {
          StopListener op( session, script, is_orphan, true );
          this->cb->on_listen_stop( op );
        }
        break;
      }
      /* _SNAP.<subject> = strip 6 char prefix */
      case IS_SNAP:
        if ( this->cb != NULL && subject_len > 6 ) {
          Subscription & script =
            this->snapshot( &subject[ 6 ], subject_len - 6 );
          SnapListener op( script, (const char *) pub.reply, pub.reply_len,
                           flags );
          this->cb->on_snapshot( op );
        }
        break;

      default:
        break;
    }
  }
  else {
    bool     is_sessions = ( ( which & 1 ) == 0 );
    uint32_t i = ( is_sessions ? ( which - 2 ) : ( which - 3 ) ) / 2;
    MDName   nm;

    MDFieldReader rd( *m );
   /* session message:
    * ## _INBOX.C0A80010.601FB1D31D6DE.2
    * null : string  45 : "C0A80010.DAEMON.152AAF64CB09BA0X7F5B5806F680"
    * null : string  23 : "C0A80010.601FB1D31D6DE" */
    if ( is_sessions ) {
      if ( i < this->host_tab.count ) {
        Host & host = this->host_tab.ptr[ i ];
        if ( host.state == RV_HOST_QUERY ) {
          this->mark_sessions( host );
          for ( bool b = rd.first( nm ); b; b = rd.next( nm ) ) {
            char * x    = NULL;
            size_t xlen = 0;
            if ( nm.fnamelen == 0 && rd.get_string( x, xlen ) )
              this->session_ref( x, xlen );
          }
          this->stop_marked_sessions( host );
          host.state = RV_HOST_STATUS;
        }
      }
    }
   /* subscriptions message:
    * ## _INBOX.C0A80010.601FC2546471F.11
    * user : string    7 : "nobody"
    * null : subject  19 : RDF.EQITY.9.N
    * null : subject  19 : RDF.EQITY.7.N
    * null : subject  19 : RDF.EQITY.8.N
    * null : subject  19 : RSF.EQITY.6.N
    * null : subject  19 : RSF.EQITY.5.N
    * end  : int       4 : 1 */
    else {
      Session * session = this->get_session( i );
      if ( session != NULL && session->state == RV_SESSION_QUERY ) {
        this->mark_subscriptions( *session );
        for ( bool b = rd.first( nm ); b; b = rd.next( nm ) ) {
          char * sub    = NULL;
          size_t sub_len = 0;
          if ( nm.fnamelen == 0 && rd.get_string( sub, sub_len ) &&
               this->is_matched( sub, sub_len ) ) {
            bool is_added;
            Subscription & script = this->listen_ref( *session, sub, sub_len,
                                                      is_added );
            if ( is_added ) {
              if ( this->cb != NULL ) {
                StartListener op( *session, script, NULL, 0, false );
                this->cb->on_listen_start( op );
              }
            }
          }
        }
        session->state = RV_SESSION_RV5;
        if ( session->has_daemon() )
          session->state = RV_SESSION_RV7;
        this->stop_marked_subscriptions( *session );
      }
    }
  }
  return true;
}

void
SubscriptionDB::gc( void ) noexcept
{
  RouteLoc loc;
  uint32_t sub_rem = 0, sess_rem = 0;
  if ( this->subscriptions.removed > this->subscriptions.active ) {
    this->subscriptions.removed = 0;
    for ( Subscription *sub = this->sub_tab.first( loc ); sub != NULL; ) {
      if ( sub->refcnt == 0 ) {
        sub = this->sub_tab.remove_and_next( sub, loc );
        sub_rem++;
      }
      else
        sub = this->sub_tab.next( loc );
    }
  }
  if ( this->sessions.removed > this->sessions.active ) {
    this->sessions.removed = 0;
    for ( Session *entry = this->session_tab.first( loc ); entry != NULL; ) {
      if ( entry->state == RV_SESSION_STOP ) {
        entry = this->session_tab.remove_and_next( entry, loc );
        sess_rem++;
      }
      else
        entry = this->session_tab.next( loc );
    }
  }
  if ( this->hosts.removed > this->hosts.active &&
       this->first_free_host == this->host_tab.count ) {
    this->hosts.removed = 0;
    for ( uint32_t i = 0; i < this->host_tab.count; i++ ) {
      if ( this->host_tab.ptr[ i ].state == RV_HOST_STOP ) {
        this->first_free_host = i;
        break;
      }
    }
  }
  if ( this->mout != NULL ) {
    if ( sub_rem + sess_rem != 0 ) {
      size_t sz = 0;
      for ( uint32_t i = 0; i < this->host_tab.count; i++ ) {
        if ( this->host_tab.ptr[ i ].sess_ht != NULL )
          sz += this->host_tab.ptr[ i ].sess_ht->mem_size();
      }
      for ( Session *entry = this->session_tab.first( loc ); entry != NULL; ) {
        if ( entry->sub_ht != NULL )
          sz += entry->sub_ht->mem_size();
        entry = this->session_tab.next( loc );
      }
      this->mout->printf( "gc, rem %u subscriptions removed : %lu bytes used, "
                          "%u sessions removed : %lu bytes used, "
                          "other %lu bytes used\n",
                          sub_rem, this->sub_tab.mem_size(), sess_rem, 
                          this->sub_tab.mem_size(),
                          this->host_tab.size * sizeof( Host ) +
                          this->host_ht->mem_size() +
                          this->sess_ht->mem_size() + sz );
    }
  }
}

