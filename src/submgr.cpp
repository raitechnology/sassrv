#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <netinet/in.h>
#include <sassrv/ev_rv_client.h>
#include <sassrv/submgr.h>
#include <raikv/ev_publish.h>

using namespace rai;
using namespace kv;
using namespace sassrv;
using namespace md;

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
const char *rai::sassrv::RvHostEntry::get_host_state_str( HostState s ) noexcept {
  return s < MAX_HOST_STATE ? host_state_str[ s ] : "bad host state"; }
const char *rai::sassrv::RvSessionEntry::get_session_state_str( SessionState s ) noexcept {
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

RvSubscriptionDB::RvSubscriptionDB( EvRvClient &c,
                                    RvSubscriptionListener *sl ) noexcept
              : client( c ), cb( sl ), host_ht( 0 ), sess_ht( 0 ),
                cur_mono( 0 ), next_session_ctr( 0 ), next_subject_ctr( 0 ),
                soft_host_query( 0 ), first_free_host( 0 ), next_gc( 0 ),
                host_inbox_base( 1000000 ), session_inbox_base( 1000 ),
                is_subscribed( false ), is_all_subscribed( false ), mout( 0 )
{
}

void
RvSubscriptionDB::release( void ) noexcept
{
  RouteLoc loc;
  uint32_t i;
  for ( RvSessionEntry *entry = this->session_tab.first( loc );
        entry != NULL; entry = this->session_tab.next( loc ) ) {
    if ( entry->state != RvSessionEntry::RV_SESSION_STOP )
      entry->stop( 0 );
  }
  for ( i = 0; i < this->host_tab.count; i++ ) {
    RvHostEntry & entry = this->host_tab.ptr[ i ];
    if ( entry.state != RvHostEntry::RV_HOST_STOP )
      entry.stop( 0 );
  }
  for ( i = 0; i < this->filters.count; i++ ) {
    Filter &f = this->filters.ptr[ i ];
    if ( f.wild != NULL ) {
      ::free( f.wild );
      f.wild = NULL;
    }
  }

  this->host_tab.clear();
  this->session_tab.release();
  this->sub_tab.release();
  this->filters.clear();
  if ( this->host_ht != NULL )
    delete this->host_ht;
  if ( this->sess_ht != NULL )
    delete this->sess_ht;

  this->cur_mono          = 0;
  this->next_session_ctr  = 0;
  this->next_subject_ctr  = 0;
  this->soft_host_query   = 0;
  this->first_free_host   = 0;
  this->next_gc           = 0;
  this->host_ht           = NULL;
  this->sess_ht           = NULL;
  this->is_subscribed     = false;
  this->is_all_subscribed = false;
  this->subscriptions.reset();
  this->sessions.reset();
  this->subscriptions.reset();
}

uint32_t
RvSubscriptionDB::next_session_id( void ) noexcept
{
  return ++this->next_session_ctr;
}

uint32_t
RvSubscriptionDB::next_subject_id( void ) noexcept
{
  return kv_hash_uint( ++this->next_subject_ctr );
}

void RvSubscriptionListener::on_listen_start( Start & ) noexcept {}
void RvSubscriptionListener::on_listen_stop ( Stop  & ) noexcept {}
void RvSubscriptionListener::on_snapshot    ( Snap  & ) noexcept {}

void
RvSubscriptionDB::add_wildcard( const char *wildcard ) noexcept
{
  size_t len    = ( wildcard == NULL ? 0 : ::strlen( wildcard ) ),
         suflen = ( len == 0 ? 1 :
                    wildcard[ len - 1 ] != '>' ) ? 2 : 0;
  char * s   = (char *) ::malloc( len + suflen + 1 );
  Filter &f = this->filters.push();
  if ( len > 0 )
    ::memcpy( s, wildcard, len );
  if ( suflen > 0 ) {
    if ( suflen == 2 )
      s[ len++ ] = '.';
    s[ len++ ] = '>';
  }
  s[ len ] = '\0';
  f.wild     = s;
  f.wild_len = len;
}

bool
RvSubscriptionDB::is_matched( const char *sub,  size_t sub_len ) noexcept
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
RvSubscriptionDB::start_subscriptions( bool all ) noexcept
{
  if ( this->is_subscribed )
    return;
  this->host_ht           = kv::UIntHashTab::resize( NULL );
  this->sess_ht           = kv::UIntHashTab::resize( NULL );
  this->is_subscribed     = true;
  this->is_all_subscribed = all;
  this->cur_mono          = this->client.poll.mono_ns / NS;
  this->next_gc           = this->cur_mono + 131;
  this->do_subscriptions( true );

  RvHostEntry &host = this->host_ref( ntohl( this->client.ipaddr ), true );
  host.state = RvHostEntry::RV_HOST_QUERY;

  RvSessionEntry &session =
    this->session_start( host.host_id, this->client.session,
                         this->client.session_len );
  session.state = RvSessionEntry::RV_SESSION_SELF;
}

void
RvSubscriptionDB::stop_subscriptions( void ) noexcept
{
  if ( ! this->is_subscribed )
    return;
  this->is_subscribed = false;
  this->do_subscriptions( false );
}

void
RvSubscriptionDB::do_subscriptions( bool is_subscribe ) noexcept
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
RvSubscriptionDB::do_wild_subscription( Filter &f,  bool is_subscribe,
                                        int k ) noexcept
{
  const char     * un    = ( is_subscribe ? "" : "un" );
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

RvHostEntry &
RvSubscriptionDB::host_ref( uint32_t host_id,  bool is_status ) noexcept
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
           this->host_tab.ptr[
             this->first_free_host ].state == RvHostEntry::RV_HOST_STOP )
        break;
    }
    i = this->first_free_host++;
    this->hosts.active++;
    this->host_ht->set_rsz( this->host_ht, host_id, pos, i );
    RvHostEntry & host = ( i < this->host_tab.count ? this->host_tab.ptr[ i ] :
                           this->host_tab.push() );
    host.start( host_id, this->cur_mono, false, is_status );
    if ( this->mout != NULL ) {
      if ( host.state == RvHostEntry::RV_HOST_QUERY )
        this->mout->printf( "! host %08X query, no start\n", host_id );
    }
    return host;
  }
  RvHostEntry          & host      = this->host_tab.ptr[ i ];
  RvHostEntry::HostState old_state = host.state;

  if ( host.state == RvHostEntry::RV_HOST_STOP ) {
    this->hosts.active++;
    host.start( host_id, this->cur_mono, false, is_status );
  }
  else if ( is_status )
    host.status( this->cur_mono );
  else
    host.ref( this->cur_mono );
  if ( this->mout != NULL ) {
    if ( old_state != host.state && host.state == RvHostEntry::RV_HOST_QUERY )
      this->mout->printf( "! host %08X query, no start\n", host_id );
  }
  return host;
}

void
RvSubscriptionDB::host_start( uint32_t host_id ) noexcept
{
  uint32_t i;
  size_t   pos;
  if ( this->mout != NULL )
    this->mout->printf( "> host start %08X %s\n", host_id,
       this->host_ht->find( host_id, pos, i ) ? "exists" : "new" );

  if ( ! this->host_ht->find( host_id, pos, i ) ) {
    i = this->host_tab.count;
    this->host_ht->set_rsz( this->host_ht, host_id, pos, i );
    RvHostEntry & host = this->host_tab.push();
    host.start( host_id, this->cur_mono, true, false );
  }
  else {
    RvHostEntry          & host      = this->host_tab.ptr[ i ];
    RvHostEntry::HostState old_state = host.state;

    host.start( host_id, this->cur_mono, true, false );
    if ( old_state != RvHostEntry::RV_HOST_STOP ) {
      host.state = RvHostEntry::RV_HOST_QUERY;
      if ( this->mout != NULL )
        this->mout->printf( "! query %08X, start with no host stop\n", host_id );
    }
  }
}

void
RvSubscriptionDB::host_stop( uint32_t host_id ) noexcept
{
  uint32_t i;
  size_t   pos;
  if ( this->mout != NULL )
    this->mout->printf( "> host stop %08X\n", host_id );
  if ( this->host_ht->find( host_id, pos, i ) ) {
    RvHostEntry &host = this->host_tab.ptr[ i ];
    this->unsub_host( host );
  }
}

void
RvSubscriptionDB::unsub_host( RvHostEntry &host ) noexcept
{
  if ( this->mout != NULL )
    this->mout->printf( "> unsub host %08X\n", host.host_id );
  size_t pos;
  for ( RvSessionEntry * entry = this->first_session( host, pos );
        entry != NULL; entry = this->next_session( host, pos ) ) {
    this->unsub_session( *entry );
  }
  host.stop( this->cur_mono );
  this->hosts.active--;
  this->hosts.removed++;
}

void
RvSubscriptionDB::unsub_session( RvSessionEntry &sess ) noexcept
{
  if ( this->mout != NULL )
    this->mout->printf( "> unsub session %08X.%u\n", sess.host_id,
                        sess.session_id );
  size_t pos;
  for ( RvSubscription * sub = this->first_subject( sess, pos ); sub != NULL;
        sub = this->next_subject( sess, pos ) ) {
    if ( --sub->refcnt == 0 ) {
      this->subscriptions.active--;
      this->subscriptions.removed++;
    }
    if ( this->cb != NULL ) {
      RvSubscriptionListener::Stop op( sess, *sub, false, false );
      this->cb->on_listen_stop( op );
    }
  }
  if ( this->sess_ht->find( sess.session_id, pos ) )
    this->sess_ht->remove( pos );
  sess.stop( this->cur_mono );
}

RvSessionEntry &
RvSubscriptionDB::session_start( uint32_t host_id,  const char *session_name,
                                 size_t session_len ) noexcept
{
  if ( this->mout != NULL )
    this->mout->printf( "> session start %08X %.*s\n", host_id,
                        (int) session_len, session_name );
  RvHostEntry    & host = this->host_ref( host_id, false );
  uint32_t         hash = kv_crc_c( session_name, session_len, 0 );
  RouteLoc         loc;
  RvSessionEntry * entry;
  RvSessionEntry::SessionState old_state = RvSessionEntry::RV_SESSION_STOP;

  entry = this->session_tab.upsert( hash, session_name, session_len, loc );
  if ( ! loc.is_new ) {
    if ( entry->state != RvSessionEntry::RV_SESSION_STOP ) {
      old_state = entry->state;
      this->rem_session( host, *entry ); /* no stop session */
    }
  }
  entry->start( host_id, this->next_session_id(), this->cur_mono, true );
  if ( old_state != RvSessionEntry::RV_SESSION_STOP ) {
    entry->state = RvSessionEntry::RV_SESSION_QUERY;
    if ( this->mout != NULL )
      this->mout->printf( "! query %08X %.*s, start with no sesion stop\n",
                          host_id, entry->len, entry->value );
  }
  this->add_session( host, *entry );
  return *entry;
}

void
RvSubscriptionDB::session_stop( uint32_t host_id,  const char *session_name,
                                size_t session_len ) noexcept
{
  if ( this->mout != NULL )
    this->mout->printf( "> session stop %08X %.*s\n", host_id,
                       (int) session_len, session_name );
  RvHostEntry    & host = this->host_ref( host_id, false );
  uint32_t         hash = kv_crc_c( session_name, session_len, 0 );
  RouteLoc         loc;
  RvSessionEntry * entry;

  entry = this->session_tab.find( hash, session_name, session_len, loc );
  if ( entry != NULL )
    this->rem_session( host, *entry );
}

RvSessionEntry &
RvSubscriptionDB::session_ref( const char *session_name,
                               size_t session_len ) noexcept
{
  if ( this->mout != NULL )
    this->mout->printf( "> session ref %.*s\n",
                       (int) session_len, session_name );
  uint32_t         hash = kv_crc_c( session_name, session_len, 0 );
  RouteLoc         loc;
  RvSessionEntry * entry;

  entry = this->session_tab.upsert( hash, session_name, session_len, loc );
  if ( loc.is_new || entry->state == RvSessionEntry::RV_SESSION_STOP ) {
    uint32_t      host_id = string_to_host_id( session_name );
    RvHostEntry & host    = this->host_ref( host_id, false );
    entry->start( host_id, this->next_session_id(), this->cur_mono, false );
    this->add_session( host, *entry );
  }
  entry->ref_mono = this->cur_mono;
  return *entry;
}

void
RvSubscriptionDB::add_session( RvHostEntry &host,
                               RvSessionEntry &sess ) noexcept
{
  if ( host.add_session( sess ) ) {
    this->sess_ht->upsert_rsz( this->sess_ht, sess.session_id, sess.hash );
    this->sessions.active++;
  }
}

void
RvSubscriptionDB::rem_session( RvHostEntry &host,
                               RvSessionEntry &sess ) noexcept
{
  this->unsub_session( sess );
  if ( host.rem_session( sess ) ) {
    this->sessions.active--;
    this->sessions.removed++;
  }
}

RvSubscription &
RvSubscriptionDB::listen_start( RvSessionEntry &session,  const char *sub,
                                size_t sub_len,  bool &is_added ) noexcept
{
  if ( this->mout != NULL )
    this->mout->printf( "> listen start %.*s %.*s\n",
                        session.len, session.value, (int) sub_len, sub );
  return this->listen_ref( session, sub, sub_len, is_added );
}

RvSubscription &
RvSubscriptionDB::listen_ref( RvSessionEntry & session,  const char *sub,
                              size_t sub_len,  bool &is_added ) noexcept
{
  if ( sub_len > 0 && sub[ sub_len - 1 ] == '\0' )
    sub_len--;
  RouteLoc         loc;
  uint32_t         subj_hash = kv_crc_c( sub, sub_len, 0 );
  RvSubscription * script;

  script = this->sub_tab.upsert( subj_hash, sub, sub_len, loc );
  if ( loc.is_new )
    script->start( this->next_subject_id(), this->cur_mono );
  else
    script->ref( this->cur_mono );
  is_added = session.add_subject( *script );
  if ( is_added && script->refcnt == 1 )
    this->subscriptions.active++;
  return *script;
}

RvSubscription &
RvSubscriptionDB::listen_stop( RvSessionEntry &session,  const char *sub,
                               size_t sub_len,  bool &is_orphan ) noexcept
{
  if ( this->mout != NULL )
    this->mout->printf( "> listen stop %.*s %.*s\n",
                        session.len, session.value, (int) sub_len, sub );
  RouteLoc         loc;
  uint32_t         subj_hash = kv_crc_c( sub, sub_len, 0 );
  RvSubscription * script;

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
    script->start( this->next_subject_id(), this->cur_mono );
  }
  if ( is_orphan ) {
    if ( this->mout != NULL )
      this->mout->printf( "! listen stop without start %.*s %.*s\n",
                        session.len, session.value, (int) sub_len, sub );
    if ( session.state != RvSessionEntry::RV_SESSION_SELF )
      session.state = RvSessionEntry::RV_SESSION_QUERY;
  }
  return *script;
}

RvSubscription &
RvSubscriptionDB::snapshot( const char *sub,  size_t sub_len ) noexcept
{
  if ( this->mout != NULL )
    this->mout->printf( "> snapshot %.*s\n", (int) sub_len, sub );
  RouteLoc         loc;
  uint32_t         subj_hash = kv_crc_c( sub, sub_len, 0 );
  RvSubscription * script;

  script = this->sub_tab.find( subj_hash, sub, sub_len, loc );
  if ( script == NULL ) {
    script = this->sub_tab.insert( subj_hash, sub, sub_len, loc );
    script->start( this->next_subject_id(), this->cur_mono );
  }
  return *script;
}

RvSessionEntry *
RvSubscriptionDB::first_session( RvHostEntry &host,  size_t &pos ) noexcept
{
  uint32_t sess_id, sess_hash;
  if ( host.sess_ht == NULL )
    return NULL;
  if ( ! host.sess_ht->first( pos ) )
    return NULL;
  host.sess_ht->get( pos, sess_id, sess_hash );
  return this->get_session( sess_id, sess_hash );
}

RvSessionEntry *
RvSubscriptionDB::next_session( RvHostEntry &host,  size_t &pos ) noexcept
{
  uint32_t sess_id, sess_hash;
  if ( ! host.sess_ht->next( pos ) )
    return NULL;
  host.sess_ht->get( pos, sess_id, sess_hash );
  return this->get_session( sess_id, sess_hash );
}

RvSessionEntry *
RvSubscriptionDB::get_session( uint32_t sess_id,  uint32_t sess_hash ) noexcept
{
  RouteLoc         loc;
  RvSessionEntry * session = this->session_tab.find_by_hash( sess_hash, loc );
  for (;;) {
    if ( session == NULL || session->session_id == sess_id )
      return session;
    session = this->session_tab.find_next_by_hash( sess_hash, loc );
  }
}

RvSessionEntry *
RvSubscriptionDB::get_session( uint32_t sess_id ) noexcept
{
  size_t   pos;
  uint32_t sess_hash;
  if ( this->sess_ht->find( sess_id, pos, sess_hash ) )
    return this->get_session( sess_id, sess_hash );
  return NULL;
}

void
RvSubscriptionDB::mark_sessions( RvHostEntry &host ) noexcept
{
  RvSessionEntry * entry;
  size_t           pos;
  if ( (entry = this->first_session( host, pos )) != NULL ) {
    do {
      if ( entry->ref_mono < host.query_mono )
        entry->ref_mono = 0;
    } while ( (entry = this->next_session( host, pos )) != NULL );
  }
}

void
RvSubscriptionDB::stop_marked_sessions( RvHostEntry &host ) noexcept
{
  RvSessionEntry * entry;
  size_t           pos;
  uint32_t         end_sess_hash[ 256 ],
                   end_sess_id[ 256 ];
  uint32_t         i = 0;

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
          if ( entry->state != RvSessionEntry::RV_SESSION_SELF ) {
            entry->state = RvSessionEntry::RV_SESSION_QUERY;
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

RvSubscription *
RvSubscriptionDB::first_subject( RvSessionEntry &session, size_t &pos ) noexcept
{
  uint32_t sub_id, sub_hash;
  if ( session.sub_ht == NULL )
    return NULL;
  if ( ! session.sub_ht->first( pos ) )
    return NULL;
  session.sub_ht->get( pos, sub_id, sub_hash );
  return this->get_subject( sub_id, sub_hash );
}

RvSubscription *
RvSubscriptionDB::next_subject( RvSessionEntry &session,  size_t &pos ) noexcept
{
  uint32_t sub_id, sub_hash;
  if ( ! session.sub_ht->next( pos ) )
    return NULL;
  session.sub_ht->get( pos, sub_id, sub_hash );
  return this->get_subject( sub_id, sub_hash );
}

RvSubscription *
RvSubscriptionDB::get_subject( uint32_t sub_id,  uint32_t sub_hash ) noexcept
{
  RouteLoc         loc;
  RvSubscription * sub = this->sub_tab.find_by_hash( sub_hash, loc );
  for (;;) {
    if ( sub == NULL || sub->subject_id == sub_id )
      return sub;
    sub = this->sub_tab.find_next_by_hash( sub_hash, loc );
  }
}

void
RvSubscriptionDB::mark_subscriptions( RvSessionEntry &session ) noexcept
{
  RvSubscription * sub;
  size_t pos;
  if ( (sub = this->first_subject( session, pos )) != NULL ) {
    do {
      if ( sub->start_mono < session.query_mono )
        sub->ref_mono = 0;
    } while ( (sub = this->next_subject( session, pos )) != NULL );
  }
}

void
RvSubscriptionDB::stop_marked_subscriptions( RvSessionEntry &session ) noexcept
{
  RvSubscription * sub;
  size_t           pos;
  uint32_t         end_sub_hash[ 256 ],
                   end_sub_id[ 256 ];
  uint32_t         i = 0;

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
          RvSubscriptionListener::Stop op( session, *sub, false, false );
          this->cb->on_listen_stop( op );
        }
      }
    }
    if ( i < 256 )
      return;
  }
}

void
RvSubscriptionDB::send_host_query( uint32_t i ) noexcept
{
  RvHostEntry & host = this->host_tab.ptr[ i ];

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

  char   daemon_inbox[ MAX_RV_INBOX_LEN ],
         inbox[ MAX_RV_INBOX_LEN ],
         host_id_buf[ 16 ];
  size_t inbox_len;

  inbox_len = this->client.make_inbox( inbox, i + this->host_inbox_base );
  MDMsgMem    mem;
  RvMsgWriter msg( mem, mem.make( 256 ), 256 );
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
  this->client.idle_push_write();
  host.query_mono = this->cur_mono;

  if ( this->mout != NULL ) {
    this->mout->printf( "> pub get session to %08X\n", host.host_id );
  }
  printf( "SDB: host %08X, get session\n", host.host_id );
}

void
RvSubscriptionDB::send_session_query( RvHostEntry &host,
                                      RvSessionEntry &session ) noexcept
{
  if ( session.query_mono + SESSION_QUERY_INTERVAL > this->cur_mono )
    return;
  if ( session.ref_mono + SESSION_TIMEOUT_INTERVAL < this->cur_mono ) {
    if ( this->mout != NULL ) {
      this->mout->printf( "! session %.*s timeout intval reached, query host %08X\n",
                          session.len, session.value, host.host_id );
    }
    host.state = RvHostEntry::RV_HOST_QUERY;
    return;
  }
  char     daemon_inbox[ MAX_RV_INBOX_LEN ],
           inbox[ MAX_RV_INBOX_LEN ],
           host_id_buf[ 16 ];
  size_t   inbox_len;
  uint32_t i = session.session_id;

  inbox_len = this->client.make_inbox( inbox, i + this->session_inbox_base );
  if ( session.len == this->client.session_len &&
       ::memcmp( session.value, this->client.session, session.len ) == 0 ) {
    session.state = RvSessionEntry::RV_SESSION_SELF;
  }
  else {
    MDMsgMem    mem;
    RvMsgWriter msg( mem, mem.make( 256 ), 256 );
    msg.append_string( "op", "get" )
       .append_string( "what", "subscriptions" )
       .append_string( SARG( "session" ), session.value, session.len )
       .update_hdr();

    host_id_to_string( host.host_id, host_id_buf );
    CatPtr p( daemon_inbox );
    p.s( "_INBOX." ).s( host_id_buf ).s( ".DAEMON" );
    EvPublish pub( daemon_inbox, p.end(), inbox, inbox_len,
                   msg.buf, msg.off, this->client.sub_route, this->client,
                   0, RVMSG_TYPE_ID );
    this->client.publish( pub );
    this->client.idle_push_write();
    session.query_mono = this->cur_mono;

    if ( this->mout != NULL ) {
      this->mout->printf( "> pub get subscriptions to %08X %.*s\n", host.host_id,
                           session.len, session.value );
    }
    printf( "SDB: session %.*s, get subscriptions\n",
            session.len, session.value );
  }
}

void
RvSubscriptionDB::process_events( void ) noexcept
{
  this->cur_mono = this->client.poll.mono_ns / NS;

  while ( this->soft_host_query > 0 ) {
    if ( this->soft_host_query < this->host_tab.count &&
         this->host_tab.ptr[
           this->soft_host_query ].state == RvHostEntry::RV_HOST_QUERY )
      break;
    RvHostEntry & host = this->host_tab.ptr[ --this->soft_host_query ];
    if ( host.state != RvHostEntry::RV_HOST_STOP ) {
      host.state = RvHostEntry::RV_HOST_QUERY;
      break;
    }
  }
  for ( uint32_t i = 0; i < this->host_tab.count; i++ ) {
    RvHostEntry & host = this->host_tab.ptr[ i ];
    uint32_t secs;
    if ( host.state == RvHostEntry::RV_HOST_STOP )
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
    if ( host.state == RvHostEntry::RV_HOST_QUERY )
      this->send_host_query( i );
    if ( host.state < RvHostEntry::RV_HOST_QUERY && host.sess_ht != NULL ) {
      RvSessionEntry * entry;
      size_t           pos;

      if ( (entry = this->first_session( host, pos )) != NULL ) {
        do {
          if ( entry->state == RvSessionEntry::RV_SESSION_QUERY ) {
            this->send_session_query( host, *entry );
            if ( host.state == RvHostEntry::RV_HOST_QUERY ) {
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
RvSubscriptionDB::process_pub( EvPublish &pub ) noexcept
{
  const char     * subject     = pub.subject;
  size_t           subject_len = pub.subject_len;
  const SubMatch * match       = match_rv_info( subject, subject_len );
  uint64_t         which       = 0;

  if ( match == NULL ) {
    which = this->client.is_inbox( subject, subject_len );
    if ( which == 0 )
      return false;
    if ( which < this->session_inbox_base && 
         which < this->host_inbox_base )
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
        RvHostEntry &host = this->host_ref( id, true );
        if ( host.data_loss != idl + odl ) {
          if ( this->mout != NULL ) {
            this->mout->printf( "! query host %08X dataloss %u -> %u\n",
                                host.host_id, host.data_loss, idl + odl );
          }
          host.state = RvHostEntry::RV_HOST_QUERY;
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
        RvSessionEntry & session  = this->session_ref( x, xlen );
        if ( session.state == RvSessionEntry::RV_SESSION_SELF )
          break;
        bool             is_added = false;
        RvSubscription & script   = this->listen_start( session, s, slen,
                                                        is_added );
        if ( this->cb != NULL ) {
          RvSubscriptionListener::Start
            op( session, script, (const char *) pub.reply,
                pub.reply_len, true );
          this->cb->on_listen_start( op );
        }
        break;
      }
      case IS_LISTEN_STOP: {
        RvSessionEntry & session = this->session_ref( x, xlen );
        if ( session.state == RvSessionEntry::RV_SESSION_SELF )
          break;
        bool             is_orphan = false;
        RvSubscription & script    = this->listen_stop( session, s, slen,
                                                        is_orphan );
        if ( this->cb != NULL ) {
          RvSubscriptionListener::Stop op( session, script, is_orphan, true );
          this->cb->on_listen_stop( op );
        }
        break;
      }
      /* _SNAP.<subject> = strip 6 char prefix */
      case IS_SNAP:
        if ( this->cb != NULL && subject_len > 6 ) {
          RvSubscription & script =
            this->snapshot( &subject[ 6 ], subject_len - 6 );
          RvSubscriptionListener::Snap
            op( script, (const char *) pub.reply, pub.reply_len, flags );
          this->cb->on_snapshot( op );
        }
        break;

      default:
        break;
    }
  }
  else {
    bool     is_host_query = ( which >= this->host_inbox_base );
    uint32_t i;
    MDName   nm;
    MDFieldReader rd( *m );

    if ( is_host_query )
      i = which - this->host_inbox_base;
    else
      i = which - this->session_inbox_base;
   /* session message:
    * ## _INBOX.C0A80010.601FB1D31D6DE.2
    * null : string  45 : "C0A80010.DAEMON.152AAF64CB09BA0X7F5B5806F680"
    * null : string  23 : "C0A80010.601FB1D31D6DE" */
    if ( is_host_query ) {
      if ( i < this->host_tab.count ) {
        RvHostEntry & host = this->host_tab.ptr[ i ];
        if ( host.state == RvHostEntry::RV_HOST_QUERY ) {
          this->mark_sessions( host );
          for ( bool b = rd.first( nm ); b; b = rd.next( nm ) ) {
            char * x    = NULL;
            size_t xlen = 0;
            if ( nm.fnamelen == 0 && rd.get_string( x, xlen ) )
              this->session_ref( x, xlen );
          }
          this->stop_marked_sessions( host );
          host.state = RvHostEntry::RV_HOST_STATUS;
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
      RvSessionEntry * session = this->get_session( i );
      if ( session != NULL &&
           session->state == RvSessionEntry::RV_SESSION_QUERY ) {
        this->mark_subscriptions( *session );
        for ( bool b = rd.first( nm ); b; b = rd.next( nm ) ) {
          char * sub    = NULL;
          size_t sub_len = 0;
          if ( nm.fnamelen == 0 && rd.get_string( sub, sub_len ) &&
               this->is_matched( sub, sub_len ) ) {
            bool is_added;
            RvSubscription & script = this->listen_ref( *session, sub, sub_len,
                                                        is_added );
            if ( is_added ) {
              if ( this->cb != NULL ) {
                RvSubscriptionListener::Start
                  op( *session, script, NULL, 0, false );
                this->cb->on_listen_start( op );
              }
            }
          }
        }
        session->state = RvSessionEntry::RV_SESSION_RV5;
        if ( session->has_daemon() )
          session->state = RvSessionEntry::RV_SESSION_RV7;
        this->stop_marked_subscriptions( *session );
      }
    }
  }
  return true;
}

void
RvSubscriptionDB::make_sync( RvMsgWriter &w ) noexcept
{
  for ( uint32_t i = 0; i < this->host_tab.count; i++ )
    this->make_host_sync( w, i );
}

bool
RvSubscriptionDB::make_host_sync( RvMsgWriter &w,  uint32_t i ) noexcept
{
  if ( i >= this->host_tab.count )
    return false;

  RvHostEntry &host = this->host_tab.ptr[ i ];
  if ( host.state == RvHostEntry::RV_HOST_UNKNOWN ||
       host.state == RvHostEntry::RV_HOST_STOP )
    return false;

  RvMsgWriter      host_sub( w.mem, NULL, 0 ),
                   sess_sub( w.mem, NULL, 0 ),
                   subs_sub( w.mem, NULL, 0 );
  RvSessionEntry * session;
  RvSubscription * script;
  size_t           pos, pos2;

  w.append_msg( SARG( "host" ), host_sub );
  host_sub.append_uint( SARG( "id" ), host.host_id );
  if ( host.data_loss != 0 )
    host_sub.append_uint( SARG( "data_loss" ), host.data_loss );

  if ( (session = this->first_session( host, pos )) != NULL ) {
    host_sub.append_msg ( SARG( "sessions" ), sess_sub );
    do {
      if ( (script = this->first_subject( *session, pos2 )) != NULL ) {
        sess_sub.append_msg( session->value, session->len, subs_sub );
        do {
          subs_sub.append_string( NULL, 0, script->value, script->len );
        } while ( (script = this->next_subject( *session, pos2 )) != NULL );
        sess_sub.update_hdr( subs_sub );
      }
      else {
        sess_sub.append_uint( session->value, session->len, (uint8_t) 0 );
      }
    } while ( (session = this->next_session( host, pos )) != NULL );
    host_sub.update_hdr( sess_sub );
  }
  w.update_hdr( host_sub );
  return true;
}

void
RvSubscriptionDB::update_sync( RvMsg &msg ) noexcept
{
  /*MDOutput mout;
  msg.print( &mout );
  mout.print_hex( &msg );*/

  MDName   nm, snm;
  MDFieldReader rd( msg );
  MDMsg * host_sub,
        * sess_sub,
        * subs_sub;

  for ( bool b = rd.first( nm ); b; b = rd.next( nm ) ) {
    if ( nm.equals( SARG( "host" ) ) && rd.get_sub_msg( host_sub ) ) {
      MDFieldReader hrd( *host_sub );
      uint32_t host_id = 0;

      for ( bool c = hrd.first( nm ); c; c = hrd.next( nm ) ) {
        if ( nm.equals( SARG( "id" ) ) )
          hrd.get_uint( host_id );

        else if ( nm.equals( SARG( "sessions" ) ) && host_id != 0 &&
                  hrd.get_sub_msg( sess_sub ) ) {
          this->host_ref( host_id, true );
          MDFieldReader srd( *sess_sub );

          for ( bool d = srd.first( snm ); d; d = srd.next( snm ) ) {
            RvSessionEntry & session =
              this->session_ref( snm.fname, snm.fnamelen - 1 );
            if ( session.state == RvSessionEntry::RV_SESSION_SELF )
              continue;

            if ( srd.type() == MD_MESSAGE && srd.get_sub_msg( subs_sub ) ) {
              MDFieldReader xrd( *subs_sub );

              for ( bool e = xrd.first( nm ); e; e = xrd.next( nm ) ) {
                char * sub    = NULL;
                size_t sublen = 0;

                if ( xrd.get_string( sub, sublen ) ) {
                  bool is_added;
                  RvSubscription & script =
                    this->listen_ref( session, sub, sublen, is_added );
                  if ( is_added ) {
                    if ( this->cb != NULL ) {
                      RvSubscriptionListener::Start
                        op( session, script, NULL, 0, false );
                      this->cb->on_listen_start( op );
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}

void
RvSubscriptionDB::gc( void ) noexcept
{
  RouteLoc loc;
  uint32_t sub_rem = 0, sess_rem = 0;
  if ( this->subscriptions.removed > this->subscriptions.active ) {
    this->subscriptions.removed = 0;
    for ( RvSubscription *sub = this->sub_tab.first( loc ); sub != NULL; ) {
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
    for ( RvSessionEntry *entry = this->session_tab.first( loc );
          entry != NULL; ) {
      if ( entry->state == RvSessionEntry::RV_SESSION_STOP ) {
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
      if ( this->host_tab.ptr[ i ].state == RvHostEntry::RV_HOST_STOP ) {
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
      for ( RvSessionEntry *entry = this->session_tab.first( loc );
            entry != NULL; ) {
        if ( entry->sub_ht != NULL )
          sz += entry->sub_ht->mem_size();
        entry = this->session_tab.next( loc );
      }
      this->mout->printf( "gc, rem %u subscriptions removed : %lu bytes used, "
                          "%u sessions removed : %lu bytes used, "
                          "other %lu bytes used\n",
                          sub_rem, this->sub_tab.mem_size(), sess_rem, 
                          this->sub_tab.mem_size(),
                          this->host_tab.size * sizeof( RvHostEntry ) +
                          this->host_ht->mem_size() +
                          this->sess_ht->mem_size() + sz );
    }
  }
}

