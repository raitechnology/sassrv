#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <tibrv/tibrv.h>
#include <raimd/md_msg.h>
#include <raimd/md_dict.h>
#include <raimd/cfile.h>
#include <raimd/app_a.h>
#include <raimd/enum_def.h>
#include <raikv/ev_publish.h>
#include <raikv/bit_set.h>
#include <raikv/zipf.h>
#include <raikv/uint_ht.h>
#include <raikv/key_hash.h>
#include <raikv/array_space.h>

using namespace rai;
using namespace kv;
using namespace md;
static const size_t   MAX_RV_INBOX_LEN = 64;
static const char     DICT_SUBJ[]     = "_TIC.REPLY.SASS.DATA.DICTIONARY";
static const int      DICT_SUBJ_LEN   = sizeof( DICT_SUBJ ) - 1;
                                           /* _INBOX.<session>.1   = control */
static const uint64_t DICT_INBOX_ID   = 2, /* _INBOX.<session>.2   = dictionary*/
                      SUB_INBOX_BASE  = 3; /* _INBOX.<session>.3++ = sub[] */
static const uint32_t DICT_TIMER_SECS = 3,
                      RATE_TIMER_SECS = 1;
static const uint64_t FIRST_TIMER_ID  = 1, /* first dict request */
                      SECOND_TIMER_ID = 2, /* second dict request */
                      RATE_TIMER_ID   = 3, /* rate timer */
                      STOP_TIMER_ID   = 4; /* stop timer */
struct LogOutput : public MDOutput {
  char     ts_buf[ 64 ];
  uint64_t ts_time;
  bool     add_timestamp;

  LogOutput() : ts_time( 0 ), add_timestamp( false ) {}
  LogOutput &ts( void ) noexcept;
};

LogOutput &
LogOutput::ts( void ) noexcept
{
  if ( this->add_timestamp ) {
    uint64_t cur_time = current_realtime_ns();
    if ( ( cur_time >> 19 ) > ( this->ts_time >> 19 ) ) {
      this->ts_time = cur_time;
      timestamp( cur_time, 3, this->ts_buf, sizeof( this->ts_buf ),
                 "%m%d %H:%M:%S" );
      size_t len = ::strlen( this->ts_buf );
      this->ts_buf[ len++ ] = ' ';
      this->ts_buf[ len++ ] = ' ';
      this->ts_buf[ len ] = '\0';
    }
    this->puts( this->ts_buf );
  }
  return *this;
}

LogOutput out;
int quit;

static void
check( tibrv_status status,  bool is_timer = false ) noexcept
{
  if ( status != TIBRV_OK && ( ! is_timer || status != TIBRV_TIMEOUT ) ) {
    out.ts().printe( "tibrv error %d: %s\n", status,
                     tibrvStatus_GetText( status ) );
    out.flush();
    exit( 1 );
  }
}

struct RvDataCallback;
struct MsgCB {
  RvDataCallback & cb;
  tibrvId sub;
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  MsgCB( RvDataCallback & c ) : cb( c ), sub( 0 ) {}
};

struct TimerCB {
  RvDataCallback & cb;
  tibrvId tm;
  uint64_t timer_id, event_id;
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  TimerCB( RvDataCallback & c,  uint64_t tid ) :
    cb( c ), tm( 0 ), timer_id( tid ), event_id( 0 ) {}
};

static const double histogram[] = {
  0.0001, 0.001, 0.01, 0.1, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0
};
static const uint32_t histogram_size = sizeof( histogram ) /
                                       sizeof( histogram[ 0 ] );
/* rv client callback closure */
struct RvDataCallback {
  tibrvQueue     q;
  tibrvTransport t;
  MDDict      * dict;            /* dictinary to use for decoding msgs */
  const char ** sub;             /* subject strings */
  size_t        sub_count,       /* count of sub[] */
                num_subs;
  uint64_t      msg_count,
                last_count,
                last_time,
                msg_bytes,
                last_bytes;
  bool          no_dictionary,   /* don't request dictionary */
                is_subscribed,   /* sub[] are subscribed */
                have_dictionary, /* set when dict request succeeded */
                dump_hex,        /* print hex of message data */
                show_rate;       /* show rate of messages recvd */
  char        * subj_buf;
  uint32_t    * rand_schedule,
              * msg_recv;
  size_t      * msg_recv_bytes;
  UIntHashTab * subj_ht,
              * coll_ht,
              * ibx_ht;
  size_t        rand_range;
  uint32_t      max_time_secs,
                msg_type_cnt[ 32 ];
  bool          use_random,
                use_zipf,
                quiet,
                track_time;
  uint64_t      seed1, seed2;
  ArrayCount<MsgCB *,64>  sub_cb,
                          ibx_cb;
  ArrayCount<double,64>   next_pub;
  ArrayCount<uint64_t,64> last_seqno;
  double        total_latency,
                miss_latency,
                trail_latency;
  uint64_t      total_count,
                miss_count,
                repeat_count,
                trail_count,
                initial_count,
                update_count,
                miss_seqno;
  uint64_t      hist[ histogram_size + 1 ],
                hist_count;
  double        start_time;

  RvDataCallback( const char **s,  size_t cnt,
                  bool nodict,  bool hex,  bool rate,  size_t n,  size_t rng,
                  bool zipf,  uint32_t secs,  bool q,  bool ts,
                  uint64_t s1, uint64_t s2 )
    : q( TIBRV_DEFAULT_QUEUE ), t( TIBRV_INVALID_ID ), dict( 0 ),
      sub( s ), sub_count( cnt ), num_subs( n ), msg_count( 0 ),
      last_count( 0 ), last_time( 0 ), msg_bytes( 0 ), last_bytes( 0 ),
      no_dictionary( nodict ), is_subscribed( false ), have_dictionary( false ), dump_hex( hex ),
      show_rate( rate ), subj_buf( 0 ), rand_schedule( 0 ), msg_recv( 0 ),
      msg_recv_bytes( 0 ), subj_ht( 0 ), coll_ht( 0 ), ibx_ht( 0 ),
      rand_range( rng ), max_time_secs( secs ), use_random( rng > cnt ),
      use_zipf( zipf ), quiet( q ), track_time( ts ),
      total_latency( 0 ), miss_latency( 0 ), trail_latency( 0 ),
      total_count( 0 ), miss_count( 0 ), repeat_count( 0 ), trail_count( 0 ),
      initial_count( 0 ), update_count( 0 ), miss_seqno( 0 ), hist_count( 0 ),
      start_time( 0 ) {
    ::memset( this->msg_type_cnt, 0, sizeof( this->msg_type_cnt ) );
    ::memset( this->hist, 0, sizeof( this->hist ) );
    this->seed1 = ( s1 == 0 ? current_monotonic_time_ns() : s1 );
    this->seed2 = ( s2 == 0 ? current_realtime_ns() : s2 );
  }

  void add_initial( size_t n,  size_t bytes ) {
    this->msg_recv[ n ] |= 1;
    this->msg_recv_bytes[ n ] += bytes;
    this->initial_count++;
  }
  void add_update( size_t n,  size_t bytes ) {
    this->msg_recv[ n ] += 1 << 1;
    this->msg_recv_bytes[ n ] += bytes;
    this->update_count++;
  }
  /* after CONNECTED message */
  void on_connect( void ) noexcept;
  /* start sub[] with inbox reply */
  void make_random( void ) noexcept;
  void make_subject( uint64_t n,  const char *&s,  size_t &slen ) noexcept;
  void start_subscriptions( void ) noexcept;
  /* when signalled, unsubscribe */
  void on_unsubscribe( void ) noexcept;
  /* when disconnected */
  void on_shutdown( void ) noexcept;
  void send_dict_request( void ) noexcept;
  /* dict from network */
  void on_dict( MDMsg *m ) noexcept;
  /* dict timeout */
  bool timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept;
  /* message from network */
  void on_msg( const char *subject,  size_t subject_len,
               const char *reply,  size_t reply_len,
               const void *msg,  size_t msg_len ) noexcept;
  void add_timer_seconds( uint32_t seconds,  uint64_t id ) noexcept;
  MsgCB *make_inbox( char *ibx,  uint32_t id ) noexcept;
  uint64_t is_inbox( const char *sub,  size_t sublen ) noexcept;
  MsgCB *subscribe( const char *sub,  const char *inbox ) noexcept;
  void unsubscribe( size_t n ) noexcept;
  void check_trail_time( void ) noexcept;
};

static void
onMsg( tibrvId,  tibrvMsg msg,  void *cl ) noexcept
{
  const void * bufp = NULL;
  const char * sub  = NULL,
             * rep  = NULL;
  uint32_t msglen   = 0,
           sublen   = 0,
           replen   = 0;

  check( tibrvMsg_GetByteSize( msg, &msglen ) );
  check( tibrvMsg_GetAsBytes( msg, &bufp ) );
  check( tibrvMsg_GetSendSubject( msg, &sub ) );

  if ( tibrvMsg_GetReplySubject( msg, &rep ) == TIBRV_OK )
    replen = ::strlen( rep );
  sublen = ::strlen( sub );

  ((MsgCB *) cl)->cb.on_msg( sub, sublen, rep, replen, bufp, msglen );
}

void
onTimer( tibrvId,  tibrvMsg,  void *cl )
{
  TimerCB &t = *(TimerCB *) cl;
  if ( ! t.cb.timer_cb( t.timer_id,  t.event_id ) ) {
    tibrvEvent_Destroy( t.tm );
    delete &t;
  }
}

void
RvDataCallback::add_timer_seconds( uint32_t seconds,  uint64_t id ) noexcept
{
  void * m = ::malloc( sizeof( TimerCB ) );
  TimerCB * cb = new ( m ) TimerCB( *this, id );
  check( tibrvEvent_CreateTimer( &cb->tm, this->q, onTimer, seconds, cb ) );
}

MsgCB *
RvDataCallback::make_inbox( char *ibx,  uint32_t id ) noexcept
{
  void * m = ::malloc( sizeof( MsgCB ) );
  MsgCB * cb = new ( m ) MsgCB( *this );
  check( tibrvTransport_CreateInbox( this->t, ibx, MAX_RV_INBOX_LEN ) );
  check( tibrvEvent_CreateListener( &cb->sub, this->q, onMsg, this->t,
                                    ibx, cb ) );
  size_t len = ::strlen( ibx );
  if ( this->ibx_ht == NULL )
    this->ibx_ht = UIntHashTab::resize( NULL );
  const char * ptr = ::strrchr( ibx, '.' );
  uint32_t h = kv::string_to_uint64( &ptr[ 1 ], &ibx[ len ] - &ptr[ 1 ] );
  this->ibx_ht->upsert_rsz( this->ibx_ht, h, id );
  this->ibx_cb[ id ] = cb;
  return cb;
}

uint64_t
RvDataCallback::is_inbox( const char *sub,  size_t sublen ) noexcept
{
  if ( sublen <= 2 || sub[ 0 ] != '_' || sub[ 1 ] != 'I' )
    return 0;
  const char * ptr = ::strrchr( sub, '.' );
  if ( ptr == NULL )
    return 0;
  uint32_t h = kv::string_to_uint64( &ptr[ 1 ], &sub[ sublen ] - &ptr[ 1 ] ),
           val;
  size_t   pos;
  if ( this->ibx_ht != NULL && this->ibx_ht->find( h, pos, val ) )
    return val;
  return 0;
}

MsgCB *
RvDataCallback::subscribe( const char *sub,  const char *inbox ) noexcept
{
  void * m = ::malloc( sizeof( MsgCB ) );
  MsgCB * cb = new ( m ) MsgCB( *this );
  check( tibrvEvent_CreateListener( &cb->sub, this->q, onMsg, this->t,
                                    sub, cb ) );

  CatMalloc snap( ::strlen( sub ) + 8 );
  snap.s( "_SNAP." ).s( sub ).end();
  tibrvMsg msg = NULL;
  check( tibrvMsg_Create( &msg ) );
  check( tibrvMsg_SetSendSubject( msg, snap.start ) );
  check( tibrvMsg_SetReplySubject( msg, inbox ) );
  check( tibrvMsg_AddU16( msg, "flags", 6 ) );
  check( tibrvTransport_Send( this->t, msg ) );
  tibrvMsg_Destroy( msg );

  return cb;
}

void
RvDataCallback::unsubscribe( size_t n ) noexcept
{
  MsgCB * cb = this->sub_cb[ n ];
  check( tibrvEvent_Destroy( cb->sub ) );
  cb->sub = TIBRV_INVALID_ID;
}

/* called after daemon responds with CONNECTED message */
void
RvDataCallback::on_connect( void ) noexcept
{
  out.ts().printf( "Connected\n" );
  if ( this->use_random ) {
    out.ts().printf( "Random seed1: 0x%016lx seed2 0x%016lx\n", this->seed1,
                     this->seed2 );
  }
  out.flush();

  if ( ! this->no_dictionary ) {
    /* if no cfile dict, request one */
    if ( this->dict == NULL || this->dict->dict_type[ 0 ] != 'c' ) {
      this->send_dict_request();
      this->add_timer_seconds( DICT_TIMER_SECS, FIRST_TIMER_ID );
      return;
    }
  }
  this->start_subscriptions();
}

void
RvDataCallback::make_random( void ) noexcept
{
  BitSpace used;
  size_t   n     = this->num_subs,
           size  = this->sub_count * n,
           range = this->rand_range * this->sub_count;
  kv::rand::xoroshiro128plus rand;
  ZipfianGen<99, 100, kv::rand::xoroshiro128plus> zipf( range, rand ) ;
  if ( range < size )
    range = size;
  rand.static_init( this->seed1, this->seed2 );

  this->rand_schedule = (uint32_t *)
    ::malloc( sizeof( this->rand_schedule[ 0 ] ) * size );
  for ( size_t cnt = 0; cnt < size; ) {
    size_t n;
    if ( this->use_zipf )
      n = zipf.next() % range;
    else
      n = rand.next() % range;
    while ( used.test_set( n ) )
      n = ( n + 1 ) % range;
    this->rand_schedule[ cnt++ ] = n;
  }
}

void
RvDataCallback::make_subject( uint64_t n,  const char *&s, size_t &slen ) noexcept
{
  if ( this->use_random ) {
    if ( this->rand_schedule == NULL )
      this->make_random();
    n = this->rand_schedule[ n ];
  }
  size_t j = n % this->sub_count;
  n   /= this->sub_count;
  s    = this->sub[ j ];
  slen = ::strlen( s );

  const char * p = ::strstr( s, "%d" );
  if ( this->num_subs > 1 || p != NULL ) {
    if ( this->subj_buf == NULL || slen + 12 > 1024 ) {
      size_t len = ( slen + 12 < 1024 ? 1024 : slen + 12 );
      this->subj_buf = (char *) ::realloc( this->subj_buf, len );
    }
    CatPtr cat( this->subj_buf );
    const char * e = &s[ slen ];
    if ( p == NULL ) {
      cat.x( s, e - s ).s( "." ).u( n );
    }
    else {
      cat.x( s, p - s ).u( n );
      p += 2;
      if ( p < e )
        cat.x( p, e - p );
    }
    s    = cat.start;
    slen = cat.end();
  }
}

/* start subscriptions from command line, inbox number indexes the sub[] */
void
RvDataCallback::start_subscriptions( void ) noexcept
{
  if ( this->is_subscribed ) /* subscribing multiple times is allowed, */
    return;                  /* but must unsub multiple times as well */

  this->start_time = current_realtime_s();
  this->subj_ht = UIntHashTab::resize( NULL );
  this->coll_ht = UIntHashTab::resize( NULL );
  size_t n   = this->sub_count * this->num_subs;
  size_t sz  = sizeof( this->msg_recv[ 0 ] ) * n,
         sz2 = sizeof( this->msg_recv_bytes[ 0 ] ) * n;
  this->msg_recv = (uint32_t *) ::malloc( sz );
  this->msg_recv_bytes = (size_t *) ::malloc( sz2 );
  ::memset( this->msg_recv, 0, sz );
  ::memset( this->msg_recv_bytes, 0, sz2 );

  for ( n = 0; ; ) {
    for ( size_t j = 0; j < this->sub_count; j++, n++ ) {
      char         inbox[ MAX_RV_INBOX_LEN ]; /* _INBOX.<session>.3 + <sub> */
      const char * subject;
      size_t       subject_len;

      this->make_inbox( inbox, n + SUB_INBOX_BASE );
      this->make_subject( n, subject, subject_len );

      out.ts().printf( "Subscribe \"%s\", reply \"%s\"\n",
                       subject, inbox );
      /* subscribe with inbox reply */
      MsgCB *cb = this->subscribe( subject, inbox );

      uint32_t h = kv_crc_c( subject, subject_len, 0 ), val;
      size_t   pos;
      if ( this->subj_ht->find( h, pos, val ) ) {
        const char * coll_sub;
        size_t       coll_sublen;
        this->subj_ht->remove( pos );

        this->make_subject( val, coll_sub, coll_sublen );
        h = kv_djb( coll_sub, coll_sublen );
        if ( ! this->coll_ht->find( h, pos ) )
          this->coll_ht->set_rsz( this->coll_ht, h, pos, val );
        else {
          out.ts().printf( "HT collision \"%.*s\", no update tracking\n",
                           (int) coll_sublen, coll_sub );
        }
        h = kv_djb( subject, subject_len );
        if ( ! this->coll_ht->find( h, pos ) )
          this->coll_ht->set_rsz( this->coll_ht, h, pos, n );
        else {
          out.ts().printf( "HT collision \"%.*s\", no update tracking\n",
                           (int) subject_len, subject );
        }
      }
      else {
        this->subj_ht->set_rsz( this->subj_ht, h, pos, n );
      }
      this->sub_cb[ n ] = cb;
    }
    if ( n >= this->num_subs * this->sub_count )
      break;
  }
  out.flush();

  if ( this->show_rate ) {
    this->last_time = current_realtime_ns();
    this->add_timer_seconds( RATE_TIMER_SECS, RATE_TIMER_ID );
  }
  if ( this->max_time_secs != 0 ) {
    this->add_timer_seconds( this->max_time_secs, STOP_TIMER_ID );
  }
  this->is_subscribed = true;
}

/* if ctrl-c, program signalled, unsubscribe the subs */
void
RvDataCallback::on_unsubscribe( void ) noexcept
{
  if ( ! this->is_subscribed )
    return;
  if ( this->track_time )
    this->check_trail_time();
  this->is_subscribed = false;
  for ( size_t n = 0; ; ) {
    for ( size_t j = 0; j < this->sub_count; j++, n++ ) {
      const char * subject;
      size_t       subject_len;
      this->make_subject( n, subject, subject_len );
      out.ts().printf( "Unsubscribe \"%.*s\" initial=%u update=%u bytes=%u\n",
                       (int) subject_len, subject, this->msg_recv[ n ] & 1,
                       this->msg_recv[ n ] >> 1,
                       (uint32_t) this->msg_recv_bytes[ n ] );
      /* subscribe with inbox reply */
      this->unsubscribe( n );
    }
    if ( n >= this->num_subs * this->sub_count )
      break;
  }
  out.flush();
}

/* when dict message is replied */
void
RvDataCallback::on_dict( MDMsg *m ) noexcept
{
  if ( m == NULL ) {
    out.ts().printe( "Dict unpack error\n" );
    return;
  }
  if ( this->have_dictionary )
    return;
  MDDictBuild dict_build;
  if ( CFile::unpack_sass( dict_build, m ) != 0 ) {
    out.ts().printe( "Dict index error\n" );
    return;
  }
  dict_build.index_dict( "cfile", this->dict );
  this->have_dictionary = true;
}

/* publish rpc to dict subject */
void
RvDataCallback::send_dict_request( void ) noexcept
{
  char inbox[ MAX_RV_INBOX_LEN ];
  this->make_inbox( inbox, DICT_INBOX_ID );

  tibrvMsg msg = NULL;
  check( tibrvMsg_Create( &msg ) );
  check( tibrvMsg_SetSendSubject( msg, DICT_SUBJ ) );
  check( tibrvMsg_SetReplySubject( msg, inbox ) );
  check( tibrvTransport_Send( this->t, msg ) );
  tibrvMsg_Destroy( msg );
}

/* dict timer expired */
bool
RvDataCallback::timer_cb( uint64_t timer_id,  uint64_t ) noexcept
{
  if ( timer_id == RATE_TIMER_ID ) {
    uint64_t ival_ns = current_realtime_ns() - this->last_time,
             count   = this->msg_count - this->last_count,
             bytes   = this->msg_bytes - this->last_bytes;
    if ( this->last_count < this->msg_count ) {
      out.ts().printf( "%.2f m/s %.2f mbit/s\n",
                       (double) count * 1000000000.0 / (double) ival_ns,
                       (double) bytes * 8.0 * 1000.0 / ival_ns );
    }
    this->last_time  += ival_ns;
    this->last_count += count;
    this->last_bytes += bytes;
    return true;
  }
  if ( timer_id == STOP_TIMER_ID ) {
    this->on_unsubscribe();
    if ( quit == 0 )
      quit = 1; /* causes poll loop to exit */
    return false;
  }
  if ( this->have_dictionary )
    return false;
  if ( timer_id == FIRST_TIMER_ID ) {
    out.ts().printf( "Dict request timeout, trying again\n" );
    this->send_dict_request();
    this->add_timer_seconds( DICT_TIMER_SECS, SECOND_TIMER_ID );
  }
  else if ( timer_id == SECOND_TIMER_ID ) {
    out.ts().printf( "Dict request timeout again, starting subs\n" );
    this->start_subscriptions();
  }
  return false; /* return false to disable recurrent timer */
}

/* when client connection stops */
void
RvDataCallback::on_shutdown( void ) noexcept
{
  out.ts().printf( "Shutdown\n" );
}

void
RvDataCallback::on_msg( const char *subject,  size_t subject_len,
                        const char *reply,  size_t reply_len,
                        const void *msg,  size_t msg_len ) noexcept
{
  size_t   pub_len = msg_len + subject_len;
  MDMsgMem mem;
  MDMsg  * m;
  uint64_t which      = this->is_inbox( subject, subject_len );
  uint32_t sub_idx    = 0;
  bool     have_sub   = false,
           is_initial = false;
  /* check if published to _INBOX.<session>. */
  if ( which != 0 ) {
    if ( which >= SUB_INBOX_BASE ) {
      uint64_t n = which - SUB_INBOX_BASE;
      this->make_subject( n, subject, subject_len );
      this->add_initial( n, pub_len );
      sub_idx  = n;
      have_sub = true;
      is_initial = true;
    }
    else if ( which == DICT_INBOX_ID ) {
      out.ts().printf( "Received dictionary message (len=%u)\n",
                       (uint32_t) pub_len );
      m = MDMsg::unpack( (void *) msg, 0, msg_len, 0, this->dict, &mem );
      this->on_dict( m );
      this->start_subscriptions();
      return;
    }
  }
  if ( which == 0 || which >= SUB_INBOX_BASE ) {
    if ( which == 0 ) {
      size_t pos;
      if ( this->subj_ht->find( kv_crc_c( subject, subject_len, 0 ), pos,
                                sub_idx ) ) {
        this->add_update( sub_idx, pub_len );
        have_sub = true;
      }
      else if ( this->coll_ht->find( kv_djb( subject, subject_len ), pos,
                                     sub_idx ) ) {
        this->add_update( sub_idx, pub_len );
        have_sub = true;
      }
    }
    if ( this->show_rate ) {
      this->msg_count++;
      this->msg_bytes += pub_len;
      return;
    }
  }
  m = MDMsg::unpack( (void *) msg, 0, msg_len, 0, this->dict, &mem );
  MDFieldIter * iter = NULL;
  double delta = 0, next_delta = 0, duration = 0;
  uint64_t  sequence = 0, last_sequence = 0;
  if ( m != NULL && m->get_field_iter( iter ) == 0 && iter->first() == 0 ) {
    MDName nm;
    MDReference mref;
    if ( iter->get_name( nm ) == 0 && iter->get_reference( mref ) == 0 ) {
      static const char msg_type[] = "MSG_TYPE";
      const MDName mtype( msg_type, sizeof( msg_type ) );
      if ( nm.equals( mtype ) &&
           ( mref.ftype == MD_UINT || mref.ftype == MD_INT ) ) {
        uint16_t t = get_int<uint16_t>( mref );
        this->msg_type_cnt[ t < 31 ? t : 31 ]++;
      }
      if ( have_sub && this->track_time ) {
        static const char volume[] = "VOLUME",
                          durati[] = "DURATION",
                          seqno[]  = "SEQ_NO";
        const MDName vol( volume, sizeof( volume ) ),
                     dur( durati, sizeof( durati ) ),
                     seq( seqno, sizeof( seqno ) );
        MDDecimal dec;
        double    next = 0, val, last, cur;
        while ( iter->next() == 0 && iter->get_name( nm ) == 0 ) {
          bool is_volume   = nm.equals( vol ),
               is_duration = false,
               is_seqno    = false;
          if ( ! is_volume ) {
            is_duration = nm.equals( dur );
            if ( ! is_duration )
              is_seqno = nm.equals( seq );
          }
          if ( ( is_volume | is_duration | is_seqno ) == true ) {
            if ( iter->get_reference( mref ) == 0 ) {
              if ( is_seqno ) {
                if ( mref.ftype == MD_INT || mref.ftype == MD_UINT ) {
                  sequence = get_uint<uint64_t>( mref );
                }
              }
              else {
                if ( mref.ftype == MD_DECIMAL ) {
                  dec.get_decimal( mref );
                  if ( dec.get_real( val ) == 0 ) {
                    if ( is_duration )
                      duration = val;
                    next += val;
                  }
                }
                else if ( mref.ftype == MD_REAL ) {
                  val = get_float<double>( mref );
                  if ( is_duration )
                    duration = val;
                  next += val;
                }
              }
            }
          }
        }
        last = this->next_pub[ sub_idx ]; /* last should be close to current */
        last_sequence = this->last_seqno[ sub_idx ];
        cur = current_realtime_s();
        if ( last != 0 ) {
          bool missed = ( sequence > ( last_sequence + 1 ) );
          delta = cur - last; /* positive is secs in before current time */
          if ( ! missed && delta > 0 ) {
            uint32_t i = 0;
            for ( ; i < histogram_size; i++ ) {
              if ( delta <= histogram[ i ] ) {
                this->hist[ i ]++;
                break;
              }
            }
            if ( i == histogram_size )
              this->hist[ i ]++;
            this->hist_count++;
          }
#if 0
          if ( missed ) {
            if ( this->quiet ) {
              out.ts().printf(
"## %.*s delta %.6f (expected=%.6f, current=%.6f, seqno=%lu, last_seqno=%lu, missed=%lu)\n",
                (int) subject_len, subject, delta, last, cur, sequence,
                last_sequence, ( sequence - ( last_sequence + 1 ) ) );
            }
          }
#endif
          if ( missed ) {
            this->miss_latency += delta;
            this->miss_count++;
            this->miss_seqno += sequence - ( last_sequence + 1 );
          }
          else {
            /* initial and update with same seqno */
            if ( sequence == ( last_sequence + 1 ) ) {
              this->total_latency += delta;
              this->total_count++;
            }
            else {
              this->repeat_count++;
            }
          }
        }
        if ( duration == 0 || ( is_initial && next < cur ) )
          next = 0;
        else
          next_delta = next - cur;
        this->next_pub.ptr[ sub_idx ] = next;
        this->last_seqno.ptr[ sub_idx ] = sequence;
      }
    }
  }
  if ( this->quiet )
    return;
  if ( delta != 0 || next_delta != 0 ) {
    out.ts().printf( "## update latency %.6f, next expected %f\n", delta,
                     next_delta );
  }
  if ( last_sequence != 0 && sequence != 0 ) {
    out.ts().printf( "## last seqno %lu, current seqno %lu\n", last_sequence,
                     sequence );
  }
  if ( which >= SUB_INBOX_BASE ) {
    out.ts().printf( "## %.*s: (inbox: %.*s)\n", (int) subject_len, subject,
                     (int) subject_len, subject );
  }
  else { /* not inbox subject */
    if ( reply_len != 0 )
      out.ts().printf( "## %.*s (reply: %.*s):\n", (int) subject_len, subject,
                       (int) reply_len, reply );
    else
      out.ts().printf( "## %.*s:\n", (int) subject_len, subject );
  }
  /* print message */
  if ( m != NULL ) {
    out.ts().printf( "## format: %s, length %u\n", m->get_proto_string(),
                     (uint32_t) msg_len );
    m->print( &out );
    if ( this->dump_hex )
      out.print_hex( m );
  }
  else if ( msg_len == 0 )
    out.ts().printf( "## No message data\n" );
  else
    out.ts().printe( "Message unpack error\n" );
  out.flush();
  return;
}

void
RvDataCallback::check_trail_time( void ) noexcept
{
  const char * subject;
  size_t       subject_len;
  double       cur, next, delta;
  uint64_t     seqno;
  cur = current_realtime_s();

  for ( uint32_t sub_idx = 0; sub_idx < this->next_pub.count; sub_idx++ ) {
    next = this->next_pub.ptr[ sub_idx ];
    seqno = this->last_seqno[ sub_idx ];
    if ( next != 0 ) {
      delta = cur - next; /* if exepcted next more than 5.0 seconds ago */
      if ( delta > 5.0 ) {
        this->make_subject( sub_idx, subject, subject_len );
        out.ts().printf(
"## %.*s delta %.6f (expected=%.6f, current=%.6f, last_seqno=%lu) (trail)\n",
          (int) subject_len, subject, delta, next, cur, seqno );
        this->trail_latency += delta;
        this->trail_count++;
      }
    }
  }
}


static const char *
get_arg( int &x, int argc, const char *argv[], int b, const char *f,
         const char *g, const char *def ) noexcept
{
  for ( int i = 1; i < argc - b; i++ ) {
    if ( ::strcmp( f, argv[ i ] ) == 0 || ::strcmp( g, argv[ i ] ) == 0 ) {
      if ( x < i + b + 1 )
        x = i + b + 1;
      return argv[ i + b ];
    }
  }
  return def; /* default value */
}
int
main( int argc, const char *argv[] )
{
  SignalHandler sighndl;
  int x = 1;
  const char * daemon  = get_arg( x, argc, argv, 1, "-d", "-daemon", "tcp:7500" ),
             * network = get_arg( x, argc, argv, 1, "-n", "-network", ""),
             * service = get_arg( x, argc, argv, 1, "-s", "-service", "7500" ),
             * path    = get_arg( x, argc, argv, 1, "-c", "-cfile", NULL ),
             * nodict  = get_arg( x, argc, argv, 0, "-x", "-nodict", NULL ),
             * dump    = get_arg( x, argc, argv, 0, "-e", "-hex", NULL ),
             * rate    = get_arg( x, argc, argv, 0, "-r", "-rate", NULL ),
             * sub_cnt = get_arg( x, argc, argv, 1, "-k", "-count", NULL ),
             * zipf    = get_arg( x, argc, argv, 0, "-Z", "-zipf", NULL ),
             * time    = get_arg( x, argc, argv, 1, "-t", "-secs", NULL ),
             * quiet   = get_arg( x, argc, argv, 0, "-q", "-quiet", NULL ),
             * s1      = get_arg( x, argc, argv, 1, "-S", "-seed1", NULL ),
             * s2      = get_arg( x, argc, argv, 1, "-T", "-seed2", NULL ),
             * use_ts  = get_arg( x, argc, argv, 0, "-A", "-stamp", NULL ),
             * log     = get_arg( x, argc, argv, 1, "-l", "-log", NULL ),
             * help    = get_arg( x, argc, argv, 0, "-h", "-help", 0 );
  int first_sub = x;
  size_t cnt = 1, range = 0, secs = 0;
  uint64_t seed1 = 0, seed2 = 0;
  MDOutput m;

  if ( help != NULL ) {
  help:;
    fprintf( stderr,
 "%s [-d daemon] [-n network] [-s service] [-c cfile_path] [-x] [-e] subject ...\n"
             "  -d daemon  = daemon port to connect\n"
             "  -n network = network\n"
             "  -s service = service\n"
             "  -c cfile   = if loading dictionary from files\n"
             "  -x         = don't load a dictionary\n"
             "  -e         = show hex dump of messages\n"
             "  -r         = show rate of messages\n"
             "  -k         = subscribe to numeric subjects (subject.%%d)\n"
             "  -Z         = use zipf(0.99) distribution\n"
             "  -t secs    = stop after seconds expire\n"
             "  -q         = quiet, don't print messages\n"
             "  -S hex     = random seed1\n"
             "  -T hex     = random seed2\n"
             "  -A         = track timestamp in message\n"
             "  -l log     = output to log with time\n"
             "  subject    = subject to subscribe\n", argv[ 0 ] );
    return 1;
  }
  if ( first_sub >= argc ) {
    fprintf( stderr, "No subjects subscribed\n" );
    goto help;
  }
  bool valid = true;
  if ( sub_cnt != NULL ) {
    size_t       len = ::strlen( sub_cnt );
    const char * p   = ::strchr( sub_cnt, ':' ),
               * e   = &sub_cnt[ len ];
    if ( p == NULL )
      p = e;
    valid = valid_uint64( sub_cnt, p - sub_cnt );
    if ( &p[ 1 ] < e )
      valid = valid_uint64( &p[ 1 ], e - &p[ 1 ] );

    if ( valid ) {
      cnt = string_to_uint64( sub_cnt, p - sub_cnt );
      if ( &p[ 1 ] < e )
        range = string_to_uint64( &p[ 1 ], e - &p[ 1 ] );
      else
        range = cnt;
      if ( cnt == 0 || range < cnt )
        valid = false;
    }
    if ( ! valid ) {
      fprintf( stderr, "Invalid -k/-count\n" );
      goto help;
    }
  }
  if ( time != NULL ) {
    MDStamp stamp;
    if( stamp.parse( time, ::strlen( time ), true ) != 0 )
      valid = false;
    if ( ! valid ) {
      fprintf( stderr, "Invalid -t/-secs\n" );
      goto help;
    }
    secs = stamp.seconds();
  }
  if ( s1 != NULL || s2 != NULL ) {
    if ( s1 == NULL || ! valid_uint64( s1, ::strlen( s1 ) ) ||
         s2 == NULL || ! valid_uint64( s2, ::strlen( s2 ) ) ) {
      fprintf( stderr, "Invalid -S/-T/-seed1/-seed2\n" );
      goto help;
    }
    seed1 = string_to_uint64( s1, ::strlen( s1 ) );
    seed2 = string_to_uint64( s2, ::strlen( s2 ) );
  }
  if ( log != NULL ) {
    out.open( log, "a" );
    out.add_timestamp = true;
  }

  RvDataCallback data( &argv[ first_sub ], argc - first_sub,
                       nodict != NULL, dump != NULL, rate != NULL, cnt,
                       range, zipf != NULL, secs, quiet != NULL,
                       use_ts != NULL, seed1, seed2 );
  /* load dictionary if present */
  if ( ! data.no_dictionary ) {
    if ( path != NULL || (path = ::getenv( "cfile_path" )) != NULL ) {
      MDDictBuild dict_build;
      /*dict_build.debug_flags = MD_DICT_PRINT_FILES;*/
      if ( AppA::parse_path( dict_build, path, "RDMFieldDictionary" ) == 0 ) {
        EnumDef::parse_path( dict_build, path, "enumtype.def" );
        dict_build.index_dict( "app_a", data.dict );
      }
      dict_build.clear_build();
      if ( CFile::parse_path( dict_build, path, "tss_fields.cf" ) == 0 ) {
        CFile::parse_path( dict_build, path, "tss_records.cf" );
        dict_build.index_dict( "cfile", data.dict );
      }
      /* must have a cfile dictionary (app_a is for marketfeed) */
      if ( data.dict != NULL && data.dict->dict_type[ 0 ] == 'c' ) {
        out.ts().printf( "Loaded dictionary from cfiles\n" );
        data.have_dictionary = true;
      }
    }
  }
  /* handle ctrl-c */
  sighndl.install();
  check( tibrv_Open() );
  check( tibrvTransport_Create( &data.t, service, network, daemon ) );
  data.on_connect();
  for (;;) {
    check( tibrvQueue_TimedDispatch( data.q, 0.05 ), true );
    /* loop 5 times before quiting, time to flush writes */
    if ( quit >= 5 )
      break;
    if ( quit || sighndl.signaled ) {
      if ( quit == 0 )
        data.on_unsubscribe();
      quit++;
    }
  }
  bool first = true;
  for ( uint32_t i = 0; i < 32; i++ ) {
    if ( data.msg_type_cnt[ i ] != 0 ) {
      char buf[ 32 ];
      if ( first ) {
        out.ts();
        first = false;
      }
      else {
        out.puts( ", " );
      }
      out.printf( "%u (%s): %u", i, md_sass_msg_type_str( i, buf ),
                   data.msg_type_cnt[ i ] );
    }
  }
  if ( ! first )
    out.puts( "\n" );
  if ( data.track_time && data.start_time != 0 ) {
    double lat = data.total_latency;
    if ( data.total_count > 0 )
      lat /= (double) data.total_count;
    uint64_t track_count =
      data.total_count + data.miss_count + data.trail_count + data.repeat_count;
    double t = current_realtime_s() - data.start_time;
    const char *u = "seconds";
    if ( t > 4 * 60 ) {
      t /= 60;
      u = "minutes";
      if ( t > 3 * 60 ) {
        t /= 60;
        u = "hours";
        if ( t > 2 * 24 ) {
          t /= 24;
          u = "days";
        }
      }
    }
    out.ts().printf( "Run time %f %s\n", t, u );
    out.ts().printf( "Initial inbox msgs %lu, update bcast msgs %lu\n",
                     data.initial_count, data.update_count );
    out.ts().printf( "Avg latency %.6f updates, %lu of %lu tracked\n",
                     lat, data.total_count, track_count );
    lat = data.miss_latency;
    if ( data.miss_count > 0 )
      lat /= (double) data.miss_count;
    out.ts().printf(
    "Miss latency %.6f miss count %lu miss seqno %lu, repeat seqno %lu\n", lat,
                     data.miss_count, data.miss_seqno, data.repeat_count );
    lat = data.trail_latency;
    if ( data.trail_count > 0 )
      lat /= (double) data.trail_count;
    out.ts().printf( "Trail latency %.6f trail count %lu\n", lat,
                     data.trail_count );
    uint32_t i = 0;
    for ( ; i < histogram_size; i++ ) {
      if ( data.hist[ i ] != 0 ) {
        out.printf( "<= %f : %lu (%.2f%%)\n",
          histogram[ i ], data.hist[ i ],
          (double) data.hist[ i ] * 100.0 / (double) data.hist_count );
      }
    }
    if ( data.hist[ i ] != 0 ) {
      out.printf( " > %f : %lu (%.2f%%)\n",
        histogram[ i-1 ], data.hist[ i ],
        (double) data.hist[ i ] * 100.0 / (double) data.hist_count );
    }
  }
  if ( data.use_random ) {
    out.ts().printf( "Random seed1: 0x%016lx seed2 0x%016lx\n", data.seed1,
                     data.seed2 );
  }
  return 0;
}

