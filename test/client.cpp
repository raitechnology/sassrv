#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sassrv/ev_rv_client.h>
#include <raimd/md_msg.h>
#include <raimd/md_dict.h>
#include <raimd/cfile.h>
#include <raimd/app_a.h>
#include <raimd/enum_def.h>
#include <raikv/ev_publish.h>
#include <raikv/bit_set.h>
#include <raikv/zipf.h>

using namespace rai;
using namespace kv;
using namespace sassrv;
using namespace md;

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

struct SubjHT {
  UInt64HashTab * ht;
  SubjHT() : ht( 0 ) {
    this->ht = UInt64HashTab::resize( NULL );
  }

  bool find( const char *sub,  size_t sublen,  uint32_t &val ) {
    uint64_t h = kv_hash_murmur64( sub, sublen, 0 ), val64;
    size_t pos;
    if ( ! this->ht->find( h, pos, val64 ) )
      return false;
    val = val64;
    return true;
  }
  void upsert( const char *sub,  size_t sublen,  uint32_t val ) {
    uint64_t h = kv_hash_murmur64( sub, sublen, 0 );
    this->ht->upsert_rsz( this->ht, h, val );
  }
};

static const double histogram[] = {
  0.0001, 0.001, 0.01, 0.1, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0
};
static const uint32_t histogram_size = sizeof( histogram ) /
                                       sizeof( histogram[ 0 ] );

/* rv client callback closure */
struct RvDataCallback : public EvConnectionNotify, public RvClientCB,
                        public EvTimerCallback {
  EvPoll      & poll;            /* poll loop data */
  EvRvClient  & client;          /* connection to rv */
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
                show_rate,       /* show rate of messages recvd */
                ignore_not_found;
  char        * subj_buf;
  uint32_t    * rand_schedule,
              * msg_recv;
  size_t      * msg_recv_bytes;
  SubjHT        subj_ht;
  size_t        rand_range;
  uint32_t      max_time_secs,
                msg_type_cnt[ 32 ],
                sub_initial_count,
                sub_update_count;
  bool          use_random,
                use_zipf,
                quiet,
                track_time,
                wild_subscribe;
  uint64_t      seed1, seed2;
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

  RvDataCallback( EvPoll &p,  EvRvClient &c,  const char **s,  size_t cnt,
                  bool nodict,  bool hex,  bool rate,  size_t n,  size_t rng,
                  bool zipf,  uint32_t secs,  bool q,  bool ts,
                  uint64_t s1, uint64_t s2 )
    : poll( p ), client( c ), dict( 0 ), sub( s ), sub_count( cnt ),
      num_subs( n ), msg_count( 0 ), last_count( 0 ), last_time( 0 ),
      msg_bytes( 0 ), last_bytes( 0 ), no_dictionary( nodict ),
      is_subscribed( false ), have_dictionary( false ), dump_hex( hex ),
      show_rate( rate ), ignore_not_found( true ), subj_buf( 0 ),
      rand_schedule( 0 ), msg_recv( 0 ), msg_recv_bytes( 0 ), rand_range( rng ),
      max_time_secs( secs ), sub_initial_count( 0 ), sub_update_count( 0 ),
      use_random( rng > n ), use_zipf( zipf ),
      quiet( q ), track_time( ts ), wild_subscribe( false ),
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
  virtual void on_connect( EvSocket &conn ) noexcept;
  /* start sub[] with inbox reply */
  void make_random( void ) noexcept;
  void make_subject( size_t n,  const char *&s,  size_t &slen ) noexcept;
  void start_subscriptions( void ) noexcept;
  /* when signalled, unsubscribe */
  void on_unsubscribe( void ) noexcept;
  /* when disconnected */
  virtual void on_shutdown( EvSocket &conn,  const char *err,
                            size_t err_len ) noexcept;
  void send_dict_request( void ) noexcept;
  /* dict from network */
  void on_dict( MDMsg *m ) noexcept;
  /* dict timeout */
  virtual bool timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept;
  /* message from network */
  virtual bool on_rv_msg( EvPublish &pub ) noexcept;
  void check_trail_time( void ) noexcept;
};

/* called after daemon responds with CONNECTED message */
void
RvDataCallback::on_connect( EvSocket &conn ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  out.ts().printf( "Connected: %.*s\n", len, conn.peer_address.buf );
  if ( this->use_random ) {
    out.ts().printf( "Random seed1: 0x%016lx seed2 0x%016lx\n", this->seed1,
                     this->seed2 );
  }
  out.flush();

  if ( ! this->no_dictionary ) {
    /* if no cfile dict, request one */
    if ( this->dict == NULL || this->dict->dict_type[ 0 ] != 'c' ) {
      this->send_dict_request();
      this->poll.timer.add_timer_seconds( *this, DICT_TIMER_SECS,
                                          FIRST_TIMER_ID, 0 );
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
RvDataCallback::make_subject( size_t n,  const char *&s, size_t &slen ) noexcept
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
      uint16_t     inbox_len;
      const char * subject;
      size_t       subject_len;

      inbox_len = this->client.make_inbox( inbox, n + SUB_INBOX_BASE );
      this->make_subject( n, subject, subject_len );

      if ( ! this->quiet ) {
        out.ts().printf( "Subscribe \"%.*s\", reply \"%.*s\"\n",
                         (int) subject_len, subject, (int) inbox_len, inbox );
      }
      else {
        if ( n == 0 )
          out.ts().printf( "Subscribe \"%.*s\"", (int) subject_len, subject );
        else
          out.printf( ", \"%.*s\"", (int) subject_len, subject );
      }
      /* subscribe with inbox reply */
      this->client.subscribe( subject, subject_len, inbox, inbox_len );
      this->wild_subscribe |=
        ( is_rv_wildcard( subject, subject_len ) != NULL );
      this->subj_ht.upsert( subject, subject_len, n );
    }
    if ( n >= this->num_subs * this->sub_count )
      break;
  }
  if ( this->quiet )
    out.puts( "\n" );
  out.flush();

  if ( this->show_rate ) {
    this->last_time = this->poll.current_coarse_ns();
    this->poll.timer.add_timer_seconds( *this, RATE_TIMER_SECS,
                                        RATE_TIMER_ID, 0 );
  }
  if ( this->max_time_secs != 0 ) {
    this->poll.timer.add_timer_seconds( *this, this->max_time_secs,
                                        STOP_TIMER_ID, 0 );
  }
  this->is_subscribed = true;
}

/* if ctrl-c, program signalled, unsubscribe the subs */
void
RvDataCallback::on_unsubscribe( void ) noexcept
{
  if ( ! this->is_subscribed )
    return;
  this->is_subscribed = false;
  for ( size_t n = 0; ; ) {
    for ( size_t j = 0; j < this->sub_count; j++, n++ ) {
      const char * subject;
      size_t       subject_len;
      this->make_subject( n, subject, subject_len );
      if ( ! this->quiet ) {
        out.ts().printf( "Unsubscribe \"%.*s\" initial=%u update=%u bytes=%u\n",
               (int) subject_len, subject, this->msg_recv[ n ] & 1,
               this->msg_recv[ n ] >> 1, (uint32_t) this->msg_recv_bytes[ n ] );
      }
      else {
        if ( n == 0 )
          out.ts().printf( "Unsubscribe \"%.*s\"", (int) subject_len, subject );
        else
          out.printf( ", \"%.*s\"", (int) subject_len, subject );
        out.printf( " %u.%u", ( this->msg_recv[ n ] >> 1 ),
                              ( this->msg_recv[ n ] & 1 ) );
      }
      if ( ( this->msg_recv[ n ] & 1 ) != 0 )
        this->sub_initial_count++;
      if ( ( this->msg_recv[ n ] >> 1 ) != 0 )
        this->sub_update_count++;
      /* subscribe with inbox reply */
      this->client.unsubscribe( subject, subject_len );
    }
    if ( n >= this->num_subs * this->sub_count )
      break;
  }
  if ( this->quiet )
    out.puts( "\n" );
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
  char     inbox[ MAX_RV_INBOX_LEN ]; /* _INBOX.<session>.2 */
  uint16_t inbox_len = this->client.make_inbox( inbox, DICT_INBOX_ID );
  /* request dictionar */
  EvPublish pub( DICT_SUBJ, DICT_SUBJ_LEN, inbox, inbox_len,
                 NULL, 0, this->client.sub_route, this->client, 0, 0 );
  this->client.publish( pub );
}

/* dict timer expired */
bool
RvDataCallback::timer_cb( uint64_t timer_id,  uint64_t ) noexcept
{
  if ( timer_id == RATE_TIMER_ID ) {
    uint64_t ival_ns = this->poll.now_ns - this->last_time,
             count   = this->msg_count - this->last_count,
             bytes   = this->msg_bytes - this->last_bytes;
    if ( this->last_count < this->msg_count ) {
      out.ts().printf( "%.2f m/s %.2f mbit/s\n",
              (double) count * 1000000000.0 / (double) ival_ns,
              (double) bytes * 8.0 * 1000.0 / ival_ns );
      out.flush();
    }
    this->last_time  += ival_ns;
    this->last_count += count;
    this->last_bytes += bytes;
    return true;
  }
  if ( timer_id == STOP_TIMER_ID ) {
    this->on_unsubscribe();
    if ( this->poll.quit == 0 )
      this->poll.quit = 1; /* causes poll loop to exit */
    return false;
  }
  if ( this->have_dictionary )
    return false;
  if ( timer_id == FIRST_TIMER_ID ) {
    out.ts().printf( "Dict request timeout, trying again\n" );
    out.flush();
    this->send_dict_request();
    this->poll.timer.add_timer_seconds( *this, DICT_TIMER_SECS,
                                        SECOND_TIMER_ID, 0 );
  }
  else if ( timer_id == SECOND_TIMER_ID ) {
    out.ts().printf( "Dict request timeout again, starting subs\n" );
    out.flush();
    this->start_subscriptions();
  }
  return false; /* return false to disable recurrent timer */
}

/* when client connection stops */
void
RvDataCallback::on_shutdown( EvSocket &conn,  const char *err,
                             size_t errlen ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  out.ts().printf( "Shutdown: %.*s %.*s\n",
                   len, conn.peer_address.buf, (int) errlen, err );
  /* if disconnected by tcp, usually a reconnect protocol, but this just exits*/
  if ( this->poll.quit == 0 )
    this->poll.quit = 1; /* causes poll loop to exit */
}

bool
RvDataCallback::on_rv_msg( EvPublish &pub ) noexcept
{
  const char * subject     = pub.subject;
  size_t       subject_len = pub.subject_len,
               pub_len     = pub.msg_len + subject_len;
  MDMsgMem mem;
  MDMsg  * m;
  size_t   n;
  uint32_t sub_idx    = 0;
  bool     have_sub   = false,
           is_initial = false;
  /* check if published to _INBOX.<session>. */
  uint64_t which = this->client.is_inbox( subject, subject_len );
  if ( which != 0 ) {
    if ( which >= SUB_INBOX_BASE ) {
      n = which - SUB_INBOX_BASE;
      this->make_subject( n, subject, subject_len );
      this->add_initial( n, pub_len );
      sub_idx  = n;
      have_sub = true;
      is_initial = true;
    }
    else if ( which == DICT_INBOX_ID ) {
      out.ts().printf( "Received dictionary message (len=%u)\n",
                       (uint32_t) pub_len );
      m = MDMsg::unpack( (void *) pub.msg, 0, pub.msg_len, 0, this->dict, mem );
      this->on_dict( m );
      this->start_subscriptions();
      return true;
    }
  }
  if ( which == 0 || which >= SUB_INBOX_BASE ) {
    if ( which == 0 ) {
      if ( this->subj_ht.find( pub.subject, pub.subject_len, sub_idx ) ) {
        this->add_update( sub_idx, pub_len );
        have_sub = true;
      }
      else if ( ! this->wild_subscribe ) {
        if ( this->ignore_not_found )
          return true;
        out.printf( "sub %.*s not found\n", (int) pub.subject_len, pub.subject );
      }
    }
    if ( this->show_rate ) {
      this->msg_count++;
      this->msg_bytes += pub_len;
      return true;
    }
  }
  m = MDMsg::unpack( (void *) pub.msg, 0, pub.msg_len, pub.msg_enc, this->dict, mem );
  MDFieldIter * iter = NULL;
  double delta = 0, next_delta = 0, duration = 0;
  double next = 0, expect = 0, cur = 0;
  uint64_t  sequence = 0, last_sequence = 0;
  if ( m != NULL && m->get_field_iter( iter ) == 0 && iter->first() == 0 ) {
    MDName nm;
    MDReference mref;
    if ( iter->get_name( nm ) == 0 && iter->get_reference( mref ) == 0 ) {
      const MDName mtype( "MSG_TYPE", 9 );
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
        double val;
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
        expect = this->next_pub[ sub_idx ]; /* next should be close to current */
        last_sequence = this->last_seqno[ sub_idx ];
        cur = current_realtime_s();
        if ( expect != 0 ) {
          bool missed = ( sequence > ( last_sequence + 1 ) );
          delta = cur - expect; /* positive is secs in before current time */
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
  if ( this->quiet ) {
    if ( delta > 2.0 || delta < -1.0 ) {
      out.ts().printf( "## %.*s update latency %.6f, next expected %f, "
                       "cur expect %f, actual time %f, next %f\n",
                       (int) pub.subject_len, pub.subject,
                       delta, next_delta, expect, cur, next );
      out.flush();
    }
    if ( subject_len <= 3 || ::memcmp( subject, "_RV.", 4 ) != 0 )
      return true;
  }
  if ( delta != 0 || next_delta != 0 ) {
    out.ts().printf( "## update latency %.6f, next expected %f, "
                     "cur expect %f, actual time %f, next %f\n",
                     delta, next_delta, expect, cur, next );
  }
  if ( last_sequence != 0 && sequence != 0 ) {
    out.ts().printf( "## last seqno %lu, current seqno %lu\n", last_sequence,
                     sequence );
  }
  if ( which >= SUB_INBOX_BASE ) {
    out.ts().printf( "## %.*s: (inbox: %.*s)\n", (int) subject_len, subject,
                     (int) pub.subject_len, pub.subject );
  }
  else { /* not inbox subject */
    if ( pub.reply_len != 0 )
      out.ts().printf( "## %.*s (reply: %.*s):\n", (int) subject_len, subject,
                        (int) pub.reply_len, (const char *) pub.reply );
    else
      out.ts().printf( "## %.*s:\n", (int) subject_len, subject );
  }
  /* print message */
  if ( m != NULL ) {
    out.ts().printf( "## format: %s, length %u\n", m->get_proto_string(),
                     pub.msg_len );
    m->print( &out );
    if ( this->dump_hex )
      out.print_hex( m );
  }
  else if ( pub.msg_len == 0 )
    out.ts().printf( "## No message data\n" );
  else
    out.ts().printe( "Message unpack error\n" );
  out.flush();
  return true;
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
             * user    = get_arg( x, argc, argv, 1, "-u", "-user", "rv_client" ),
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
  int first_sub = x, idle_count = 0;
  size_t cnt = 1, range = 0, secs = 0;
  uint64_t seed1 = 0, seed2 = 0;

  if ( help != NULL ) {
  help:;
    fprintf( stderr,
 "%s [-d daemon] [-n network] [-s service] [-c cfile_path] [-x] [-e] subject ...\n"
             "  -d daemon  = daemon port to connect\n"
             "  -n network = network\n"
             "  -s service = service\n"
             "  -u user    = user name\n"
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
      if ( cnt == 0 )
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

  EvPoll poll;
  poll.init( 5, false );

  EvRvClientParameters parm( daemon, network, service, user, 0 );
  EvRvClient           conn( poll );
  RvDataCallback       data( poll, conn, &argv[ first_sub ], argc - first_sub,
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
        printf( "Loaded dictionary from cfiles\n" );
        data.have_dictionary = true;
      }
    }
  }
  /* connect to daemon */
  if ( ! conn.connect( parm, &data, &data ) ) {
    fprintf( stderr, "Failed to connect to daemon\n" );
    return 1;
  }
  /* handle ctrl-c */
  sighndl.install();
  for (;;) {
    /* loop 5 times before quiting, time to flush writes */
    if ( poll.quit >= 5 && idle_count > 0 )
      break;
    /* dispatch network events */
    int idle = poll.dispatch();
    if ( idle == EvPoll::DISPATCH_IDLE )
      idle_count++;
    else
      idle_count = 0;
    /* wait for network events */
    poll.wait( idle_count > 255 ? 100 : 0 );
    if ( sighndl.signaled ) {
      if ( poll.quit == 0 )
        data.on_unsubscribe();
      poll.quit++;
    }
  }
  const char *s = "";
  for ( uint32_t i = 0; i < 32; i++ ) {
    if ( data.msg_type_cnt[ i ] != 0 ) {
      char buf[ 32 ];
      out.printf( "%s%u (%s): %u", s, i, md_sass_msg_type_str( i, buf ),
                  data.msg_type_cnt[ i ] );
      s = ", ";
    }
  }
  if ( ::strlen( s ) > 0 )
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
    out.ts().printf( "Run time %f %s, subjects %lu initial=%u update=%u\n",
                     t, u, data.num_subs * data.sub_count,
                     data.sub_initial_count, data.sub_update_count );
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

