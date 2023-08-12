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
#include <raimd/tib_msg.h>
#include <raimd/tib_sass_msg.h>
#include <raimd/json_msg.h>
#include <raimd/rv_msg.h>
#include <raikv/ev_publish.h>
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
                      PUB_TIMER_USECS = 1;
static const uint64_t FIRST_TIMER_ID  = 1, /* first dict request */
                      SECOND_TIMER_ID = 2, /* second dict request */
                      PUB_TIMER_ID    = 3;

struct RandBuf : public ArrayCount<uint64_t, 1024> {
  uint64_t hd, tl;
  RandBuf() : hd( 0 ), tl( 0 ) {}
  uint64_t index( uint64_t off ) {
    return this->ptr[ this->tl + off ];
  }
  uint64_t avail( void ) {
    return this->hd - this->tl;
  }
  uint64_t consume( void ) {
    uint64_t off = this->tl++;
    return this->ptr[ off ];
  }
  void append( uint64_t n ) {
    if ( this->hd < this->count )
      this->ptr[ this->hd++ ] = n;
    else {
      this->push( n );
      this->hd++;
    }
  }
  void adjust( void ) {
    if ( this->tl > this->count / 2 ) {
      ::memmove( this->ptr, &this->ptr[ this->tl ],
                 sizeof( this->ptr[ 0 ] ) * this->avail() );
      this->hd -= this->tl;
      this->tl = 0;
    }
  }
};

/* rv client callback closure */
struct RvDataCallback : public EvConnectionNotify, public RvClientCB,
                        public EvTimerCallback, public BPData {
  EvPoll      & poll;            /* poll loop data */
  EvRvClient  & client;          /* connection to rv */
  MDDict      * dict;            /* dictinary to use for decoding msgs */
  const char ** sub;             /* subject strings */
  char        * sub_buf,
              * payload,
              * msg_buf;
  size_t        sub_n,
                sub_count,       /* count of sub[] */
                pub_count,
                current_pub,
                total_pub,
                rate_accum,
                i, j, k,
                max_len,
                payload_bytes,
                msg_buf_len;
  uint64_t      rate_per_sec,
                start_time_ns,
                ns_per_msg;
  uint16_t      rectype;
  bool          no_dictionary,   /* don't request dictionary */
                have_dictionary, /* set when dict request succeeded */
                use_json,
                use_rv,
                use_tibmsg,
                verbose,
                dump_hex,
                has_rate_timer,
                use_random,
                use_zipf,
                track_time;

  kv::rand::xoroshiro128plus rand;
  ZipfianGen<99, 100, kv::rand::xoroshiro128plus> zipf;
  uint32_t   ** sequence;
  RandBuf       rand_buf;

  RvDataCallback( EvPoll &p,  EvRvClient &c,  const char **s,  size_t n,
                  size_t cnt,  size_t pcnt,  size_t sz,  bool nd,  bool uj,
                  bool ur,  bool ut,  bool verb,  bool hex,  size_t rate,
                  bool ra,  bool zi,  bool ts )
    : poll( p ), client( c ), dict( 0 ), sub( s ), sub_buf( 0 ), payload( 0 ),
      msg_buf( 0 ), sub_n( n ), sub_count( cnt ), pub_count( pcnt ),
      current_pub( 0 ), total_pub( n * cnt * pcnt ), rate_accum( 0 ),
      i( 0 ), j( 0 ), k( 0 ), max_len( 0 ), payload_bytes( sz ),
      msg_buf_len( 0 ), rate_per_sec( rate ), start_time_ns( 0 ),
      ns_per_msg( 0 ), rectype( 0 ), no_dictionary( nd ),
      have_dictionary( false ), use_json( uj ), use_rv( ur ),
      use_tibmsg( ut ), verbose( hex | verb ), dump_hex( hex ),
      has_rate_timer( false ), use_random( ra | zi ), use_zipf( zi ),
      track_time( ts ), zipf( n * cnt * pcnt, this->rand ), sequence( 0 ) {
    this->rand.static_init( current_monotonic_time_ns(),
                            current_realtime_ns() );
    if ( rate > 0 )
      this->ns_per_msg = 1000000000 / rate;
    this->init_subjects();
    this->bp_flags  = BP_FORWARD | BP_NOTIFY;
  }

  void init_subjects( void ) noexcept;
  /* after CONNECTED message */
  virtual void on_connect( EvSocket &conn ) noexcept;
  /* start sub[] with inbox reply */
  void run_publishers( void ) noexcept;
  /* when disconnected */
  virtual void on_shutdown( EvSocket &conn,  const char *err,
                            size_t err_len ) noexcept;
  void send_dict_request( void ) noexcept;
  /* dict from network */
  void on_dict( MDMsg *m ) noexcept;
  /* dict timeout */
  void start_pub_timer( void ) noexcept;
  bool calc_rate_limit( void ) noexcept;
  bool same_sub( uint64_t sub_idx,  uint64_t sub_num,  uint64_t n ) noexcept;
  void rand_next( uint64_t &sub_idx,  uint64_t &sub_num,
                  uint64_t &delta ) noexcept;
  void get_random_index( uint64_t &sub_idx,  uint64_t &sub_num,
                         uint32_t &seqno,  uint64_t &delta ) noexcept;
  void make_subject( size_t sub_idx,  size_t sub_num,
                     const char *&s,  size_t &slen ) noexcept;
  virtual bool timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept;
  /* message from network */
  virtual bool on_rv_msg( EvPublish &pub ) noexcept;
  /* flush send, ready to send more */
  virtual void on_write_ready( void ) noexcept;
};

void
RvDataCallback::init_subjects( void ) noexcept
{
  size_t len = 0;
  for ( size_t i = 0; i < this->sub_n; i++ ) {
    const char *s = this->sub[ i ];
    if ( ::strstr( s, "%d" ) == NULL && this->sub_count > 1 ) {
      size_t x   = ::strlen( s );
      char * tmp = (char *) ::malloc( x + 4 );
      ::memcpy( tmp, s, x );
      ::memcpy( &tmp[ x ], ".%d", 4 );
      s = tmp;
      this->sub[ i ] = s;
    }
    len = max_int<size_t>( ::strlen( s ), len );
  }
  this->sub_buf = (char *) ::malloc( len + 24 );
  this->max_len = len + 24;
  this->msg_buf_len = 1024;
  if ( this->payload_bytes > 0 ) {
    this->msg_buf_len += this->payload_bytes;
    this->payload = (char *) ::malloc( this->payload_bytes );
    ::memset( this->payload, 'A', this->payload_bytes - 1 );
    this->payload[ this->payload_bytes - 1 ] = '\0';
  }
  this->msg_buf = (char *) ::malloc( this->msg_buf_len );
}

/* called after daemon responds with CONNECTED message */
void
RvDataCallback::on_connect( EvSocket &conn ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  printf( "Connected: %.*s\n", len, conn.peer_address.buf );
  fflush( stdout );

  if ( ! this->no_dictionary ) {
    /* if no cfile dict, request one */
    if ( this->dict == NULL || this->dict->dict_type[ 0 ] != 'c' ) {
      this->send_dict_request();
      this->poll.timer.add_timer_seconds( *this, DICT_TIMER_SECS,
                                          FIRST_TIMER_ID, 0 );
      return;
    }
  }
  this->run_publishers();
}

struct WriteParam {
  const char * sub;
  size_t       sublen;
  uint64_t     seqno,
               cur_time,
               delta;
  const char * payload;
  size_t       payload_bytes;
  uint16_t     rectype;
  bool         track_time;

  WriteParam( const char *s,  size_t len,  uint64_t num,  uint64_t t,
              uint64_t del,  const char *pay,  size_t pay_bytes,
              uint16_t rec,  bool track )
    : sub( s ), sublen( len ), seqno( num ), cur_time( t ), delta( del ),
      payload( pay ), payload_bytes( pay_bytes ), rectype( rec ),
      track_time( track ) {}
};

template< class Writer >
size_t
write_msg( Writer &writer,  WriteParam &p ) noexcept
{
  static char msg_type[]    = "MSG_TYPE",
              rec_type[]    = "REC_TYPE",
              seq_no[]      = "SEQ_NO",
              rec_status[]  = "REC_STATUS",
              symbol[]      = "SYMBOL",
              bid_size[]    = "BIDSIZE",
              ask_size[]    = "ASKSIZE",
              bid[]         = "BID",
              ask[]         = "ASK",
              timact[]      = "TIMACT",
              trade_date[]  = "TRADE_DATE",
              volume[]      = "VOLUME",
              duration[]    = "DURATION",
              buffer_data[] = "BUFFER_DATA";
  MDDecimal dec;
  MDTime time;
  MDDate date;

  short type = ( p.seqno == 0 ? 8 : 1 );
  writer.append_int( msg_type, sizeof( msg_type ), type );
  if ( p.rectype != 0 )
    writer.append_int( rec_type, sizeof( rec_type ), p.rectype );
  writer.append_int( seq_no, sizeof( seq_no ),  p.seqno );
  writer.append_int( rec_status, sizeof( rec_status ), (short) 0 );
  const char *s = &p.sub[ p.sublen ];
  int dots = 0;
  for ( ; s > p.sub; s-- ) {
    if ( s[ -1 ] == '.' )
      if ( ++dots == 2 )
        break;
  }
  writer.append_string( symbol, sizeof( symbol ), s,
                        &p.sub[ p.sublen + 1 ] - s );
  writer.append_real( bid_size, sizeof( bid_size ), 10.0 );
  writer.append_real( ask_size, sizeof( ask_size ), 20.0 );

  dec.ival = 17500;
  dec.hint = MD_DEC_LOGn10_3; /* / 1000 */
  writer.append_decimal( bid, sizeof( bid ), dec );

  dec.ival = 17750;
  dec.hint = MD_DEC_LOGn10_3; /* / 1000 */
  writer.append_decimal( ask, sizeof( ask ), dec );

  time.hour       = 13;
  time.minute     = 15;
  time.sec        = 0;
  time.resolution = MD_RES_MINUTES;
  time.fraction   = 0;
  writer.append_time( timact, sizeof( timact ), time );

  date.year = 2019;
  date.mon  = 4;
  date.day  = 9;
  writer.append_date( trade_date, sizeof( trade_date ), date );

  if ( p.track_time ) {
    dec.ival = p.cur_time;
    dec.hint = MD_DEC_LOGn10_9;
    writer.append_decimal( volume, sizeof( volume ), dec );
    dec.ival = p.delta;
    dec.hint = MD_DEC_LOGn10_9;
    writer.append_decimal( duration, sizeof( duration ), dec );
  }
  if ( p.payload_bytes > 0 ) {
    writer.append_string( buffer_data, sizeof( buffer_data ),
                          p.payload, p.payload_bytes );
  }
  return writer.update_hdr();
}

void
RvDataCallback::start_pub_timer( void ) noexcept
{
  uint32_t t;
  t = (uint32_t) ( 1000000.0 / (double) this->rate_per_sec );
  if ( t < 10 ) t = 10;
  this->poll.timer.add_timer_micros( *this, t, PUB_TIMER_ID, 0 );
  this->has_rate_timer = true;
}

bool
RvDataCallback::calc_rate_limit( void ) noexcept
{
  if ( this->rate_per_sec == 0 )
    return false;
  if ( ! this->has_rate_timer )
    this->start_pub_timer();

  uint64_t ns = this->poll.current_mono_ns();
  if ( this->start_time_ns == 0 )
    this->start_time_ns = ns;
  if ( ns == this->start_time_ns )
    this->rate_accum = 0;
  else {
    this->rate_accum = (size_t) (
      (double) ( ( ns - this->start_time_ns ) / 1000000000.0 ) *
        (double) this->rate_per_sec );
  }
  return true;
}

bool
RvDataCallback::same_sub( uint64_t sub_idx,  uint64_t sub_num,
                          uint64_t n ) noexcept
{
  if ( this->sub_n > 1 ) {
    return ( sub_idx == n % this->sub_n &&
             sub_num == ( n / this->sub_n ) % this->sub_count );
  }
  return sub_num == n % this->sub_count;
}

void
RvDataCallback::rand_next( uint64_t &sub_idx,  uint64_t &sub_num,
                           uint64_t &delta ) noexcept
{
  uint64_t n;
  if ( this->rand_buf.avail() > 0 )
    n = this->rand_buf.consume();
  else
    n = this->use_zipf ? this->zipf.next() :
        ( this->rand.next() % this->total_pub );

  if ( this->sub_n > 1 ) {
    sub_idx = n % this->sub_n;
    sub_num = ( n / this->sub_n ) % this->sub_count;
  }
  else {
    sub_idx = 0;
    sub_num = n % this->sub_count;
  }
  if ( ! this->track_time || this->ns_per_msg == 0 ) {
    delta = 0;
    return;
  }
  uint64_t avail = this->rand_buf.avail(),
           left  = this->total_pub - this->current_pub, m;

  for ( delta = 1; delta <= avail; delta++ ) {
    m = this->rand_buf.index( delta - 1 );
    if ( this->same_sub( sub_idx, sub_num, m ) )
      goto found_delta;
  }
  for ( ; delta < left; delta++ ) {
    m = this->use_zipf ? this->zipf.next() :
        ( this->rand.next() % this->total_pub );
    this->rand_buf.append( m );
    if ( this->same_sub( sub_idx, sub_num, m ) )
      goto found_delta;
  }
  delta = 0;
found_delta:;
  this->rand_buf.adjust();
}

void
RvDataCallback::get_random_index( uint64_t &sub_idx,  uint64_t &sub_num,
                                  uint32_t &seqno,  uint64_t &delta ) noexcept
{
  this->rand_next( sub_idx, sub_num, delta );

  if ( this->sequence == NULL ) {
    this->sequence = (uint32_t **)
      ::malloc( sizeof( this->sequence[ 0 ] ) * this->sub_n );
    for ( size_t x = 0; x < this->sub_n; x++ ) {
      size_t sz = sizeof( this->sequence[ 0 ][ 0 ] ) * this->sub_count;
      this->sequence[ x ] = (uint32_t *) ::malloc( sz );
      ::memset( this->sequence[ x ], 0, sz );
    }
  }
  seqno = this->sequence[ sub_idx ][ sub_num ]++;
}

void
RvDataCallback::make_subject( size_t sub_idx,  size_t sub_num,
                              const char *&s,  size_t &slen ) noexcept
{
  s    = this->sub[ sub_idx ];
  slen = ::strlen( s );

  const char * e = &s[ slen ],
             * p = ::strstr( s, "%d" );
  if ( p != NULL ) {
    CatPtr cat( this->sub_buf );
    if ( p > s )
      cat.x( s, p - s );
    cat.u( sub_num );
    p += 2;
    if ( p < e )
      cat.x( p, e - p );

    s    = cat.start;
    slen = cat.end();
  }
}

/* start subscriptions from command line, inbox number indexes the sub[] */
void
RvDataCallback::run_publishers( void ) noexcept
{
  uint8_t * buf_ptr;
  size_t    msg_len;
  uint32_t  msg_enc = 0;
  bool      is_rate_limited;

  if ( this->poll.quit != 0 )
    return;
  is_rate_limited = this->calc_rate_limit();
  for (;;) {
    if ( this->current_pub >= this->total_pub || this->poll.quit != 0 ) {
      if ( this->poll.quit == 0 )
        this->poll.quit = 1;
      return;
    }
    if ( is_rate_limited && this->current_pub >= this->rate_accum )
      return;
    this->current_pub++;

    uint64_t     sub_idx  = this->i,
                 sub_num  = this->j;
    uint32_t     seqno    = this->k;
    uint64_t     delta    = 0,
                 cur_time = 0;
    const char * subject;
    size_t       subject_len;

    if ( this->use_random ) {
      this->get_random_index( sub_idx, sub_num, seqno, delta );
      if ( this->track_time ) {
        cur_time = this->poll.current_coarse_ns();
        delta   *= this->ns_per_msg;
      }
    }
    else if ( this->track_time ) {
      cur_time = this->poll.current_coarse_ns();
      delta    = this->sub_n * this->sub_count * this->ns_per_msg;
    }
    this->make_subject( sub_idx, sub_num, subject, subject_len );

    WriteParam param( subject, subject_len, seqno, cur_time, delta,
                      this->payload, this->payload_bytes,
                      this->rectype, this->track_time );
    MDMsgMem mem;
    if ( this->use_json ) {
      JsonMsgWriter jsonmsg( mem, this->msg_buf, this->msg_buf_len );
      msg_len = write_msg<JsonMsgWriter>( jsonmsg, param );
      buf_ptr = jsonmsg.buf;
      msg_enc = JSON_TYPE_ID;
    }
    else if ( this->use_rv ) {
      RvMsgWriter rvmsg( mem, this->msg_buf, this->msg_buf_len );
      msg_len = write_msg<RvMsgWriter>( rvmsg, param );
      buf_ptr = rvmsg.buf;
      msg_enc = RVMSG_TYPE_ID;
    }
    else if ( this->use_tibmsg || ! this->have_dictionary ) {
      TibMsgWriter tibmsg( mem, this->msg_buf, this->msg_buf_len );
      msg_len = write_msg<TibMsgWriter>( tibmsg, param );
      buf_ptr = tibmsg.buf;
      msg_enc = TIBMSG_TYPE_ID;
    }
    else {
      TibSassMsgWriter tibmsg( mem, this->dict, this->msg_buf,
                               this->msg_buf_len );
      msg_len = write_msg<TibSassMsgWriter>( tibmsg, param );
      buf_ptr = tibmsg.buf;
      msg_enc = TIB_SASS_TYPE_ID;
    }
    if ( ++this->i == this->sub_n ) {
      this->i = 0;
      if ( ++this->j == this->sub_count ) {
        this->j = 0;
        this->k++;
      }
    }
    if ( this->verbose ) {
      MDMsg *m;
      MDOutput mout;
      mout.printf( "## %s seqno=%u\n", subject, seqno );
      m = MDMsg::unpack( buf_ptr, 0, msg_len, 0, this->dict, mem );
      /* print message */
      if ( m != NULL ) {
        printf( "## format: %s, length %u\n", m->get_proto_string(),
                (uint32_t) msg_len );
        m->print( &mout );
      }
      if ( this->dump_hex ) {
        mout.print_hex( buf_ptr, msg_len );
      }
      fflush( stdout );
    }
    EvPublish pub( subject, subject_len, NULL, 0, buf_ptr, msg_len,
                   this->client.sub_route, this->client, 0, msg_enc );
    if ( ! this->client.publish( pub ) ) {
      /* wait for ready */
      if ( this->has_back_pressure( this->poll, this->client.fd ) )
        return;
    }
  }
}

void
RvDataCallback::on_write_ready( void ) noexcept
{
  this->run_publishers();
}

/* when dict message is replied */
void
RvDataCallback::on_dict( MDMsg *m ) noexcept
{
  if ( m == NULL ) {
    fprintf( stderr, "Dict unpack error\n" );
    return;
  }
  if ( this->have_dictionary )
    return;
  MDDictBuild dict_build;
  if ( CFile::unpack_sass( dict_build, m ) != 0 ) {
    fprintf( stderr, "Dict index error\n" );
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
  if ( timer_id == PUB_TIMER_ID ) {
    this->run_publishers();
    return true;
  }
  if ( this->have_dictionary )
    return false;
  if ( timer_id == FIRST_TIMER_ID ) {
    printf( "Dict request timeout, trying again\n" );
    this->send_dict_request();
    this->poll.timer.add_timer_seconds( *this, DICT_TIMER_SECS,
                                        SECOND_TIMER_ID, 0 );
  }
  else if ( timer_id == SECOND_TIMER_ID ) {
    printf( "Dict request timeout again, starting publisher\n" );
    this->run_publishers();
  }
  fflush( stdout );
  return false; /* return false to disable recurrent timer */
}

/* when client connection stops */
void
RvDataCallback::on_shutdown( EvSocket &conn,  const char *err,
                             size_t errlen ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  printf( "Shutdown: %.*s %.*s\n",
          len, conn.peer_address.buf, (int) errlen, err );
  fflush( stdout );
  /* if disconnected by tcp, usually a reconnect protocol, but this just exits*/
  if ( this->poll.quit == 0 )
    this->poll.quit = 1; /* causes poll loop to exit */
}

bool
RvDataCallback::on_rv_msg( EvPublish &pub ) noexcept
{
  MDMsgMem mem;
  MDMsg  * m = MDMsg::unpack( (void *) pub.msg, 0, pub.msg_len, 0, this->dict,
                              mem );
  /* check if published to _INBOX.<session>. */
  uint64_t which = this->client.is_inbox( pub.subject, pub.subject_len );
  if ( which != 0 ) {
    size_t idx = which - SUB_INBOX_BASE;
    if ( which >= SUB_INBOX_BASE && idx < this->sub_count ) {
      printf( "## %s: (inbox: %.*s)\n", this->sub[ idx ],
               (int) pub.subject_len, pub.subject );
    }
    else if ( which == DICT_INBOX_ID ) {
      printf( "Received dictionary message\n" );
      this->on_dict( m );
      this->run_publishers();
      return true;
    }
    else {
      printf( "## Unknown inbox message (%u)\n", (uint32_t) idx );
    }
  }
  else { /* not inbox subject */
    if ( pub.reply_len != 0 )
      printf( "## %.*s (reply: %.*s):\n", (int) pub.subject_len, pub.subject,
              (int) pub.reply_len, (const char *) pub.reply );
    else
      printf( "## %.*s:\n", (int) pub.subject_len, pub.subject );
  }
  /* print message */
  if ( m != NULL ) {
    printf( "## format: %s, length %u\n", m->get_proto_string(), pub.msg_len );
    MDOutput mout;
    m->print( &mout );
    if ( this->dump_hex )
      mout.print_hex( m );
  }
  fflush( stdout );
  if ( m == NULL )
    fprintf( stderr, "Message unpack error\n" );
  return true;
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
  const char * daemon     = get_arg( x, argc, argv, 1, "-d", "-daemon", "tcp:7500" ),
             * network    = get_arg( x, argc, argv, 1, "-n", "-network", ""),
             * service    = get_arg( x, argc, argv, 1, "-s", "-service", "7500" ),
             * path       = get_arg( x, argc, argv, 1, "-c", "-cfile", NULL ),
             * pub_count  = get_arg( x, argc, argv, 1, "-p", "-pub", "1" ),
             * sub_count  = get_arg( x, argc, argv, 1, "-k", "-sub", "1" ),
             * payload    = get_arg( x, argc, argv, 1, "-z", "-payload", "0" ),
             * rectype    = get_arg( x, argc, argv, 1, "-f", "-rectype", NULL ),
             * nodict     = get_arg( x, argc, argv, 0, "-x", "-nodict", NULL ),
             * verbose    = get_arg( x, argc, argv, 0, "-v", "-verbose", NULL ),
             * dump       = get_arg( x, argc, argv, 0, "-e", "-hex", NULL ),
             * use_json   = get_arg( x, argc, argv, 0, "-j", "-json", NULL ),
             * use_rv     = get_arg( x, argc, argv, 0, "-r", "-rv", NULL ),
             * use_tibmsg = get_arg( x, argc, argv, 0, "-t", "-tibmsg", NULL ),
             * msg_rate   = get_arg( x, argc, argv, 1, "-m", "-rate", NULL ),
             * random     = get_arg( x, argc, argv, 0, "-R", "-random", NULL ),
             * zipf       = get_arg( x, argc, argv, 0, "-Z", "-zipf", NULL ),
             * use_ts     = get_arg( x, argc, argv, 0, "-T", "-stamp", NULL ),
             * help       = get_arg( x, argc, argv, 0, "-h", "-help", 0 );
  int first_sub = x, idle_count = 0;

  if ( help != NULL ) {
  help:;
    fprintf( stderr,
 "%s [-d daemon] [-n network] [-s service] [-c cfile_path] [-x] "
    "[-m rate] [-z size] [-f rectype] [-k sub_count] [-p times] subject ...\n"
             "  -d daemon  = daemon port to connect (tcp:7500)\n"
             "  -n network = network\n"
             "  -s service = service (7500)\n"
             "  -c cfile   = if loading dictionary from files\n"
             "  -p count   = number of times to publish a record (1)\n"
             "  -k count   = number of subjects to publish (1)\n"
             "  -z size    = payload bytes added to message (0)\n"
             "  -f rectype = add REC_TYPE field to message\n"
             "  -x         = don't load a dictionary\n"
             "  -v         = print messages\n"
             "  -e         = show hex dump of messages\n"
             "  -j         = publish json format\n"
             "  -r         = publish rvmsg format\n"
             "  -t         = publish tibmsg format\n"
             "  -m         = msgs per second\n"
             "  -R         = random subjects\n"
             "  -Z         = random, zipf(0.99) distribution\n"
             "  -T         = add timestamp to message\n"
             "  subject    = subject to publish\n", argv[ 0 ] );
    return 1;
  }
  if ( first_sub >= argc ) {
    fprintf( stderr, "No subjects subscribed\n" );
    goto help;
  }
  if ( ! valid_uint64( sub_count, ::strlen( sub_count ) ) ||
       ! valid_uint64( pub_count, ::strlen( pub_count ) ) ||
       ! valid_uint64( payload, ::strlen( payload ) ) ) {
    fprintf( stderr, "Invalid -p/-k/-z,-sub/-pub/-payload\n" );
    goto help;
  }
  if ( msg_rate != NULL &&
       ! valid_uint64( msg_rate, ::strlen( msg_rate ) ) ) {
    fprintf( stderr, "Invalid -m/-rate\n" );
    goto help;
  }
  EvPoll poll;
  poll.init( 5, false );

  bool nd = ( nodict     != NULL ),
       uj = ( use_json   != NULL ),
       ur = ( use_rv     != NULL ),
       ut = ( use_tibmsg != NULL ),
       vb = ( verbose    != NULL ),
       du = ( dump       != NULL ),
       ra = ( random     != NULL ),
       zi = ( zipf       != NULL ),
       ts = ( use_ts     != NULL );
  size_t sub_cnt = string_to_uint64( sub_count, ::strlen( sub_count ) ),
         pub_cnt = string_to_uint64( pub_count, ::strlen( pub_count ) ),
         pay_siz = string_to_uint64( payload, ::strlen( payload ) ),
         rate    = 0,
         n       = (size_t) ( argc - first_sub );

  if ( msg_rate != NULL )
    rate = string_to_uint64( msg_rate, ::strlen( msg_rate ) );

  EvRvClientParameters parm( daemon, network, service, 0 );
  EvRvClient           conn( poll );
  RvDataCallback       data( poll, conn, &argv[ first_sub ], n,
                         sub_cnt, pub_cnt, pay_siz, nd || uj || ur || ut,
                         uj, ur, ut, vb, du, rate, ra, zi, ts );
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
  if ( rectype != NULL ) {
    uint16_t formclass = 0;
    if ( rectype[ 0 ] >= '0' && rectype[ 0 ] <= '9' )
      formclass = string_to_uint64( rectype, ::strlen( rectype ) );
    if ( formclass == 0 ) {
      if ( data.have_dictionary ) {
        MDLookup by( rectype, ::strlen( rectype ) );
        if ( data.dict->get( by ) ) {
          if ( by.ftype == MD_MESSAGE )
            formclass = by.fid;
        }
      }
    }
    if ( formclass == 0 ) {
      fprintf( stderr, "unable to resolve rec type %s\n", rectype );
      return 1;
    }
    if ( rectype[ 0 ] < '0' || rectype[ 0 ] > '9' )
      printf( "Resolve rec type %u (%s)\n", formclass, rectype );
    data.rectype = formclass;
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
      poll.quit++;
    }
  }
  return 0;
}

