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
                show_rate;       /* show rate of messages recvd */
  char        * subj_buf;
  uint32_t    * rand_schedule,
              * msg_recv;
  size_t      * msg_recv_bytes;
  UIntHashTab * subj_ht,
              * coll_ht;
  size_t        rand_range;
  uint32_t      max_time_secs,
                msg_type_cnt[ 32 ];
  bool          use_random,
                use_zipf,
                quiet;
  uint64_t      seed1, seed2;

  RvDataCallback( EvPoll &p,  EvRvClient &c,  const char **s,  size_t cnt,
                  bool nodict,  bool hex,  bool rate,  size_t n,  size_t rng,
                  bool zipf,  uint32_t secs,  bool q, uint64_t s1, uint64_t s2 )
    : poll( p ), client( c ), dict( 0 ), sub( s ), sub_count( cnt ),
      num_subs( n ), msg_count( 0 ), last_count( 0 ), last_time( 0 ),
      msg_bytes( 0 ), last_bytes( 0 ), no_dictionary( nodict ),
      is_subscribed( false ), have_dictionary( false ), dump_hex( hex ),
      show_rate( rate ), subj_buf( 0 ), rand_schedule( 0 ), msg_recv( 0 ),
      msg_recv_bytes( 0 ), subj_ht( 0 ), coll_ht( 0 ), rand_range( rng ),
      max_time_secs( secs ), use_random( rng > cnt ), use_zipf( zipf ),
      quiet( q ) {
    ::memset( this->msg_type_cnt, 0, sizeof( this->msg_type_cnt ) );
    this->seed1 = ( s1 == 0 ? current_monotonic_time_ns() : s1 );
    this->seed2 = ( s2 == 0 ? current_realtime_ns() : s2 );
  }

  void add_initial( size_t n,  size_t bytes ) {
    this->msg_recv[ n ] |= 1;
    this->msg_recv_bytes[ n ] += bytes;
  }
  void add_update( size_t n,  size_t bytes ) {
    this->msg_recv[ n ] += 1 << 1;
    this->msg_recv_bytes[ n ] += bytes;
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
  virtual bool on_msg( EvPublish &pub ) noexcept;
};

/* called after daemon responds with CONNECTED message */
void
RvDataCallback::on_connect( EvSocket &conn ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  printf( "Connected: %.*s\n", len, conn.peer_address.buf );
  if ( this->use_random ) {
    printf( "Random seed1: 0x%016lx seed2 0x%016lx\n", this->seed1,
            this->seed2 );
  }
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
      uint16_t     inbox_len;
      const char * subject;
      size_t       subject_len;

      inbox_len = this->client.make_inbox( inbox, n + SUB_INBOX_BASE );
      this->make_subject( n, subject, subject_len );

      printf( "Subscribe \"%.*s\", reply \"%.*s\"\n",
              (int) subject_len, subject, (int) inbox_len, inbox );
      /* subscribe with inbox reply */
      this->client.subscribe( subject, subject_len, inbox, inbox_len );

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
          printf( "HT collision \"%.*s\", no update tracking\n",
                  (int) coll_sublen, coll_sub );
        }
        h = kv_djb( subject, subject_len );
        if ( ! this->coll_ht->find( h, pos ) )
          this->coll_ht->set_rsz( this->coll_ht, h, pos, n );
        else {
          printf( "HT collision \"%.*s\", no update tracking\n",
                  (int) subject_len, subject );
        }
      }
      else {
        this->subj_ht->set_rsz( this->subj_ht, h, pos, n );
      }
    }
    if ( n >= this->num_subs * this->sub_count )
      break;
  }
  fflush( stdout );

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
      printf( "Unsubscribe \"%.*s\" initial=%u update=%u bytes=%u\n",
              (int) subject_len, subject, this->msg_recv[ n ] & 1,
              this->msg_recv[ n ] >> 1, (uint32_t) this->msg_recv_bytes[ n ] );
      /* subscribe with inbox reply */
      this->client.unsubscribe( subject, subject_len );
    }
    if ( n >= this->num_subs * this->sub_count )
      break;
  }
  fflush( stdout );
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
  if ( timer_id == RATE_TIMER_ID ) {
    uint64_t ival_ns = this->poll.now_ns - this->last_time,
             count   = this->msg_count - this->last_count,
             bytes   = this->msg_bytes - this->last_bytes;
    if ( this->last_count < this->msg_count ) {
      printf( "%.2f m/s %.2f mbit/s\n",
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
    if ( this->poll.quit == 0 )
      this->poll.quit = 1; /* causes poll loop to exit */
    return false;
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
    printf( "Dict request timeout again, starting subs\n" );
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
  printf( "Shutdown: %.*s %.*s\n",
          len, conn.peer_address.buf, (int) errlen, err );
  /* if disconnected by tcp, usually a reconnect protocol, but this just exits*/
  if ( this->poll.quit == 0 )
    this->poll.quit = 1; /* causes poll loop to exit */
}

bool
RvDataCallback::on_msg( EvPublish &pub ) noexcept
{
  const char * subject     = pub.subject;
  size_t       subject_len = pub.subject_len,
               pub_len     = pub.msg_len + subject_len;
  MDMsgMem mem;
  MDMsg  * m;
  size_t   n;
  /* check if published to _INBOX.<session>. */
  uint64_t which = this->client.is_inbox( subject, subject_len );
  if ( which != 0 ) {
    if ( which >= SUB_INBOX_BASE ) {
      n = which - SUB_INBOX_BASE;
      this->make_subject( n, subject, subject_len );
      this->add_initial( n, pub_len );
    }
    else if ( which == DICT_INBOX_ID ) {
      printf( "Received dictionary message (len=%u)\n", (uint32_t) pub_len );
      m = MDMsg::unpack( (void *) pub.msg, 0, pub.msg_len, 0, this->dict, &mem);
      this->on_dict( m );
      this->start_subscriptions();
      return true;
    }
  }
  if ( which == 0 || which >= SUB_INBOX_BASE ) {
    if ( which == 0 ) {
      size_t pos;
      uint32_t val;
      if ( this->subj_ht->find( pub.subj_hash, pos, val ) )
        this->add_update( val, pub_len );
      else if ( this->coll_ht->find( kv_djb( subject, subject_len ), pos, val ) )
        this->add_update( val, pub_len );
    }
    if ( this->show_rate ) {
      this->msg_count++;
      this->msg_bytes += pub_len;
      return true;
    }
  }
  m = MDMsg::unpack( (void *) pub.msg, 0, pub.msg_len, 0, this->dict, &mem );
  MDFieldIter * iter = NULL;
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
    }
  }
  if ( this->quiet )
    return true;
  if ( which >= SUB_INBOX_BASE ) {
    printf( "## %.*s: (inbox: %.*s)\n", (int) subject_len, subject,
            (int) pub.subject_len, pub.subject );
  }
  else { /* not inbox subject */
    if ( pub.reply_len != 0 )
      printf( "## %.*s (reply: %.*s):\n", (int) subject_len, subject,
              (int) pub.reply_len, (const char *) pub.reply );
    else
      printf( "## %.*s:\n", (int) subject_len, subject );
  }
  /* print message */
  if ( m != NULL ) {
    printf( "## format: %s, length %u\n", m->get_proto_string(), pub.msg_len );
    MDOutput mout;
    m->print( &mout );
    if ( this->dump_hex )
      mout.print_hex( m );
  }
  else if ( pub.msg_len == 0 )
    printf( "## No message data\n" );
  else
    fprintf( stderr, "Message unpack error\n" );
  fflush( stdout );
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

  EvPoll poll;
  poll.init( 5, false );

  EvRvClientParameters parm( daemon, network, service, 0 );
  EvRvClient           conn( poll );
  RvDataCallback       data( poll, conn, &argv[ first_sub ], argc - first_sub,
                             nodict != NULL, dump != NULL, rate != NULL, cnt,
                             range, zipf != NULL, secs, quiet != NULL,
                             seed1, seed2 );
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
      printf( "%s%u: %u", s, i, data.msg_type_cnt[ i ] );
      s = ", ";
    }
  }
  if ( ::strlen( s ) > 0 )
    printf( "\n" );
  if ( data.use_random ) {
    printf( "Random seed1: 0x%016lx seed2 0x%016lx\n", data.seed1,
            data.seed2 );
  }
  return 0;
}

