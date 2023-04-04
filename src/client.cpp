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
                      RATE_TIMER_ID   = 3; /* rate timer */

/* rv client callback closure */
struct RvDataCallback : public EvConnectionNotify, public RvClientCB,
                        public EvTimerCallback {
  EvPoll      & poll;            /* poll loop data */
  EvRvClient  & client;          /* connection to rv */
  MDDict      * dict;            /* dictinary to use for decoding msgs */
  const char ** sub;             /* subject strings */
  size_t        sub_count;       /* count of sub[] */
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

  RvDataCallback( EvPoll &p,  EvRvClient &c,  const char **s,  size_t cnt,
                  bool nodict,  bool hex,  bool rate )
    : poll( p ), client( c ), dict( 0 ), sub( s ), sub_count( cnt ),
      msg_count( 0 ), last_count( 0 ), last_time( 0 ), msg_bytes( 0 ),
      last_bytes( 0 ), no_dictionary( nodict ), is_subscribed( false ),
      have_dictionary( false ), dump_hex( hex ), show_rate( rate ) {}

  /* after CONNECTED message */
  virtual void on_connect( EvSocket &conn ) noexcept;
  /* start sub[] with inbox reply */
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

/* start subscriptions from command line, inbox number indexes the sub[] */
void
RvDataCallback::start_subscriptions( void ) noexcept
{
  if ( this->is_subscribed ) /* subscribing multiple times is allowed, */
    return;                  /* but must unsub multiple times as well */
  for ( size_t i = 0; i < this->sub_count; i++ ) {
    char     inbox[ MAX_RV_INBOX_LEN ]; /* _INBOX.<session>.3 + <sub> */
    uint16_t inbox_len = this->client.make_inbox( inbox, i + SUB_INBOX_BASE );
    size_t   sub_len   = ::strlen( this->sub[ i ] );
    printf( "Subscribe \"%.*s\", reply \"%.*s\"\n",
            (int) sub_len, this->sub[ i ], (int) inbox_len, inbox );
    /* subscribe with inbox reply */
    this->client.subscribe( this->sub[ i ], sub_len, inbox, inbox_len );
  }
  if ( this->show_rate ) {
    this->last_time = this->poll.current_coarse_ns();
    this->poll.timer.add_timer_seconds( *this, RATE_TIMER_SECS,
                                        RATE_TIMER_ID, 0 );
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
  for ( size_t i = 0; i < this->sub_count; i++ ) {
    size_t sub_len = ::strlen( this->sub[ i ] );
    printf( "Unsubscribe \"%.*s\"\n", (int) sub_len, this->sub[ i ] );
    /* unsubscribe sub */
    this->client.unsubscribe( this->sub[ i ], sub_len );
  }
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
                 NULL, 0, this->client.sub_route, this->client, 0, 0, 0 );
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
  MDMsgMem mem;
  MDMsg  * m;
  /* check if published to _INBOX.<session>. */
  uint64_t which = this->client.is_inbox( pub.subject, pub.subject_len );
  if ( which != 0 ) {
    size_t idx = which - SUB_INBOX_BASE;
    if ( which >= SUB_INBOX_BASE && idx < this->sub_count ) {
      if ( this->show_rate ) {
        this->msg_count++;
        return true;
      }
      printf( "## %s: (inbox: %.*s)\n", this->sub[ idx ],
               (int) pub.subject_len, pub.subject );
    }
    else if ( which == DICT_INBOX_ID ) {
      printf( "Received dictionary message\n" );
      m = MDMsg::unpack( (void *) pub.msg, 0, pub.msg_len, 0, this->dict,
                         &mem );
      this->on_dict( m );
      this->start_subscriptions();
      return true;
    }
    else {
      printf( "## Unknown inbox message (%lu)\n", idx );
    }
  }
  else { /* not inbox subject */
    if ( this->show_rate ) {
      this->msg_count++;
      this->msg_bytes += pub.msg_len + pub.subject_len;
      return true;
    }
    if ( pub.reply_len != 0 )
      printf( "## %.*s (reply: %.*s):\n", (int) pub.subject_len, pub.subject,
              (int) pub.reply_len, (const char *) pub.reply );
    else
      printf( "## %.*s:\n", (int) pub.subject_len, pub.subject );
  }
  m = MDMsg::unpack( (void *) pub.msg, 0, pub.msg_len, 0, this->dict, &mem );
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
             * help    = get_arg( x, argc, argv, 0, "-h", "-help", 0 );
  int first_sub = x, idle_count = 0;

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
             "  subject    = subject to subscribe\n", argv[ 0 ] );
    return 1;
  }
  if ( first_sub >= argc ) {
    fprintf( stderr, "No subjects subscribed\n" );
    goto help;
  }

  EvPoll poll;
  poll.init( 5, false );

  EvRvClientParameters parm( daemon, network, service, 0 );
  EvRvClient           conn( poll );
  RvDataCallback       data( poll, conn, &argv[ first_sub ], argc - first_sub,
                             nodict != NULL, dump != NULL, rate != NULL );
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
  return 0;
}

