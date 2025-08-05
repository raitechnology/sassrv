#ifndef __rai__sassrv__rv7cpp_h__
#define __rai__sassrv__rv7cpp_h__

using namespace rai;
using namespace md;
using namespace kv;
using namespace sassrv;

namespace rv7 {

typedef enum { TIBRV_NONE     = 0,
               TIBRV_TIMER    = TIBRV_TIMER_EVENT,
               TIBRV_IO       = TIBRV_IO_EVENT,
               TIBRV_LISTENER = TIBRV_LISTEN_EVENT,
               TIBRV_QUEUE,
               TIBRV_QUEUE_GROUP,
               TIBRV_TRANSPORT,
               TIBRV_DISPATCHER,
               TIBRV_FTMEMBER,
               TIBRV_FTMONITOR } ElemType;

struct tibrv_Elem {
  tibrvId  id;
  ElemType type;
  void   * ptr;
};

struct EvPipe;
struct api_Queue;
struct api_Transport;

struct Tibrv_API {
  EvPoll          poll;
  tibrvId         next_id, free_id, map_size;
  int             idle_count;
  tibrv_Elem    * map;
  pthread_mutex_t map_mutex;
  pthread_cond_t  cond;
  EvPipe        * ev_read;
  int             pfd[ 2 ];
  api_Queue     * default_queue;
  api_Transport * process_tport;
  void * operator new( size_t, void *ptr ) { return ptr; }
  Tibrv_API() : next_id( 11 ), free_id( 0 ), map_size( 0 ), idle_count( 0 ),
               map( 0 ), ev_read( 0 ), default_queue( 0 ), process_tport( 0 ) {}
  bool do_poll( uint64_t nsecs,  bool once ) noexcept;

  template<class T>
  T *make( ElemType type,  size_t add = 0,  tibrvId id = 0 ) {
    void * mem;
    if ( type == TIBRV_TRANSPORT )
      mem = aligned_malloc( sizeof( T ) + add );
    else
      mem = ::malloc( sizeof( T ) + add );

    pthread_mutex_lock( &this->map_mutex );
    if ( id == 0 ) {
      if ( this->free_id != 0 ) {
        for (;;) {
          id = this->free_id++;
          if ( id >= this->next_id ) {
            id = this->next_id++;
            this->free_id = 0;
            break;
          }
          if ( this->map[ id ].ptr == NULL )
            break;
        }
      }
      else {
        id = this->next_id++;
      }
    }
    T *p = new ( mem ) T( *this, id );
    if ( id >= this->map_size ) {
      this->map = (tibrv_Elem *)
        ::realloc( this->map, ( this->map_size + 16 ) * sizeof( tibrv_Elem ) );
      ::memset( &this->map[ this->map_size ], 0, 16 * sizeof( tibrv_Elem ) );
      this->map_size += 16;
    }
    this->map[ id ].id   = id;
    this->map[ id ].type = type;
    this->map[ id ].ptr  = p;
    pthread_mutex_unlock( &this->map_mutex );
    return p;
  }

  template<class T>
  T *get( tibrvId id, ElemType type ) {
    pthread_mutex_lock( &this->map_mutex );
    bool b = ( id < this->map_size && id == this->map[ id ].id &&
               type == this->map[ id ].type );
    T  * p = (T *) ( b ? this->map[ id ].ptr : NULL );
    pthread_mutex_unlock( &this->map_mutex );
    return p;
  }

  template<class T>
  T *rem( tibrvId id, ElemType type ) {
    pthread_mutex_lock( &this->map_mutex );
    bool b = ( id < this->map_size && id == this->map[ id ].id &&
               type == this->map[ id ].type );
    T  * p = NULL;
    if ( b ) {
      p = (T *) this->map[ id ].ptr;
      this->map[ id ].ptr = NULL;
    }
    if ( this->free_id == 0 || id < this->free_id )
      this->free_id = id;
    pthread_mutex_unlock( &this->map_mutex );
    return p;
  }

  void set_string( char *&str,  const char *value ) {
    if ( str != NULL ) { ::free( str ); str = NULL; }
    if ( value != NULL ) { str = ::strdup( value ); }
  }

  tibrv_status Open( void ) noexcept;
  tibrv_status CreateListener( tibrvEvent * event,  tibrvQueue queue, tibrvTransport tport,  tibrvEventCallback cb, tibrvEventVectorCallback vcb,  const char * subj, const void * closure ) noexcept;
  tibrv_status CreateTimer( tibrvEvent * event,  tibrvQueue queue, tibrvEventCallback cb,  tibrv_f64 ival, const void * closure ) noexcept;
  tibrv_status DestroyEvent( tibrvEvent event, tibrvEventOnComplete cb ) noexcept;
  tibrv_status GetEventType( tibrvEvent event,  tibrvEventType * type ) noexcept;
  tibrv_status GetEventQueue( tibrvEvent event,  tibrvQueue * queue ) noexcept;
  tibrv_status GetListenerSubject( tibrvEvent event,  const char ** subject ) noexcept;
  tibrv_status GetListenerTransport( tibrvEvent event,  tibrvTransport * tport ) noexcept;
  tibrv_status GetTimerInterval( tibrvEvent event,  tibrv_f64 * ival ) noexcept;
  tibrv_status ResetTimerInterval( tibrvEvent event,  tibrv_f64 ival ) noexcept;
  tibrv_status CreateQueue( tibrvQueue * q ) noexcept;
  tibrv_status TimedDispatchQueue( tibrvQueue q, tibrv_f64 timeout ) noexcept;
  tibrv_status TimedDispatchQueueOneEvent( tibrvQueue q, tibrv_f64 timeout ) noexcept;
  tibrv_status DestroyQueue( tibrvQueue q, tibrvQueueOnComplete cb, const void * cl ) noexcept;
  tibrv_status GetQueueCount( tibrvQueue q, tibrv_u32 * num ) noexcept;
  tibrv_status GetQueuePriority( tibrvQueue q, tibrv_u32 * priority ) noexcept;
  tibrv_status SetQueuePriority( tibrvQueue q, tibrv_u32 prio ) noexcept;
  tibrv_status GetQueueLimitPolicy( tibrvQueue q, tibrvQueueLimitPolicy * policy, tibrv_u32 * max_ev, tibrv_u32 * discard ) noexcept;
  tibrv_status SetQueueLimitPolicy( tibrvQueue q, tibrvQueueLimitPolicy policy, tibrv_u32 max_ev, tibrv_u32 discard ) noexcept;
  tibrv_status SetQueueName( tibrvQueue q, const char * name ) noexcept;
  tibrv_status GetQueueName( tibrvQueue q, const char ** name ) noexcept;
  tibrv_status SetQueueHook( tibrvQueue q, tibrvQueueHook hook, void * closure ) noexcept;
  tibrv_status GetQueueHook( tibrvQueue q, tibrvQueueHook * hook ) noexcept;
  tibrv_status CreateQueueGroup( tibrvQueueGroup * grp ) noexcept;
  tibrv_status TimedDispatchGroup( tibrvQueueGroup grp, tibrv_f64 timeout ) noexcept;
  tibrv_status DestroyQueueGroup( tibrvQueueGroup grp ) noexcept;
  tibrv_status AddQueueGroup( tibrvQueueGroup grp, tibrvQueue q ) noexcept;
  tibrv_status RemoveQueueGroup( tibrvQueueGroup grp, tibrvQueue q ) noexcept;
  tibrv_status CreateTransport( tibrvTransport * tport, const char * service, const char * network, const char * daemon ) noexcept;
  tibrv_status Send( tibrvTransport tport, tibrvMsg msg ) noexcept;
  tibrv_status Sendv( tibrvTransport tport, tibrvMsg * vec, tibrv_u32 cnt ) noexcept;
  tibrv_status SendRequest( tibrvTransport tport, tibrvMsg msg, tibrvMsg * reply, tibrv_f64 idle_timeout ) noexcept;
  tibrv_status SendReply( tibrvTransport tport, tibrvMsg msg, tibrvMsg request_msg ) noexcept;
  tibrv_status DestroyTransport( tibrvTransport tport ) noexcept;
  tibrv_status CreateInbox( tibrvTransport tport, char * inbox_str, tibrv_u32 inbox_len ) noexcept;
  tibrv_status GetService( tibrvTransport tport, const char ** service_string ) noexcept;
  tibrv_status GetNetwork( tibrvTransport tport, const char ** network_string ) noexcept;
  tibrv_status GetDaemon( tibrvTransport tport, const char ** daemon_string ) noexcept;
  tibrv_status SetDescription( tibrvTransport tport, const char * descr ) noexcept;
  tibrv_status GetDescription( tibrvTransport tport, const char ** descr ) noexcept;
  tibrv_status SetSendingWaitLimit( tibrvTransport tport, tibrv_u32 num_bytes ) noexcept;
  tibrv_status GetSendingWaitLimit( tibrvTransport tport, tibrv_u32 * num_bytes ) noexcept;
  tibrv_status SetBatchMode( tibrvTransport tport, tibrvTransportBatchMode mode ) noexcept;
  tibrv_status SetBatchSize( tibrvTransport tport, tibrv_u32 num_bytes ) noexcept;
  tibrv_status RequestReliability( tibrvTransport tport, tibrv_f64 reliability ) noexcept;
  tibrv_status CreateDispatcher( tibrvDispatcher * disp, tibrvDispatchable able, tibrv_f64 idle_timeout ) noexcept;
  tibrv_status JoinDispatcher( tibrvDispatcher disp ) noexcept;
  tibrv_status SetDispatcherName( tibrvDispatcher disp, const char * name ) noexcept;
  tibrv_status GetDispatcherName( tibrvDispatcher disp, const char ** name ) noexcept;
  tibrv_status CreateFtMember( tibrvftMember * memb, tibrvQueue q, tibrvftMemberCallback cb, tibrvTransport tport, const char * name, tibrv_u16 weight, tibrv_u16 active_goal, tibrv_f64 hb_ival, tibrv_f64 prepare_ival, tibrv_f64 activate_ival, const void * closure ) noexcept;
  tibrv_status DestroyFtMember( tibrvftMember memb ) noexcept;
  tibrv_status DestroyExFtMember( tibrvftMember memb, tibrvftMemberOnComplete cb ) noexcept;
  tibrv_status GetFtMemberQueue( tibrvftMember memb, tibrvQueue * q ) noexcept;
  tibrv_status GetFtMemberTransport( tibrvftMember memb, tibrvTransport * tport ) noexcept;
  tibrv_status GetFtMemberGroupName( tibrvftMember memb, const char ** name ) noexcept;
  tibrv_status GetFtMemberWeight( tibrvftMember memb, tibrv_u16 * weight ) noexcept;
  tibrv_status SetFtMemberWeight( tibrvftMember memb, tibrv_u16 weight ) noexcept;
  tibrv_status CreateFtMonitor( tibrvftMonitor * m, tibrvQueue q, tibrvftMonitorCallback cb, tibrvTransport tport, const char * name, tibrv_f64 lost_ival, const void * closure ) noexcept;
  tibrv_status DestroyFtMonitor( tibrvftMonitor m ) noexcept;
  tibrv_status DestroyExFtMonitor( tibrvftMonitor m, tibrvftMonitorOnComplete cb ) noexcept;
  tibrv_status GetFtMonitorQueue( tibrvftMonitor m, tibrvQueue * q ) noexcept;
  tibrv_status GetFtMonitorTransport( tibrvftMonitor m, tibrvTransport * tport ) noexcept;
  tibrv_status GetFtMonitorGroupName( tibrvftMonitor m, const char ** name ) noexcept;
};

struct api_Msg;
struct TibrvQueueEvent {
  Tibrv_API              & api;
  TibrvQueueEvent        * next, * back;
  api_Msg                * msg, ** vec;
  tibrvEventCallback       cb;
  tibrvEventVectorCallback vcb;
  const void             * cl;
  tibrvEvent               id;
  tibrv_u32                cnt;

  void * operator new( size_t, void *ptr ) { return ptr; }
  TibrvQueueEvent( Tibrv_API &a,  tibrvId i,  tibrvEventCallback e,
                   tibrvEventVectorCallback v, const void *c,  api_Msg *m )
    : api( a ), next( 0 ), back( 0 ), msg( m ), vec( 0 ), cb( e ), vcb( v ),
      cl( c ), id( i ), cnt( 1 ) {}
  void dispatch( void ) noexcept;
  void release( api_Msg *m ) noexcept;
  void release( api_Msg **vec,  tibrv_u32 count ) noexcept;
};

typedef DLinkList< TibrvQueueEvent > TibrvQueueEventList;

struct MsgTether : public DLinkList< api_Msg > {
  pthread_mutex_t mutex;
  uint64_t serial;
  MsgTether() : serial( 0 ) {
    pthread_mutex_init( &this->mutex, NULL );
  }
};

struct api_QueueGroup;
struct api_Queue {
  Tibrv_API           & api;
  api_Queue           * next, * back;
  tibrvQueue            id;
  tibrv_u32             priority,
                        count;
  tibrvQueueHook        hook;
  void                * hook_cl;
  char                * name;
  tibrvQueueLimitPolicy policy;
  tibrv_u32             max_ev,
                        discard;
  pthread_mutex_t       mutex;
  pthread_cond_t        cond;
  TibrvQueueEventList   list;
  MsgTether             tether;
  MDMsgMem              mem_x[ 2 ];
  uint8_t               mptr;
  bool                  done;
  tibrvQueueOnComplete  cb;
  const void          * cl;
  api_QueueGroup      * grp;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  api_Queue( Tibrv_API &a,  tibrvId i ) : api( a ), next( 0 ), back( 0 ),
      id( i ), priority( 0 ), count( 0 ), hook( 0 ), hook_cl( 0 ), name( 0 ),
      policy( TIBRVQUEUE_DISCARD_NONE ), max_ev( 0 ), discard( 0 ), mptr( 0 ),
      done( false ), cb( 0 ), cl( 0 ), grp( 0 ) {
    pthread_mutex_init( &this->mutex, NULL );
    pthread_cond_init( &this->cond, NULL );
  }
  bool push( tibrvId id,  tibrvEventCallback cb,  tibrvEventVectorCallback vcb,
             const void *cl,  api_Msg *msg ) noexcept;
  tibrv_status finish_queue( void ) noexcept;
};

typedef DLinkList< api_Queue > TibrvQueueList;

struct api_QueueGroup {
  Tibrv_API     & api;
  TibrvQueueList  list;
  tibrvQueueGroup id;
  pthread_mutex_t mutex;
  pthread_cond_t  cond;
  tibrv_u32       count;
  bool            update,
                  done;
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  api_QueueGroup( Tibrv_API &a, tibrvId i ) : api( a ), id( i ), count( 0 ),
      update( false ), done( false ) {
    pthread_mutex_init( &this->mutex, NULL );
    pthread_cond_init( &this->cond, NULL );
  }
};

struct api_Dispatcher {
  Tibrv_API     & api;
  tibrvDispatcher id;
  tibrvQueue      queue;
  tibrv_f64       idle_timeout;
  char          * name;
  bool            quit,
                  done;
  pthread_mutex_t mutex;
  pthread_cond_t  cond;
  pthread_t       thr_id;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  api_Dispatcher( Tibrv_API &a,  tibrvId i ) : api( a ), id( i ),
      queue( 0 ), idle_timeout( 0 ), name( 0 ),
      quit( false ), done( false ), thr_id( 0 ) {
    pthread_mutex_init( &this->mutex, NULL );
    pthread_cond_init( &this->cond, NULL );
  }
};

struct api_Listener {
  Tibrv_API              & api;
  api_Listener           * next, * back;
  char                   * subject;
  const void             * cl;
  uint16_t                 len, wild;
  uint32_t                 hash;
  tibrvEventCallback       cb;
  tibrvEventVectorCallback vcb;
  tibrvEvent               id;
  tibrvQueue               queue;
  tibrvTransport           tport;
  
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  api_Listener( Tibrv_API &a,  tibrvId i ) : api( a ), next( 0 ), back( 0 ),
    subject( 0 ), cl( 0 ), len( 0 ), wild( 0 ), hash( 0 ), cb( 0 ), vcb( 0 ),
    id( i ), queue( 0 ), tport( 0 ) {}
};                

typedef DLinkList< api_Listener > TibrvListenerList;

struct api_Listener_ht {           
  TibrvListenerList * ht;
  size_t              mask,
                      count;
  api_Listener_ht( void ) : ht( 0 ), mask( 0 ), count( 0 ) {}
  void init( size_t sz ) {
    this->mask  = sz - 1;
    this->count = 0;
    sz *= sizeof( this->ht[ 0 ] );
    this->ht = (TibrvListenerList *) ::malloc( sz );
    ::memset( (void *) this->ht, 0, sz );
  }
  void resize( void ) {
    size_t sz  = this->mask + 1,
           nsz = sz * 2;
    TibrvListenerList * oht = this->ht;
    this->init( oht == NULL ? 16 : nsz );
    if ( oht != NULL ) {
      for ( size_t i = 0; i < sz; i++ ) {
        while ( ! oht[ i ].is_empty() ) {
          api_Listener * l = oht[ i ].pop_hd();
          this->push( l );
        }
      }
      ::free( oht );
    }
  }
  void fini( void ) {
    ::free( this->ht );
    this->ht    = 0;
    this->mask  = 0;
    this->count = 0;
  }
  void push( api_Listener *l ) {
    if ( this->count >= this->mask )
      this->resize();
    size_t i = l->hash & this->mask;
    this->ht[ i ].push_tl( l );
    this->count++;
  }
  void remove( api_Listener *l ) {
    size_t i = l->hash & this->mask;
    this->ht[ i ].pop( l );
    this->count--;
  }
};

struct api_Msg;
struct api_Rpc {
  api_Rpc    * next,
             * back;
  const char * subject;
  uint32_t     hash;
  uint16_t     len;
  api_Msg    * reply;
  
  api_Rpc( const char *s,  uint16_t l,  uint32_t h ) : next( 0 ), back( 0 ),
    subject( s ), hash( h ), len( l ), reply( 0 ) {}
};                

typedef DLinkList< api_Rpc > TibrvRpcList;

struct api_Transport : public EvConnectionNotify, public RvClientCB,
                       public kv::EvSocket {
  Tibrv_API     & api;
  EvRvClient      client;
  const PeerId  * me;
  api_Listener_ht ht;
  TibrvRpcList    rpc_list;
  UIntHashTab   * wild_ht;
  tibrvTransport  id;
  tibrv_u32       inbox_count,
                  wait_limit,
                  batch_size;
  tibrvTransportBatchMode
                  batch_mode;
  char          * descr;
  pthread_mutex_t mutex;
  pthread_cond_t  cond;

  struct TportReconnectArgs { /* saved state for reconnecting */
    char session[ 64 ];
    char * service, * network, * daemon;
    uint16_t session_len;
    TportReconnectArgs()
      : service( 0 ), network( 0 ), daemon( 0 ), session_len( 0 ) {}
  };
  TportReconnectArgs x;
  bool reconnect_active,
       is_destroyed;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { aligned_free( ptr ); }
  api_Transport( Tibrv_API &a, tibrvId i ) :
    kv::EvSocket( a.poll, a.poll.register_type( "api_Transport" ) ),
    api( a ), client( a.poll ), me( &this->client ), wild_ht( 0 ), id( i ),
    inbox_count( 1 ), wait_limit( 0 ), batch_size( 0 ),
    batch_mode( TIBRV_TRANSPORT_DEFAULT_BATCH ), descr( 0 ),
    reconnect_active( false ), is_destroyed( false ) {
    pthread_mutex_init( &this->mutex, NULL );
    pthread_cond_init( &this->cond, NULL );
  }
  virtual void on_connect( EvSocket &conn ) noexcept;
  virtual void on_shutdown( EvSocket &conn,  const char *err,
                            size_t err_len ) noexcept;
  virtual bool on_rv_msg( EvPublish &pub ) noexcept;
  void add_wildcard( uint16_t pref ) noexcept;
  void remove_wildcard( uint16_t pref ) noexcept;

  virtual bool on_msg( kv::EvPublish &pub ) noexcept;
  virtual void write( void ) noexcept;
  virtual void read( void ) noexcept;
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
};

struct api_Timer : public EvTimerCallback {
  Tibrv_API        & api;
  tibrvEvent         id;
  tibrvQueue         queue;
  tibrvEventCallback cb;
  const void       * cl;
  tibrv_f64          ival;
  bool               in_queue;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  api_Timer( Tibrv_API &a,  tibrvId i ) : api( a ), id( i ), queue( 0 ),
      cb( 0 ), cl( 0 ), ival( 0 ), in_queue( false ) {}
  virtual bool timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept;
  virtual ~api_Timer() {}
};

struct TibrvMsgRef {
  TibrvMsgRef        * next,
                     * back;
  MDMemBlock_t       * blk_ptr;
  uint32_t             mem_off;
  uint64_t             serial;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  TibrvMsgRef() : next( 0 ), back( 0 ), blk_ptr( 0 ), mem_off( 0 ),
                  serial( 0 ) {}
};

typedef DLinkList< TibrvMsgRef > TibrvMsgRefList;

struct api_Msg {
  api_Msg       * next,
                * back;
  MsgTether     * owner;
  const char    * subject,
                * reply;
  const void    * msg_data;
  uint16_t        subject_len,
                  reply_len;
  uint32_t        msg_enc,
                  msg_len;
  tibrvEvent      event;
  RvMsg         * rvmsg;
  MDFieldReader * rd;
  MDMsgMem        mem;
  RvMsgWriter     wr;
  const void    * cl;
  uint32_t        wr_refs,
                  rd_refs;
  bool            in_queue;
  MsgTether       tether;
  uint64_t        serial;
  TibrvMsgRefList refs;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  api_Msg( tibrvEvent ev ) :
    next( 0 ), back( 0 ), owner( 0 ), subject( 0 ), reply( 0 ), msg_data( 0 ),
    subject_len( 0 ), reply_len( 0 ), msg_enc( 0 ), msg_len( 0 ), event( ev ),
    rvmsg( 0 ), rd( 0 ), wr( this->mem, NULL, 0 ), cl( 0 ), wr_refs( 0 ),
    rd_refs( 0 ), in_queue( false ), serial( 0 ) {}
  ~api_Msg() noexcept;
  void release( void ) noexcept;

  static api_Msg * make( EvPublish &pub, RvMsg *rvmsg, MsgTether *tether,
                         tibrvEvent ev, const void *cl ) noexcept;
  api_Msg * make_submsg( void ) noexcept;

  void reset( void ) {
    this->subject     = NULL;
    this->reply       = NULL;
    this->msg_data    = NULL;
    this->subject_len = 0;
    this->reply_len   = 0;
    this->msg_enc     = 0;
    this->msg_len     = 0;
    this->rvmsg       = NULL;
    this->rd          = NULL;
    this->wr.buf      = NULL;
    this->wr.buflen   = 0;
    this->release();
    this->wr.reset();
    this->mem.reuse();
  }
};
struct EvPipeRec;
struct EvPipe : public EvConnection {
  int write_fd;

  void * operator new( size_t, void *ptr ) { return ptr; }
  EvPipe( EvPoll &poll,  int wfd ) :
    EvConnection( poll, poll.register_type( "tibrv_api" ) ), write_fd( wfd ) {}
  bool start( int rfd,  const char *name ) noexcept;
  virtual void process( void ) noexcept final;
  virtual void release( void ) noexcept final {}

  void subscribe( EvPipeRec &rec ) noexcept;
  void unsubscribe( EvPipeRec &rec ) noexcept;
  void create_timer( EvPipeRec &rec ) noexcept;
  void destroy_timer( EvPipeRec &rec ) noexcept;
  void reset_timer( EvPipeRec &rec ) noexcept;
  void create_tport( EvPipeRec &rec ) noexcept;
  void close_tport( EvPipeRec &rec ) noexcept;
  void tport_send( EvPipeRec &rec ) noexcept;
  void tport_sendv( EvPipeRec &rec ) noexcept;

  void exec( EvPipeRec &rec ) noexcept;
};

#define OP_SUBSCRIBE        &EvPipe::subscribe
#define OP_UNSUBSCRIBE      &EvPipe::unsubscribe
#define OP_CREATE_TIMER     &EvPipe::create_timer
#define OP_DESTROY_TIMER    &EvPipe::destroy_timer
#define OP_RESET_TIMER      &EvPipe::reset_timer
#define OP_CREATE_TPORT     &EvPipe::create_tport
#define OP_CLOSE_TPORT      &EvPipe::close_tport
#define OP_TPORT_SEND       &EvPipe::tport_send
#define OP_TPORT_SENDV      &EvPipe::tport_sendv

struct EvPipeRec {
  void ( EvPipe::*func )( EvPipeRec &rec ) noexcept;
  api_Transport   * t;
  api_Listener    * l;
  api_Timer       * timer;
  pthread_mutex_t * mutex;
  pthread_cond_t  * cond;
  EvPublish       * pub;
  tibrv_u32         cnt;
  EvRvClientParameters
                  * parm;
  bool            * complete;

  EvPipeRec( void ( EvPipe::*f )( EvPipeRec &rec ),
             api_Transport   * transport,
             EvRvClientParameters * p,
             pthread_mutex_t * m,
             pthread_cond_t  * c )
    : func( f ), t( transport ), l( 0 ), timer( 0 ),
      mutex( m ), cond( c ), pub( 0 ), cnt( 0 ), parm( p ), complete( 0 ) {}

  EvPipeRec( void ( EvPipe::*f )( EvPipeRec &rec ),
             api_Transport   * transport,
             api_Listener    * listener,
             pthread_mutex_t * m,
             pthread_cond_t  * c )
    : func( f ), t( transport ), l( listener ), timer( 0 ),
      mutex( m ), cond( c ), pub( 0 ), cnt( 0 ), parm( 0 ), complete( 0 ) {}

  EvPipeRec( void ( EvPipe::*f )( EvPipeRec &rec ),
             api_Timer       * tmr,
             pthread_mutex_t * m,
             pthread_cond_t  * c )
    : func( f ), t( 0 ), l( 0 ), timer( tmr ),
      mutex( m ), cond( c ), pub( 0 ), cnt( 0 ), parm( 0 ), complete( 0 ) {}

  EvPipeRec( void ( EvPipe::*f )( EvPipeRec &rec ),
             api_Transport   * transport,
             EvPublish       * p,
             tibrv_u32         count,
             pthread_mutex_t * m,
             pthread_cond_t  * c )
    : func( f ), t( transport ), l( 0 ), timer( 0 ),
      mutex( m ), cond( c ), pub( p ), cnt( count ), parm( 0 ), complete( 0 ) {}

  EvPipeRec() : func( NULL ), t( 0 ), l( 0 ), timer( 0 ),
                mutex( 0 ), cond( 0 ), pub( 0 ), cnt( 0 ), parm( 0 ),
                complete( 0 ) {}
};

}
#endif
