#ifndef __rai__sassrv__rv7ftcpp_h__
#define __rai__sassrv__rv7ftcpp_h__

#include <sassrv/rv7cpp.h>

namespace rv7 {

struct api_FtMember;
struct api_FtMonitor;

struct TibrvFtPeer {
  TibrvFtPeer   * next,
                * back;
  api_FtMember  * me;
  api_FtMonitor * mon;
  const char    * inbox;
  tibrv_f64       hb_ival,
                  prepare_ival,
                  activate_ival;
  tibrv_u64       last_seen,
                  sync_time,
                  last_sync,
                  hb_ns,
                  activate_ns;
  tibrv_u16       weight,
                  active_goal,
                  rank;
  tibrvEvent      host_stop_id,
                  session_stop_id,
                  unreachable_id;
  uint8_t         pub_err;
  bool            is_running,
                  is_stopped,
                  is_unresponsive;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  TibrvFtPeer( api_FtMember *m,  api_FtMonitor *n = NULL )
    : next( 0 ), back( 0 ), me( m ), mon( n ), inbox( 0 ), hb_ival( 0 ),
      prepare_ival( 0 ), activate_ival( 0 ), last_seen( 0 ),
      sync_time( 0 ), last_sync( 0 ), hb_ns( 0 ), activate_ns( 0 ),
      weight( 0 ), active_goal( 0 ), rank( 0 ),
      host_stop_id( 0 ), session_stop_id( 0 ), unreachable_id( 0 ),
      pub_err( 0 ), is_running( false ), is_stopped( false ),
      is_unresponsive( false ) {}

  void unreachable_cb( tibrvMsg m ) noexcept;
  void session_stop_cb( tibrvMsg m ) noexcept;
  void host_stop_cb( tibrvMsg m ) noexcept;
  void unreachable_mon_cb( tibrvMsg m ) noexcept;
  void session_stop_mon_cb( tibrvMsg m ) noexcept;
  void host_stop_mon_cb( tibrvMsg m ) noexcept;
  void print( void ) noexcept;
  void start_listeners( Tibrv_API &api,
                        tibrvQueue queue,
                        tibrvTransport tport,
                        tibrvEventCallback unreachable_cb,
                        tibrvEventCallback session_stop_cb,
                        tibrvEventCallback host_stop_cb ) noexcept;
  void stop_events( Tibrv_API &api ) noexcept;
};
typedef DLinkList< TibrvFtPeer > TibrvFtList;

enum FtState {
  RVFT_NO_STATE    = 0,
  RVFT_INBOX       = 1,
  RVFT_STATUS      = 2,
  RVFT_SYNC        = 3,
  RVFT_STOP        = 4,
  RVFT_START       = 5,
  RVFT_ACTIVE      = 6,
  RVFT_ACTIVE_STOP = 7
};

struct api_FtMember {
  Tibrv_API           & api;
  tibrvftMember         id;
  tibrvQueue            queue;
  tibrvftMemberCallback cb;
  const void          * cl;
  tibrvTransport        tport;
  const char          * name;
  TibrvFtPeer           me;
  TibrvFtList           peers;
  pthread_mutex_t       mutex;
  tibrvEvent            cb_id[ 8 ],
                        activate_id,
                        prepare_id,
                        hb_id;
  tibrv_u64             start_time,
                        prepare_time,
                        fterr_time;
  bool                  is_destroyed;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  api_FtMember( Tibrv_API &a,  tibrvId i ) : api( a ), id( i ), queue( 0 ),
      cb( 0 ), cl( 0 ), tport( 0 ), name( 0 ), me( this ), activate_id( 0 ),
      prepare_id( 0 ), hb_id( 0 ), start_time( 0 ), prepare_time( 0 ),
      fterr_time( 0 ), is_destroyed( false ) {
    for ( int i = 0; i < 8; i++ ) this->cb_id[ i ] = 0;
    pthread_mutex_init( &this->mutex, NULL );
  }
  ~api_FtMember() {}
  void publish( api_Transport *t,  const char *sub,  uint8_t flds ) noexcept;
  bool publish_rvftsub( const char *cl,  const char *nm,
                        const char *de = NULL ) noexcept;
  void update_peer( TibrvFtPeer *p,  tibrvMsg msg,  FtState state ) noexcept;
  tibrv_u64 update_time( void ) noexcept;
  void stop_timers( void ) noexcept;
  tibrv_status prepare( void ) noexcept;
  bool is_prepared( void ) noexcept;
  void inbox_cb( tibrvMsg m ) noexcept;
  void start_cb( tibrvMsg m ) noexcept;
  void sync_cb( tibrvMsg m ) noexcept;
  void stop_cb( tibrvMsg m ) noexcept;
  void status_cb( tibrvMsg m ) noexcept;
  void active_hb_cb( tibrvMsg m ) noexcept;
  void active_start_cb( tibrvMsg m ) noexcept;
  void active_stop_cb( tibrvMsg m ) noexcept;
  void prepare_timer_cb( void ) noexcept;
  void activate_timer_cb( void ) noexcept;
  bool activate_now( void ) noexcept;
  bool do_callback( tibrvftAction action ) noexcept;
  void hb_timer_cb( void ) noexcept;
};

struct api_FtMonitor {
  Tibrv_API            & api;
  tibrvftMember          id;
  tibrvQueue             queue;
  tibrvftMonitorCallback cb;
  const void           * cl;
  tibrvTransport         tport;
  const char           * name;
  tibrv_f64              lost_ival;
  tibrv_u64              lost_ns;
  TibrvFtList            peers;
  pthread_mutex_t        mutex;
  tibrvEvent             cb_id[ 4 ],
                         inactive_id;
  uint16_t               last_active_count;
  bool                   is_destroyed;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  api_FtMonitor( Tibrv_API &a,  tibrvId i ) : api( a ), id( i ), queue( 0 ),
      cb( 0 ), cl( 0 ), tport( 0 ), name( 0 ), lost_ival( 0 ), lost_ns( 0 ),
      inactive_id( 0 ), last_active_count( 0 ), is_destroyed( false ) {
    for ( int i = 0; i < 4; i++ ) this->cb_id[ i ] = 0;
    pthread_mutex_init( &this->mutex, NULL );
  }
  ~api_FtMonitor() {}

  void update_peer( TibrvFtPeer *p,  tibrvMsg msg,  FtState state ) noexcept;
  void inactive_cb( void ) noexcept;
  void stop_cb( tibrvMsg m ) noexcept;
  void active_hb_cb( tibrvMsg m ) noexcept;
  void active_start_cb( tibrvMsg m ) noexcept;
  void active_stop_cb( tibrvMsg m ) noexcept;
};

}
#endif
