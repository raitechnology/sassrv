#ifndef __rai_sassrv__mc_h__
#define __rai_sassrv__mc_h__

#include <raikv/ev_net.h>

namespace rai {
namespace trdp {

enum { DATA     = 1,
       NAK      = 3,
       SPM      = 4,
       NAK_ERR  = 5,
       P2P_DATA = 6,
       P2P_SYN  = 7,
       P2P_ACK  = 8,
       P2P_NAK  = 9 };

struct TrdpTsid {
  uint32_t src_addr,
           src_id;
  TrdpTsid( const TrdpTsid &b ) : src_addr( b.src_addr ), src_id( b.src_id ) {}
  TrdpTsid( uint32_t a,  uint32_t i ) : src_addr( a ), src_id( i ) {}
  TrdpTsid() {}
  bool operator==( const TrdpTsid &b ) const {
    return this->src_addr == b.src_addr && this->src_id == b.src_id;
  }
  TrdpTsid & operator=( const TrdpTsid &b ) {
    this->src_addr = b.src_addr;
    this->src_id   = b.src_id;
    return *this;
  }
  size_t operator&( size_t mod ) const {
    return ( this->src_addr ^ this->src_id ) & mod;
  }
};

static const size_t TRDP_HDR_SIZE = 20,
                    TRDP_DATA_HDR_SIZE = 32;
struct TrdpHdr {
  uint32_t magic,
           src_addr,
           src_id,
           dest_addr;
  uint16_t pkt_type,
           status;
  union {
    struct {
      uint32_t seqno,
               total_len;
      uint16_t data_len,
               frag_num;
    } data;
    struct {
      uint32_t seqno_len,
               seqno[ 1 ];
    } nak;
    uint32_t seqno;
  };
  TrdpTsid tsid( void ) const {
    return TrdpTsid( this->src_addr, this->src_id );
  }
  int type( void ) const { return this->pkt_type & 0xff; }
  bool copy_in( const void *msg_buf,  size_t msg_len ) noexcept;
};

struct TrdpWindowPkt {
  TrdpWindowPkt * next,
                * back;
  uint64_t        mono_time;
  uint32_t        seqno,
                  total_len;
  uint16_t        data_len,
                  frag_num;
  char            data[ 4 ];

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  TrdpWindowPkt( const TrdpHdr &hdr,  const void * data,  uint64_t ns )
    : next( 0 ), back( 0 ), mono_time( ns ),
      seqno( hdr.data.seqno ),
      total_len( hdr.data.total_len ),
      data_len( hdr.data.data_len ),
      frag_num( hdr.data.frag_num ) {
    ::memcpy( this->data, data, hdr.data.data_len );
  }
};

typedef kv::DLinkList<TrdpWindowPkt> TrdpWindowPktList;

struct TrdpWindow {
  TrdpTsid tsid;
  uint32_t last_seqno,
           save_size,
           output_len,
           output_off;
  uint64_t lost_seqno,
           repeat_seqno,
           reorder_seqno,
           nak_count,
           last_lost,
           last_repeat,
           last_reorder,
           last_nak,
           last_mono_time;
  void   * output_buf;
  TrdpWindowPktList list;

  TrdpWindowPkt *merge( const TrdpHdr &hdr,  const void *data,
                        uint64_t cur_mono_time ) noexcept;
};

static inline int32_t
seqno_diff( uint32_t a, uint32_t b ) {
  return (int32_t) a - (int32_t) b;
}

typedef kv::IntHashTabT<TrdpTsid, uint32_t> TrdpHashTab;
typedef kv::ArrayCount<TrdpWindow, 8> TrdpWindows;

struct TrdpWindowDB {
  void * operator new( size_t, void *ptr ) { return ptr; }
  TrdpHashTab * hash;
  TrdpWindows   windows;
  kv::ArrayCount<uint32_t, 8> lost_seqno_list,
                              repeat_seqno_list,
                              reorder_seqno_list,
                              nak_count_list;
  uint64_t      msgs_recv, last_msgs_recv, output_bytes, last_output_bytes,
                last_mono_time, last_lost, last_repeat, last_reorder, last_nak;
  kv::EvConnection * conn;
  TrdpWindowDB()
    : hash( 0 ), msgs_recv( 0 ), last_msgs_recv( 0 ), output_bytes( 0 ),
      last_output_bytes( 0 ), last_mono_time( 0 ), last_lost( 0 ),
      last_repeat( 0 ), last_reorder( 0 ), last_nak( 0 ), conn( 0 ) {}
  TrdpWindow & get_window( TrdpTsid &tsid ) noexcept;
  void process_msg( void *msg,  size_t msg_len,
                    uint64_t cur_mono_time ) noexcept;
  void output( TrdpWindow &w,  size_t len,  const void *data ) noexcept;
  void recv_msg( const void *,  size_t ) noexcept;
  uint64_t lost_seqno( void ) noexcept;
  uint64_t repeat_seqno( void ) noexcept;
  uint64_t reorder_seqno( void ) noexcept;
  uint64_t nak_count( void ) noexcept;
};


struct UdpSvc : public kv::EvUdp {
  void * operator new( size_t, void *ptr ) { return ptr; }
  TrdpWindowDB  & db;

  UdpSvc( kv::EvPoll &p,  TrdpWindowDB *d )
    : EvUdp( p, 0 ), db( *d ) {}
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_shutdown( void ) noexcept;
  virtual void process_close( void ) noexcept;
  static void print_addr( const char *what, const void *sa ) noexcept;
  TrdpWindow & get_window( const TrdpTsid &tsid ) noexcept;
};  

struct TrdpSvc /*: public kv::EvTimerCallback o*/{
  void * operator new( size_t, void *ptr ) { return ptr; }
  TrdpWindowDB db;
  kv::ArrayCount<UdpSvc *, 8> udp;
  TrdpSvc() {}
  /*virtual bool timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept;*/

  static TrdpSvc * create( kv::EvPoll &p,  const char *network,
                           const char *service ) noexcept;
};

}
}
#endif
