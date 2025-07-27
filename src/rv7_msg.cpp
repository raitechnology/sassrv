#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <pthread.h>

#include <raimd/md_msg.h>
#include <raimd/md_dict.h>
#include <sassrv/ev_rv_client.h>
#include <sassrv/rv7api.h>
#include <sassrv/rv7cpp.h>

using namespace rv7;

namespace {
struct StrOutput : public MDOutput {
  ArrayOutput out;
  virtual int puts( const char *s ) noexcept;
  virtual int printf( const char *fmt,  ... ) noexcept final __attribute__((format(printf,2,3)));
  StrOutput() {}
};

int StrOutput::puts( const char *s ) noexcept {
  if ( s[ 0 ] == '}' && s[ 1 ] == '\0' ) {
    if ( this->out.ptr[ this->out.count - 1 ] == ' ' ) {
      this->out.ptr[ this->out.count - 1 ] = '}';
      return 0;
    }
  }
  return this->out.puts( s );
}

int
StrOutput::printf( const char *fmt,  ... ) noexcept
{
  va_list args;
  va_start( args, fmt );
  int n = this->out.vprintf( fmt, args );
  va_end( args );
  return n;
}
}

extern "C" {

tibrv_status
tibrvMsg_Create( tibrvMsg * msg )
{
  *msg = new ( ::malloc( sizeof( api_Msg ) ) ) api_Msg( 0 );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_CreateEx( tibrvMsg * msg,  tibrv_u32 /*initial*/ )
{
  return tibrvMsg_Create( msg );
}

tibrv_status
tibrvMsg_Destroy( tibrvMsg msg )
{
  if ( msg != NULL && ((api_Msg *) msg)->owner == NULL )
    delete (api_Msg *) msg;
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_Detach( tibrvMsg msg )
{
  api_Msg *m = (api_Msg *) msg;
  MsgTether *t;
  if ( msg != NULL && (t = m->owner) != NULL ) {
    pthread_mutex_lock( &t->mutex );
    m->owner->pop( m );
    m->owner = NULL;
    pthread_mutex_unlock( &t->mutex );
  }
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_Reset( tibrvMsg msg )
{
  api_Msg * m = (api_Msg *) msg;
  m->reset();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_Expand( tibrvMsg /*msg*/,  tibrv_i32 /*add*/ )
{
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_SetSendSubject( tibrvMsg msg,  const char * subject )
{
  api_Msg * m = (api_Msg *) msg;
  m->subject_len = ( subject == NULL ? 0: ::strlen( subject ) );
  m->subject = ( subject == NULL ? NULL :
                 m->mem.stralloc( m->subject_len, subject ) );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_GetSendSubject( tibrvMsg msg,  const char ** subject )
{
  api_Msg * m = (api_Msg *) msg;
  *subject = m->subject;
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_SetReplySubject( tibrvMsg msg,  const char * reply )
{
  api_Msg * m = (api_Msg *) msg;
  m->reply_len = ( reply == NULL ? 0 : ::strlen( reply ) );
  m->reply = ( reply == NULL ? NULL : m->mem.stralloc( m->reply_len, reply ) );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_GetReplySubject( tibrvMsg msg,  const char ** reply )
{
  api_Msg * m = (api_Msg *) msg;
  if ( (*reply = m->reply) != NULL )
    return TIBRV_OK;
  return TIBRV_NOT_FOUND;
}

tibrv_status
tibrvMsg_GetEvent( tibrvMsg msg,  tibrvEvent * id )
{
  *id = ((api_Msg *) msg)->event;
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_GetClosure( tibrvMsg msg,  void ** closure )
{
  *closure = (void *) ((api_Msg *) msg)->cl;
  return TIBRV_OK;
}

static inline void *
get_as_bytes( tibrvMsg msg, tibrv_u32 *size )
{
  api_Msg * m = (api_Msg *) msg;
  tibrv_u32 z = m->wr.update_hdr();
  if ( z == 8 ) {
    if ( m->msg_enc == RVMSG_TYPE_ID && m->msg_len > 8 ) {
      if ( size != NULL )
        *size = m->msg_len;
      return (void *) m->msg_data;
    }
  }
  if ( size != NULL )
    *size = z;
  return m->wr.buf;
}

static inline RvMsg *
get_as_rvmsg( tibrvMsg msg )
{
  RvMsg * rvmsg = ((api_Msg *) msg)->rvmsg;
  if ( rvmsg == NULL ) {
    tibrv_u32 sz;
    void    * buf = get_as_bytes( msg, &sz );
    rvmsg = RvMsg::unpack_rv( buf, 0, sz, 0, NULL, ((api_Msg *) msg)->mem );
  }
  return rvmsg;
}

tibrv_status
tibrvMsg_GetNumFields( tibrvMsg msg,  tibrv_u32 * num_flds )
{
  RvMsg * rvmsg = get_as_rvmsg( msg );
  MDFieldIter * iter;
  tibrv_u32     i = 0;
  if ( rvmsg != NULL && rvmsg->get_field_iter( iter ) == 0 ) {
    if ( iter->first() == 0 )
      for ( i = 1; iter->next() == 0; i++ )
        ;
  }
  *num_flds = i;
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_GetByteSize( tibrvMsg msg,  tibrv_u32 * size )
{
  RvMsg * rvmsg = ((api_Msg *) msg)->rvmsg;
  if ( rvmsg != NULL )
    *size = rvmsg->msg_end - rvmsg->msg_off;
  else
    get_as_bytes( msg, size );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_ConvertToString( tibrvMsg msg,  const char ** str )
{
  api_Msg * m = (api_Msg *) msg;
  RvMsg * rvmsg = get_as_rvmsg( msg );
  StrOutput tmp;
  tmp.puts( "{" );
  rvmsg->print( &tmp, 0, "%s=", NULL );
  tmp.puts( "}" );
  *str = m->mem.stralloc( tmp.out.count, tmp.out.ptr );

  return TIBRV_OK;
}

tibrv_status
tibrvMsg_Print( tibrvMsg msg )
{
  RvMsg * rvmsg = get_as_rvmsg( msg );
  MDOutput tmp;
  tmp.puts( "{" );
  rvmsg->print( &tmp, 0, "%s=", NULL );
  tmp.puts( "}\n" );
  tmp.flush();

  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddField( tibrvMsg msg,  tibrvMsgField * field )
{
  switch ( field->type ) {
    case TIBRVMSG_MSG        : return tibrvMsg_AddMsgEx( msg, field->name, field->data.msg, field->id );
    case TIBRVMSG_DATETIME   : return tibrvMsg_AddDateTimeEx( msg, field->name, &field->data.date, field->id );
    case TIBRVMSG_OPAQUE     : return tibrvMsg_AddOpaqueEx( msg, field->name, field->data.buf, field->size, field->id );
    case TIBRVMSG_STRING     : return tibrvMsg_AddStringEx( msg, field->name, field->data.str, field->id );
    case TIBRVMSG_BOOL       : return tibrvMsg_AddBoolEx( msg, field->name, field->data.boolean, field->id );
    case TIBRVMSG_I8         : return tibrvMsg_AddI8Ex( msg, field->name, field->data.i8, field->id );
    case TIBRVMSG_U8         : return tibrvMsg_AddU8Ex( msg, field->name, field->data.u8, field->id );
    case TIBRVMSG_I16        : return tibrvMsg_AddI16Ex( msg, field->name, field->data.i16, field->id );
    case TIBRVMSG_U16        : return tibrvMsg_AddU16Ex( msg, field->name, field->data.u16, field->id );
    case TIBRVMSG_I32        : return tibrvMsg_AddI32Ex( msg, field->name, field->data.i32, field->id );
    case TIBRVMSG_U32        : return tibrvMsg_AddU32Ex( msg, field->name, field->data.u32, field->id );
    case TIBRVMSG_I64        : return tibrvMsg_AddI64Ex( msg, field->name, field->data.i64, field->id );
    case TIBRVMSG_U64        : return tibrvMsg_AddU64Ex( msg, field->name, field->data.u64, field->id );
    case TIBRVMSG_F32        : return tibrvMsg_AddF32Ex( msg, field->name, field->data.f32, field->id );
    case TIBRVMSG_F64        : return tibrvMsg_AddF64Ex( msg, field->name, field->data.f64, field->id );
    case TIBRVMSG_IPPORT16   : return tibrvMsg_AddIPPort16Ex( msg, field->name, field->data.ipport16, field->id );
    case TIBRVMSG_IPADDR32   : return tibrvMsg_AddIPAddr32Ex( msg, field->name, field->data.ipaddr32, field->id );
    case TIBRVMSG_ENCRYPTED  : return TIBRV_NOT_PERMITTED;
    case TIBRVMSG_NONE       : return TIBRV_OK;
    case TIBRVMSG_I8ARRAY    : return tibrvMsg_AddI8ArrayEx( msg, field->name, (tibrv_i8 *) field->data.array, field->count, field->id );
    case TIBRVMSG_U8ARRAY    : return tibrvMsg_AddU8ArrayEx( msg, field->name, (tibrv_u8 *) field->data.array, field->count, field->id );
    case TIBRVMSG_I16ARRAY   : return tibrvMsg_AddI16ArrayEx( msg, field->name, (tibrv_i16 *) field->data.array, field->count, field->id );
    case TIBRVMSG_U16ARRAY   : return tibrvMsg_AddU16ArrayEx( msg, field->name, (tibrv_u16 *) field->data.array, field->count, field->id );
    case TIBRVMSG_I32ARRAY   : return tibrvMsg_AddI32ArrayEx( msg, field->name, (tibrv_i32 *) field->data.array, field->count, field->id );
    case TIBRVMSG_U32ARRAY   : return tibrvMsg_AddU32ArrayEx( msg, field->name, (tibrv_u32 *) field->data.array, field->count, field->id );
    case TIBRVMSG_I64ARRAY   : return tibrvMsg_AddI64ArrayEx( msg, field->name, (tibrv_i64 *) field->data.array, field->count, field->id );
    case TIBRVMSG_U64ARRAY   : return tibrvMsg_AddU64ArrayEx( msg, field->name, (tibrv_u64 *) field->data.array, field->count, field->id );
    case TIBRVMSG_F32ARRAY   : return tibrvMsg_AddF32ArrayEx( msg, field->name, (tibrv_f32 *) field->data.array, field->count, field->id );
    case TIBRVMSG_F64ARRAY   : return tibrvMsg_AddF64ArrayEx( msg, field->name, (tibrv_f64 *) field->data.array, field->count, field->id );
    case TIBRVMSG_XML        : return tibrvMsg_AddXmlEx( msg, field->name, field->data.buf, field->size, field->id );
    case TIBRVMSG_STRINGARRAY: return tibrvMsg_AddStringArrayEx( msg, field->name, (const char **) field->data.array, field->count, field->id );
    case TIBRVMSG_MSGARRAY   : return tibrvMsg_AddMsgArrayEx( msg, field->name, (tibrvMsg *) field->data.array, field->count, field->id );
    default                  : return TIBRV_NOT_PERMITTED;
  }
}

tibrv_status
tibrvMsg_UpdateField( tibrvMsg msg,  tibrvMsgField * field )
{
  switch ( field->type ) {
    case TIBRVMSG_MSG        : return tibrvMsg_UpdateMsgEx( msg, field->name, field->data.msg, field->id );
    case TIBRVMSG_DATETIME   : return tibrvMsg_UpdateDateTimeEx( msg, field->name, &field->data.date, field->id );
    case TIBRVMSG_OPAQUE     : return tibrvMsg_UpdateOpaqueEx( msg, field->name, field->data.buf, field->size, field->id );
    case TIBRVMSG_STRING     : return tibrvMsg_UpdateStringEx( msg, field->name, field->data.str, field->id );
    case TIBRVMSG_BOOL       : return tibrvMsg_UpdateBoolEx( msg, field->name, field->data.boolean, field->id );
    case TIBRVMSG_I8         : return tibrvMsg_UpdateI8Ex( msg, field->name, field->data.i8, field->id );
    case TIBRVMSG_U8         : return tibrvMsg_UpdateU8Ex( msg, field->name, field->data.u8, field->id );
    case TIBRVMSG_I16        : return tibrvMsg_UpdateI16Ex( msg, field->name, field->data.i16, field->id );
    case TIBRVMSG_U16        : return tibrvMsg_UpdateU16Ex( msg, field->name, field->data.u16, field->id );
    case TIBRVMSG_I32        : return tibrvMsg_UpdateI32Ex( msg, field->name, field->data.i32, field->id );
    case TIBRVMSG_U32        : return tibrvMsg_UpdateU32Ex( msg, field->name, field->data.u32, field->id );
    case TIBRVMSG_I64        : return tibrvMsg_UpdateI64Ex( msg, field->name, field->data.i64, field->id );
    case TIBRVMSG_U64        : return tibrvMsg_UpdateU64Ex( msg, field->name, field->data.u64, field->id );
    case TIBRVMSG_F32        : return tibrvMsg_UpdateF32Ex( msg, field->name, field->data.f32, field->id );
    case TIBRVMSG_F64        : return tibrvMsg_UpdateF64Ex( msg, field->name, field->data.f64, field->id );
    case TIBRVMSG_IPPORT16   : return tibrvMsg_UpdateIPPort16Ex( msg, field->name, field->data.ipport16, field->id );
    case TIBRVMSG_IPADDR32   : return tibrvMsg_UpdateIPAddr32Ex( msg, field->name, field->data.ipaddr32, field->id );
    case TIBRVMSG_ENCRYPTED  : return TIBRV_NOT_PERMITTED;
    case TIBRVMSG_NONE       : return TIBRV_OK;
    case TIBRVMSG_I8ARRAY    : return tibrvMsg_UpdateI8ArrayEx( msg, field->name, (tibrv_i8 *) field->data.array, field->count, field->id );
    case TIBRVMSG_U8ARRAY    : return tibrvMsg_UpdateU8ArrayEx( msg, field->name, (tibrv_u8 *) field->data.array, field->count, field->id );
    case TIBRVMSG_I16ARRAY   : return tibrvMsg_UpdateI16ArrayEx( msg, field->name, (tibrv_i16 *) field->data.array, field->count, field->id );
    case TIBRVMSG_U16ARRAY   : return tibrvMsg_UpdateU16ArrayEx( msg, field->name, (tibrv_u16 *) field->data.array, field->count, field->id );
    case TIBRVMSG_I32ARRAY   : return tibrvMsg_UpdateI32ArrayEx( msg, field->name, (tibrv_i32 *) field->data.array, field->count, field->id );
    case TIBRVMSG_U32ARRAY   : return tibrvMsg_UpdateU32ArrayEx( msg, field->name, (tibrv_u32 *) field->data.array, field->count, field->id );
    case TIBRVMSG_I64ARRAY   : return tibrvMsg_UpdateI64ArrayEx( msg, field->name, (tibrv_i64 *) field->data.array, field->count, field->id );
    case TIBRVMSG_U64ARRAY   : return tibrvMsg_UpdateU64ArrayEx( msg, field->name, (tibrv_u64 *) field->data.array, field->count, field->id );
    case TIBRVMSG_F32ARRAY   : return tibrvMsg_UpdateF32ArrayEx( msg, field->name, (tibrv_f32 *) field->data.array, field->count, field->id );
    case TIBRVMSG_F64ARRAY   : return tibrvMsg_UpdateF64ArrayEx( msg, field->name, (tibrv_f64 *) field->data.array, field->count, field->id );
    case TIBRVMSG_XML        : return tibrvMsg_UpdateXmlEx( msg, field->name, field->data.buf, field->size, field->id );
    case TIBRVMSG_STRINGARRAY: return tibrvMsg_UpdateStringArrayEx( msg, field->name, (const char **) field->data.array, field->count, field->id );
    case TIBRVMSG_MSGARRAY   : return tibrvMsg_UpdateMsgArrayEx( msg, field->name, (tibrvMsg *) field->data.array, field->count, field->id );
    default                  : return TIBRV_NOT_PERMITTED;
  }
}

tibrv_status
tibrvMsg_CreateFromBytes( tibrvMsg * msg,  const void * bytes )
{
  MDMsgMem      mem;
  size_t        sz = get_u32<MD_BIG>( &((uint8_t *) bytes)[ 0 ] );
  RvMsg       * m  = RvMsg::unpack_rv( (void *) bytes, 0, sz, 0, NULL, mem );
  tibrv_status status = TIBRV_OK;
  if ( m == NULL )
    status = TIBRV_CORRUPT_MSG;
  else
    status = tibrvMsg_Create( msg );
  if ( status != TIBRV_OK ) {
    *msg = NULL;
    return status;
  }
  tibrvMsg new_msg = * msg;
  api_Msg & x = *(api_Msg *) new_msg;
  x.wr.append_rvmsg( *m );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_GetAsBytes( tibrvMsg msg,   const void ** ptr )
{
  RvMsg * rvmsg = ((api_Msg *) msg)->rvmsg;
  if ( rvmsg != NULL )
    *ptr = &((uint8_t *) rvmsg->msg_buf)[ rvmsg->msg_off ];
  else
    *ptr = get_as_bytes( msg, NULL );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_GetAsBytesCopy( tibrvMsg msg, void * ptr, tibrv_u32 size )
{
  RvMsg * rvmsg = ((api_Msg *) msg)->rvmsg;
  const void * tmp;
  tibrv_u32 tmp_size;
  if ( rvmsg != NULL ) {
    tmp = &((uint8_t *) rvmsg->msg_buf)[ rvmsg->msg_off ];
    tmp_size = rvmsg->msg_end - rvmsg->msg_off;
  }
  else {
    tmp = get_as_bytes( msg, &tmp_size );
  }
  if ( tmp_size > size )
    return TIBRV_INVALID_ARG;
  ::memcpy( ptr, tmp, tmp_size );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_CreateCopy( const tibrvMsg msg,  tibrvMsg * copy )
{
  api_Msg * m  = (api_Msg *) msg,
          * cp = new ( ::malloc( sizeof( api_Msg ) ) ) api_Msg( 0 );
  if ( m->subject_len > 0 ) {
    cp->subject_len = m->subject_len;
    cp->subject     = cp->mem.stralloc( m->subject_len, m->subject );
  }
  if ( m->reply_len > 0 ) {
    cp->reply_len = m->reply_len;
    cp->reply     = cp->mem.stralloc( m->reply_len, m->reply );
  }
  if ( m->msg_enc == RVMSG_TYPE_ID ) {
    cp->msg_enc  = RVMSG_TYPE_ID;
    cp->msg_len  = m->msg_len;
    cp->msg_data = cp->mem.memalloc( m->msg_len, m->msg_data );
  }
  else if ( m->rvmsg != NULL )
    cp->wr.append_rvmsg( *m->rvmsg );
  else
    cp->wr.append_writer( m->wr );
  *copy = cp;
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_MarkReferences( tibrvMsg msg )
{
  api_Msg     * m   = (api_Msg *) msg;
  TibrvMsgRef * ref = new ( ::malloc( sizeof( TibrvMsgRef ) ) ) TibrvMsgRef();
  ref->blk_ptr = m->mem.blk_ptr;
  ref->mem_off = m->mem.mem_off;
  ref->serial  = m->tether.serial;
  m->refs.push_hd( ref );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_ClearReferences( tibrvMsg msg )
{
  api_Msg * m = (api_Msg *) msg, * sub, * next;
  pthread_mutex_lock( &m->tether.mutex );
  if ( ! m->refs.is_empty() ) {
    TibrvMsgRef * ref = m->refs.pop_hd();
    for ( sub = m->tether.hd; sub != NULL; sub = next ) {
      next = sub->next;
      if ( sub->serial > ref->serial ) {
        m->tether.pop( sub );
        sub->owner = NULL;
        delete sub;
      }
    }
    m->mem.reset( ref->blk_ptr, ref->mem_off );
    delete ref;
  }
  pthread_mutex_unlock( &m->tether.mutex );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_GetCurrentTime( tibrvMsgDateTime * cur )
{
  struct timespec ts;
  clock_gettime( CLOCK_REALTIME, &ts );
  cur->sec  = ts.tv_sec;
  cur->nsec = ts.tv_nsec - ( ts.tv_nsec % 1000 );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_GetCurrentTimeString( char * local,  char * gmt )
{
  const char *fmt = "%Y-%m-%d %H:%M:%S";
  struct timespec ts;
  struct tm tm;
  clock_gettime( CLOCK_REALTIME, &ts );
  if ( gmt != NULL ) {
    gmtime_r( &ts.tv_sec, &tm );
    strftime( gmt, TIBRVMSG_DATETIME_STRING_SIZE, fmt, &tm );
    /*154,979,000 Z*/
    char * p = &gmt[ ::strlen( gmt ) ],
         * e = &gmt[ TIBRVMSG_DATETIME_STRING_SIZE ];
    snprintf( p, e-p, "%luZ",
              ( ts.tv_nsec - ( ts.tv_nsec % 1000 ) ) + 1000000000 );
    *p = '.';
  }
  if ( local != NULL ) {
    localtime_r( &ts.tv_sec, &tm );
    strftime( local, TIBRVMSG_DATETIME_STRING_SIZE, fmt, &tm );
  }
  return TIBRV_OK;
}
}

namespace {

static inline size_t name_len( const char * name,  size_t add )
  { return ( name == NULL ? 0 : ::strlen( name ) + 1 ) + add; }
const char *fid_name( char *fbuf,  const char *name,  uint16_t id ) {
  size_t len = name_len( name, 0 ); 
  if ( len >= 255 - 2 )
    len = 255 - 2;
  ::memcpy( fbuf, name, len );
  if ( len > 0 )
    fbuf[ len - 1 ] = '\0';
  fbuf[ len ] = (char) ( id >> 8 );
  fbuf[ len + 1 ] = (char) ( id & 0xff );
  return fbuf;
}

#define FNAME_ARG ( id == 0 ? name : fid_name( fbuf, name, id ) ), \
                  name_len( name, id == 0 ? 0 : 2 )

static inline RvMsgWriter &
get_writer( tibrvMsg msg ) {
  api_Msg *m = ((api_Msg *) msg);
  m->wr_refs++;
  return m->wr;
}

static inline MDFieldReader &
get_reader( tibrvMsg msg )
{
  api_Msg * m = (api_Msg *) msg;
  bool updated = m->rd_refs != m->wr_refs;
  if ( m->rd == NULL || updated ) {
    RvMsg * rvmsg = m->rvmsg;
    if ( rvmsg == NULL || updated ) {
      tibrv_u32 sz;
      void    * buf = get_as_bytes( msg, &sz );
      rvmsg = RvMsg::unpack_rv( buf, 0, sz, 0, NULL, m->mem );
    }
    m->rd = new ( m->mem.make( sizeof( MDFieldReader ) ) )
            MDFieldReader( *rvmsg );
    m->rd_refs = m->wr_refs;
  }
  return *m->rd;
}

template<class T>
static inline tibrv_status
get_value( MDFieldReader &rd, T * value, MDType type )
{
  if ( rd.get_value( value, sizeof( *value ), type ) )
    return TIBRV_OK;
  return TIBRV_ARG_CONFLICT;
}

template<class T>
static inline tibrv_status
get_value( tibrvMsg msg, const char *name, T * value, tibrv_u16 id,
           MDType type )
{
  char fbuf[ 256 ];
  MDFieldReader & rd = get_reader( msg );
  if ( rd.find( FNAME_ARG ) )
    return get_value( rd, value, type );
  return TIBRV_NOT_FOUND;
}

static inline tibrv_status
get_string( MDFieldReader &rd, char ** value,  tibrv_u32 *len )
{
  size_t sz;
  if ( rd.get_string( *value, sz ) ) {
    *len = sz;
    return TIBRV_OK;
  }
  return TIBRV_ARG_CONFLICT;
}

static inline tibrv_status
get_string( tibrvMsg msg, const char *name, char ** value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  MDFieldReader & rd = get_reader( msg );
  tibrv_u32 len;
  if ( rd.find( FNAME_ARG ) )
    return get_string( rd, value, &len );
  return TIBRV_NOT_FOUND;
}

static inline tibrv_status
get_opaque( MDFieldReader &rd, void ** value, tibrv_u32 *len )
{
  size_t sz;
  if ( rd.get_opaque( *value, sz ) ) {
    *len = (tibrv_u32) sz;
    return TIBRV_OK;
  }
  return TIBRV_ARG_CONFLICT;
}

static inline tibrv_status
get_opaque( tibrvMsg msg, const char *name, void ** value, tibrv_u32 *len,
            tibrv_u16 id )
{
  char fbuf[ 256 ];
  MDFieldReader & rd = get_reader( msg );
  if ( rd.find( FNAME_ARG ) )
    return get_opaque( rd, value, len );
  return TIBRV_NOT_FOUND;
}

template<class T>
static inline tibrv_status
get_array_value( MDFieldReader &rd, T ** value, MDType type,  tibrv_u32 *count )
{
  size_t cnt;
  *count = 0;
  if ( ! rd.get_array_count( cnt ) )
    return TIBRV_ARG_CONFLICT;
  if ( cnt > 0 ) {
    *value = (T *) rd.iter->iter_msg().mem->make( cnt * sizeof( T ) );
    if ( ! rd.get_array_value( *value, cnt, sizeof( T ), type ) )
      return TIBRV_ARG_CONFLICT;
    *count = (tibrv_u32) cnt;
  }
  else {
    *value = NULL;
  }
  return TIBRV_OK;
}

template<class T>
static inline tibrv_status
get_array_value( tibrvMsg msg, const char *name, T ** value, tibrv_u16 id,
                 MDType type,  tibrv_u32 *count )
{
  char fbuf[ 256 ];
  MDFieldReader & rd = get_reader( msg );
  if ( rd.find( FNAME_ARG ) )
    return get_array_value( rd, value, type, count );
  return TIBRV_NOT_FOUND;
}

static inline tibrv_status
get_msg( tibrvMsg msg,  MDFieldReader &rd,  tibrvMsg *sub )
{
  MDMsg *rvmsg;
  *sub = NULL;
  if ( rd.iter->iter_msg().get_sub_msg( rd.mref, rvmsg, rd.iter ) == 0 ) {
    api_Msg * smsg = ((api_Msg *) msg)->make_submsg();
    char * msg_buf = &((char *) rvmsg->msg_buf)[ rvmsg->msg_off ];
    size_t msg_len = rvmsg->msg_end - rvmsg->msg_off;
    smsg->rvmsg = new ( smsg->mem.make( sizeof( RvMsg ) ) )
      RvMsg( smsg->mem.memalloc( msg_len, msg_buf ), 0, msg_len, rvmsg->dict,
             smsg->mem );
    *sub = smsg;
    return TIBRV_OK;
  }
  return TIBRV_ARG_CONFLICT;
}

static inline tibrv_status
get_datetime( MDFieldReader &rd,  tibrvMsgDateTime *value )
{
  MDStamp stamp;
  if ( get_value( rd, &stamp, MD_DATETIME ) == TIBRV_OK ) {
    uint64_t ns = stamp.nanos();
    value->sec  = ns / 1000000000;
    value->nsec = ns % 1000000000;
    return TIBRV_OK;
  }
  return TIBRV_NOT_FOUND;
}

static inline tibrv_status
get_msg_array( tibrvMsg msg,  MDFieldReader &rd,  tibrvMsg **array,  tibrv_u32 *count )
{
  size_t cnt;
  *count = 0;
  if ( ! rd.get_array_count( cnt ) )
    return TIBRV_ARG_CONFLICT;
  if ( cnt > 0 ) {
    MDMsgMem * mem = rd.iter->iter_msg().mem;
    tibrvMsg * ar  = (tibrvMsg *) mem->make( cnt * sizeof( tibrvMsg * ) );
    for ( size_t i = 0; i < cnt; i++ ) {
      api_Msg * m = ((api_Msg *) msg)->make_submsg();
      MDReference aref;
      if ( rd.iter->iter_msg().get_array_ref( rd.mref, i, aref ) == 0 ) {
        m->msg_enc  = RVMSG_TYPE_ID;
        m->msg_data = m->mem.memalloc( aref.fsize, aref.fptr );
        m->msg_len  = aref.fsize;
        ar[ i ] = m;
      }
      else {
        return TIBRV_ARG_CONFLICT;
      }
    }
    *array = ar;
    *count = (tibrv_u32) cnt;
  }
  return TIBRV_OK;
}

static tibrv_status
get_field( tibrvMsg msg,  MDFieldReader &rd,  tibrvMsgField * field ) noexcept
{
  MDName nm;
  ((RvFieldIter *) rd.iter)->get_name( nm );
  field->name  = nm.fname;
  field->size  = 0;
  field->count = 0;
  field->type  = ((RvFieldIter *) rd.iter)->type;
  field->id    = nm.fid;
  memset( &field->data, 0, sizeof( field->data ) );
  switch ( field->type ) {
    case TIBRVMSG_MSG        : return get_msg( msg, rd, &field->data.msg );
    case TIBRVMSG_DATETIME   : return get_datetime( rd, &field->data.date );
    case TIBRVMSG_OPAQUE     : return get_opaque( rd, (void **) &field->data.buf, &field->size );
    case TIBRVMSG_STRING     : return get_string( rd, (char **) &field->data.str, &field->size );
    case TIBRVMSG_BOOL       : return get_value( rd, &field->data.boolean, MD_BOOLEAN );
    case TIBRVMSG_I8         : return get_value( rd, &field->data.i8, MD_INT );
    case TIBRVMSG_U8         : return get_value( rd, &field->data.u8, MD_UINT );
    case TIBRVMSG_I16        : return get_value( rd, &field->data.i16, MD_INT );
    case TIBRVMSG_U16        : return get_value( rd, &field->data.u16, MD_UINT );
    case TIBRVMSG_I32        : return get_value( rd, &field->data.i32, MD_INT );
    case TIBRVMSG_U32        : return get_value( rd, &field->data.u32, MD_UINT );
    case TIBRVMSG_I64        : return get_value( rd, &field->data.i64, MD_INT );
    case TIBRVMSG_U64        : return get_value( rd, &field->data.u64, MD_UINT );
    case TIBRVMSG_F32        : return get_value( rd, &field->data.f32, MD_REAL );
    case TIBRVMSG_F64        : return get_value( rd, &field->data.f64, MD_REAL );
    case TIBRVMSG_IPPORT16   : return get_value( rd, &field->data.ipport16, MD_IPDATA );
    case TIBRVMSG_IPADDR32   : return get_value( rd, &field->data.ipaddr32, MD_IPDATA );
    case TIBRVMSG_ENCRYPTED  : return TIBRV_NOT_PERMITTED;
    case TIBRVMSG_NONE       : return TIBRV_OK;
    case TIBRVMSG_I8ARRAY    : return get_array_value( rd, (tibrv_i8 **) &field->data.array, MD_INT, &field->count );
    case TIBRVMSG_U8ARRAY    : return get_array_value( rd, (tibrv_u8 **) &field->data.array, MD_UINT, &field->count );
    case TIBRVMSG_I16ARRAY   : return get_array_value( rd, (tibrv_i16 **) &field->data.array, MD_INT, &field->count );
    case TIBRVMSG_U16ARRAY   : return get_array_value( rd, (tibrv_u16 **) &field->data.array, MD_UINT, &field->count );
    case TIBRVMSG_I32ARRAY   : return get_array_value( rd, (tibrv_i32 **) &field->data.array, MD_INT, &field->count );
    case TIBRVMSG_U32ARRAY   : return get_array_value( rd, (tibrv_u32 **) &field->data.array, MD_UINT, &field->count );
    case TIBRVMSG_I64ARRAY   : return get_array_value( rd, (tibrv_i64 **) &field->data.array, MD_INT, &field->count );
    case TIBRVMSG_U64ARRAY   : return get_array_value( rd, (tibrv_u64 **) &field->data.array, MD_UINT, &field->count );
    case TIBRVMSG_F32ARRAY   : return get_array_value( rd, (tibrv_f32 **) &field->data.array, MD_REAL, &field->count );
    case TIBRVMSG_F64ARRAY   : return get_array_value( rd, (tibrv_f64 **) &field->data.array, MD_REAL, &field->count );
    case TIBRVMSG_XML        : return get_string( rd, (char **) &field->data.str, &field->size );
    case TIBRVMSG_STRINGARRAY: return get_array_value( rd, (char ***) &field->data.array, MD_STRING, &field->count );
    case TIBRVMSG_MSGARRAY   : return get_msg_array( msg, rd, (tibrvMsg **) &field->data.array, &field->count );
    default                  : return TIBRV_NOT_PERMITTED;
  }
}

struct UpdGeom {
  MDMsgMem        mem;
  RvMsgWriter   & wr;
  RvMsg         * rvmsg;
  MDFieldReader   rd;
  void          * fld_data;
  size_t          fld_size,
                  fld_end;

  UpdGeom( RvMsgWriter &w )
    : wr( w ),
      rvmsg( RvMsg::unpack_rv( w.buf, 0, w.update_hdr(), 0, NULL, this->mem ) ),
      rd( *this->rvmsg ), fld_data( 0 ), fld_size( 0 ), fld_end( 0 ) {}

  UpdGeom( RvMsgWriter &w, const char *fname,  size_t fnamelen )
    : wr( w ),
      rvmsg( RvMsg::unpack_rv( w.buf, 0, w.update_hdr(), 0, NULL, this->mem ) ),
      rd( *this->rvmsg ), fld_data( 0 ), fld_size( 0 ), fld_end( 0 ) {
    if ( this->rd.find( fname, fnamelen ) )
      this->prefix();
  }

  void prefix( void ) {
    this->fld_end = this->rd.iter->field_end;
    if ( this->wr.off > this->fld_end ) {
      this->fld_size = this->wr.off - this->fld_end;
      this->fld_data = this->mem.make( this->fld_size );
      ::memcpy( this->fld_data, &this->wr.buf[ this->fld_end ], this->fld_size );
    }
    this->wr.off = this->rd.iter->field_start;
  }
  void fin( void ) {
    if ( this->fld_size > 0 ) {
      if ( this->wr.off != this->fld_end )
        this->wr.append_buffer( this->fld_data, this->fld_size );
      else
        this->wr.off += this->fld_size;
    }
  }
};
}
extern "C" {

tibrv_status
tibrvMsg_AddIPAddr32Ex( tibrvMsg msg,  const char * name,  tibrv_ipaddr32 value,  tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_ipdata( FNAME_ARG, value );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddIPPort16Ex( tibrvMsg msg, const char * name, tibrv_ipport16 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_ipdata( FNAME_ARG, value );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddDateTimeEx( tibrvMsg msg, const char * name, const tibrvMsgDateTime * value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  MDStamp stamp( (uint64_t) value->sec * 1000000ULL +
                 (uint64_t) value->nsec / 1000ULL, MD_RES_MICROSECS );
  get_writer( msg ).append_stamp( FNAME_ARG, stamp );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddBoolEx( tibrvMsg msg, const char * name, tibrv_bool value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  uint8_t v = ( value ? 1 : 0 );
  get_writer( msg ).append_type( FNAME_ARG, v, MD_BOOLEAN );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddI8Ex( tibrvMsg msg, const char * name, tibrv_i8 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_INT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddI8ArrayEx( tibrvMsg msg, const char * name, const tibrv_i8 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_INT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddU8Ex( tibrvMsg msg, const char * name, tibrv_u8 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_UINT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddU8ArrayEx( tibrvMsg msg, const char * name, const tibrv_u8 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_UINT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddI16Ex( tibrvMsg msg, const char * name, tibrv_i16 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_INT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddI16ArrayEx( tibrvMsg msg, const char * name, const tibrv_i16 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_INT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddU16Ex( tibrvMsg msg, const char * name, tibrv_u16 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_UINT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddU16ArrayEx( tibrvMsg msg, const char * name, const tibrv_u16 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_UINT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddI32Ex( tibrvMsg msg, const char * name, tibrv_i32 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_INT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddI32ArrayEx( tibrvMsg msg, const char * name, const tibrv_i32 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_INT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddU32Ex( tibrvMsg msg, const char * name, tibrv_u32 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_UINT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddU32ArrayEx( tibrvMsg msg, const char * name, const tibrv_u32 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_UINT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddI64Ex( tibrvMsg msg, const char * name, tibrv_i64 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_INT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddI64ArrayEx( tibrvMsg msg, const char * name, const tibrv_i64 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_INT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddU64Ex( tibrvMsg msg, const char * name, tibrv_u64 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_UINT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddU64ArrayEx( tibrvMsg msg, const char * name, const tibrv_u64 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_UINT );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddF32Ex( tibrvMsg msg, const char * name, tibrv_f32 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_REAL );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddF32ArrayEx( tibrvMsg msg, const char * name, const tibrv_f32 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_REAL );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddF64Ex( tibrvMsg msg, const char * name, tibrv_f64 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_type( FNAME_ARG, value, MD_REAL );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddF64ArrayEx( tibrvMsg msg, const char * name, const tibrv_f64 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_array_type( FNAME_ARG, array, num, MD_REAL );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddStringArrayEx( tibrvMsg msg, const char * name, const char ** value, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_string_array( FNAME_ARG, (char **) value, num, 0 );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddMsgEx( tibrvMsg msg, const char * name, tibrvMsg value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  RvMsgWriter & w = get_writer( msg );
  RvMsgWriter & v = get_writer( value );
  RvMsgWriter submsg( w.mem(), NULL, 0 );
  w.append_msg( FNAME_ARG, submsg );
  submsg.append_writer( v );
  w.update_hdr( submsg );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddMsgArrayEx( tibrvMsg msg, const char * name, const tibrvMsg * value, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  RvMsgWriter & w = get_writer( msg );
  size_t aroff;
  w.append_msg_array( FNAME_ARG, aroff );
  for ( tibrv_u32 i = 0; i < num; i++ ) {
    RvMsgWriter & v = get_writer( value[ i ] );
    RvMsgWriter submsg( w.mem(), NULL, 0 );
    w.append_msg_elem( submsg );
    submsg.append_writer( v );
    w.update_hdr( submsg );
  }
  w.update_array_hdr( aroff, num );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddStringEx( tibrvMsg msg, const char * name, const char * value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  size_t len = ( value == NULL ? 0 : ::strlen( value ) + 1 );
  get_writer( msg ).append_string( FNAME_ARG, value, len );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddOpaqueEx( tibrvMsg msg, const char * name, const void * value, tibrv_u32 size, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_opaque( FNAME_ARG, value, size );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_AddXmlEx( tibrvMsg msg, const char * name, const void * value, tibrv_u32 size, tibrv_u16 id )
{
  char fbuf[ 256 ];
  get_writer( msg ).append_xml( FNAME_ARG, (const char *) value, size );
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_GetFieldEx( tibrvMsg msg, const char * name, tibrvMsgField * field, tibrv_u16 id )
{
  char fbuf[ 256 ];
  MDFieldReader & rd = get_reader( msg );
  if ( rd.find( FNAME_ARG ) )
    return get_field( msg, rd, field );
  return TIBRV_NOT_FOUND;
}

tibrv_status
tibrvMsg_GetFieldInstance( tibrvMsg msg,  const char * name,
                           tibrvMsgField * field,  tibrv_u32 inst )
{
  MDFieldReader & rd = get_reader( msg );
  if ( inst > 0 ) {
    size_t name_len = ( name == NULL ? 0 : ::strlen( name ) + 1 );
    for ( bool b = rd.first(); b; b = rd.next() ) {
      if ( ((RvFieldIter *) rd.iter)->is_named( name, name_len ) ) {
        if ( --inst == 0 )
          return get_field( msg, rd, field );
      }
    }
  }
  return TIBRV_NOT_FOUND;
}

tibrv_status
tibrvMsg_GetFieldByIndex( tibrvMsg msg,  tibrvMsgField * field,  tibrv_u32 idx )
{
  MDFieldReader & rd = get_reader( msg );
  bool b = true;
  if ( idx == 0 || rd.iter->field_index > idx )
    b = rd.first();
  while ( b && rd.iter->field_index < idx )
    b = rd.next();
  if ( b ) {
    if ( rd.iter->field_index == idx ) {
      rd.iter->get_reference( rd.mref );
      return get_field( msg, rd, field );
    }
  }
  return TIBRV_NOT_FOUND;
}

tibrv_status
tibrvMsg_RemoveFieldInstance( tibrvMsg msg,  const char * name, tibrv_u32 inst )
{
  UpdGeom g( get_writer( msg ) );
  MDFieldReader & rd = g.rd;
  if ( inst > 0 ) {
    size_t name_len = ( name == NULL ? 0 : ::strlen( name ) + 1 );
    for ( bool b = rd.first(); b; b = rd.next() ) {
      if ( ((RvFieldIter *) rd.iter)->is_named( name, name_len ) ) {
        if ( --inst == 0 ) {
          g.prefix();
          g.fin();
          return TIBRV_OK;
        }
      }
    }
  }
  return TIBRV_NOT_FOUND;
}

tibrv_status
tibrvMsg_GetMsgEx( tibrvMsg msg, const char * name, tibrvMsg * sub, tibrv_u16 id )
{
  char fbuf[ 256 ];
  MDFieldReader & rd = get_reader( msg );
  *sub = NULL;
  if ( rd.find( FNAME_ARG ) )
    return get_msg( msg, rd, sub );
  return TIBRV_NOT_FOUND;
}

tibrv_status
tibrvMsg_GetIPAddr32Ex( tibrvMsg msg, const char * name, tibrv_ipaddr32 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_IPDATA );
}

tibrv_status
tibrvMsg_GetIPPort16Ex( tibrvMsg msg, const char * name, tibrv_ipport16 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_IPDATA );
}

tibrv_status
tibrvMsg_GetDateTimeEx( tibrvMsg msg, const char * name, tibrvMsgDateTime * value, tibrv_u16 id )
{
  MDStamp stamp;
  if ( get_value( msg, name, &stamp, id, MD_DATETIME ) == TIBRV_OK ) {
    uint64_t ns = stamp.nanos();
    value->sec  = ns / 1000000000;
    value->nsec = ns % 1000000000;
    return TIBRV_OK;
  }
  return TIBRV_NOT_FOUND;
}

tibrv_status
tibrvMsg_GetBoolEx( tibrvMsg msg, const char * name, tibrv_bool * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_BOOLEAN );
}

tibrv_status
tibrvMsg_GetI8Ex( tibrvMsg msg, const char * name, tibrv_i8 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_INT );
}

tibrv_status
tibrvMsg_GetI8ArrayEx( tibrvMsg msg, const char * name, const tibrv_i8 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_i8 **) array, id, MD_INT, count );
}

tibrv_status
tibrvMsg_GetU8Ex( tibrvMsg msg, const char * name, tibrv_u8 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_UINT );
}

tibrv_status
tibrvMsg_GetU8ArrayEx( tibrvMsg msg, const char * name, const tibrv_u8 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_u8 **) array, id, MD_UINT, count );
}

tibrv_status
tibrvMsg_GetI16Ex( tibrvMsg msg, const char * name, tibrv_i16 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_INT );
}

tibrv_status
tibrvMsg_GetI16ArrayEx( tibrvMsg msg, const char * name, const tibrv_i16 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_i16 **) array, id, MD_INT, count );
}

tibrv_status
tibrvMsg_GetU16Ex( tibrvMsg msg, const char * name, tibrv_u16 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_UINT );
}

tibrv_status
tibrvMsg_GetU16ArrayEx( tibrvMsg msg, const char * name, const tibrv_u16 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_u16 **) array, id, MD_UINT, count );
}

tibrv_status
tibrvMsg_GetI32Ex( tibrvMsg msg, const char * name, tibrv_i32 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_INT );
}

tibrv_status
tibrvMsg_GetI32ArrayEx( tibrvMsg msg, const char * name, const tibrv_i32 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_i32 **) array, id, MD_INT, count );
}

tibrv_status
tibrvMsg_GetU32Ex( tibrvMsg msg, const char * name, tibrv_u32 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_UINT );
}

tibrv_status
tibrvMsg_GetU32ArrayEx( tibrvMsg msg, const char * name, const tibrv_u32 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_u32 **) array, id, MD_UINT, count );
}

tibrv_status
tibrvMsg_GetI64Ex( tibrvMsg msg, const char * name, tibrv_i64 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_INT );
}

tibrv_status
tibrvMsg_GetI64ArrayEx( tibrvMsg msg, const char * name, const tibrv_i64 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_i64 **) array, id, MD_INT, count );
}

tibrv_status
tibrvMsg_GetU64Ex( tibrvMsg msg, const char * name, tibrv_u64 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_UINT );
}

tibrv_status
tibrvMsg_GetU64ArrayEx( tibrvMsg msg, const char * name, const tibrv_u64 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_u64 **) array, id, MD_UINT, count );
}

tibrv_status
tibrvMsg_GetF32Ex( tibrvMsg msg, const char * name, tibrv_f32 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_REAL );
}

tibrv_status
tibrvMsg_GetF32ArrayEx( tibrvMsg msg, const char * name, const tibrv_f32 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_f32 **) array, id, MD_REAL, count );
}

tibrv_status
tibrvMsg_GetF64Ex( tibrvMsg msg, const char * name, tibrv_f64 * value, tibrv_u16 id )
{
  return get_value( msg, name, value, id, MD_REAL );
}

tibrv_status
tibrvMsg_GetF64ArrayEx( tibrvMsg msg, const char * name, const tibrv_f64 ** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (tibrv_f64 **) array, id, MD_REAL, count );
}

tibrv_status
tibrvMsg_GetStringArrayEx( tibrvMsg msg, const char * name, const char *** array, tibrv_u32 *count, tibrv_u16 id )
{
  return get_array_value( msg, name, (char ***) array, id, MD_STRING, count );
}

tibrv_status
tibrvMsg_GetMsgArrayEx( tibrvMsg msg, const char * name, const tibrvMsg ** array, tibrv_u32 *count, tibrv_u16 id )
{
  char fbuf[ 256 ];
  MDFieldReader & rd = get_reader( msg );
  *array = NULL;
  *count = 0;
  if ( rd.find( FNAME_ARG ) )
    return get_msg_array( msg, rd, (tibrvMsg **) array, count );
  return TIBRV_NOT_FOUND;
}

tibrv_status
tibrvMsg_GetStringEx( tibrvMsg msg, const char * name, const char ** value, tibrv_u16 id )
{
  return get_string( msg, name, (char **) value, id );
}

tibrv_status
tibrvMsg_GetOpaqueEx( tibrvMsg msg, const char * name, const void ** value, tibrv_u32*size, tibrv_u16 id)
{
  return get_opaque( msg, name, (void **) value, size, id );
}

tibrv_status
tibrvMsg_GetXmlEx( tibrvMsg msg, const char * name, const void ** value, tibrv_u32*size, tibrv_u16 id)
{
  char fbuf[ 256 ];
  MDFieldReader & rd = get_reader( msg );
  if ( rd.find( FNAME_ARG ) )
    return get_string( rd, (char **) value, size );
  return TIBRV_NOT_FOUND;
}

tibrv_status
tibrvMsg_RemoveFieldEx( tibrvMsg msg, const char * name, tibrv_u16 id)
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateMsgEx( tibrvMsg msg, const char * name, tibrvMsg value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  RvMsgWriter & v = get_writer( value );
  RvMsgWriter submsg( g.wr.mem(), NULL, 0 );
  g.wr.append_msg( FNAME_ARG, submsg );
  submsg.append_writer( v );
  g.wr.update_hdr( submsg );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateIPAddr32Ex( tibrvMsg msg, const char * name, tibrv_ipaddr32 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_ipdata( FNAME_ARG, value );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateIPPort16Ex( tibrvMsg msg, const char * name, tibrv_ipport16 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_ipdata( FNAME_ARG, value );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateDateTimeEx( tibrvMsg msg, const char * name, const tibrvMsgDateTime * value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  MDStamp stamp( (uint64_t) value->sec * 1000000ULL +
                 (uint64_t) value->nsec / 1000ULL, MD_RES_MICROSECS );
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_stamp( FNAME_ARG, stamp );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateBoolEx( tibrvMsg msg, const char * name, tibrv_bool value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  uint8_t v = ( value ? 1 : 0 );
  g.wr.append_type( FNAME_ARG, v, MD_BOOLEAN );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateI8Ex( tibrvMsg msg, const char * name, tibrv_i8 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_INT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateI8ArrayEx( tibrvMsg msg, const char * name, const tibrv_i8 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_INT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateU8Ex( tibrvMsg msg, const char * name, tibrv_u8 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_UINT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateU8ArrayEx( tibrvMsg msg, const char * name, const tibrv_u8 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_UINT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateI16Ex( tibrvMsg msg, const char * name, tibrv_i16 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_INT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateI16ArrayEx( tibrvMsg msg, const char * name, const tibrv_i16 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_INT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateU16Ex( tibrvMsg msg, const char * name, tibrv_u16 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_UINT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateU16ArrayEx( tibrvMsg msg, const char * name, const tibrv_u16 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_UINT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateI32Ex( tibrvMsg msg, const char * name, tibrv_i32 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_INT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateI32ArrayEx( tibrvMsg msg, const char * name, const tibrv_i32 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_INT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateU32Ex( tibrvMsg msg, const char * name, tibrv_u32 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_UINT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateU32ArrayEx( tibrvMsg msg, const char * name, const tibrv_u32 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_UINT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateI64Ex( tibrvMsg msg, const char * name, tibrv_i64 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_INT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateI64ArrayEx( tibrvMsg msg, const char * name, const tibrv_i64 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_INT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateU64Ex( tibrvMsg msg, const char * name, tibrv_u64 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_UINT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateU64ArrayEx( tibrvMsg msg, const char * name, const tibrv_u64 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_UINT );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateF32Ex( tibrvMsg msg, const char * name, tibrv_f32 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_REAL );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateF32ArrayEx( tibrvMsg msg, const char * name, const tibrv_f32 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_REAL );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateF64Ex( tibrvMsg msg, const char * name, tibrv_f64 value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_type( FNAME_ARG, value, MD_REAL );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateF64ArrayEx( tibrvMsg msg, const char * name, const tibrv_f64 * array, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_array_type( FNAME_ARG, array, num, MD_REAL );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateStringArrayEx( tibrvMsg msg, const char * name, const char ** value, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_string_array( FNAME_ARG, (char **) value, num, 0 );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateMsgArrayEx( tibrvMsg msg, const char * name, const tibrvMsg * value, tibrv_u32 num, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  size_t aroff;
  g.wr.append_msg_array( FNAME_ARG, aroff );
  for ( tibrv_u32 i = 0; i < num; i++ ) {
    RvMsgWriter & v = get_writer( value[ i ] );
    RvMsgWriter submsg( g.wr.mem(), NULL, 0 );
    g.wr.append_msg_elem( submsg );
    submsg.append_writer( v );
    g.wr.update_hdr( submsg );
  }
  g.wr.update_array_hdr( aroff, num );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateStringEx( tibrvMsg msg, const char * name, const char * value, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  size_t len = ( value == NULL ? 0 : ::strlen( value ) + 1 );
  g.wr.append_string( FNAME_ARG, value, len );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateOpaqueEx( tibrvMsg msg, const char * name, const void * value, tibrv_u32 size, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_opaque( FNAME_ARG, value, size );
  g.fin();
  return TIBRV_OK;
}

tibrv_status
tibrvMsg_UpdateXmlEx( tibrvMsg msg, const char * name, const void * value, tibrv_u32 size, tibrv_u16 id )
{
  char fbuf[ 256 ];
  UpdGeom g( get_writer( msg ), FNAME_ARG );
  g.wr.append_xml( FNAME_ARG, (const char *) value, size );
  g.fin();
  return TIBRV_OK;
}

}
