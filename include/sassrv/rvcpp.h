#ifndef __rai_sassrv__rvcpp_h__
#define __rai_sassrv__rvcpp_h__

#include <rvapi.h>

struct RvSession;
struct RvSignal;
struct RvListener;
struct RvTimer;
struct RvSender;
struct RvDatum;

struct RvError {
  int err;
  RvError( int e ) : err( e ) {}
  const char *description() const { return rv_ErrorText( NULL, this->err ); }
  bool operator==( int e ) const { return this->err == e; }
  bool operator!=( int e ) const { return this->err != e; }
};

struct RvSignalCallback {
  virtual void onSignal( RvSignal *invoker ) = 0;
};

struct RvSignal {
  RvSession        * sess;
  rv_Signal          signal;
  RvSignalCallback * callback;

  RvSignal( RvSession *s,  RvSignalCallback *cb )
    : sess( s ), signal( 0 ), callback( cb ) {}

  static void bounce( rv_Signal, void *closure ) {
    RvSignal *s = (RvSignal *) closure;
    s->callback->onSignal( s );
  }
  RvSession *session( void ) const { return this->sess; }
};

struct RvDataCallback {
  virtual void onData( const char    * subject,
                       RvSender      * replySender,
                       const RvDatum & data,
                       RvListener    * invoker ) = 0;
};

struct RvSender {
  RvSession * sess;
  rv_Name     name;

  RvSender( RvSession *s, rv_Name n ) : sess( s ), name( n ) {}
  void send( const char *subject, const RvDatum &data );
  RvSession *session( void ) const { return this->sess; }
  const char *subject( void ) const { return this->name; }
};

struct RvDatum {
  void * msg;
  size_t msg_len;
  int    type;

  RvDatum( void *d, size_t sz, int t ) : msg( d ), msg_len( sz ), type( t ) {}
  void * data( void ) const { return this->msg; }
  size_t size( void ) const { return this->msg_len; }
  int    print( void ) const {
    return rv_Print( (void *) this->msg, this->msg_len, this->type );
  }
};

struct RvListener {
  RvSession      * sess;
  rv_Listener      listener;
  RvDataCallback * callback;

  RvListener( RvSession *s, RvDataCallback *cb )
    : sess( s ), listener( 0 ), callback( cb ) {}

  static void bounce( rv_Listener, rv_Name subject, rv_Name reply,
                      rvmsg_Type type, size_t data_len, void* data, void *cl ) {
    RvListener *l = (RvListener *) cl;
    RvSender sender( l->sess, reply );
    RvDatum datum( data, data_len, type );
    l->callback->onData( subject, reply ? &sender : NULL, datum, l );
  }
  const char *subject( void ) const {
    return rv_Subject( this->listener );
  }
  RvSession *session( void ) const { return this->sess; }
};

struct RvTimerCallback {
  virtual void onTimer( RvTimer *invoker ) = 0;
};

struct RvTimer {
  RvSession       * sess;
  rv_Timer          timer;
  RvTimerCallback * callback;
  int               repeat;

  RvTimer( RvSession *s, RvTimerCallback *cb,  int rep )
    : sess( s ), timer( 0 ), callback( cb ), repeat( rep ) {}

  static void bounce( rv_Timer, void *closure ) {
    RvTimer *t = (RvTimer *) closure;
    t->callback->onTimer( t );
    if ( ! t->repeat && t->timer != NULL ) {
      rv_DestroyTimer( t->timer );
      t->timer = NULL;
    }
  }
  RvSession *session( void ) const { return this->sess; }
};

struct RvSession {
  rv_Session session;

  RvSession( rv_Session sess ) : session( sess ) {}

  RvError status( void ) {
    return RvError( RV_OK );
  }

  RvSignal * newSignal( int sig, RvSignalCallback *handler ) {
    RvSignal * s = new RvSignal( this, handler );
    rv_CreateSignal( this->session, &s->signal, sig, RvSignal::bounce, s );
    return s;
  }

  RvListener * newListener( const char *subject, int, RvDataCallback *handler ) {
    RvListener * l = new RvListener( this, handler );
    rv_ListenSubject( this->session, &l->listener, subject,
                      RvListener::bounce, NULL, l );
    return l;
  }

  RvTimer * newTimer( float ms, int repeat, RvTimerCallback *handler ) {
    RvTimer * t = new RvTimer( this, handler, repeat );
    rv_CreateTimer( this->session, &t->timer, ms, RvTimer::bounce, t );
    return t;
  }

  rv_Session rv_session( void ) { return this->session; }
};

void
RvSender::send( const char *subject, const RvDatum &data ) {
  rv_Send( this->session()->rv_session(), subject, RVMSG_OPAQUE,
           data.size(), data.data() );
}

#endif
