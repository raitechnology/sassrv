#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <sassrv/ev_rv.h>
#include <raikv/mainloop.h>

using namespace rai;
using namespace sassrv;
using namespace kv;

struct Args : public MainLoopVars { /* argv[] parsed args */
  int rv_port;
  Args() : rv_port( 0 ) {}
};

struct Loop : public MainLoop<Args> {
  Loop( EvShm &m,  Args &args,  int num, bool (*ini)( void * ) ) :
    MainLoop<Args>( m, args, num, ini ) {}

 EvRvListen * rv_sv;
  bool rv_init( void ) {
    return Listen<EvRvListen>( 0, this->r.rv_port, this->rv_sv,
                               this->r.tcp_opts ); }

  bool init( void ) {
    if ( this->thr_num == 0 )
      printf( "rv:                   %d\n", this->r.rv_port );
    int cnt = this->rv_init();
    if ( this->thr_num == 0 )
      fflush( stdout );
    return cnt > 0;
  }

  static bool initialize( void *me ) noexcept;
};

bool
Loop::initialize( void *me ) noexcept
{
  return ((Loop *) me)->init();
}

int
main( int argc, const char *argv[] )
{
  EvShm shm;
  Args  r;

  r.add_desc( "  -r rv    = listen rv port        (7500)\n" );
  if ( ! r.parse_args( argc, argv ) )
    return 1;
  if ( shm.open( r.map_name, r.db_num ) != 0 )
    return 1;
  printf( "rv_version:           " kv_stringify( SASSRV_VER ) "\n" );
  shm.print();
  r.rv_port = r.parse_port( argc, argv, "-r", "7500" );
  Runner<Args, Loop> runner( r, shm, Loop::initialize );
  if ( r.thr_error == 0 )
    return 0;
  return 1;
}

