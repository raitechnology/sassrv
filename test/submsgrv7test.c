/* lazy submsg tether: GetMsg/GetMsgArray + Mark/ClearReferences in a loop, RSS must stay flat */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sassrv/rv7api.h>
static long
rss_kb( void )
{
  FILE *f = fopen( "/proc/self/status", "r" );
  char  l[ 256 ];
  long  v = 0;
  while ( f && fgets( l, sizeof l, f ) )
    if ( sscanf( l, "VmRSS: %ld", &v ) == 1 )
      break;
  if ( f )
    fclose( f );
  return v;
}
int
main( void )
{
  tibrvMsg    m, in, out, ar[ 2 ];
  tibrv_u32   n;
  const char *s;
  tibrv_Open();
  tibrvMsg_Create( &in );
  tibrvMsg_AddString( in, "s", "hello" );
  tibrvMsg_Create( &ar[ 0 ] );
  tibrvMsg_AddI32( ar[ 0 ], "i", 1 );
  tibrvMsg_Create( &ar[ 1 ] );
  tibrvMsg_AddI32( ar[ 1 ], "i", 2 );
  long r0 = 0;
  for ( int i = 0; i < 300000; i++ ) {
    tibrvMsg_Create( &m );
    tibrvMsg_AddMsg( m, "sub", in );
    tibrvMsg_AddMsgArray( m, "mar", (const tibrvMsg *) ar, 2 );
    tibrvMsg_MarkReferences(
      m ); /* mark before any submsg exists (tether NULL) */
    if ( tibrvMsg_GetMsg( m, "sub", &out ) != TIBRV_OK ) {
      puts( "GetMsg fail" );
      return 1;
    } /* creates the lazy tether */
    tibrvMsg_GetString( out, "s", &s );
    if ( strcmp( s, "hello" ) ) {
      puts( "bad sub" );
      return 1;
    }
    const tibrvMsg *mar;
    if ( tibrvMsg_GetMsgArray( m, "mar", &mar, &n ) != TIBRV_OK || n != 2 ) {
      puts( "GetMsgArray fail" );
      return 1;
    }
    if ( i & 1 )
      tibrvMsg_ClearReferences(
        m ); /* half: drop submsgs newer than the mark */
    tibrvMsg_MarkReferences( m );
    tibrvMsg_ClearReferences( m );
    tibrvMsg_Destroy( m );
    if ( i == 20000 )
      r0 = rss_kb();
  }
  long r1 = rss_kb();
  printf( "submsg loop: RSS %ld -> %ld kB  %s\n", r0, r1,
          ( r1 - r0 ) < 2048 ? "OK" : "GROWING" );
  tibrvMsg_Destroy( in );
  tibrvMsg_Destroy( ar[ 0 ] );
  tibrvMsg_Destroy( ar[ 1 ] );
  tibrv_Close();
  return ( r1 - r0 ) < 2048 ? 0 : 1;
}
