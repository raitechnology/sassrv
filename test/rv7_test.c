#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <signal.h>
#include <string.h>
#include <time.h>
#include <arpa/inet.h>

#ifndef REALRV
#include "sassrv/rv7api.h"
#else
#include <tibrv/tibrv.h>
#endif

struct HexDump {
  char line[ 80 ];
  uint32_t boff, hex, ascii;
  uint64_t stream_off;
};

static char
hex_char( uint8_t x )
{
  return x < 10 ? ( '0' + x ) : ( 'a' - 10 + x );
}

static size_t
fill_line( struct HexDump *hex, const void *ptr,  size_t off,  size_t len )
{
  while ( off < len && hex->boff < 16 ) {
    uint8_t b = ((uint8_t *) ptr)[ off++ ];
    hex->line[ hex->hex ]   = hex_char( b >> 4 );
    hex->line[ hex->hex+1 ] = hex_char( b & 0xf );
    hex->hex += 3;
    if ( b >= ' ' && b <= 127 )
      hex->line[ hex->ascii ] = b;
    hex->ascii++;
    if ( ( ++hex->boff & 0x3 ) == 0 )
      hex->hex++;
  }
  return off;
}

static void
init_line( struct HexDump *hex )
{
  uint64_t j, k = hex->stream_off;
  memset( hex->line, ' ', 79 );
  hex->line[ 79 ] = '\0';
  hex->line[ 5 ] = hex_char( k & 0xf );
  k >>= 4; j = 4;
  while ( k > 0 ) {
    hex->line[ j ] = hex_char( k & 0xf );
    if ( j-- == 0 )
      break;
    k >>= 4;
  }
}

static void
flush_line( struct HexDump *hex )
{
  hex->stream_off += hex->boff;
  hex->boff  = 0;
  hex->hex   = 9;
  hex->ascii = 61;
  init_line( hex );
}

static void
first_line( struct HexDump *hex )
{
  hex->boff = 0; hex->stream_off = 0;
  flush_line( hex );
}

void
print_hex( const void *buf,  size_t buflen )
{
  struct HexDump hex;
  size_t off;
  first_line( &hex );
  for ( off = 0; off < buflen; ) {
    off = fill_line( &hex, buf, off, buflen );
    printf( "%s\n", hex.line );
    flush_line( &hex );
  }
}

int
main( int argc, char **argv )
{
  const char *ar[]     = { "one", "two", "three", "four" };
  tibrv_u32 ip   = htonl( ( 127 << 24 ) | 1 ), ip2;
  tibrv_u16 port = htons( 7500 ), port2;
  char buf[ 1024 ];
  tibrvMsgDateTime dt, dt2;
  tibrvMsg msg, sub, x;
  const char *msgString;

  (void) argc; (void) argv;
  printf( "%s\n\n", tibrv_Version() );
  tibrvMsg_Create(&msg);
  tibrvMsg_Create(&sub);

  tibrvMsg_AddStringEx( msg, "field", "string", 1 );
  tibrvMsg_AddStringEx( msg, "mar", "xyz", 131 );
  tibrvMsg_AddStringEx( msg, "test", "rm", 8 );
  tibrvMsg_AddStringEx( sub, "field2", "string2", 129 );
  tibrvMsg_AddIPAddr32Ex( sub, "ip", ip, 2 );
  tibrvMsg_AddIPPort16Ex( sub, "port", port, 3 );

  tibrvMsg mar[] = { sub, sub, sub };
  tibrvMsg_UpdateMsgArrayEx( msg, "mar", mar, 3, 131 );
  if ( tibrvMsg_ConvertToString( msg, &msgString ) == TIBRV_OK ) {
    printf("msg1: %s\n\n", msgString);
  }

  char xml[] = "<hello>this is a test a larger test with more chars that a signle byte can hold as homer would say hold my beer while i test this test</hello>";
  tibrvMsg_AddXmlEx( sub, "xml", xml, sizeof( xml ) - 1, 5 );
  tibrvMsg_GetCurrentTime( &dt );
  tibrvMsg_AddDateTimeEx( sub, "time", &dt, 4 );
  tibrvMsg_AddStringArrayEx( sub, "ar", ar, 4, 130 );
  tibrvMsg_UpdateStringEx( sub, "field2", "string3", 129 );
  char xmlu[] = "<hello>this is a test</hello>";
  tibrvMsg_UpdateXmlEx( sub, "xml", xmlu, sizeof( xmlu ) - 1, 5 );
  tibrvMsg_RemoveField( msg, "test" );
  tibrvMsg_RemoveField( msg, "testx" );

  tibrvMsg_UpdateMsg( msg, "submsg", sub );
  if ( tibrvMsg_ConvertToString( msg, &msgString ) == TIBRV_OK ) {
    printf("msg2: %s\n\n", msgString);
  }
  if ( tibrvMsg_GetMsg( msg, "submsg", &x ) == TIBRV_OK ) {
    if ( tibrvMsg_ConvertToString( x, &msgString ) == TIBRV_OK ) {
      printf("submsg: %s\n\n", msgString );
    }
    /*tibrvMsg_Destroy( x );*/
  }
  const void  * xml2;
  const char ** sar;
  tibrv_u32    size2, sar_count;

  if ( tibrvMsg_GetXml( sub, "xml", &xml2, &size2 ) == TIBRV_OK ) {
    printf( "xml: %.*s\n\n", (int) size2, (char *) xml2 );
  }
  if ( tibrvMsg_GetIPAddr32( sub, "ip", &ip2 ) == TIBRV_OK ) {
    printf( "ip: %u.%u.%u.%u\n\n",
            ((tibrv_u8 *) &ip2)[ 0 ], ((tibrv_u8 *) &ip2)[ 1 ],
            ((tibrv_u8 *) &ip2)[ 2 ], ((tibrv_u8 *) &ip2)[ 3 ] );
  }
  if ( tibrvMsg_GetIPPort16( sub, "port", &port2 ) == TIBRV_OK ) {
    printf( "port: %u\n\n", ntohs( port2 ) );
  }
  if ( tibrvMsg_GetStringArray( sub, "ar", &sar, &sar_count ) == TIBRV_OK ) {
    printf( "sar: %s, %s, %s, %s\n\n", sar[ 0 ], sar[ 1 ], sar[ 2 ], sar[ 3 ] );
  }
  if ( tibrvMsg_GetDateTime( sub, "time", &dt2 ) == TIBRV_OK ) {
    printf( "time: eq(%d.%d)\n\n", dt.sec == dt2.sec, dt.nsec == dt2.nsec );
  }
  const tibrvMsg * mre;
  tibrv_u32  cnt;
  if ( tibrvMsg_GetMsgArray( msg, "mar", &mre, &cnt ) == TIBRV_OK ) {
    tibrv_u32 i;
    for ( i = 0; i < cnt; i++ ) {
      if ( tibrvMsg_ConvertToString( mre[ i ], &msgString ) == TIBRV_OK ) {
        printf("[ %u ]: %s\n", i, msgString );
      }
    }
  }
  memset( buf, 0, sizeof( buf ) );
  tibrvMsg_GetAsBytesCopy( msg, buf, sizeof( buf ) );
  print_hex( buf, ntohl( *(uint32_t *) buf ) );
  printf( "\n" );

  tibrvMsg_Destroy( msg );
  tibrvMsg_Destroy( sub );

  return 0;
}

