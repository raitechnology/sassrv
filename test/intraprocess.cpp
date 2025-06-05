#include <cstdio>
#include <thread>
#include <chrono>

#include <sassrv/rv7api.h>

#define IN_SUBJECT "TEST"

void
pubthread(void )
{
  for (;;)
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    tibrvMsg msg;
    tibrvMsg_Create(&msg);
    tibrvMsg_SetSendSubject(msg, IN_SUBJECT);
    int value = 1;
    tibrvMsg_UpdateI32(msg, "event", value);
    #ifdef PRINTF
    printf("PUBLISH event=%d\n", value);
    #endif
    tibrvTransport_Send(TIBRV_PROCESS_TRANSPORT, msg);
    tibrvMsg_Destroy(msg);
  }
}

void
onMsg( tibrvEvent event, tibrvMsg msg, void *closure )
{
  #ifdef PRINTF  
  const char* subject = "<error>";
  const char* buffer = "<error>";

  tibrvMsg_GetSendSubject(msg, &subject);
  tibrvMsg_ConvertToString(msg, &buffer);
  printf("RECEIVED [%s] %s\n", subject, buffer);
  #endif
}

int
main(int argc, const char** argv) 
{
  tibrv_Open();

  tibrvQueue queue;
  tibrvQueue_Create(&queue);

  tibrvEvent listener;
  tibrvEvent_CreateListener(
          &listener,
          queue,
          onMsg,
          TIBRV_PROCESS_TRANSPORT,
          IN_SUBJECT,
          NULL
  );
  printf("START LISTENING : %s\n", IN_SUBJECT);

  std::thread th(pubthread);

  tibrv_status status = TIBRV_OK;

  for (status = tibrvQueue_Dispatch(queue); status == TIBRV_OK; status = tibrvQueue_Dispatch(queue));

  printf("STOP - %d : %s", status, tibrvStatus_GetText(status));

  tibrvEvent_Destroy(listener);
  tibrvQueue_Destroy(queue);
  tibrv_Close();
  return 0;
}
