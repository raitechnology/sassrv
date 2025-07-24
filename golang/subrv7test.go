package main

/*
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <time.h>
#include <sassrv/rv7api.h>
#include <raimd/md_msg.h>
#include <raimd/rv_msg.h>
#include <raimd/dict_load.h>

typedef struct {
  MDMsgMem_t * mem;
  MDOutput_t * mout;
  MDDict_t   * dict;
  const char * sub;
} rv_closure_t;

extern void go_inbox_callback(tibrvEvent event, tibrvMsg message, void* closure);
extern void go_my_callback(tibrvEvent event, tibrvMsg message, void* closure);
extern void go_dict_callback(tibrvEvent event, tibrvMsg message, void* closure);

static inline void inbox_callback_bridge(tibrvEvent event, tibrvMsg message, void* closure) { go_inbox_callback(event, message, closure); }
static inline void my_callback_bridge(tibrvEvent event, tibrvMsg message, void* closure) { go_my_callback(event, message, closure); }
static inline void dict_callback_bridge(tibrvEvent event, tibrvMsg message, void* closure) { go_dict_callback(event, message, closure); }

static inline rv_closure_t* create_closure(void) {
  rv_closure_t* cl = malloc(sizeof(rv_closure_t));
  md_output_init(&cl->mout);
  md_msg_mem_create(&cl->mem);
  cl->sub = NULL;
  cl->dict = NULL;
  return cl;
}

static inline void set_closure_sub(rv_closure_t* cl, const char* sub) { cl->sub = sub; }
static inline void set_closure_dict(rv_closure_t* cl, MDDict_t* dict) { cl->dict = dict; }
static inline const char* get_closure_sub(rv_closure_t* cl) { return cl->sub; }
static inline MDDict_t* get_closure_dict(rv_closure_t* cl) { return cl->dict; }
static inline MDMsgMem_t* get_closure_mem(rv_closure_t* cl) { return cl->mem; }
static inline MDOutput_t* get_closure_mout(rv_closure_t* cl) { return cl->mout; }
static inline void free_closure(rv_closure_t* cl) { if (cl) { free(cl); } }
static inline tibrvEventCallback get_inbox_callback(void) { return inbox_callback_bridge; }
static inline tibrvEventCallback get_my_callback(void) { return my_callback_bridge; }
static inline tibrvEventCallback get_dict_callback(void) { return dict_callback_bridge; }

*/
import "C"

import (
	"flag"
	"fmt"
	"os"
	"unsafe"
)

var (
	serviceStr *string = flag.String("service", "", "service parameter")
	networkStr *string = flag.String("network", "", "network parameter")
	daemonStr  *string = flag.String("daemon", "", "daemon parameter")
)

type ClosureData struct {
	closure *C.rv_closure_t
	subject string
}

var closures []*ClosureData

//export go_inbox_callback
func go_inbox_callback(event C.tibrvEvent, message C.tibrvMsg, closure unsafe.Pointer) {
	var sendSubject *C.char
	var buffer *C.tibrv_u8
	var size C.tibrv_u32
	cl := (*C.rv_closure_t)(closure)
	var localTime [C.TIBRVMSG_DATETIME_STRING_SIZE]C.char
	var gmtTime [C.TIBRVMSG_DATETIME_STRING_SIZE]C.char

	C.tibrvMsg_GetSendSubject(message, &sendSubject)
	C.tibrvMsg_GetAsBytes(message, (*unsafe.Pointer)(unsafe.Pointer(&buffer)))
	C.tibrvMsg_GetByteSize(message, &size)
	C.tibrvMsg_GetCurrentTimeString(&localTime[0], &gmtTime[0])

	C.md_msg_mem_reuse(C.get_closure_mem(cl))
	rvmsg := C.rv_msg_unpack(unsafe.Pointer(buffer), 0, C.size_t(size), 0, C.get_closure_dict(cl), C.get_closure_mem(cl))

	fmt.Printf("%s (%s): subject=%s (%s)\n", 
		C.GoString(&localTime[0]), 
		C.GoString(&gmtTime[0]),
		C.GoString(C.get_closure_sub(cl)), 
		C.GoString(sendSubject))
	
	C.md_msg_print(rvmsg, C.get_closure_mout(cl))
	C.fflush(C.stdout)
}

//export go_my_callback
func go_my_callback(event C.tibrvEvent, message C.tibrvMsg, closure unsafe.Pointer) {
	var sendSubject *C.char
	var replySubject *C.char
	var buffer *C.tibrv_u8
	var size C.tibrv_u32
	cl := (*C.rv_closure_t)(closure)
	var localTime [C.TIBRVMSG_DATETIME_STRING_SIZE]C.char
	var gmtTime [C.TIBRVMSG_DATETIME_STRING_SIZE]C.char

	C.tibrvMsg_GetSendSubject(message, &sendSubject)
	C.tibrvMsg_GetReplySubject(message, &replySubject)
	C.tibrvMsg_GetAsBytes(message, (*unsafe.Pointer)(unsafe.Pointer(&buffer)))
	C.tibrvMsg_GetByteSize(message, &size)
	C.tibrvMsg_GetCurrentTimeString(&localTime[0], &gmtTime[0])

	C.md_msg_mem_reuse(C.get_closure_mem(cl))
	rvmsg := C.rv_msg_unpack(unsafe.Pointer(buffer), 0, C.size_t(size), 0, C.get_closure_dict(cl), C.get_closure_mem(cl))

	if replySubject != nil {
		fmt.Printf("%s (%s): subject=%s, reply=%s\n", 
			C.GoString(&localTime[0]), 
			C.GoString(&gmtTime[0]),
			C.GoString(sendSubject), 
			C.GoString(replySubject))
	} else {
		fmt.Printf("%s (%s): subject=%s\n", 
			C.GoString(&localTime[0]), 
			C.GoString(&gmtTime[0]),
			C.GoString(sendSubject))
	}
	
	C.md_msg_print(rvmsg, C.get_closure_mout(cl))
	C.fflush(C.stdout)
}

//export go_dict_callback
func go_dict_callback(event C.tibrvEvent, message C.tibrvMsg, closure unsafe.Pointer) {
	var buffer *C.tibrv_u8
	var size C.tibrv_u32
	cl := (*C.rv_closure_t)(closure)

	C.tibrvMsg_GetAsBytes(message, (*unsafe.Pointer)(unsafe.Pointer(&buffer)))
	C.tibrvMsg_GetByteSize(message, &size)
	C.md_msg_mem_reuse(C.get_closure_mem(cl))
	rvmsg := C.rv_msg_unpack(unsafe.Pointer(buffer), 0, C.size_t(size), 0, nil, C.get_closure_mem(cl))
	dict := C.md_load_sass_dict(rvmsg)
	C.set_closure_dict(cl, dict)
	if dict != nil {
		fmt.Printf("Received dictionary\n")
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [-service service] [-network network] [-daemon daemon] subject_list\n", os.Args[0])
	os.Exit(1)
}

func main() {
	flag.Parse()
	subjects := flag.Args()

	if len(subjects) == 0 {
		usage()
	}

	var err C.tibrv_status
	var transport C.tibrvTransport
	var dictId C.tibrvEvent

	progname := C.CString(os.Args[0])
	defer C.free(unsafe.Pointer(progname))

	// Convert Go strings to C strings for the transport creation
	var serviceC, networkC, daemonC *C.char
	if *serviceStr != "" {
		serviceC = C.CString(*serviceStr)
		defer C.free(unsafe.Pointer(serviceC))
	}
	if *networkStr != "" {
		networkC = C.CString(*networkStr)
		defer C.free(unsafe.Pointer(networkC))
	}
	if *daemonStr != "" {
		daemonC = C.CString(*daemonStr)
		defer C.free(unsafe.Pointer(daemonC))
	}

	err = C.tibrv_Open()
	if err != C.TIBRV_OK {
		fmt.Fprintf(os.Stderr, "%s: Failed to open TIB/Rendezvous: %s\n", 
			os.Args[0], C.GoString(C.tibrvStatus_GetText(err)))
		os.Exit(1)
	}

	err = C.tibrvTransport_Create(&transport, serviceC, networkC, daemonC)
	if err != C.TIBRV_OK {
		fmt.Fprintf(os.Stderr, "%s: Failed to initialize transport: %s\n", 
			os.Args[0], C.GoString(C.tibrvStatus_GetText(err)))
		os.Exit(1)
	}

	C.tibrvTransport_SetDescription(transport, progname)

	// Create main closure for dictionary handling
	mainClosure := C.create_closure()
	defer C.free_closure(mainClosure)

	// Create inbox for dictionary requests
	var inbox [64]C.char
	err = C.tibrvTransport_CreateInbox(transport, &inbox[0], 64)
	if err == C.TIBRV_OK {
		err = C.tibrvEvent_CreateListener(&dictId, C.TIBRV_DEFAULT_QUEUE,
			C.get_dict_callback(), transport,
			&inbox[0], unsafe.Pointer(mainClosure))
	}

	if err == C.TIBRV_OK {
		// Try to get dictionary up to 3 times
		for times := 0; times < 3; times++ {
			var snapMsg C.tibrvMsg
			C.tibrvMsg_Create(&snapMsg)
			dictSubj := C.CString("_TIC.REPLY.SASS.DATA.DICTIONARY")
			C.tibrvMsg_SetSendSubject(snapMsg, dictSubj)
			C.tibrvMsg_SetReplySubject(snapMsg, &inbox[0])
			err = C.tibrvTransport_Send(transport, snapMsg)
			C.tibrvMsg_Destroy(snapMsg)
			C.free(unsafe.Pointer(dictSubj))

			if err != C.TIBRV_OK {
				fmt.Fprintf(os.Stderr, "Dictionary failed\n")
				os.Exit(1)
			}

			// Wait for dictionary response
			for C.get_closure_dict(mainClosure) == nil {
				err = C.tibrvQueue_TimedDispatch(C.TIBRV_DEFAULT_QUEUE, 10.0)
				if err != C.TIBRV_OK {
					break
				}
			}

			if C.get_closure_dict(mainClosure) != nil {
				break
			}

			if times == 2 {
				fmt.Fprintf(os.Stderr, "Dictionary timeout, tried 3 times\n")
				os.Exit(1)
			} else {
				fmt.Fprintf(os.Stderr, "Dictionary timeout, retrying\n")
			}
		}
	}

	// Create listeners for each subject
	listenIds := make([]C.tibrvEvent, len(subjects))
	inboxIds := make([]C.tibrvEvent, len(subjects))
	closures = make([]*ClosureData, len(subjects))

	for i, subject := range subjects {
		fmt.Printf("tibrvlisten: Listening to subject %s\n", subject)

		// Create closure for this subject
		closures[i] = &ClosureData{
			closure: C.create_closure(),
			subject: subject,
		}
		defer C.free_closure(closures[i].closure)

		subjectC := C.CString(subject)
		defer C.free(unsafe.Pointer(subjectC))

		C.set_closure_sub(closures[i].closure, subjectC)
		C.set_closure_dict(closures[i].closure, C.get_closure_dict(mainClosure))

		// Create inbox for this subject
		var subjectInbox [64]C.char
		err = C.tibrvTransport_CreateInbox(transport, &subjectInbox[0], 64)
		if err == C.TIBRV_OK {
			err = C.tibrvEvent_CreateListener(&inboxIds[i], C.TIBRV_DEFAULT_QUEUE,
				C.get_inbox_callback(), transport, &subjectInbox[0],
				unsafe.Pointer(closures[i].closure))
		}

		if err == C.TIBRV_OK {
			err = C.tibrvEvent_CreateListener(&listenIds[i], C.TIBRV_DEFAULT_QUEUE,
				C.get_my_callback(), transport,
				subjectC, unsafe.Pointer(mainClosure))
		}

		if err == C.TIBRV_OK {
			// Send snapshot request
			var snapMsg C.tibrvMsg
			C.tibrvMsg_Create(&snapMsg)
			snapSubject := fmt.Sprintf("_SNAP.%s", subject)
			snapSubjectC := C.CString(snapSubject)
			C.tibrvMsg_SetSendSubject(snapMsg, snapSubjectC)
			C.tibrvMsg_SetReplySubject(snapMsg, &subjectInbox[0])
			flagsC := C.CString("flags")
			C.tibrvMsg_AddU16Ex(snapMsg, flagsC, 6, 0)
			C.free(unsafe.Pointer(flagsC))
			err = C.tibrvTransport_Send(transport, snapMsg)
			C.tibrvMsg_Destroy(snapMsg)
			C.free(unsafe.Pointer(snapSubjectC))
		}

		if err != C.TIBRV_OK {
			fmt.Fprintf(os.Stderr, "%s: Error %s listening to \"%s\"\n", 
				os.Args[0], C.GoString(C.tibrvStatus_GetText(err)), subject)
			os.Exit(2)
		}
	}

	// Main dispatch loop
	for {
		err = C.tibrvQueue_TimedDispatch(C.TIBRV_DEFAULT_QUEUE, C.TIBRV_WAIT_FOREVER)
		if err != C.TIBRV_OK {
			break
		}
	}

	C.tibrv_Close()
}
