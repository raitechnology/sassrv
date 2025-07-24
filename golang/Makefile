# raimd makefile
lsb_dist     := $(shell if [ -f /etc/os-release ] ; then \
                  grep '^NAME=' /etc/os-release | sed 's/.*=[\"]*//' | sed 's/[ \"].*//' ; \
                  elif [ -x /usr/bin/lsb_release ] ; then \
                  lsb_release -is ; else echo Linux ; fi)
lsb_dist_ver := $(shell if [ -f /etc/os-release ] ; then \
		  grep '^VERSION=' /etc/os-release | sed 's/.*=[\"]*//' | sed 's/[ \"].*//' ; \
                  elif [ -x /usr/bin/lsb_release ] ; then \
                  lsb_release -rs | sed 's/[.].*//' ; else uname -r | sed 's/[-].*//' ; fi)
uname_m      := $(shell uname -m)

short_dist_lc := $(patsubst CentOS,rh,$(patsubst RedHatEnterprise,rh,\
                   $(patsubst RedHat,rh,\
                     $(patsubst Fedora,fc,$(patsubst Ubuntu,ub,\
                       $(patsubst Debian,deb,$(patsubst SUSE,ss,$(lsb_dist))))))))
short_dist    := $(shell echo $(short_dist_lc) | tr a-z A-Z)

# this is where the targets are compiled
build_dir ?= $(short_dist)$(lsb_dist_ver)_$(uname_m)$(port_extra)

.EXPORT_ALL_VARIABLES:

# Build configuration
PROGRAM = subrv7test
SOURCE = subrv7test.go

# CGO configuration  
CGO_CFLAGS = -I$(PWD)/../include -I$(PWD)/../../raimd/include -I$(PWD)/../../raikv/include -I$(PWD)/../../libdecnumber/include
CGO_LDFLAGS = -L$(PWD)/../$(build_dir)/lib64 -L$(PWD)/../../raimd/$(build_dir)/lib64
CGO_LDFLAGS += -L$(PWD)/../../raikv/$(build_dir)/lib64 -L$(PWD)/../../libdecnumber/$(build_dir)/lib64 
CGO_LDFLAGS += -lsassrv -lrv7lib -lraimd -lraikv -ldecnumber -lpcre2-8 -lcares -lm -lpthread -lrt
CGO_LDFLAGS += -Wl,-rpath,$(PWD)/../$(build_dir)/lib64 -Wl,-rpath,$(PWD)/../../raimd/$(build_dir)/lib64
CGO_LDFLAGS += -Wl,-rpath,$(PWD)/../../raikv/$(build_dir)/lib64 -Wl,-rpath,$(PWD)/../../libdecnumber/$(build_dir)/lib64 

# Default target
all: $(PROGRAM)

# Build the program
$(PROGRAM): $(SOURCE)
	go build -o $(PROGRAM) $(SOURCE)

# Clean build artifacts
clean:
	rm -f $(PROGRAM)

.PHONY: all clean
