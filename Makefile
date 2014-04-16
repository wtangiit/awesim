ifndef CODESBASE
CODESBASE=/Users/wtang/workspace/codes-base/install
endif
ifndef CODESNET
CODESNET=/Users/wtang/workspace/codes-net/install
endif
ifndef ROSS
ROSS=/Users/wtang/workspace/ROSS/install
endif

# ross conflates CFLAGS with CPPFLAGS, so use just this one
override CPPFLAGS += $(shell $(ROSS)/bin/ross-config --cflags) -I$(CODESBASE)/include -I$(CODESNET)/include/codes -I/usr/local/Cellar/glib/2.40.0/include/glib-2.0 -I/usr/local/Cellar/glib/2.40.0/lib/glib-2.0/include -I/usr/local/opt/gettext/include
CC = $(shell $(ROSS)/bin/ross-config --cc)
LDFLAGS = $(shell $(ROSS)/bin/ross-config --ldflags) -L$(CODESBASE)/lib -L$(CODESNET)/lib
LDLIBS = $(shell $(ROSS)/bin/ross-config --libs) -lcodes-net -lcodes-base -L/usr/local/Cellar/glib/2.40.0/lib -L/usr/local/opt/gettext/lib -lglib-2.0 -lintl 

SOURCES=awesim.c lp_awe_server.c lp_awe_client.c lp_shock.c util.c
#OBJECTS=$(SOURCES:.c=.o)
EXECUTABLE=awesim

awesim: $(SOURCES)

clean:   
	rm -f $(EXECUTABLE)
	
