.PHONY: clean ll-test

DEFAULT: test
# Define the tools we are going to use
CC= g++
LD = g++

LIBS = -lm -lpthread -lrt -latomic
CFLAGS = -g -Wall -Wno-unused -Wno-return-type -Wno-sign-compare -std=c++17 -I. -O3
CFLAGS += -I../../include
OBJDIR = ../obj

JUNK = *~ $(OBJDIR)/*.o $(OBJDIR)/*.a $(OBJDIR)/*_ut  *-test *-*

# tests
run-all-test: ll-test

intset-test:  $(OBJDIR)/intset_test.o 
	$(LD) $(CFLAGS) -o $@ $^ $(LIBS)

ll-test:  $(OBJDIR)/linkedlist_test.o 
	$(LD) $(CFLAGS) -o $@ $^ $(LIBS)

$(OBJDIR)/intset_test.o: intset_test.cc
	$(CC) $(CFLAGS) -c -o $@ $<

$(OBJDIR)/linkedlist_test.o: linkedlist_test.cc
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f  test $(JUNK) 
