.PHONY: clean run-all-ut run-all-test 

DEFAULT: test
# Define the tools we are going to use
CC= g++
LD = g++

LIBS = -lm -lpthread -lrt -latomic
CFLAGS = -g -Wall -Wno-unused -Wno-return-type -Wno-sign-compare -std=c++17 -I.
CFLAGS += -I../../include
OBJDIR = ../obj

JUNK = *~ $(OBJDIR)/*.o $(OBJDIR)/*.a $(OBJDIR)/*_ut  *-test

# tests
run-all-test: ll-test

ll-test:  $(OBJDIR)/linkedlist_test.o 
	$(LD) $(CFLAGS) -o $@ $^ $(LIBS)

$(OBJDIR)/linkedlist_test.o: linkedlist_test.cc
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f  test $(JUNK)
