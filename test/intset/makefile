.PHONY: clean ll-test

DEFAULT: test
# Define the tools we are going to use
CC= g++
LD = g++

LIBS = -lm -lpthread -lrt -latomic
CFLAGS = -g -Wall -Wno-unused -Wno-return-type -Wno-sign-compare -std=c++17 -I.
CFLAGS += -I../../include
OBJDIR = ../obj

JUNK = *~ $(OBJDIR)/*.o $(OBJDIR)/*.a $(OBJDIR)/*_ut  *-test *-*

# tests
run-all-test: ll-test

intset-ll:  $(OBJDIR)/intset_ll_test.o 
	$(LD) $(CFLAGS) -o $@ $^ $(LIBS)

intset-ht:  $(OBJDIR)/intset_ht_test.o 
	$(LD) $(CFLAGS) -o $@ $^ $(LIBS)

ll-test:  $(OBJDIR)/linkedlist_test.o 
	$(LD) $(CFLAGS) -o $@ $^ $(LIBS)

$(OBJDIR)/intset_ll_test.o: intset_ll_test.cc
	$(CC) $(CFLAGS) -c -o $@ $<

$(OBJDIR)/intset_ht_test.o: intset_ht_test.cc
	$(CC) $(CFLAGS) -c -o $@ $<

$(OBJDIR)/linkedlist_test.o: linkedlist_test.cc
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f  test $(JUNK) 
