.PHONY: clean run-all-ut run-all-test 

DEFAULT: test
# Define the tools we are going to use
CC= g++
LD = g++

LIBS = -lm -lpthread -lrt -latomic
CFLAGS = -g -Wall -Wno-unused -Wno-sign-compare -std=c++17 -I. -O3
OBJDIR = .

include ../unittest/makefile

JUNK = *~ $(OBJDIR)/*.o $(OBJDIR)/*.a $(OBJDIR)/*_ut  *-test

# unit tests
run-all-ut: 

bloomfilter-ut: $(OBJDIR)/bloomfilter_ut
	./$(OBJDIR)/bloomfilter_ut

clean:
	rm -f  test $(JUNK)
