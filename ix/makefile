
include ../makefile.inc

# reset is to remove created table files
all: reset libix.a ixtest1 ixtest2 ixtest_private

# lib file dependencies
libix.a: libix.a(ix.o)  # and possibly other .o files

# c file dependencies
ix.o: ix.h

ixtest1.o: ixtest_util.h

ixtest2.o: ixtest_util.h

ixtest_private.o: ixtest_util.h

# binary dependencies
ixtest1: ixtest1.o libix.a $(CODEROOT)/rbf/librbf.a 

ixtest2: ixtest2.o libix.a $(CODEROOT)/rbf/librbf.a 

ixtest_private: ixtest_private.o libix.a $(CODEROOT)/rbf/librbf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

.PHONY: clean
# reset is to remove created table files
clean: reset
	-rm ixtest1 ixtest2 ixtest_private *.a *.o
	$(MAKE) -C $(CODEROOT)/rbf clean
	
# remove created table files
reset:
	-rm Age_idx* EmpName_idx *.out EmpName_*Idx
