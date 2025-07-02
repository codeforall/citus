# Makefile for Citus common static library

citus_subdir = src/backend/common
citus_top_builddir = ../../..

# Include global makefile settings.
# This will bring in PGXS variables and rules.
# It should also provide CPPFLAGS including src/include and src/include/common
include $(citus_top_builddir)/Makefile.global

# Object files to be included in the static library
OBJS = listutils.o citus_safe_lib.o

# Static library name
LIBNAME = libcituscommon.a

all: $(LIBNAME)

$(LIBNAME): $(OBJS)
	$(AR) crs $(LIBNAME) $(OBJS)

# Standard PGXS clean rules
clean:
	rm -f $(OBJS) $(LIBNAME)

# PGXS doesn't have a default install rule for static libraries,
# and we don't need to install this one.
install:

# Ensure VPATH is set up correctly for out-of-tree builds if Makefile.global doesn't fully handle it
# for this new subdirectory type. $(srcdir) should point to citus_abs_srcdir.
# Explicit rules for .o files if VPATH and implicit rules aren't enough:
# listutils.o: $(srcdir)/listutils.c
#	$(COMPILE.c) $(OUTPUT_OPTION) $<
# citus_safe_lib.o: $(srcdir)/citus_safe_lib.c
#	$(COMPILE.c) $(OUTPUT_OPTION) $<

.PHONY: all clean install
