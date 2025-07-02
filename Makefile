# Citus toplevel Makefile

citus_subdir = .
citus_top_builddir = .
extension_dir = $(shell $(PG_CONFIG) --sharedir)/extension

# Hint that configure should be run first
ifeq (,$(wildcard Makefile.global))
  $(error ./configure needs to be run before compiling Citus)
endif

include Makefile.global

# Build common library first
all: common columnar extension

# build common static library
common:
	$(MAKE) -C src/backend/common all

# build columnar only
columnar: common
	$(MAKE) -C src/backend/columnar all

# build extension
extension: common $(citus_top_builddir)/src/include/citus_version.h
	$(MAKE) -C src/backend/distributed/ all

install-columnar: columnar
	$(MAKE) -C src/backend/columnar install

install-extension: extension
	$(MAKE) -C src/backend/distributed/ install

install-headers: extension
	$(MKDIR_P) '$(DESTDIR)$(includedir_server)/distributed/'
# generated headers are located in the build directory
	$(INSTALL_DATA) $(citus_top_builddir)/src/include/citus_version.h '$(DESTDIR)$(includedir_server)/'
# the rest in the source tree
	$(INSTALL_DATA) $(citus_abs_srcdir)/src/include/distributed/*.h '$(DESTDIR)$(includedir_server)/distributed/'

clean-extension:
	$(MAKE) -C src/backend/distributed/ clean
	$(MAKE) -C src/backend/columnar/ clean
	$(MAKE) -C src/backend/common/ clean

clean-full:
	$(MAKE) -C src/backend/distributed/ clean-full
	$(MAKE) -C src/backend/columnar/ clean-full # Assuming columnar might have a clean-full
	$(MAKE) -C src/backend/common/ clean # common might not have clean-full, just clean
.PHONY: common extension install-extension clean-extension clean-full

install-downgrades:
	$(MAKE) -C src/backend/columnar/ install-downgrades # Assuming columnar has downgrades
	$(MAKE) -C src/backend/distributed/ install-downgrades


# Add to generic targets
install: install-extension install-headers
clean: clean-extension

# install-all should install everything
install-all: install-columnar install-extension install-headers install-downgrades


# apply or check style
reindent:
	${citus_abs_top_srcdir}/ci/fix_style.sh
check-style:
	black . --check --quiet
	isort . --check --quiet
	flake8
	cd ${citus_abs_top_srcdir} && citus_indent --quiet --check
.PHONY: reindent check-style

# depend on install-all so that downgrade scripts are installed as well
check: all install-all
	# explicetely does not use $(MAKE) to avoid parallelism
	make -C src/test/regress check

.PHONY: all check clean install install-downgrades install-all
