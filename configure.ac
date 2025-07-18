# Citus autoconf input script.
#
# Converted into an actual configure script by autogen.sh. This
# conversion only has to be done when configure.in changes. To avoid
# everyone needing autoconf installed, the resulting files are checked
# into the SCM.

AC_INIT([Citus], [13.2devel])
AC_COPYRIGHT([Copyright (c) Citus Data, Inc.])

# we'll need sed and awk for some of the version commands
AC_PROG_SED
AC_PROG_AWK

# CITUS_NAME definition
AC_DEFINE_UNQUOTED(CITUS_NAME, "$PACKAGE_NAME", [Citus full name as a string])

case $PACKAGE_NAME in
  'Citus Enterprise') citus_edition=enterprise ;;
               Citus) citus_edition=community ;;
                   *) AC_MSG_ERROR([Unrecognized package name.]) ;;
esac

# CITUS_EDITION definition
AC_DEFINE_UNQUOTED(CITUS_EDITION, "$citus_edition", [Citus edition as a string])

# CITUS_MAJORVERSION definition
[CITUS_MAJORVERSION=`expr "$PACKAGE_VERSION" : '\([0-9][0-9]*\.[0-9][0-9]*\)'`]
AC_DEFINE_UNQUOTED(CITUS_MAJORVERSION, "$CITUS_MAJORVERSION", [Citus major version as a string])

# CITUS_VERSION definition
PGAC_ARG_REQ(with, extra-version, [STRING], [append STRING to version],
             [CITUS_VERSION="$PACKAGE_VERSION$withval"],
             [CITUS_VERSION="$PACKAGE_VERSION"])
AC_DEFINE_UNQUOTED(CITUS_VERSION, "$CITUS_VERSION", [Citus version as a string])

# CITUS_VERSION_NUM definition
# awk -F is a regex on some platforms, and not on others, so make "." a tab
[CITUS_VERSION_NUM="`echo "$PACKAGE_VERSION" | sed 's/[A-Za-z].*$//' |
tr '.' '	' |
$AWK '{printf "%d%02d%02d", $1, $2, (NF >= 3) ? $3 : 0}'`"]
AC_DEFINE_UNQUOTED(CITUS_VERSION_NUM, $CITUS_VERSION_NUM, [Citus version as a number])

# CITUS_EXTENSIONVERSION definition
[CITUS_EXTENSIONVERSION="`grep '^default_version' $srcdir/src/backend/distributed/citus.control | cut -d\' -f2`"]
AC_DEFINE_UNQUOTED([CITUS_EXTENSIONVERSION], "$CITUS_EXTENSIONVERSION", [Extension version expected by this Citus build])

# Re-check for flex. That allows to compile citus against a postgres
# which was built without flex available (possible because generated
# files are included)
AC_PATH_PROG([FLEX], [flex])

# Locate pg_config binary
AC_ARG_VAR([PG_CONFIG], [Location to find pg_config for target PostgreSQL instalation (default PATH)])
AC_ARG_VAR([PATH], [PATH for target PostgreSQL install pg_config])

if test -z "$PG_CONFIG"; then
  AC_PATH_PROG(PG_CONFIG, pg_config)
fi

if test -z "$PG_CONFIG"; then
   AC_MSG_ERROR([Could not find pg_config. Set PG_CONFIG or PATH.])
fi

# check we're building against a supported version of PostgreSQL
citusac_pg_config_version=$($PG_CONFIG --version 2>/dev/null)
version_num=$(echo "$citusac_pg_config_version"|
              $SED -e 's/^PostgreSQL \([[0-9]]*\)\(\.[[0-9]]*\)\{0,1\}\(.*\)$/\1\2/')

# if PostgreSQL version starts with two digits, the major version is those digits
version_num=$(echo "$version_num"| $SED -e 's/^\([[0-9]]\{2\}\)\(.*\)$/\1/')

if test -z "$version_num"; then
  AC_MSG_ERROR([Could not detect PostgreSQL version from pg_config.])
fi

PGAC_ARG_BOOL(with, pg-version-check, yes,
              [do not check postgres version during configure])
AC_SUBST(with_pg_version_check)

if test "$with_pg_version_check" = no; then
    AC_MSG_NOTICE([building against PostgreSQL $version_num (skipped compatibility check)])
elif test "$version_num" != '15' -a  "$version_num" != '16' -a  "$version_num" != '17'; then
   AC_MSG_ERROR([Citus is not compatible with the detected PostgreSQL version ${version_num}.])
else
   AC_MSG_NOTICE([building against PostgreSQL $version_num])
fi;

# Check whether we're building inside the source tree, if not, prepare
# the build directory.
if test "$srcdir" -ef '.' ; then
  vpath_build=no
else
  vpath_build=yes
  _AS_ECHO_N([preparing build tree... ])
  citusac_abs_top_srcdir=`cd "$srcdir" && pwd`
  $SHELL "$citusac_abs_top_srcdir/prep_buildtree" "$citusac_abs_top_srcdir" "." \
      || AC_MSG_ERROR(failed)
  AC_MSG_RESULT(done)
fi
AC_SUBST(vpath_build)

# Allow to overwrite the C compiler, default to the one postgres was
# compiled with. We don't want autoconf's default CFLAGS though, so save
# those.
SAVE_CFLAGS="$CFLAGS"
AC_PROG_CC([$($PG_CONFIG --cc)])
CFLAGS="$SAVE_CFLAGS"

host_guess=`${SHELL} $srcdir/config/config.guess`

# Create compiler version string
if test x"$GCC" = x"yes" ; then
  cc_string=`${CC} --version | sed q`
  case $cc_string in [[A-Za-z]]*) ;; *) cc_string="GCC $cc_string";; esac
elif test x"$SUN_STUDIO_CC" = x"yes" ; then
  cc_string=`${CC} -V 2>&1 | sed q`
else
  cc_string=$CC
fi

AC_CHECK_SIZEOF([void *])

AC_DEFINE_UNQUOTED(CITUS_VERSION_STR,
                   ["$PACKAGE_NAME $CITUS_VERSION on $host_guess, compiled by $cc_string, `expr $ac_cv_sizeof_void_p \* 8`-bit"],
                   [A string containing the version number, platform, and C compiler])

# Locate source and build directory of the postgres we're building
# against. Can't rely on either still being present, but e.g. optional
# test infrastructure can rely on it.
POSTGRES_SRCDIR=$(grep ^abs_top_srcdir $(dirname $($PG_CONFIG --pgxs))/../Makefile.global|cut -d ' ' -f3-)
POSTGRES_BUILDDIR=$(grep ^abs_top_builddir $(dirname $($PG_CONFIG --pgxs))/../Makefile.global|cut -d ' ' -f3-)


# check for a number of CFLAGS that make development easier

# CITUSAC_PROG_CC_CFLAGS_OPT
# -----------------------
# Given a string, check if the compiler supports the string as a
# command-line option. If it does, add the string to CFLAGS.
AC_DEFUN([CITUSAC_PROG_CC_CFLAGS_OPT],
[define([Ac_cachevar], [AS_TR_SH([citusac_cv_prog_cc_cflags_$1])])dnl
AC_CACHE_CHECK([whether $CC supports $1], [Ac_cachevar],
[citusac_save_CFLAGS=$CFLAGS
flag=$1
case $flag in -Wno*)
	 flag=-W$(echo $flag | cut -c 6-)
esac
CFLAGS="$citusac_save_CFLAGS $flag"
ac_save_c_werror_flag=$ac_c_werror_flag
ac_c_werror_flag=yes
_AC_COMPILE_IFELSE([AC_LANG_PROGRAM()],
                   [Ac_cachevar=yes],
                   [Ac_cachevar=no])
ac_c_werror_flag=$ac_save_c_werror_flag
CFLAGS="$citusac_save_CFLAGS"])
if test x"$Ac_cachevar" = x"yes"; then
  CITUS_CFLAGS="$CITUS_CFLAGS $1"
fi
undefine([Ac_cachevar])dnl
])# CITUSAC_PROG_CC_CFLAGS_OPT

CITUSAC_PROG_CC_CFLAGS_OPT([-std=gnu99])
CITUSAC_PROG_CC_CFLAGS_OPT([-Wall])
CITUSAC_PROG_CC_CFLAGS_OPT([-Wextra])
# disarm options included in the above, which are too noisy for now
CITUSAC_PROG_CC_CFLAGS_OPT([-Wno-unused-parameter])
CITUSAC_PROG_CC_CFLAGS_OPT([-Wno-sign-compare])
CITUSAC_PROG_CC_CFLAGS_OPT([-Wno-missing-field-initializers])
CITUSAC_PROG_CC_CFLAGS_OPT([-Wno-clobbered])
CITUSAC_PROG_CC_CFLAGS_OPT([-Wno-gnu-variable-sized-type-not-at-end])
CITUSAC_PROG_CC_CFLAGS_OPT([-Wno-declaration-after-statement])
# And add a few extra warnings
CITUSAC_PROG_CC_CFLAGS_OPT([-Wendif-labels])
CITUSAC_PROG_CC_CFLAGS_OPT([-Wmissing-format-attribute])
CITUSAC_PROG_CC_CFLAGS_OPT([-Wmissing-declarations])
CITUSAC_PROG_CC_CFLAGS_OPT([-Wmissing-prototypes])
CITUSAC_PROG_CC_CFLAGS_OPT([-Wshadow])
CITUSAC_PROG_CC_CFLAGS_OPT([-Werror=vla])  # visual studio does not support these
CITUSAC_PROG_CC_CFLAGS_OPT([-Werror=implicit-int])
CITUSAC_PROG_CC_CFLAGS_OPT([-Werror=implicit-function-declaration])
CITUSAC_PROG_CC_CFLAGS_OPT([-Werror=return-type])
# Security flags
# Flags taken from: https://liquid.microsoft.com/Web/Object/Read/ms.security/Requirements/Microsoft.Security.SystemsADM.10203#guide
# We do not enforce the following flag because it is only available on GCC>=8
CITUSAC_PROG_CC_CFLAGS_OPT([-fstack-clash-protection])

#
# --enable-coverage enables generation of code coverage metrics with gcov
#
AC_ARG_ENABLE([coverage], AS_HELP_STRING([--enable-coverage], [build with coverage testing instrumentation]))
if test "$enable_coverage" = yes; then
    CITUS_CFLAGS="$CITUS_CFLAGS -O0 -g --coverage"
    CITUS_CPPFLAGS="$CITUS_CPPFLAGS -DNDEBUG"
    CITUS_LDFLAGS="$CITUS_LDFLAGS --coverage"
fi

#
# libcurl
#
PGAC_ARG_BOOL(with, libcurl, yes,
              [do not use libcurl for anonymous statistics collection],
              [AC_DEFINE([HAVE_LIBCURL], 1, [Define to 1 to build with libcurl support. (--with-libcurl)])])

if test "$with_libcurl" = yes; then
  AC_CHECK_LIB(curl, curl_global_init, [],
              [AC_MSG_ERROR([libcurl not found
If you have libcurl already installed, see config.log for details on the
failure. It is possible the compiler isn't looking in the proper directory.
Use --without-libcurl to disable anonymous statistics collection.])])
  AC_CHECK_HEADER(curl/curl.h, [], [AC_MSG_ERROR([libcurl header not found
If you have libcurl already installed, see config.log for details on the
failure.  It is possible the compiler isn't looking in the proper directory.
Use --without-libcurl to disable libcurl support.])])
fi

# REPORTS_BASE_URL definition
PGAC_ARG_REQ(with, reports-hostname, [HOSTNAME],
             [Use HOSTNAME as hostname for statistics collection and update checks],
             [REPORTS_BASE_URL="https://${withval}"],
             [REPORTS_BASE_URL="https://reports.citusdata.com"])
AC_DEFINE_UNQUOTED(REPORTS_BASE_URL, "$REPORTS_BASE_URL",
                   [Base URL for statistics collection and update checks])

#
# LZ4
#
PGAC_ARG_BOOL(with, lz4, yes,
              [do not use lz4],
              [AC_DEFINE([HAVE_CITUS_LIBLZ4], 1, [Define to 1 to build with lz4 support. (--with-lz4)])])
AC_SUBST(with_lz4)

if test "$with_lz4" = yes; then
  AC_CHECK_LIB(lz4, LZ4_compress_default, [],
              [AC_MSG_ERROR([lz4 library not found
If you have lz4 installed, see config.log for details on the
failure.  It is possible the compiler isn't looking in the proper directory.
Use --without-lz4 to disable lz4 support.])])
  AC_CHECK_HEADER(lz4.h, [], [AC_MSG_ERROR([lz4 header not found
If you have lz4 already installed, see config.log for details on the
failure.  It is possible the compiler isn't looking in the proper directory.
Use --without-lz4 to disable lz4 support.])])
fi

#
# ZSTD
#
PGAC_ARG_BOOL(with, zstd, yes,
              [do not use zstd])
AC_SUBST(with_zstd)

if test "$with_zstd" = yes; then
  AC_CHECK_LIB(zstd, ZSTD_decompress, [],
              [AC_MSG_ERROR([zstd library not found
If you have zstd installed, see config.log for details on the
failure.  It is possible the compiler isn't looking in the proper directory.
Use --without-zstd to disable zstd support.])])
  AC_CHECK_HEADER(zstd.h, [], [AC_MSG_ERROR([zstd header not found
If you have zstd already installed, see config.log for details on the
failure.  It is possible the compiler isn't looking in the proper directory.
Use --without-zstd to disable zstd support.])])
fi


PGAC_ARG_BOOL(with, security-flags, no,
              [use security flags])
AC_SUBST(with_security_flags)

if test "$with_security_flags" = yes; then
# Flags taken from: https://liquid.microsoft.com/Web/Object/Read/ms.security/Requirements/Microsoft.Security.SystemsADM.10203#guide

# We always want to have some compiler flags for security concerns.
SECURITY_CFLAGS="-fstack-protector-strong -D_FORTIFY_SOURCE=2 -O2 -z noexecstack -fpic -shared -Wl,-z,relro -Wl,-z,now -Wformat -Wformat-security -Werror=format-security"
CITUS_CFLAGS="$CITUS_CFLAGS $SECURITY_CFLAGS"
AC_MSG_NOTICE([Blindly added security flags for linker: $SECURITY_CFLAGS])

# We always want to have some clang flags for security concerns.
# This doesn't include "-Wl,-z,relro -Wl,-z,now" on purpuse, because bitcode is not linked.
# This doesn't include -fsanitize=cfi because it breaks builds on many distros including
# Debian/Buster, Debian/Stretch, Ubuntu/Bionic, Ubuntu/Xenial and EL7.
SECURITY_BITCODE_CFLAGS="-fsanitize=safe-stack -fstack-protector-strong -flto -fPIC -Wformat -Wformat-security -Werror=format-security"
CITUS_BITCODE_CFLAGS="$CITUS_BITCODE_CFLAGS $SECURITY_BITCODE_CFLAGS"
AC_MSG_NOTICE([Blindly added security flags for llvm: $SECURITY_BITCODE_CFLAGS])

AC_MSG_WARN([If you run into issues during linking or bitcode compilation, you can use --without-security-flags.])
fi

# Check if git is installed, when installed the gitref of the checkout will be baked in the application
AC_PATH_PROG(GIT_BIN, git)
AC_CHECK_FILE(.git,[HAS_DOTGIT=yes], [HAS_DOTGIT=])

AC_SUBST(CITUS_CFLAGS, "$CITUS_CFLAGS")
AC_SUBST(CITUS_BITCODE_CFLAGS, "$CITUS_BITCODE_CFLAGS")
AC_SUBST(CITUS_CPPFLAGS, "$CITUS_CPPFLAGS")
AC_SUBST(CITUS_LDFLAGS, "$LIBS $CITUS_LDFLAGS")
AC_SUBST(POSTGRES_SRCDIR, "$POSTGRES_SRCDIR")
AC_SUBST(POSTGRES_BUILDDIR, "$POSTGRES_BUILDDIR")
AC_SUBST(HAS_DOTGIT, "$HAS_DOTGIT")

AC_CONFIG_FILES([Makefile.global])
AC_CONFIG_HEADERS([src/include/citus_config.h] [src/include/citus_version.h])
AH_TOP([
/*
 * citus_config.h.in is generated by autoconf/autoheader and
 * converted into citus_config.h by configure.  Include when code needs to
 * depend on determinations made by configure.
 *
 * Do not manually edit!
 */
])
AC_OUTPUT
