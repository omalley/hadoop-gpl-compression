# This file is part of Hadoop-Gpl-Compression.
#
# Hadoop-Gpl-Compression is free software: you can redistribute it
# and/or modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation, either version 3 of
# the License, or (at your option) any later version.
#
# Hadoop-Gpl-Compression is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Hadoop-Gpl-Compression.  If not, see
# <http://www.gnu.org/licenses/>.

#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(gpl-compression, 0.1.0, omalley@apache.org)

AC_CONFIG_AUX_DIR([config])
AM_INIT_AUTOMAKE([subdir-objects foreign no-dist])

AC_CONFIG_SRCDIR([impl/lzo/LzoCompressor.c])
AC_CONFIG_HEADER([impl/config.h])
AC_CONFIG_FILES([Makefile])

AC_PREFIX_DEFAULT(`pwd`/../install)
AC_GNU_SOURCE

CHECK_INSTALL_CFLAG

# Checks for programs.
AC_PROG_CC
AC_PROG_LIBTOOL

# Checks for libraries.
AC_CHECK_LIB([dl], [dlopen])

# Checks for header files.
AC_LANG(C)
AC_CHECK_HEADERS([unistd.h stdio.h stddef.h])
AC_CHECK_HEADERS([lzo/lzo2a.h], 
  COMPUTE_NEEDED_DSO(lzo2,HADOOP_LZO_LIBRARY), 
  AC_MSG_ERROR(lzo headers were not found... 
               gpl-compression library needs lzo to build. 
               Please install the requisite lzo development package.))

# Checks for typedefs, structures, and compiler characteristics.
AC_SYS_LARGEFILE
AC_HEADER_STDBOOL
AC_C_CONST
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_FUNC_STRERROR_R

# Checks for library functions.
AC_CHECK_FUNCS([mkdir uname memset])

AC_OUTPUT
