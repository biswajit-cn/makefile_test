# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.16

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/ubuntu/biswajit/golib/src/github.com/makefile_test

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/ubuntu/biswajit/golib/src/github.com/makefile_test

# Include any dependencies generated for this target.
include CMakeFiles/caller.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/caller.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/caller.dir/flags.make

CMakeFiles/caller.dir/caller.c.o: CMakeFiles/caller.dir/flags.make
CMakeFiles/caller.dir/caller.c.o: caller.c
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/ubuntu/biswajit/golib/src/github.com/makefile_test/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building C object CMakeFiles/caller.dir/caller.c.o"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -o CMakeFiles/caller.dir/caller.c.o   -c /home/ubuntu/biswajit/golib/src/github.com/makefile_test/caller.c

CMakeFiles/caller.dir/caller.c.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing C source to CMakeFiles/caller.dir/caller.c.i"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -E /home/ubuntu/biswajit/golib/src/github.com/makefile_test/caller.c > CMakeFiles/caller.dir/caller.c.i

CMakeFiles/caller.dir/caller.c.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling C source to assembly CMakeFiles/caller.dir/caller.c.s"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -S /home/ubuntu/biswajit/golib/src/github.com/makefile_test/caller.c -o CMakeFiles/caller.dir/caller.c.s

# Object files for target caller
caller_OBJECTS = \
"CMakeFiles/caller.dir/caller.c.o"

# External object files for target caller
caller_EXTERNAL_OBJECTS =

caller: CMakeFiles/caller.dir/caller.c.o
caller: CMakeFiles/caller.dir/build.make
caller: ./libreconstructor.a
caller: CMakeFiles/caller.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/ubuntu/biswajit/golib/src/github.com/makefile_test/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking C executable caller"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/caller.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/caller.dir/build: caller

.PHONY : CMakeFiles/caller.dir/build

CMakeFiles/caller.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/caller.dir/cmake_clean.cmake
.PHONY : CMakeFiles/caller.dir/clean

CMakeFiles/caller.dir/depend:
	cd /home/ubuntu/biswajit/golib/src/github.com/makefile_test && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/ubuntu/biswajit/golib/src/github.com/makefile_test /home/ubuntu/biswajit/golib/src/github.com/makefile_test /home/ubuntu/biswajit/golib/src/github.com/makefile_test /home/ubuntu/biswajit/golib/src/github.com/makefile_test /home/ubuntu/biswajit/golib/src/github.com/makefile_test/CMakeFiles/caller.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/caller.dir/depend

