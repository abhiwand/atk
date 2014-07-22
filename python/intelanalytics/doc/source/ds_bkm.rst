==================
Best Known Methods
==================

------
Python
------

Tab Completion
==============
Allows you to use the tab key to complete your typing for you.

If you are running with a standard Python REPL (not iPython, bPython, or the like) you will have to set up the tab completion manually:

Create a .pythonrc file in your home directory with the following contents::

    import rlcompleter, readline
    readline.parse_and_bind('tab:complete')

Or you can just run the two lines in your REPL session.

Note:
    If the .pythonrc does not take effect, add PYTHONSTARTUP in your .bashrc file::

        export PYTHONSTARTUP=~/.pythonrc

