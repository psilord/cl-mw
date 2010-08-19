
LISP = sbcl

all: exec

debug: monte-carlo-pi.dbg

exec: monte-carlo-pi

monte-carlo-pi: monte-carlo-pi.lisp package.lisp
	echo "(require :cl-mw.examples.monte-carlo-pi) (use-package :cl-mw.examples.monte-carlo-pi) (mw-dump-exec :exec-name \"./monte-carlo-pi\")" | $(LISP) --disable-debugger

monte-carlo-pi.dbg: monte-carlo-pi.lisp package.lisp
	echo "(require :cl-mw.examples.monte-carlo-pi) (use-package :cl-mw.examples.monte-carlo-pi) (mw-dump-exec \"./monte-carlo-pi.dbg\")" | $(LISP)

clean:
	rm -f *.fasl *~ *.so monte-carlo-pi monte-carlo-pi.dbg *.rsc
