
LISP = sbcl

all: exec

debug: argument-processing.dbg

exec: argument-processing

argument-processing: argument-processing.lisp package.lisp
	echo "(require :cl-mw.examples.argument-processing) (use-package :cl-mw.examples.argument-processing) (mw-dump-exec :exec-name \"./argument-processing\")" | $(LISP) --disable-debugger

argument-processing.dbg: argument-processing.lisp package.lisp
	echo "(require :cl-mw.examples.argument-processing) (use-package :cl-mw.examples.argument-processing) (mw-dump-exec :exec-name \"./argument-processing.dbg\")" | $(LISP)

clean:
	rm -f *.fasl *~ *.so argument-processing argument-processing.dbg *.rsc
