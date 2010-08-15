all: exec

debug: higher-order.dbg

exec: higher-order

higher-order: higher-order.lisp package.lisp
	echo "(require :cl-mw.examples.higher-order) (use-package :cl-mw.examples.higher-order) (mw-dump-exec :exec-name \"./higher-order\")" | sbcl --disable-debugger

higher-order.dbg: higher-order.lisp package.lisp
	echo "(require :cl-mw.examples.higher-order) (use-package :cl-mw.examples.higher-order) (mw-dump-exec :exec-name \"./higher-order.dbg\")" | sbcl

clean:
	rm -f *.fasl *~ *.so higher-order higher-order.dbg *.rsc
