
all: exec

debug: ping.dbg

exec: ping

ping: ping.lisp package.lisp
	echo "(require :cl-mw.examples.ping) (use-package :cl-mw.examples.ping) (mw-dump-exec :exec-name \"./ping\")" | sbcl --disable-debugger

ping.dbg: ping.lisp package.lisp
	echo "(require :cl-mw.examples.ping) (use-package :cl-mw.examples.ping) (mw-dump-exec \"./ping.dbg\")" | sbcl

clean:
	rm -f *.fasl *~ *.so ping ping.dbg *.rsc
