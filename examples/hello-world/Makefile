all: exec

debug: hello-world.dbg

exec: hello-world

hello-world: hello-world.lisp package.lisp
	echo "(require :cl-mw.examples.hello-world) (use-package :cl-mw.examples.hello-world) (mw-dump-exec :exec-name \"./hello-world\")" | sbcl --disable-debugger

hello-world.dbg: hello-world.lisp package.lisp
	echo "(require :cl-mw.examples.hello-world) (use-package :cl-mw.examples.hello-world) (mw-dump-exec :exec-name \"./hello-world.dbg\")" | sbcl

clean:
	rm -f *.fasl *~ *.so hello-world hello-world.dbg *.rsc
