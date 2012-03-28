
LISP = sbcl

all: exec

debug: with-task-policy.dbg

exec: with-task-policy

with-task-policy: with-task-policy.lisp package.lisp
	echo "(require :cl-mw.examples.with-task-policy) (use-package :cl-mw.examples.with-task-policy) (mw-dump-exec :exec-name \"./with-task-policy\")" | $(LISP) --disable-debugger

with-task-policy.dbg: with-task-policy.lisp package.lisp
	echo "(require :cl-mw.examples.with-task-policy) (use-package :cl-mw.examples.with-task-policy) (mw-dump-exec :exec-name \"./with-task-policy.dbg\")" | $(LISP)

clean:
	rm -f *.fasl *~ *.so with-task-policy with-task-policy.dbg *.rsc
