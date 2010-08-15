all: a.out

a.out:
	echo "(require :cl-mw) (in-package :cl-mw)" | sbcl
	
clean:
	(cd ../examples/hello-world && make clean)
	(cd ../examples/ping && make clean)
	(cd ../examples/monte-carlo-pi && make clean)
	(cd ../examples/higher-order && make clean)
	rm -f *.fasl *~
