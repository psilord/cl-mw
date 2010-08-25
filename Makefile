all: examples/hello-world/hello-world \
	examples/ping/ping \
	examples/monte-carlo-pi/monte-carlo-pi \
	examples/higher-order/higher-order

examples/hello-world/hello-world:
	(cd examples/hello-world && make)

examples/ping/ping:
	(cd examples/ping && make)

examples/monte-carlo-pi/monte-carlo-pi:
	(cd examples/monte-carlo-pi && make)

examples/higher-order/higher-order:
	(cd examples/higher-order && make)
	
clean:
	(cd src && touch package.lisp)
	(cd examples/hello-world && make clean && touch package.lisp)
	(cd examples/ping && make clean && touch package.lisp)
	(cd examples/monte-carlo-pi && make clean && touch package.lisp)
	(cd examples/higher-order && make clean && touch package.lisp)
