# Change this to be however you'd like to invoke your sbcl.
LISP = sbcl


RELEASE_TAG = HEAD
LIB_NAME = cl-mw
APPEND = tee -a
RLOG = release.log

# Do not ever make this nothing or a dot character!
RDIR_PREFIX = release_dir-

# Either we were supplied a tag, or we use the head.
ifeq ($(RELEASE_TAG),HEAD)
	VERSION = HEAD.$(shell git show --pretty=format:"%h" HEAD | head -1)
else
	VERSION = $(RELEASE_TAG)
endif

RDIR = $(RDIR_PREFIX)$(VERSION)

all: examples/hello-world/hello-world \
	examples/ping/ping \
	examples/monte-carlo-pi/monte-carlo-pi \
	examples/higher-order/higher-order

docs:
	(cd doc && make clean && make all)

examples/hello-world/hello-world:
	(cd examples/hello-world && make LISP=$(LISP))

examples/ping/ping:
	(cd examples/ping && make LISP=$(LISP))

examples/monte-carlo-pi/monte-carlo-pi:
	(cd examples/monte-carlo-pi && make LISP=$(LISP))

examples/higher-order/higher-order:
	(cd examples/higher-order && make LISP=$(LISP))

# The documentation pages are generated from the actual release tag.
release: ensure-release-tag-ok
	@echo "making clean" | $(APPEND) $(RLOG)
	@make clean >> $(RLOG) 2>&1
	@mkdir -p $(RDIR) | $(APPEND) $(RLOG)
	@echo "generating release tarball: $(LIB_NAME)-$(VERSION).tar.gz" | $(APPEND) $(RLOG)
	@git archive --format=tar --prefix=$(LIB_NAME)-$(VERSION)/ $(RELEASE_TAG) | gzip > $(RDIR)/$(LIB_NAME)-$(VERSION).tar.gz
	@(cd $(RDIR) && tar xf $(LIB_NAME)-$(VERSION).tar.gz)
	@echo "generating documentation: ref-$(VERSION).pdf" | $(APPEND) $(RLOG)
	@(cd $(RDIR)/$(LIB_NAME)-$(VERSION)/doc && make ref.pdf) >> $(RLOG) 2>&1
	@(mv $(RDIR)/$(LIB_NAME)-$(VERSION)/doc/ref.pdf $(RDIR)/ref-$(VERSION).pdf) >> $(RLOG) 2>&1
	@echo "generating documentation html pages: ref-$(VERSION)" | $(APPEND) $(RLOG)
	@(cd $(RDIR)/$(LIB_NAME)-$(VERSION)/doc && make ref.html) >> $(RLOG) 2>&1
	@(mv $(RDIR)/$(LIB_NAME)-$(VERSION)/doc/ref $(RDIR)/ref-$(VERSION)) >> $(RLOG) 2>&1
	@echo "all done, cleaning up" | $(APPEND) $(RLOG)
	@rm -rf $(RDIR)/$(LIB_NAME)-$(VERSION)

# If we supplied a release tag object which isn't valid, this will abort the
# make.
ensure-release-tag-ok:
	@git show $(RELEASE_TAG) > /dev/null 2>&1 || (echo "The specified release tag '$(RELEASE_TAG)' is not a valid tag, sha1, or object!" && /bin/false)
	
clean:
	rm -rf ./$(RLOG)
	rm -rf ./$(RDIR_PREFIX)*
	(cd doc && make clean)
	(cd src && touch package.lisp)
	(cd examples/hello-world && make clean && touch package.lisp)
	(cd examples/ping && make clean && touch package.lisp)
	(cd examples/monte-carlo-pi && make clean && touch package.lisp)
	(cd examples/higher-order && make clean && touch package.lisp)
