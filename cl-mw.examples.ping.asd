(defpackage #:cl-mw-examples-ping-asd
  (:use #:cl #:asdf))

(in-package #:cl-mw-examples-ping-asd)

(defsystem #:cl-mw.examples.ping
  :depends-on (#:cl-mw)
  :components (
               ;; The ping example application
               (:module module-example-ping
                        :pathname "examples/ping"
                        :components ((:file "package")
                                     (:file "ping"
                                            :depends-on ("package"))))))

